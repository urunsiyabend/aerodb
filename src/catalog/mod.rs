use std::collections::HashMap;
use std::io;
use log::debug;
use crate::storage::btree::BTree;
use crate::storage::row::{Row, RowData, ColumnValue, ColumnType};
use crate::storage::pager::Pager;
use crate::storage::page::PAGE_SIZE;
use crate::sql::ast::{Expr};

/// In‐memory representation of a table’s metadata.
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub name: String,
    pub root_page: u32,
    pub columns: Vec<(String, ColumnType)>, // column name and type
    pub not_null: Vec<bool>,
    pub default_values: Vec<Option<Expr>>,
    pub auto_increment: Vec<bool>,
    pub fks: Vec<crate::sql::ast::ForeignKey>,
    pub primary_key: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct SequenceInfo {
    pub key: i32,
    pub current_value: i64,
    pub start_value: i64,
    pub increment: i64,
}

#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub table_name: String,
    pub column_name: String,
    pub root_page: u32,
}

/// The Catalog holds all user tables. Internally, it also persists itself in B-Tree page 1.
pub struct Catalog {
    tables: HashMap<String, TableInfo>,
    indexes: HashMap<String, IndexInfo>,
    sequences: HashMap<String, SequenceInfo>,
    pub(crate) pager: Pager,
}

impl Catalog {
    /// Open (or create) a catalog. This ensures page 1 exists as a leaf root,
    /// then reads any existing rows from page 1 into `tables`.
    pub fn open(mut pager: Pager) -> io::Result<Self> {
        // If the file has ≤1 pages total, allocate page 1 and initialize it as a leaf.
        if pager.file_length_pages() <= 1 {
            // Allocate page 1
            pager.allocate_page()?;
            let page = pager.get_page(1)?;
            crate::storage::page::set_node_type(&mut page.data, crate::storage::page::NODE_LEAF);
            crate::storage::page::set_is_root(&mut page.data, true);
            crate::storage::page::set_parent(&mut page.data, 0);
            crate::storage::page::set_cell_count(&mut page.data, 0);
            pager.flush_page(1)?;
        }

        // Ensure page 2 exists for sequence catalog
        if pager.file_length_pages() <= 2 {
            pager.allocate_page()?;
            let page = pager.get_page(2)?;
            crate::storage::page::set_node_type(&mut page.data, crate::storage::page::NODE_LEAF);
            crate::storage::page::set_is_root(&mut page.data, true);
            crate::storage::page::set_parent(&mut page.data, 0);
            crate::storage::page::set_cell_count(&mut page.data, 0);
            pager.flush_page(2)?;
        }

        // Now read all catalog entries (if any) from page 1
        let mut tables = HashMap::new();
        {
            let mut catalog_btree = BTree::open_root(&mut pager, 1)?;
            let mut cursor = catalog_btree.scan_all_rows();
            while let Some(blob_row) = cursor.next() {
                let (table_name, root_page, columns, not_null, defaults, auto_inc, fks, pk) = Self::deserialize_catalog_row(&blob_row)?;
                tables.insert(
                    table_name.clone(),
                    TableInfo { name: table_name, root_page, columns, not_null, default_values: defaults, fks, auto_increment: auto_inc, primary_key: if pk.is_empty() { None } else { Some(pk) } },
                );
            }
        }

        // Load sequences from page 2
        let mut sequences = HashMap::new();
        {
            let mut seq_btree = BTree::open_root(&mut pager, 2)?;
            let mut cursor = seq_btree.scan_all_rows();
            while let Some(row) = cursor.next() {
                let (name, current, start, inc) = Self::deserialize_sequence_row(&row)?;
                sequences.insert(name.clone(), SequenceInfo { key: row.key, current_value: current, start_value: start, increment: inc });
            }
        }

        Ok(Catalog { tables, indexes: HashMap::new(), sequences, pager })
    }

    fn reload_tables(&mut self) -> io::Result<()> {
        self.tables.clear();
        let mut catalog_btree = BTree::open_root(&mut self.pager, 1)?;
        let mut cursor = catalog_btree.scan_all_rows();
        while let Some(blob_row) = cursor.next() {
            let (table_name, root_page, columns, not_null, defaults, auto_inc, fks, pk) = Self::deserialize_catalog_row(&blob_row)?;
            self.tables.insert(
                table_name.clone(),
                TableInfo { name: table_name, root_page, columns, not_null, default_values: defaults, fks, auto_increment: auto_inc, primary_key: if pk.is_empty() { None } else { Some(pk) } },
            );
        }
        // reload sequences
        self.sequences.clear();
        {
            let mut seq_btree = BTree::open_root(&mut self.pager, 2)?;
            let mut cursor = seq_btree.scan_all_rows();
            while let Some(row) = cursor.next() {
                let (name, cur, start, inc) = Self::deserialize_sequence_row(&row)?;
                self.sequences.insert(name.clone(), SequenceInfo { key: row.key, current_value: cur, start_value: start, increment: inc });
            }
        }
        Ok(())
    }

    pub(crate) fn update_catalog_root(&mut self, name: &str, new_root: u32) -> io::Result<()> {
        let (target_key, columns, not_null, defaults, ai_vec, fks, pk_cols) = {
            let mut tree = BTree::open_root(&mut self.pager, 1)?;
            let mut cursor = tree.scan_all_rows();
            let mut found = None;
            let mut cols = Vec::new();
            let mut nn_vec = Vec::new();
            let mut fk_vec = Vec::new();
            let mut def_vec = Vec::new();
            let mut ai_vec = Vec::new();
            let mut pk_vec = Vec::new();
            while let Some(row) = cursor.next() {
                let (tbl, _rp, c, nn, def, ai, f, pk) = Self::deserialize_catalog_row(&row)?;
                if tbl == name {
                    found = Some(row.key);
                    cols = c;
                    nn_vec = nn;
                    def_vec = def;
                    ai_vec = ai;
                    fk_vec = f;
                    pk_vec = pk;
                    break;
                }
            }
            (found, cols, nn_vec, def_vec, ai_vec, fk_vec, pk_vec)
        };

        if let Some(key) = target_key {
            let mut tree = BTree::open_root(&mut self.pager, 1)?;
            tree.delete(key)?;
            let cols: Vec<_> = columns
                .iter()
                .cloned()
                .zip(not_null.iter().cloned())
                .zip(defaults.iter().cloned())
                .zip(ai_vec.iter().cloned())
                .map(|((((n, t), nn), d), a)| (n, t, nn, d, a))
                .collect();
            tree.insert(key, Self::serialize_catalog_row(name, new_root, &cols, &fks, &pk_cols))?;
            let new_root_page = tree.root_page();
            if new_root_page != 1 {
                let src_buf = {
                    let src = self.pager.get_page(new_root_page)?;
                    let mut buf = [0u8; PAGE_SIZE];
                    buf.copy_from_slice(&src.data);
                    buf
                };
                {
                    let dst = self.pager.get_page(1)?;
                    dst.data.copy_from_slice(&src_buf);
                }
                self.pager.flush_page(1)?;
            }
        }
        Ok(())
    }

    pub fn begin_transaction(&mut self, name: Option<String>) -> io::Result<()> {
        debug!("Transaction started with name: {:?}", name);
        self.pager.begin_transaction(name)
    }

    pub fn commit_transaction(&mut self) -> io::Result<()> {
        debug!("Transaction committed");
        self.pager.commit_transaction()
    }

    pub fn rollback_transaction(&mut self) -> io::Result<()> {
        self.pager.rollback_transaction()?;
        self.reload_tables()
    }

    /// Create a new table with `name` and `columns`. Allocates a fresh page for the table’s root,
    /// then inserts one catalog row into page 1 (the catalog B-Tree), and updates `tables`.
    pub fn create_table(&mut self, name: &str, columns: Vec<(String, ColumnType)>) -> io::Result<()> {
        let cols_with_nn: Vec<_> = columns.into_iter().map(|(n,t)| (n,t,false,None,false)).collect();
        self.create_table_with_fks(name, cols_with_nn, Vec::new(), None)
    }

    pub fn create_table_with_fks(&mut self, name: &str, columns: Vec<(String, ColumnType, bool, Option<Expr>, bool)>, fks: Vec<crate::sql::ast::ForeignKey>, primary_key: Option<Vec<String>>) -> io::Result<()> {
        if self.tables.contains_key(name) {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Table {} already exists", name),
            ));
        }

        // Allocate a new leaf page for this table’s data
        let new_root = self.pager.allocate_page()?;
        {
            let page = self.pager.get_page(new_root)?;
            crate::storage::page::set_node_type(&mut page.data, crate::storage::page::NODE_LEAF);
            crate::storage::page::set_is_root(&mut page.data, true);
            crate::storage::page::set_parent(&mut page.data, 0);
            crate::storage::page::set_cell_count(&mut page.data, 0);
            self.pager.flush_page(new_root)?;
        }

        // Build the catalog row payload: [name_len][name][root_page][num_columns][col1_len][col1]...
        let pk_cols = primary_key.clone().unwrap_or_default();
        let blob_data = Self::serialize_catalog_row(name, new_root, &columns, &fks, &pk_cols);

        // Use a synthetic key = (current number of tables + 1)
        let key = (self.tables.len() as i32) + 1;
        {
            let mut catalog_btree = BTree::open_root(&mut self.pager, 1)?;
            catalog_btree.insert(key, blob_data)?;
        }

        // Update in-memory
        let mut cols = Vec::new();
        let mut not_null = Vec::new();
        let mut defaults = Vec::new();
        let mut auto_inc = Vec::new();
        for (n, t, nn, d, ai) in columns {
            cols.push((n, t));
            not_null.push(nn);
            defaults.push(d);
            auto_inc.push(ai);
        }
        self.tables.insert(
            name.to_string(),
            TableInfo { name: name.to_string(), root_page: new_root, columns: cols, not_null, default_values: defaults, fks, auto_increment: auto_inc, primary_key },
        );
        Ok(())
    }

    pub fn create_index(&mut self, index_name: &str, table_name: &str, column_name: &str) -> io::Result<()> {
        if self.indexes.contains_key(index_name) {
            return Err(io::Error::new(io::ErrorKind::Other, "Index already exists"));
        }
        let (table_root, col_idx) = {
            let table = self.get_table(table_name)?;
            let idx = table
                .columns
                .iter()
                .position(|(c, _)| c == column_name)
                .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Column not found"))?;
            (table.root_page, idx)
        };

        let mut root_page = self.pager.allocate_page()?;
        {
            let page = self.pager.get_page(root_page)?;
            crate::storage::page::set_node_type(&mut page.data, crate::storage::page::NODE_LEAF);
            crate::storage::page::set_is_root(&mut page.data, true);
            crate::storage::page::set_parent(&mut page.data, 0);
            crate::storage::page::set_cell_count(&mut page.data, 0);
            self.pager.flush_page(root_page)?;
        }

        // Build index from existing rows
        {
            let mut table_btree = BTree::open_root(&mut self.pager, table_root)?;
            let mut cursor = table_btree.scan_all_rows();
            let mut rows = Vec::new();
            while let Some(row) = cursor.next() {
                rows.push(row);
            }
            drop(cursor);
            let mut index_tree = BTree::open_root(&mut self.pager, root_page)?;
            for row in rows {
                if let Some(val) = row.data.0.get(col_idx).cloned() {
                    root_page = Catalog::insert_index_value(&mut index_tree, val, row.key)?;
                }
            }
            // update root_page in case tree changed
            self.pager.flush_page(root_page)?;
        }

        self.indexes.insert(
            index_name.to_string(),
            IndexInfo {
                name: index_name.to_string(),
                table_name: table_name.to_string(),
                column_name: column_name.to_string(),
                root_page,
            },
        );
        Ok(())
    }

    fn insert_index_value(index_tree: &mut BTree, value: ColumnValue, row_key: i32) -> io::Result<u32> {
        let hash = Catalog::hash_value(&value);
        if let Some(mut existing) = index_tree.find(hash)? {
            if let ColumnValue::Text(ref s) = existing.data.0[0] {
                if *s == Self::value_to_string(&value) {
                    existing.data.0.push(ColumnValue::Integer(row_key));
                    index_tree.delete(hash)?;
                    index_tree.insert(hash, existing.data)?;
                    return Ok(index_tree.root_page());
                }
            }
        }
        let data = RowData(vec![ColumnValue::Text(Self::value_to_string(&value)), ColumnValue::Integer(row_key)]);
        index_tree.insert(hash, data)?;
        Ok(index_tree.root_page())
    }

    fn value_to_string(val: &ColumnValue) -> String {
        match val {
            ColumnValue::Null => "NULL".into(),
            ColumnValue::Integer(i) => i.to_string(),
            ColumnValue::Text(s) => s.clone(),
            ColumnValue::Boolean(b) => b.to_string(),
            ColumnValue::Char(s) => s.clone(),
            ColumnValue::Double(f) => f.to_string(),
            ColumnValue::Date(d) => ColumnValue::Date(*d).to_string_value(),
            ColumnValue::DateTime(ts) => ColumnValue::DateTime(*ts).to_string_value(),
            ColumnValue::Timestamp(ts) => ColumnValue::Timestamp(*ts).to_string_value(),
            ColumnValue::Time(t) => ColumnValue::Time(*t).to_string_value(),
            ColumnValue::Year(y) => ColumnValue::Year(*y).to_string_value(),
        }
    }

    pub fn hash_value(val: &ColumnValue) -> i32 {
        match val {
            ColumnValue::Null => 0,
            ColumnValue::Integer(i) => *i,
            ColumnValue::Text(s) => {
                use std::hash::{Hash, Hasher};
                let mut h = std::collections::hash_map::DefaultHasher::new();
                s.hash(&mut h);
                (h.finish() as i64 & 0x7FFF_FFFF) as i32
            }
            ColumnValue::Boolean(b) => if *b { 1 } else { 0 },
            ColumnValue::Char(s) => {
                use std::hash::{Hash, Hasher};
                let mut h = std::collections::hash_map::DefaultHasher::new();
                s.hash(&mut h);
                (h.finish() as i64 & 0x7FFF_FFFF) as i32
            }
            ColumnValue::Double(f) => *f as i32,
            ColumnValue::Date(d) => *d,
            ColumnValue::DateTime(ts) => (*ts % i32::MAX as i64) as i32,
            ColumnValue::Timestamp(ts) => (*ts % i32::MAX as i64) as i32,
            ColumnValue::Time(t) => *t,
            ColumnValue::Year(y) => *y as i32,
        }
    }

    pub fn create_sequence(&mut self, name: &str, start: i64, increment: i64) -> io::Result<()> {
        if self.sequences.contains_key(name) {
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, format!("Sequence '{}' already exists", name)));
        }
        let key = (self.sequences.len() as i32) + 1;
        let current = start - increment;
        {
            let mut tree = BTree::open_root(&mut self.pager, 2)?;
            tree.insert(key, Self::serialize_sequence_row(name, current, start, increment))?;
        }
        self.sequences.insert(name.to_string(), SequenceInfo { key, current_value: current, start_value: start, increment });
        Ok(())
    }

    pub fn next_sequence_value(&mut self, name: &str) -> io::Result<i64> {
        let info = self.sequences.get_mut(name).ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("Sequence '{}' not found", name)))?;
        info.current_value += info.increment;
        {
            let mut tree = BTree::open_root(&mut self.pager, 2)?;
            tree.delete(info.key)?;
            tree.insert(info.key, Self::serialize_sequence_row(name, info.current_value, info.start_value, info.increment))?;
        }
        Ok(info.current_value)
    }

    pub fn update_sequence_current(&mut self, name: &str, new_current: i64) -> io::Result<()> {
        let info = self.sequences.get_mut(name).ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "sequence not found"))?;
        if new_current > info.current_value {
            info.current_value = new_current;
            let mut tree = BTree::open_root(&mut self.pager, 2)?;
            tree.delete(info.key)?;
            tree.insert(info.key, Self::serialize_sequence_row(name, info.current_value, info.start_value, info.increment))?;
        }
        Ok(())
    }

    pub fn insert_into_indexes(&mut self, table_name: &str, row_data: &RowData) -> io::Result<()> {
        let indices: Vec<IndexInfo> = self.indexes.values().cloned().collect();
        for idx in indices {
            if idx.table_name == table_name {
                let col_pos = self
                    .tables
                    .get(table_name)
                    .and_then(|t| t.columns.iter().position(|(c, _)| c == &idx.column_name))
                    .unwrap();
                if let Some(val) = row_data.0.get(col_pos).cloned() {
                    let mut tree = BTree::open_root(&mut self.pager, idx.root_page)?;
                    let key = match row_data.0[0] {
                        ColumnValue::Integer(i) => i,
                        _ => continue,
                    };
                    let new_root = Catalog::insert_index_value(&mut tree, val, key)?;
                    if let Some(idx_info) = self.indexes.get_mut(&idx.name) {
                        idx_info.root_page = new_root;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn find_index(&self, table: &str, column: &str) -> Option<&IndexInfo> {
        self.indexes.values().find(|idx| idx.table_name == table && idx.column_name == column)
    }

    pub fn remove_from_indexes(&mut self, table_name: &str, row_data: &RowData, row_key: i32) -> io::Result<()> {
        let indices: Vec<IndexInfo> = self.indexes.values().cloned().collect();
        for idx in indices {
            if idx.table_name == table_name {
                let col_pos = self
                    .tables
                    .get(table_name)
                    .and_then(|t| t.columns.iter().position(|(c, _)| c == &idx.column_name))
                    .unwrap();
                if let Some(val) = row_data.0.get(col_pos).cloned() {
                    let mut tree = BTree::open_root(&mut self.pager, idx.root_page)?;
                    let hash = Catalog::hash_value(&val);
                    if let Some(mut entry) = tree.find(hash)? {
                        if let ColumnValue::Text(ref stored) = entry.data.0[0] {
                            if *stored == Self::value_to_string(&val) {
                                let mut keep = Vec::new();
                                for v in entry.data.0.iter().skip(1) {
                                    if let ColumnValue::Integer(k) = v {
                                        if *k != row_key {
                                            keep.push(*k);
                                        }
                                    }
                                }
                                tree.delete(hash)?;
                                if !keep.is_empty() {
                                    let mut data = vec![ColumnValue::Text(stored.clone())];
                                    for k in keep {
                                        data.push(ColumnValue::Integer(k));
                                    }
                                    tree.insert(hash, RowData(data))?;
                                }
                                if let Some(idx_info) = self.indexes.get_mut(&idx.name) {
                                    idx_info.root_page = tree.root_page();
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Look up a table’s metadata, or return an error if it doesn’t exist.
    pub fn get_table(&self, name: &str) -> io::Result<&TableInfo> {
        self.tables.get(name).ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, format!("No such table: {}", name))
        })
    }

    /// Mutable variant of `get_table` so callers can update the metadata.
    pub fn get_table_mut(&mut self, name: &str) -> io::Result<&mut TableInfo> {
        self.tables.get_mut(name).ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, format!("No such table: {}", name))
        })
    }

    pub fn all_tables(&self) -> Vec<TableInfo> {
        self.tables.values().cloned().collect()
    }

    /// Drop a table if it exists. Returns true if the table was removed.
    pub fn drop_table(&mut self, name: &str) -> io::Result<bool> {
        if !self.tables.contains_key(name) {
            return Ok(false);
        }

        // Find catalog row key corresponding to this table
        let key_opt = {
            let mut catalog_btree = BTree::open_root(&mut self.pager, 1)?;
            let mut cursor = catalog_btree.scan_all_rows();
            let mut found = None;
            while let Some(row) = cursor.next() {
                let (table_name, _rp, _cols, _nn, _defaults, _ai, _fks, _) = Self::deserialize_catalog_row(&row)?;
                if table_name == name {
                    found = Some(row.key);
                    break;
                }
            }
            found
        };

        if let Some(key) = key_opt {
            let mut catalog_btree = BTree::open_root(&mut self.pager, 1)?;
            catalog_btree.delete(key)?;
            let new_root = catalog_btree.root_page();
            if new_root != 1 {
                let src_buf = {
                    let src = self.pager.get_page(new_root)?;
                    let mut buf = [0u8; PAGE_SIZE];
                    buf.copy_from_slice(&src.data);
                    buf
                };
                {
                    let dst = self.pager.get_page(1)?;
                    dst.data.copy_from_slice(&src_buf);
                }
                self.pager.flush_page(1)?;
            }
            self.tables.remove(name);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Serialize a catalog row into a UTF-8 string:
    ///
    /// [u32 name_len][name_bytes][u32 root_page][u16 num_columns]
    /// for each column: [u32 col_len][col_bytes]
    /// then [u16 num_fks] followed by each foreign key description
    fn serialize_catalog_row(
        name: &str,
        root_page: u32,
        columns: &[(String, ColumnType, bool, Option<Expr>, bool)],
        fks: &[crate::sql::ast::ForeignKey],
        pk: &[String],
    ) -> RowData {
        let mut vals = Vec::new();
        vals.push(ColumnValue::Text(name.to_string()));
        vals.push(ColumnValue::Integer(root_page as i32));
        vals.push(ColumnValue::Integer(columns.len() as i32));
        for (name, ty, nn, default, ai) in columns {
            vals.push(ColumnValue::Text(name.clone()));
            vals.push(ColumnValue::Integer(ty.to_code()));
            match ty {
                ColumnType::Char(size) => vals.push(ColumnValue::Integer(*size as i32)),
                ColumnType::SmallInt { width, unsigned } => {
                    vals.push(ColumnValue::Integer(*width as i32));
                    vals.push(ColumnValue::Integer(if *unsigned { 1 } else { 0 }));
                }
                ColumnType::MediumInt { width, unsigned } => {
                    vals.push(ColumnValue::Integer(*width as i32));
                    vals.push(ColumnValue::Integer(if *unsigned { 1 } else { 0 }));
                }
                ColumnType::Double { precision, scale, unsigned } => {
                    vals.push(ColumnValue::Integer(*precision as i32));
                    vals.push(ColumnValue::Integer(*scale as i32));
                    vals.push(ColumnValue::Integer(if *unsigned { 1 } else { 0 }));
                }
                _ => {}
            }
            vals.push(ColumnValue::Integer(if *nn { 1 } else { 0 }));
            match default {
                Some(expr) => {
                    vals.push(ColumnValue::Integer(1));
                    vals.push(ColumnValue::Text(crate::sql::ast::expr_to_string(expr).into()));
                }
                None => {
                    vals.push(ColumnValue::Integer(0));
                }
            }
            vals.push(ColumnValue::Integer(if *ai { 1 } else { 0 }));
        }
        vals.push(ColumnValue::Integer(fks.len() as i32));
        for fk in fks {
            vals.push(ColumnValue::Integer(fk.columns.len() as i32));
            for c in &fk.columns {
                vals.push(ColumnValue::Text(c.clone()));
            }
            vals.push(ColumnValue::Text(fk.parent_table.clone()));
            vals.push(ColumnValue::Integer(fk.parent_columns.len() as i32));
            for pc in &fk.parent_columns {
                vals.push(ColumnValue::Text(pc.clone()));
            }
            let to_code = |a: &Option<crate::sql::ast::Action>| match a {
                Some(crate::sql::ast::Action::Cascade) => 1,
                _ => 0,
            };
            vals.push(ColumnValue::Integer(to_code(&fk.on_delete)));
            vals.push(ColumnValue::Integer(to_code(&fk.on_update)));
        }
        vals.push(ColumnValue::Integer(pk.len() as i32));
        for c in pk {
            vals.push(ColumnValue::Text(c.clone()));
        }
        RowData(vals)
    }

    /// Deserialize a catalog row back into (table_name, root_page, Vec<columns>, Vec<ForeignKey>).
    fn deserialize_catalog_row(row: &Row) -> io::Result<(String, u32, Vec<(String, ColumnType)>, Vec<bool>, Vec<Option<Expr>>, Vec<bool>, Vec<crate::sql::ast::ForeignKey>, Vec<String>)> {
        let values = &row.data.0;
        if values.len() < 3 {
            return Err(io::Error::new(io::ErrorKind::Other, "catalog row too short"));
        }
        let name = match &values[0] {
            ColumnValue::Text(s) => s.clone(),
            _ => return Err(io::Error::new(io::ErrorKind::Other, "catalog name not text")),
        };
        let root_page = match values[1] {
            ColumnValue::Integer(i) => i as u32,
            _ => return Err(io::Error::new(io::ErrorKind::Other, "root page not int")),
        };
        let num_cols = match values[2] {
            ColumnValue::Integer(i) => i as usize,
            _ => return Err(io::Error::new(io::ErrorKind::Other, "num cols not int")),
        };
        let mut columns = Vec::new();
        let mut not_null = Vec::new();
        let mut defaults = Vec::new();
        let mut auto_inc: Vec<bool> = Vec::new();
        let mut idx = 3;
        for _ in 0..num_cols {
            let name = match &values[idx] {
                ColumnValue::Text(s) => s.clone(),
                _ => return Err(io::Error::new(io::ErrorKind::Other, "column name not text")),
            };
            idx += 1;
            let ty_code = match values.get(idx) {
                Some(ColumnValue::Integer(code)) => *code,
                _ => return Err(io::Error::new(io::ErrorKind::Other, "column type missing")),
            };
            idx += 1;
            let ty = match ColumnType::from_code(ty_code) {
                Some(ColumnType::Char(_)) => {
                    let size = match values.get(idx) {
                        Some(ColumnValue::Integer(sz)) => *sz as usize,
                        _ => return Err(io::Error::new(io::ErrorKind::Other, "char size")),
                    };
                    idx += 1;
                    ColumnType::Char(size)
                }
                Some(ColumnType::SmallInt { .. }) => {
                    let width = match values.get(idx) { Some(ColumnValue::Integer(w)) => *w as usize, _ => 0 };
                    idx += 1;
                    let unsigned = match values.get(idx) { Some(ColumnValue::Integer(u)) => *u == 1, _ => false };
                    idx += 1;
                    ColumnType::SmallInt { width, unsigned }
                }
                Some(ColumnType::MediumInt { .. }) => {
                    let width = match values.get(idx) { Some(ColumnValue::Integer(w)) => *w as usize, _ => 0 };
                    idx += 1;
                    let unsigned = match values.get(idx) { Some(ColumnValue::Integer(u)) => *u == 1, _ => false };
                    idx += 1;
                    ColumnType::MediumInt { width, unsigned }
                }
                Some(ColumnType::Double { .. }) => {
                    let precision = match values.get(idx) { Some(ColumnValue::Integer(p)) => *p as usize, _ => 10 };
                    idx += 1;
                    let scale = match values.get(idx) { Some(ColumnValue::Integer(s)) => *s as usize, _ => 0 };
                    idx += 1;
                    let unsigned = match values.get(idx) { Some(ColumnValue::Integer(u)) => *u == 1, _ => false };
                    idx += 1;
                    ColumnType::Double { precision, scale, unsigned }
                }
                Some(ColumnType::Date) => ColumnType::Date,
                Some(other) => other,
                None => return Err(io::Error::new(io::ErrorKind::Other, "bad type")),
            };
            let nn = match values.get(idx) { Some(ColumnValue::Integer(i)) => *i == 1, _ => false };
            idx += 1;
            let has_default = match values.get(idx) { Some(ColumnValue::Integer(i)) => *i == 1, _ => false };
            idx += 1;
            let default = if has_default {
                if let Some(ColumnValue::Text(s)) = values.get(idx) {
                    idx += 1;
                    Some(crate::sql::ast::parse_default_expr(s))
                } else { None }
            } else { None };
            let ai = match values.get(idx) { Some(ColumnValue::Integer(i)) => *i == 1, _ => false };
            idx += 1;
            columns.push((name, ty));
            not_null.push(nn);
            defaults.push(default);
            auto_inc.push(ai);
        }
        let num_fks = match values.get(idx) {
            Some(ColumnValue::Integer(i)) => *i as usize,
            _ => 0,
        };
        idx += 1;
        let mut fks = Vec::new();
        for _ in 0..num_fks {
            let num_cols = match values.get(idx) {
                Some(ColumnValue::Integer(i)) => *i as usize,
                _ => return Err(io::Error::new(io::ErrorKind::Other, "fk cols")),
            };
            idx += 1;
            let mut cols = Vec::new();
            for _ in 0..num_cols {
                if let Some(ColumnValue::Text(c)) = values.get(idx) {
                    cols.push(c.clone());
                    idx += 1;
                } else {
                    return Err(io::Error::new(io::ErrorKind::Other, "fk col name"));
                }
            }
            let parent_table = match values.get(idx) {
                Some(ColumnValue::Text(s)) => s.clone(),
                _ => return Err(io::Error::new(io::ErrorKind::Other, "fk parent table")),
            };
            idx += 1;
            let num_pcols = match values.get(idx) {
                Some(ColumnValue::Integer(i)) => *i as usize,
                _ => return Err(io::Error::new(io::ErrorKind::Other, "fk pcols")),
            };
            idx += 1;
            let mut parent_cols = Vec::new();
            for _ in 0..num_pcols {
                if let Some(ColumnValue::Text(pc)) = values.get(idx) {
                    parent_cols.push(pc.clone());
                    idx += 1;
                } else {
                    return Err(io::Error::new(io::ErrorKind::Other, "fk pcol name"));
                }
            }
            let action_from = |v: i32| {
                if v == 1 { Some(crate::sql::ast::Action::Cascade) } else { Some(crate::sql::ast::Action::NoAction) }
            };
            let on_delete = match values.get(idx) {
                Some(ColumnValue::Integer(i)) => action_from(*i),
                _ => None,
            };
            idx += 1;
            let on_update = match values.get(idx) {
                Some(ColumnValue::Integer(i)) => action_from(*i),
                _ => None,
            };
            idx += 1;
            fks.push(crate::sql::ast::ForeignKey { columns: cols, parent_table, parent_columns: parent_cols, on_delete, on_update });
        }
        let pk_len = match values.get(idx) { Some(ColumnValue::Integer(i)) => *i as usize, _ => 0 };
        idx += 1;
        let mut pk_cols = Vec::new();
        for _ in 0..pk_len {
            if let Some(ColumnValue::Text(c)) = values.get(idx) {
                pk_cols.push(c.clone());
                idx += 1;
            }
        }
        Ok((name, root_page, columns, not_null, defaults, auto_inc, fks, pk_cols))
    }

    fn serialize_sequence_row(name: &str, current: i64, start: i64, increment: i64) -> RowData {
        RowData(vec![
            ColumnValue::Text(name.to_string()),
            ColumnValue::Integer(current as i32),
            ColumnValue::Integer(start as i32),
            ColumnValue::Integer(increment as i32),
        ])
    }

    fn deserialize_sequence_row(row: &Row) -> io::Result<(String, i64, i64, i64)> {
        let vals = &row.data.0;
        if vals.len() < 4 { return Err(io::Error::new(io::ErrorKind::Other, "sequence row")); }
        let name = match &vals[0] { ColumnValue::Text(s) => s.clone(), _ => return Err(io::Error::new(io::ErrorKind::Other, "seq name")) };
        let cur = match vals[1] { ColumnValue::Integer(i) => i as i64, _ => 0 };
        let start = match vals[2] { ColumnValue::Integer(i) => i as i64, _ => 1 };
        let inc = match vals[3] { ColumnValue::Integer(i) => i as i64, _ => 1 };
        Ok((name, cur, start, inc))
    }
}

