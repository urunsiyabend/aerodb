use std::collections::HashMap;
use std::io;
use log::debug;
use crate::storage::btree::BTree;
use crate::storage::row::{Row, RowData, ColumnValue, ColumnType};
use crate::storage::pager::Pager;
use crate::storage::page::PAGE_SIZE;

/// In‐memory representation of a table’s metadata.
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub name: String,
    pub root_page: u32,
    pub columns: Vec<(String, ColumnType)>, // column name and type
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

        // Now read all catalog entries (if any) from page 1
        let mut tables = HashMap::new();
        {
            let mut catalog_btree = BTree::open_root(&mut pager, 1)?;
            let mut cursor = catalog_btree.scan_all_rows();
            while let Some(blob_row) = cursor.next() {
                let (table_name, root_page, columns) = Self::deserialize_catalog_row(&blob_row)?;
                tables.insert(
                    table_name.clone(),
                    TableInfo { name: table_name, root_page, columns },
                );
            }
        }

        Ok(Catalog { tables, indexes: HashMap::new(), pager })
    }

    fn reload_tables(&mut self) -> io::Result<()> {
        self.tables.clear();
        let mut catalog_btree = BTree::open_root(&mut self.pager, 1)?;
        let mut cursor = catalog_btree.scan_all_rows();
        while let Some(blob_row) = cursor.next() {
            let (table_name, root_page, columns) = Self::deserialize_catalog_row(&blob_row)?;
            self.tables.insert(
                table_name.clone(),
                TableInfo { name: table_name, root_page, columns },
            );
        }
        Ok(())
    }

    pub(crate) fn update_catalog_root(&mut self, name: &str, new_root: u32) -> io::Result<()> {
        let (target_key, columns) = {
            let mut tree = BTree::open_root(&mut self.pager, 1)?;
            let mut cursor = tree.scan_all_rows();
            let mut found = None;
            let mut cols = Vec::new();
            while let Some(row) = cursor.next() {
                let (tbl, _rp, c) = Self::deserialize_catalog_row(&row)?;
                if tbl == name {
                    found = Some(row.key);
                    cols = c;
                    break;
                }
            }
            (found, cols)
        };

        if let Some(key) = target_key {
            let mut tree = BTree::open_root(&mut self.pager, 1)?;
            tree.delete(key)?;
            tree.insert(key, Self::serialize_catalog_row(name, new_root, &columns))?;
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
        let blob_data = Self::serialize_catalog_row(name, new_root, &columns);

        // Use a synthetic key = (current number of tables + 1)
        let key = (self.tables.len() as i32) + 1;
        {
            let mut catalog_btree = BTree::open_root(&mut self.pager, 1)?;
            catalog_btree.insert(key, blob_data)?;
        }

        // Update in-memory
        self.tables.insert(
            name.to_string(),
            TableInfo { name: name.to_string(), root_page: new_root, columns },
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
            ColumnValue::Integer(i) => i.to_string(),
            ColumnValue::Text(s) => s.clone(),
            ColumnValue::Boolean(b) => b.to_string(),
        }
    }

    pub fn hash_value(val: &ColumnValue) -> i32 {
        match val {
            ColumnValue::Integer(i) => *i,
            ColumnValue::Text(s) => {
                use std::hash::{Hash, Hasher};
                let mut h = std::collections::hash_map::DefaultHasher::new();
                s.hash(&mut h);
                (h.finish() as i64 & 0x7FFF_FFFF) as i32
            }
            ColumnValue::Boolean(b) => if *b { 1 } else { 0 },
        }
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
                let (table_name, _rp, _cols) = Self::deserialize_catalog_row(&row)?;
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
    fn serialize_catalog_row(name: &str, root_page: u32, columns: &[(String, ColumnType)]) -> RowData {
        let mut vals = Vec::new();
        vals.push(ColumnValue::Text(name.to_string()));
        vals.push(ColumnValue::Integer(root_page as i32));
        vals.push(ColumnValue::Integer(columns.len() as i32));
        for (name, ty) in columns {
            vals.push(ColumnValue::Text(name.clone()));
            vals.push(ColumnValue::Integer(ty.to_code()));
        }
        RowData(vals)
    }

    /// Deserialize a catalog row back into (table_name, root_page, Vec<columns>).
    fn deserialize_catalog_row(row: &Row) -> io::Result<(String, u32, Vec<(String, ColumnType)>)> {
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
        let mut idx = 3;
        for _ in 0..num_cols {
            let name = match &values[idx] {
                ColumnValue::Text(s) => s.clone(),
                _ => return Err(io::Error::new(io::ErrorKind::Other, "column name not text")),
            };
            idx += 1;
            let ty = match values.get(idx) {
                Some(ColumnValue::Integer(code)) => ColumnType::from_code(*code).ok_or_else(|| io::Error::new(io::ErrorKind::Other, "bad type"))?,
                _ => return Err(io::Error::new(io::ErrorKind::Other, "column type missing")),
            };
            idx += 1;
            columns.push((name, ty));
        }
        Ok((name, root_page, columns))
    }
}
