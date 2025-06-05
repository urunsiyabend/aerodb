use std::collections::HashMap;
use std::io;

use crate::storage::btree::BTree;
use crate::storage::row::{Row, RowData, ColumnValue, ColumnType};
use crate::storage::pager::Pager;

/// In‐memory representation of a table’s metadata.
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub name: String,
    pub root_page: u32,
    pub columns: Vec<(String, ColumnType)>, // column name and type
}

/// The Catalog holds all user tables. Internally, it also persists itself in B-Tree page 1.
pub struct Catalog {
    tables: HashMap<String, TableInfo>,
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

        Ok(Catalog { tables, pager })
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
