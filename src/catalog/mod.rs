use std::collections::HashMap;
use std::io;

use crate::storage::btree::{BTree, Row};
use crate::storage::pager::Pager;

/// In‐memory representation of a table’s metadata.
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub name: String,
    pub root_page: u32,
    pub columns: Vec<String>, // column names, in order
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
    pub fn create_table(&mut self, name: &str, columns: Vec<String>) -> io::Result<()> {
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
        let blob_bytes = Self::serialize_catalog_row(name, new_root, &columns);

        // Use a synthetic key = (current number of tables + 1)
        let key = (self.tables.len() as i32) + 1;
        {
            let mut catalog_btree = BTree::open_root(&mut self.pager, 1)?;
            catalog_btree.insert(key, &blob_bytes)?;
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

    /// Serialize a catalog row into a UTF-8 string:
    ///
    /// [u32 name_len][name_bytes][u32 root_page][u16 num_columns]
    /// for each column: [u32 col_len][col_bytes]
    fn serialize_catalog_row(name: &str, root_page: u32, columns: &[String]) -> Vec<u8> {
        let mut buf = Vec::new();
        let name_bytes = name.as_bytes();
        buf.extend(&(name_bytes.len() as u32).to_le_bytes());
        buf.extend(name_bytes);
        buf.extend(&root_page.to_le_bytes());
        buf.extend(&(columns.len() as u16).to_le_bytes());
        for col in columns {
            let cbytes = col.as_bytes();
            buf.extend(&(cbytes.len() as u32).to_le_bytes());
            buf.extend(cbytes);
        }
        buf
    }

    /// Deserialize a catalog row back into (table_name, root_page, Vec<columns>).
    fn deserialize_catalog_row(row: &Row) -> io::Result<(String, u32, Vec<String>)> {
        let bytes = &row.payload[..];
        let mut offset = 0;

        let name_len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let name = String::from_utf8_lossy(&bytes[offset..offset + name_len]).to_string();
        offset += name_len;

        let root_page = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;

        let num_cols = u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;

        let mut columns = Vec::with_capacity(num_cols);
        for _ in 0..num_cols {
            let col_len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            let col_name = String::from_utf8_lossy(&bytes[offset..offset + col_len]).to_string();
            offset += col_len;
            columns.push(col_name);
        }

        Ok((name, root_page, columns))
    }
}

#[cfg(test)]
mod tests {
    use super::*; // bring Catalog, Pager, BTree, etc. into scope
    use std::fs;

    #[test]
    fn create_100_users_and_select_all() {
        // 1) Remove any existing test.db
        let filename = "test.db";
        let _ = fs::remove_file(filename);

        // 2) Open a new Catalog (which owns its Pager internally)
        let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();

        // 3) CREATE TABLE users (id, name, email)
        catalog
            .create_table(
                "users",
                vec!["id".into(), "name".into(), "email".into()],
            )
            .unwrap();

        // 4) Insert 100 rows:
        //    For each i in 1..=100, build [u16 num_cols][u32 len][bytes]…
        //    then open that table’s B-Tree and insert.
        for i in 1..=100 {
            let values = vec![
                i.to_string(),
                format!("user{}", i),
                format!("u{}@example.com", i),
            ];
            // Serialize into buf: [u16 num_cols][u32 len1][bytes1][u32 len2][bytes2]…
            let mut buf = Vec::new();
            let col_count = (values.len() as u16).to_le_bytes();
            buf.extend(&col_count);
            for v in &values {
                let vb = v.as_bytes();
                let len = (vb.len() as u32).to_le_bytes();
                buf.extend(&len);
                buf.extend(vb);
            }

            // Look up the root_page for “users” and open its B-Tree.
            let root_page = catalog.get_table("users").unwrap().root_page;
            let mut table_btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
            table_btree.insert(i as i32, &buf[..]).unwrap();
        }

        // 5) Now scan all rows in “users” and check we see exactly 100, with correct keys.
        let root_page = catalog.get_table("users").unwrap().root_page;
        let mut table_btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
        let rows: Vec<_> = table_btree.scan_all_rows().collect();
        assert_eq!(rows.len(), 100, "Expected 100 rows in users, got {}", rows.len());

        // Verify first and last keys:
        assert_eq!(rows.first().unwrap().key, 1);
        assert_eq!(rows.last().unwrap().key, 100);
    }
}
