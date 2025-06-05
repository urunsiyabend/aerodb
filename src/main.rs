// src/main.rs

mod storage;
mod sql;
mod catalog;

use std::io::{self, Write};
use log::{debug, info, warn};

use crate::storage::pager::Pager;
use crate::storage::btree::BTree;
use crate::catalog::Catalog;
use crate::sql::parser::parse_statement;
use crate::sql::ast::{Statement, Expr};

// const DATABASE_FILE: &str = "data.aerodb";
const DATABASE_FILE: &str = "data.aerodb";

fn execute_delete(catalog: &mut Catalog, table_name: &str, selection: Option<Expr>) -> io::Result<()> {
    if let Ok(table_info) = catalog.get_table(table_name) {
        let root_page = table_info.root_page;
        let columns = table_info.columns.clone();
        let keys = {
            let mut scan_tree = BTree::open_root(&mut catalog.pager, root_page)?;
            let mut cursor = scan_tree.scan_all_rows();
            let mut collected = Vec::new();
            while let Some(row) = cursor.next() {
                if let Some(ref expr) = selection {
                    let bytes = &row.payload[..];
                    let mut offset = 0;
                    let num_cols = u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap()) as usize;
                    offset += 2;
                    let mut values = std::collections::HashMap::new();
                    for col in columns.iter().take(num_cols) {
                        let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
                        offset += 4;
                        let v = String::from_utf8_lossy(&bytes[offset..offset + len]).to_string();
                        offset += len;
                        values.insert(col.clone(), v);
                    }
                    if crate::sql::ast::evaluate_expression(expr, &values) {
                        collected.push(row.key);
                    }
                } else {
                    collected.push(row.key);
                }
            }
            drop(cursor);
            collected
        };

        if !keys.is_empty() {
            let mut table_btree = BTree::open_root(&mut catalog.pager, root_page)?;
            for k in keys {
                table_btree.delete(k)?;
            }
            let new_root = table_btree.root_page();
            drop(table_btree);
            if new_root != root_page {
                if let Ok(t) = catalog.get_table_mut(table_name) {
                    t.root_page = new_root;
                }
            }
        }
    }
    Ok(())
}

fn main() -> io::Result<()> {
    env_logger::init();
    info!("AeroDB v0.4 (extended SQL support + catalog). Type .exit to quit.");

    let mut catalog = Catalog::open(Pager::new(DATABASE_FILE)?)?;

    loop {
        print!("aerodb> ");
        io::stdout().flush()?;

        let mut input = String::new();
        if io::stdin().read_line(&mut input)? == 0 {
            break; // EOF
        }
        let trimmed = input.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.eq_ignore_ascii_case(".exit") || trimmed.eq_ignore_ascii_case("exit") {
            break;
        }

        match parse_statement(trimmed) {
            Ok(stmt) => match stmt {
                Statement::CreateTable { table_name, columns } => {
                    debug!("CREATE TABLE {} {:?}", table_name, columns);
                    if let Err(e) = catalog.create_table(&table_name, columns.clone()) {
                        warn!("Error creating table {}: {}", table_name, e);
                    } else {
                        info!("Table '{}' created", table_name);
                    }
                }
                Statement::Insert { table_name, values } => {
                    debug!("INSERT INTO {} VALUES {:?}", table_name, values);

                    // First, try to get the table’s metadata (immutable borrow of `catalog`)
                    match catalog.get_table(&table_name) {
                        Ok(table_info) => {
                            // Extract root_page and drop the borrow of `table_info` immediately.
                            let root_page = table_info.root_page;
                            // Now the immutable borrow of `catalog` ends here,
                            // so we can borrow `catalog.pager` mutably below.

                            // Build the row payload: [u16 num_columns][u32 len1][v1]…
                            let mut buf = Vec::new();
                            let key: i32 = values[0]
                                .parse()
                                .map_err(|_| io::Error::new(io::ErrorKind::Other, "Key must be an integer"))?;
                            let col_count = values.len() as u16;
                            buf.extend(&col_count.to_le_bytes());
                            for v in &values {
                                let vb = v.as_bytes();
                                buf.extend(&(vb.len() as u32).to_le_bytes());
                                buf.extend(vb);
                            }
                            {
                                let mut table_btree = BTree::open_root(&mut catalog.pager, root_page)?;
                                if let Err(e) = table_btree.insert(key, &buf) {
                                    warn!("Error inserting into {}: {}", table_name, e);
                                } else {
                                    info!("Row inserted into '{}'", table_name);
                                    let new_root = table_btree.root_page();
                                    if new_root != root_page {
                                        if let Ok(t) = catalog.get_table_mut(&table_name) {
                                            t.root_page = new_root;
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Table '{}' not found: {}", table_name, e);
                        }
                    }
                }
                Statement::Select { table_name, selection, limit, offset, order_by: _ } => {
                    debug!("SELECT * FROM {}", table_name);

                    // First, get the table metadata (immutable borrow)
                    match catalog.get_table(&table_name) {
                        Ok(table_info) => {
                            let root_page = table_info.root_page;
                            let columns = table_info.columns.clone();
                            // Immutable borrow ends here.

                            // Now we can borrow `catalog.pager` mutably to scan the table.
                            {
                                let mut table_btree = BTree::open_root(
                                    &mut catalog.pager,
                                    root_page,
                                )?;
                                let mut cursor = table_btree.scan_rows_with_bounds(offset.unwrap_or(0), limit);

                                println!("-- Contents of table '{}':", table_name);
                                while let Some(row) = cursor.next() {
                                    let bytes = &row.payload[..];
                                    let mut offset = 0;
                                    let num_cols = u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap()) as usize;
                                    offset += 2;
                                    let mut vals = Vec::with_capacity(num_cols);
                                    for _ in 0..num_cols {
                                        let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
                                        offset += 4;
                                        let v = String::from_utf8_lossy(&bytes[offset..offset + len]).to_string();
                                        offset += len;
                                        vals.push(v);
                                    }
                                    if selection.is_none() {
                                        println!("{:?}", vals);
                                    } else {
                                        let mut map = std::collections::HashMap::new();
                                        for (col, val) in columns.iter().zip(vals.iter()) {
                                            map.insert(col.clone(), val.clone());
                                        }
                                        if crate::sql::ast::evaluate_expression(selection.as_ref().unwrap(), &map) {
                                            println!("{:?}", vals);
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Table '{}' not found: {}", table_name, e);
                        }
                    }
                }
                Statement::Delete { table_name, selection } => {
                    debug!("DELETE FROM {}", table_name);
                    if let Err(e) = execute_delete(&mut catalog, &table_name, selection) {
                        warn!("Error deleting from {}: {}", table_name, e);
                    }
                }
                Statement::Exit => break,
            },
            Err(e) => warn!("Parse error: {}", e),
        }
    }

    info!("Goodbye!");
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*; // bring Catalog, Pager, BTree, etc. into scope
    use crate::sql::ast::evaluate_expression;
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
        for i in 1..=100 {
            let values = vec![
                i.to_string(),
                format!("user{}", i),
                format!("u{}@example.com", i),
            ];
            // Serialize into buf: [u16 num_cols][u32 len1][bytes1][u32 len2][bytes2]...
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
            let new_root = table_btree.root_page();
            if new_root != root_page {
                catalog.get_table_mut("users").unwrap().root_page = new_root;
            }
        }

        // 5) Now scan all rows in “users” and collect them.
        let root_page = catalog.get_table("users").unwrap().root_page;
        let mut table_btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
        let mut rows: Vec<_> = table_btree.scan_all_rows().collect();

        // Assert we got exactly 100 rows, with keys 1 and 100 at the ends
        assert_eq!(rows.len(), 100, "Expected 100 rows in users, got {}", rows.len());
        assert_eq!(rows.first().unwrap().key, 1);
        assert_eq!(rows.last().unwrap().key, 100);
    }

    #[test]
    fn select_where_clause() {
        // Setup new DB
        let filename = "test_where.db";
        let _ = fs::remove_file(filename);
        let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();

        // Create table and insert a few rows
        catalog
            .create_table("users", vec!["id".into(), "name".into()])
            .unwrap();
        for i in 1..=3 {
            let values = vec![i.to_string(), format!("user{}", i)];
            let mut buf = Vec::new();
            let col_count = (values.len() as u16).to_le_bytes();
            buf.extend(&col_count);
            for v in &values {
                let vb = v.as_bytes();
                let len = (vb.len() as u32).to_le_bytes();
                buf.extend(&len);
                buf.extend(vb);
            }
            let root_page = catalog.get_table("users").unwrap().root_page;
            let mut table_btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
            table_btree.insert(i as i32, &buf[..]).unwrap();
            let new_root = table_btree.root_page();
            if new_root != root_page {
                catalog.get_table_mut("users").unwrap().root_page = new_root;
            }
        }

        // Parse select with WHERE
        let stmt = parse_statement("SELECT * FROM users WHERE name = user2").unwrap();
        match stmt {
            Statement::Select { table_name, selection, .. } => {
                assert_eq!(table_name, "users");
                assert!(selection.is_some());
                // Execute simple evaluation of WHERE on all rows
                let table_info = catalog.get_table(&table_name).unwrap().clone();
                let root_page = table_info.root_page;
                let columns = table_info.columns;
                let mut table_btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
                let mut cursor = table_btree.scan_all_rows();
                let mut found = Vec::new();
                while let Some(row) = cursor.next() {
                    // Deserialize row to map
                    let bytes = &row.payload[..];
                    let mut offset = 0;
                    let num_cols = u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap()) as usize;
                    offset += 2;
                    let mut values = std::collections::HashMap::new();
                    for col in columns.iter().take(num_cols) {
                        let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
                        offset += 4;
                        let v = String::from_utf8_lossy(&bytes[offset..offset + len]).to_string();
                        offset += len;
                        values.insert(col.clone(), v);
                    }
                    if evaluate_expression(selection.as_ref().unwrap(), &values) {
                        found.push(row);
                    }
                }
                assert_eq!(found.len(), 1);
                assert_eq!(found[0].key, 2);
            }
            _ => panic!("Expected select statement"),
        }
    }

    #[test]
    fn delete_where_clause() {
        // Setup new DB
        let filename = "test_delete.db";
        let _ = fs::remove_file(filename);
        let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();

        // Create table and insert a few rows
        catalog
            .create_table("users", vec!["id".into(), "name".into()])
            .unwrap();
        for i in 1..=3 {
            let values = vec![i.to_string(), format!("user{}", i)];
            let mut buf = Vec::new();
            let col_count = (values.len() as u16).to_le_bytes();
            buf.extend(&col_count);
            for v in &values {
                let vb = v.as_bytes();
                let len = (vb.len() as u32).to_le_bytes();
                buf.extend(&len);
                buf.extend(vb);
            }
            let root_page = catalog.get_table("users").unwrap().root_page;
            let mut table_btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
            table_btree.insert(i as i32, &buf[..]).unwrap();
            let new_root = table_btree.root_page();
            if new_root != root_page {
                catalog.get_table_mut("users").unwrap().root_page = new_root;
            }
        }

        // Parse delete with WHERE
        let stmt = parse_statement("DELETE FROM users WHERE name = user2").unwrap();
        match stmt {
            Statement::Delete { table_name, selection } => {
                assert_eq!(table_name, "users");
                assert!(selection.is_some());
                execute_delete(&mut catalog, &table_name, selection).unwrap();
                let root_page = catalog.get_table("users").unwrap().root_page;
                let mut table_btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
                let remaining: Vec<_> = table_btree.scan_all_rows().collect();
                assert_eq!(remaining.len(), 2);
                assert!(remaining.iter().all(|r| r.key != 2));
            }
            _ => panic!("Expected delete statement"),
        }
    }

    #[test]
    fn delete_persists_after_reopen() {
        let filename = "test_delete_reopen.db";
        let _ = fs::remove_file(filename);
        let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();

        catalog
            .create_table("users", vec!["id".into(), "name".into()])
            .unwrap();
        for i in 1..=3 {
            let values = vec![i.to_string(), format!("user{}", i)];
            let mut buf = Vec::new();
            let col_count = (values.len() as u16).to_le_bytes();
            buf.extend(&col_count);
            for v in &values {
                let vb = v.as_bytes();
                let len = (vb.len() as u32).to_le_bytes();
                buf.extend(&len);
                buf.extend(vb);
            }
            let root_page = catalog.get_table("users").unwrap().root_page;
            let mut btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
            btree.insert(i as i32, &buf[..]).unwrap();
            let new_root = btree.root_page();
            if new_root != root_page {
                catalog.get_table_mut("users").unwrap().root_page = new_root;
            }
        }

        execute_delete(
            &mut catalog,
            "users",
            Some(Expr::Equals {
                left: "id".into(),
                right: "1".into(),
            }),
        )
        .unwrap();

        let root_page = catalog.get_table("users").unwrap().root_page;
        let mut btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
        assert!(btree.find(1).unwrap().is_none());
        let remaining: Vec<_> = btree.scan_all_rows().collect();
        assert_eq!(remaining.len(), 2);
    }

    #[test]
    fn delete_rebalances_tree() {
        let filename = "test_delete_rebalance.db";
        let _ = fs::remove_file(filename);
        let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();

        catalog
            .create_table("nums", vec!["id".into()])
            .unwrap();

        for i in 1..=500 {
            let values = vec![i.to_string()];
            let mut buf = Vec::new();
            let col_count = (values.len() as u16).to_le_bytes();
            buf.extend(&col_count);
            for v in &values {
                let vb = v.as_bytes();
                let len = (vb.len() as u32).to_le_bytes();
                buf.extend(&len);
                buf.extend(vb);
            }
            let root_page = catalog.get_table("nums").unwrap().root_page;
            let mut btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
            btree.insert(i as i32, &buf[..]).unwrap();
            let new_root = btree.root_page();
            drop(btree);
            if new_root != root_page {
                catalog.get_table_mut("nums").unwrap().root_page = new_root;
            }
        }

        let root_page = catalog.get_table("nums").unwrap().root_page;
        let mut btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
        assert!(btree.delete(150).unwrap());
        let new_root = btree.root_page();
        drop(btree);
        if new_root != root_page {
            catalog.get_table_mut("nums").unwrap().root_page = new_root;
        }

        let mut btree = BTree::open_root(&mut catalog.pager, new_root).unwrap();

        assert!(btree.find(150).unwrap().is_none());
        let remaining: Vec<_> = btree.scan_all_rows().collect();
        assert_eq!(remaining.len(), 499);
    }

    #[test]
    fn delete_collapse_root() {
        let filename = "test_delete_collapse.db";
        let _ = fs::remove_file(filename);
        let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();

        catalog
            .create_table("nums", vec!["id".into()])
            .unwrap();

        for i in 1..=600 {
            let values = vec![i.to_string()];
            let mut buf = Vec::new();
            let col_count = (values.len() as u16).to_le_bytes();
            buf.extend(&col_count);
            for v in &values {
                let vb = v.as_bytes();
                let len = (vb.len() as u32).to_le_bytes();
                buf.extend(&len);
                buf.extend(vb);
            }
            let root_page = catalog.get_table("nums").unwrap().root_page;
            let mut btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
            btree.insert(i as i32, &buf[..]).unwrap();
            let new_root = btree.root_page();
            drop(btree);
            if new_root != root_page {
                catalog.get_table_mut("nums").unwrap().root_page = new_root;
            }
        }

        let root_page = catalog.get_table("nums").unwrap().root_page;
        let mut btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
        for k in 2..=600 {
            btree.delete(k).unwrap();
        }
        let new_root = btree.root_page();
        drop(btree);
        if new_root != root_page {
            catalog.get_table_mut("nums").unwrap().root_page = new_root;
        }

        let root_page = catalog.get_table("nums").unwrap().root_page;
        let page = catalog.pager.get_page(root_page).unwrap();
        assert_eq!(crate::storage::page::get_node_type(&page.data), crate::storage::page::NODE_LEAF);
    }

    #[test]
    fn scan_with_limit_and_offset() {
        let filename = "test_limit_offset.db";
        let _ = fs::remove_file(filename);
        let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();

        catalog
            .create_table("nums", vec!["id".into()])
            .unwrap();

        for i in 1..=300 {
            let values = vec![i.to_string()];
            let mut buf = Vec::new();
            let col_count = (values.len() as u16).to_le_bytes();
            buf.extend(&col_count);
            for v in &values {
                let vb = v.as_bytes();
                let len = (vb.len() as u32).to_le_bytes();
                buf.extend(&len);
                buf.extend(vb);
            }
            let root_page = catalog.get_table("nums").unwrap().root_page;
            let mut btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
            btree.insert(i as i32, &buf[..]).unwrap();
            let new_root = btree.root_page();
            drop(btree);
            if new_root != root_page {
                catalog.get_table_mut("nums").unwrap().root_page = new_root;
            }
        }

        let root_page = catalog.get_table("nums").unwrap().root_page;
        let mut btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
        let cursor = btree.scan_rows_with_bounds(10, Some(5));
        let keys: Vec<_> = cursor.map(|r| r.key).collect();
        assert_eq!(keys, vec![11, 12, 13, 14, 15]);
    }

    #[test]
    fn parse_select_limit_offset_order() {
        let stmt = parse_statement("SELECT * FROM nums LIMIT 5 OFFSET 2 ORDER BY").unwrap();
        match stmt {
            Statement::Select { table_name, selection, limit, offset, order_by } => {
                assert_eq!(table_name, "nums");
                assert!(selection.is_none());
                assert_eq!(limit, Some(5));
                assert_eq!(offset, Some(2));
                assert!(order_by);
            }
            _ => panic!("Expected select statement"),
        }
    }
}
