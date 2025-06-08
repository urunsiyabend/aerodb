// src/main.rs

mod storage;
mod sql;
mod catalog;
mod execution;
mod transaction;

use std::io::{self, Write};
use log::{debug, info, warn};

use crate::storage::pager::Pager;
use crate::storage::btree::BTree;
use crate::storage::row::{RowData, ColumnValue, ColumnType, build_row_data};
use crate::catalog::Catalog;
use crate::sql::parser::parse_statement;
use crate::sql::ast::{Statement, Expr};
use crate::execution::{execute_delete, execute_select_with_indexes, handle_statement};

// const DATABASE_FILE: &str = "data.aerodb";
const DATABASE_FILE: &str = "data.aerodb";


fn main() -> io::Result<()> {
    env_logger::init();
    info!("AeroDB v0.5(Transaction with WAL). Type .exit to quit.");

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
            Ok(stmt) => {
                if let Statement::Exit = stmt {
                    break;
                }
                if let Err(e) = handle_statement(&mut catalog, stmt) {
                    warn!("Execution error: {}", e);
                }
            }
            Err(e) => warn!("Parse error: {}", e),
        }
    }

    info!("Goodbye!");
    Ok(())
}


#[cfg(all(test, feature = "main-tests"))]
mod tests {
    use super::*; // bring Catalog, Pager, BTree, etc. into scope
    use crate::sql::ast::evaluate_expression;
    use crate::storage::row::ColumnType;
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
                vec![
                    ("id".into(), ColumnType::Integer, false),
                    ("name".into(), ColumnType::Text, false),
                    ("email".into(), ColumnType::Text, false),
                ],
            )
            .unwrap();

        // 4) Insert 100 rows:
        for i in 1..=100 {
            let values = vec![
                i.to_string(),
                format!("user{}", i),
                format!("u{}@example.com", i),
            ];
            let cols = values.iter().map(|v| ColumnValue::Text(v.clone())).collect();
            let row_data = RowData(cols);

            // Look up the root_page for “users” and open its B-Tree.
            let root_page = catalog.get_table("users").unwrap().root_page;
            let mut table_btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
            table_btree.insert(i as i32, row_data).unwrap();
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
            .create_table(
                "users",
                vec![
                    ("id".into(), ColumnType::Integer, false),
                    ("name".into(), ColumnType::Text, false),
                ],
            )
            .unwrap();
        for i in 1..=3 {
            let values = vec![i.to_string(), format!("user{}", i)];
            let cols = values.iter().map(|v| ColumnValue::Text(v.clone())).collect();
            let row_data = RowData(cols);
            let root_page = catalog.get_table("users").unwrap().root_page;
            let mut table_btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
            table_btree.insert(i as i32, row_data).unwrap();
            let new_root = table_btree.root_page();
            if new_root != root_page {
                catalog.get_table_mut("users").unwrap().root_page = new_root;
            }
        }

        // Parse select with WHERE
        let stmt = parse_statement("SELECT * FROM users WHERE name = user2").unwrap();
        match stmt {
            Statement::Select { from, where_predicate, .. } => {
                let from_table = match from.first().unwrap() {
                    crate::sql::ast::TableRef::Named { name, .. } => name.clone(),
                    _ => panic!("expected named table"),
                };
                assert_eq!(from_table, "users");
                assert!(where_predicate.is_some());
                // Execute simple evaluation of WHERE on all rows
                let table_info = catalog.get_table(&from_table).unwrap().clone();
                let root_page = table_info.root_page;
                let columns = table_info.columns;
                let mut table_btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
                let mut cursor = table_btree.scan_all_rows();
                let mut found = Vec::new();
                while let Some(row) = cursor.next() {
                    let mut values = std::collections::HashMap::new();
                    for ((col, _), val) in columns.iter().zip(row.data.0.iter()) {
                        let v = val.to_string_value();
                        values.insert(col.clone(), v);
                    }
                    if evaluate_expression(where_predicate.as_ref().unwrap(), &values) {
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
            .create_table(
                "users",
                vec![
                    ("id".into(), ColumnType::Integer, false),
                    ("name".into(), ColumnType::Text, false),
                ],
            )
            .unwrap();
        for i in 1..=3 {
            let values = vec![i.to_string(), format!("user{}", i)];
            let cols = values.iter().map(|v| ColumnValue::Text(v.clone())).collect();
            let row_data = RowData(cols);
            let root_page = catalog.get_table("users").unwrap().root_page;
            let mut table_btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
            table_btree.insert(i as i32, row_data).unwrap();
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
                let deleted = crate::execution::execute_delete(&mut catalog, &table_name, selection).unwrap();
                assert_eq!(deleted, 1);
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
            .create_table(
                "users",
                vec![
                    ("id".into(), ColumnType::Integer, false),
                    ("name".into(), ColumnType::Text, false),
                ],
            )
            .unwrap();
        for i in 1..=3 {
            let values = vec![i.to_string(), format!("user{}", i)];
            let cols = values.iter().map(|v| ColumnValue::Text(v.clone())).collect();
            let row_data = RowData(cols);
            let root_page = catalog.get_table("users").unwrap().root_page;
            let mut btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
            btree.insert(i as i32, row_data).unwrap();
            let new_root = btree.root_page();
            if new_root != root_page {
                catalog.get_table_mut("users").unwrap().root_page = new_root;
            }
        }

        let deleted = crate::execution::execute_delete(
            &mut catalog,
            "users",
            Some(Expr::Equals {
                left: "id".into(),
                right: "1".into(),
            }),
        )
        .unwrap();
        assert_eq!(deleted, 1);

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
            .create_table(
                "nums",
                vec![("id".into(), ColumnType::Integer, false)],
            )
            .unwrap();

        for i in 1..=500 {
            let values = vec![i.to_string()];
            let cols = values.iter().map(|v| ColumnValue::Text(v.clone())).collect();
            let row_data = RowData(cols);
            let root_page = catalog.get_table("nums").unwrap().root_page;
            let mut btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
            btree.insert(i as i32, row_data).unwrap();
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
            .create_table("nums", vec![("id".into(), ColumnType::Integer, false)])
            .unwrap();

        for i in 1..=100 {
            let values = vec![i.to_string()];
            let cols = values.iter().map(|v| ColumnValue::Text(v.clone())).collect();
            let row_data = RowData(cols);
            let root_page = catalog.get_table("nums").unwrap().root_page;
            let mut btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
            btree.insert(i as i32, row_data).unwrap();
            let new_root = btree.root_page();
            drop(btree);
            if new_root != root_page {
                catalog.get_table_mut("nums").unwrap().root_page = new_root;
            }
        }

        let root_page = catalog.get_table("nums").unwrap().root_page;
        let mut btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
        for k in 2..=100 {
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
            .create_table("nums", vec![("id".into(), ColumnType::Integer, false)])
            .unwrap();

        for i in 1..=300 {
            let values = vec![i.to_string()];
            let cols = values.iter().map(|v| ColumnValue::Text(v.clone())).collect();
            let row_data = RowData(cols);
            let root_page = catalog.get_table("nums").unwrap().root_page;
            let mut btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
            btree.insert(i as i32, row_data).unwrap();
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
        let stmt =
            parse_statement("SELECT * FROM nums LIMIT 5 OFFSET 2 ORDER BY id DESC").unwrap();
        match stmt {
            Statement::Select { from, .. } => {
                if let Some(crate::sql::ast::TableRef::Named { name, .. }) = from.first() {
                    assert_eq!(name, "nums");
                } else { panic!("expected named table") }
            }
            _ => panic!("Expected select statement"),
        }
    }

    #[test]
    fn parse_order_by_variants() {
        let stmt = parse_statement("SELECT * FROM users ORDER BY id").unwrap();
        match stmt {
            Statement::Select { from, .. } => {
                if let Some(crate::sql::ast::TableRef::Named { name, .. }) = from.first() {
                    assert_eq!(name, "users");
                } else { panic!("expected named table") }
            }
            _ => panic!("Expected select"),
        }

        let stmt = parse_statement("SELECT * FROM users ORDER BY id ASC").unwrap();
        match stmt {
            Statement::Select { from, .. } => {
                if let Some(crate::sql::ast::TableRef::Named { name, .. }) = from.first() {
                    assert_eq!(name, "users");
                } else { panic!("expected named table") }
            }
            _ => panic!("Expected select"),
        }

        let stmt = parse_statement("SELECT * FROM users ORDER BY id DESC").unwrap();
        match stmt {
            Statement::Select { from, .. } => {
                if let Some(crate::sql::ast::TableRef::Named { name, .. }) = from.first() {
                    assert_eq!(name, "users");
                } else { panic!("expected named table") }
            }
            _ => panic!("Expected select"),
        }
    }

    #[test]
    fn parse_insert_quotes_numbers() {
        let stmt = parse_statement("INSERT INTO t VALUES (1, 'foo', 42)").unwrap();
        match stmt {
            Statement::Insert { values, .. } => {
                assert_eq!(values, vec!["1", "foo", "42"]);
            }
            _ => panic!("Expected insert"),
        }
    }

    #[test]
    fn parse_create_with_types() {
        let stmt = parse_statement("CREATE TABLE t (id INTEGER, name TEXT, active BOOLEAN)").unwrap();
        match stmt {
            Statement::CreateTable { table_name, columns, if_not_exists, .. } => {
                assert_eq!(table_name, "t");
                assert_eq!(columns,
                    vec![
                        ("id".into(), ColumnType::Integer, false),
                        ("name".into(), ColumnType::Text, false),
                        ("active".into(), ColumnType::Boolean, false),
                    ]
                );
                assert!(!if_not_exists);
            }
            _ => panic!("Expected create table"),
        }
    }

    #[test]
    fn scan_descending_order() {
        let filename = "test_order_desc.db";
        let _ = fs::remove_file(filename);
        let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();

        catalog
            .create_table("nums", vec![("id".into(), ColumnType::Integer, false)])
            .unwrap();

        for i in 1..=3 {
            let values = vec![i.to_string()];
            let cols = values.iter().map(|v| ColumnValue::Text(v.clone())).collect();
            let row_data = RowData(cols);
            let root_page = catalog.get_table("nums").unwrap().root_page;
            let mut btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
            btree.insert(i as i32, row_data).unwrap();
            let new_root = btree.root_page();
            drop(btree);
            if new_root != root_page {
                catalog.get_table_mut("nums").unwrap().root_page = new_root;
            }
        }

        let root_page = catalog.get_table("nums").unwrap().root_page;
        let mut btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
        let rows = btree.scan_rows_desc_with_bounds(0, None);
        let keys: Vec<_> = rows.into_iter().map(|r| r.key).collect();
        assert_eq!(keys, vec![3, 2, 1]);
    }

    #[test]
    fn build_row_data_type_mismatch() {
        use crate::storage::row::build_row_data;
        let columns = vec![
            ("id".into(), ColumnType::Integer, false),
            ("name".into(), ColumnType::Text, false),
        ];
        let values = vec!["abc".to_string(), "bob".to_string()];
        let result = build_row_data(&values, &columns);
        assert!(result.is_err());
    }

    #[test]
    fn parse_drop_table() {
        let stmt = parse_statement("DROP TABLE IF EXISTS t").unwrap();
        match stmt {
            Statement::DropTable { table_name, if_exists } => {
                assert_eq!(table_name, "t");
                assert!(if_exists);
            }
            _ => panic!("Expected drop table"),
        }
    }

    #[test]
    fn drop_table_removes_catalog() {
        let filename = "test_drop_table.db";
        let _ = fs::remove_file(filename);
        let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();
        catalog
            .create_table(
                "users",
                vec![("id".into(), ColumnType::Integer, false)],
            )
            .unwrap();
        assert!(catalog.get_table("users").is_ok());
        assert!(catalog.drop_table("users").unwrap());
        assert!(catalog.get_table("users").is_err());

        drop(catalog);
        let catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();
        assert!(catalog.get_table("users").is_err());
    }

    #[test]
    fn parse_create_index() {
        let stmt = parse_statement("CREATE INDEX idx_name ON users (name)").unwrap();
        match stmt {
            Statement::CreateIndex { index_name, table_name, column_name } => {
                assert_eq!(index_name, "idx_name");
                assert_eq!(table_name, "users");
                assert_eq!(column_name, "name");
            }
            _ => panic!("Expected create index"),
        }
    }

    #[test]
    fn index_insert_and_select() {
        let filename = "test_index.db";
        let _ = fs::remove_file(filename);
        let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();

        catalog
            .create_table(
                "users",
                vec![
                    ("id".into(), ColumnType::Integer, false),
                    ("name".into(), ColumnType::Text, false),
                ],
            )
            .unwrap();

        catalog
            .create_index("idx_name", "users", "name")
            .unwrap();

        for i in 1..=3 {
            let values = vec![i.to_string(), format!("user{}", i)];
            let stmt = Statement::Insert { table_name: "users".into(), values };
            match stmt {
                Statement::Insert { table_name, values } => {
                    let table_info = catalog.get_table(&table_name).unwrap();
                    let row_data = build_row_data(&values, &table_info.columns).unwrap();
                    let key = match row_data.0[0] {
                        ColumnValue::Integer(k) => k,
                        _ => unreachable!(),
                    };
                    let root_page = table_info.root_page;
                    let mut bt = BTree::open_root(&mut catalog.pager, root_page).unwrap();
                    bt.insert(key, row_data.clone()).unwrap();
                    let new_root = bt.root_page();
                    drop(bt);
                    if new_root != root_page {
                        catalog.get_table_mut(&table_name).unwrap().root_page = new_root;
                    }
                    catalog.insert_into_indexes(&table_name, &row_data).unwrap();
                }
                _ => unreachable!(),
            }
        }

        let stmt = parse_statement("SELECT * FROM users WHERE name = user2").unwrap();
        match stmt {
            Statement::Select { from, where_predicate: selection, .. } => {
                let table_name = match from.first().unwrap() {
                    crate::sql::ast::TableRef::Named { name, .. } => name.clone(),
                    _ => panic!("expected table"),
                };
                let mut results = Vec::new();
                let used = crate::execution::execute_select_with_indexes(&mut catalog, &table_name, selection, &mut results).unwrap();
                assert!(used, "index should be used for equality predicate");
                assert_eq!(results.len(), 1);
                assert_eq!(results[0].key, 2);
            }
            _ => panic!("Expected select"),
        }
    }

    #[test]
    fn index_deletes_are_visible() {
        let filename = "test_index_delete.db";
        let _ = fs::remove_file(filename);
        let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();

        catalog
            .create_table(
                "users",
                vec![
                    ("id".into(), ColumnType::Integer, false),
                    ("name".into(), ColumnType::Text, false),
                ],
            )
            .unwrap();
        catalog.create_index("idx_name", "users", "name").unwrap();

        for i in 1..=3 {
            let values = vec![i.to_string(), format!("user{}", i)];
            let table_info = catalog.get_table("users").unwrap();
            let row_data = build_row_data(&values, &table_info.columns).unwrap();
            let key = match row_data.0[0] {
                ColumnValue::Integer(k) => k,
                _ => unreachable!(),
            };
            let root_page = table_info.root_page;
            let mut bt = BTree::open_root(&mut catalog.pager, root_page).unwrap();
            bt.insert(key, row_data.clone()).unwrap();
            let new_root = bt.root_page();
            drop(bt);
            if new_root != root_page {
                catalog.get_table_mut("users").unwrap().root_page = new_root;
            }
            catalog.insert_into_indexes("users", &row_data).unwrap();
        }

        // Delete the row with name = user2
        let expr = Expr::Equals { left: "name".into(), right: "user2".into() };
        let deleted = crate::execution::execute_delete(&mut catalog, "users", Some(expr)).unwrap();
        assert_eq!(deleted, 1);

        let stmt = parse_statement("SELECT * FROM users WHERE name = user2").unwrap();
        match stmt {
            Statement::Select { from, where_predicate: selection, .. } => {
                let table_name = match from.first().unwrap() {
                    crate::sql::ast::TableRef::Named { name, .. } => name.clone(),
                    _ => panic!("expected named table"),
                };
                let mut results = Vec::new();
                let used = crate::execution::execute_select_with_indexes(&mut catalog, &table_name, selection, &mut results).unwrap();
                assert!(used, "index should be used on delete check");
                assert!(results.is_empty());
            }
            _ => panic!("Expected select"),
        }
    }

    #[test]
    fn index_handles_duplicates() {
        let filename = "test_index_dup.db";
        let _ = fs::remove_file(filename);
        let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();

        catalog
            .create_table(
                "users",
                vec![
                    ("id".into(), ColumnType::Integer, false),
                    ("name".into(), ColumnType::Text, false),
                ],
            )
            .unwrap();
        catalog.create_index("idx_name", "users", "name").unwrap();

        for i in 1..=3 {
            let values = vec![i.to_string(), "dup".to_string()];
            let table_info = catalog.get_table("users").unwrap();
            let row_data = build_row_data(&values, &table_info.columns).unwrap();
            let key = match row_data.0[0] {
                ColumnValue::Integer(k) => k,
                _ => unreachable!(),
            };
            let root_page = table_info.root_page;
            let mut bt = BTree::open_root(&mut catalog.pager, root_page).unwrap();
            bt.insert(key, row_data.clone()).unwrap();
            let new_root = bt.root_page();
            drop(bt);
            if new_root != root_page {
                catalog.get_table_mut("users").unwrap().root_page = new_root;
            }
            catalog.insert_into_indexes("users", &row_data).unwrap();
        }

        let stmt = parse_statement("SELECT * FROM users WHERE name = dup").unwrap();
        match stmt {
            Statement::Select { from, where_predicate: selection, .. } => {
                let table_name = match from.first().unwrap() {
                    crate::sql::ast::TableRef::Named { name, .. } => name.clone(),
                    _ => panic!("expected named table"),
                };
                let mut results = Vec::new();
                let used = crate::execution::execute_select_with_indexes(&mut catalog, &table_name, selection, &mut results).unwrap();
                assert!(used, "index should be used for duplicate values");
                let keys: Vec<i32> = results.iter().map(|r| r.key).collect();
                assert_eq!(keys, vec![1, 2, 3]);
            }
            _ => panic!("Expected select"),
        }
    }

    #[test]
    fn index_not_used_when_missing() {
        let filename = "test_index_missing.db";
        let _ = fs::remove_file(filename);
        let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();

        catalog
            .create_table(
                "users",
                vec![
                    ("id".into(), ColumnType::Integer, false),
                    ("name".into(), ColumnType::Text, false),
                ],
            )
            .unwrap();

        for i in 1..=2 {
            let values = vec![i.to_string(), format!("user{}", i)];
            let table_info = catalog.get_table("users").unwrap();
            let row_data = build_row_data(&values, &table_info.columns).unwrap();
            let key = match row_data.0[0] {
                ColumnValue::Integer(k) => k,
                _ => unreachable!(),
            };
            let root_page = table_info.root_page;
            let mut bt = BTree::open_root(&mut catalog.pager, root_page).unwrap();
            bt.insert(key, row_data.clone()).unwrap();
            if bt.root_page() != root_page {
                catalog.get_table_mut("users").unwrap().root_page = bt.root_page();
            }
            catalog.insert_into_indexes("users", &row_data).unwrap();
        }

        let stmt = parse_statement("SELECT * FROM users WHERE id = 1").unwrap();
        match stmt {
            Statement::Select { from, where_predicate: selection, .. } => {
                let table_name = match from.first().unwrap() {
                    crate::sql::ast::TableRef::Named { name, .. } => name.clone(),
                    _ => panic!("expected named table"),
                };
                let mut results = Vec::new();
                let used = crate::execution::execute_select_with_indexes(&mut catalog, &table_name, selection, &mut results).unwrap();
                assert!(!used, "no index should be used when none defined");
                assert_eq!(results.len(), 1);
                assert_eq!(results[0].key, 1);
            }
            _ => panic!("Expected select"),
        }
    }

    #[test]
    fn index_not_used_for_inequality() {
        let filename = "test_index_inequality.db";
        let _ = fs::remove_file(filename);
        let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();

        catalog
            .create_table(
                "users",
                vec![
                    ("id".into(), ColumnType::Integer, false),
                    ("name".into(), ColumnType::Text, false),
                ],
            )
            .unwrap();
        catalog.create_index("idx_name", "users", "name").unwrap();

        for i in 1..=2 {
            let values = vec![i.to_string(), format!("user{}", i)];
            let table_info = catalog.get_table("users").unwrap();
            let row_data = build_row_data(&values, &table_info.columns).unwrap();
            let key = match row_data.0[0] {
                ColumnValue::Integer(k) => k,
                _ => unreachable!(),
            };
            let root_page = table_info.root_page;
            let mut bt = BTree::open_root(&mut catalog.pager, root_page).unwrap();
            bt.insert(key, row_data.clone()).unwrap();
            if bt.root_page() != root_page {
                catalog.get_table_mut("users").unwrap().root_page = bt.root_page();
            }
            catalog.insert_into_indexes("users", &row_data).unwrap();
        }

        let stmt = parse_statement("SELECT * FROM users WHERE name != user1").unwrap();
        match stmt {
            Statement::Select { from, where_predicate: selection, .. } => {
                let table_name = match from.first().unwrap() {
                    crate::sql::ast::TableRef::Named { name, .. } => name.clone(),
                    _ => panic!("expected named table"),
                };
                let mut results = Vec::new();
                let used = crate::execution::execute_select_with_indexes(&mut catalog, &table_name, selection, &mut results).unwrap();
                assert!(!used, "index should not be used for inequality");
                let keys: Vec<i32> = results.iter().map(|r| r.key).collect();
                assert_eq!(keys, vec![2]);
            }
            _ => panic!("Expected select"),
        }
    }

    #[test]
    fn handle_statement_insert() {
        let filename = "test_handle.db";
        let _ = fs::remove_file(filename);
        let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();

        let create = Statement::CreateTable {
            table_name: "users".into(),
            columns: vec![
                ("id".into(), ColumnType::Integer, false),
                ("name".into(), ColumnType::Text, false),
            ],
            fks: Vec::new(),
            if_not_exists: false,
        };
        handle_statement(&mut catalog, create).unwrap();

        let insert = Statement::Insert { table_name: "users".into(), values: vec!["1".into(), "bob".into()] };
        handle_statement(&mut catalog, insert).unwrap();

        let root_page = catalog.get_table("users").unwrap().root_page;
        let mut btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
        let rows: Vec<_> = btree.scan_all_rows().collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key, 1);
    }

    #[test]
    fn parse_update_simple() {
        let stmt =
            parse_statement("UPDATE users SET name = bob WHERE id = 1").unwrap();
        match stmt {
            Statement::Update {
                table_name,
                assignments,
                selection,
            } => {
                assert_eq!(table_name, "users");
                assert_eq!(assignments, vec![("name".into(), "bob".into())]);
                match selection.unwrap() {
                    Expr::Equals { left, right } => {
                        assert_eq!(left, "id");
                        assert_eq!(right, "1");
                    }
                    _ => panic!("Expected equals expression"),
                }
            }
            _ => panic!("Expected update statement"),
        }
    }

    #[test]
    fn execute_update_simple() {
        let filename = "test_update.db";
        let _ = fs::remove_file(filename);
        let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();

        catalog
            .create_table(
                "users",
                vec![
                    ("id".into(), ColumnType::Integer, false),
                    ("name".into(), ColumnType::Text, false),
                ],
            )
            .unwrap();

        for i in 1..=2 {
            let insert = Statement::Insert {
                table_name: "users".into(),
                values: vec![i.to_string(), format!("user{}", i)],
            };
            handle_statement(&mut catalog, insert).unwrap();
        }

        let stmt =
            parse_statement("UPDATE users SET name = bob WHERE id = 1").unwrap();
        match stmt {
            Statement::Update {
                table_name,
                assignments,
                selection,
            } => {
                let updated = crate::execution::execute_update(
                    &mut catalog,
                    &table_name,
                    assignments,
                    selection,
                )
                .unwrap();
                assert_eq!(updated, 1);
            }
            _ => panic!("Expected update statement"),
        }

        let root_page = catalog.get_table("users").unwrap().root_page;
        let mut btree = BTree::open_root(&mut catalog.pager, root_page).unwrap();
        let row = btree.find(1).unwrap().unwrap();
        match &row.data.0[1] {
            ColumnValue::Text(s) => assert_eq!(s, "bob"),
            _ => panic!("Expected text"),
        }
    }

    #[test]
    fn parse_transaction_statements() {
        let stmt = parse_statement("BEGIN TRANSACTION tx1").unwrap();
        match stmt {
            Statement::BeginTransaction { name } => assert_eq!(name, Some("tx1".into())),
            _ => panic!("Expected begin transaction"),
        }

        let stmt = parse_statement("BEGIN TRANSACTION").unwrap();
        match stmt {
            Statement::BeginTransaction { name } => assert_eq!(name, None),
            _ => panic!("Expected begin transaction"),
        }

        let stmt = parse_statement("BEGIN").unwrap();
        match stmt {
            Statement::BeginTransaction { name } => assert_eq!(name, None),
            _ => panic!("Expected begin transaction"),
        }

        let stmt = parse_statement("COMMIT").unwrap();
        matches!(stmt, Statement::Commit);

        let stmt = parse_statement("ROLLBACK").unwrap();
        matches!(stmt, Statement::Rollback);
    }

    #[test]
    fn transaction_commit_persists() {
        let filename = "test_tx_commit.db";
        let _ = fs::remove_file(filename);
        let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();

        catalog
            .create_table(
                "items",
                vec![("id".into(), ColumnType::Integer, false)],
            )
            .unwrap();

        catalog.begin_transaction(Some("t1".into())).unwrap();
        let insert = Statement::Insert { table_name: "items".into(), values: vec!["1".into()] };
        handle_statement(&mut catalog, insert).unwrap();
        catalog.commit_transaction().unwrap();

        drop(catalog);
        let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();
        let root_page = catalog.get_table("items").unwrap().root_page;
        let mut bt = BTree::open_root(&mut catalog.pager, root_page).unwrap();
        assert!(bt.find(1).unwrap().is_some());
    }

    #[test]
    fn transaction_commit_update_persists() {
        let filename = "test_tx_update_commit.db";
        let _ = fs::remove_file(filename);
        let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();

        catalog
            .create_table(
                "users",
                vec![("id".into(), ColumnType::Integer, false), ("name".into(), ColumnType::Text, false)],
            )
            .unwrap();

        let insert = Statement::Insert { table_name: "users".into(), values: vec!["1".into(), "user1".into()] };
        handle_statement(&mut catalog, insert).unwrap();
        let insert2 = Statement::Insert { table_name: "users".into(), values: vec!["2".into(), "user2".into()] };
        handle_statement(&mut catalog, insert2).unwrap();

        catalog.begin_transaction(None).unwrap();
        let update = Statement::Update {
            table_name: "users".into(),
            assignments: vec![("name".into(), "new_user2".into())],
            selection: Some(Expr::Equals { left: "name".into(), right: "user2".into() }),
        };
        handle_statement(&mut catalog, update).unwrap();
        catalog.commit_transaction().unwrap();

        drop(catalog);
        let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();
        let root_page = catalog.get_table("users").unwrap().root_page;
        let mut bt = BTree::open_root(&mut catalog.pager, root_page).unwrap();
        let row = bt.find(2).unwrap().unwrap();
        if let ColumnValue::Text(ref name) = row.data.0[1] {
            assert_eq!(name, "new_user2");
        } else {
            panic!("expected text")
        }
    }

    #[test]
    fn transaction_rollback_discards() {
        let filename = "test_tx_rollback.db";
        let _ = fs::remove_file(filename);
        let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();

        catalog
            .create_table(
                "items",
                vec![("id".into(), ColumnType::Integer, false)],
            )
            .unwrap();

        catalog.begin_transaction(Some("t1".into())).unwrap();
        let insert = Statement::Insert { table_name: "items".into(), values: vec!["1".into()] };
        handle_statement(&mut catalog, insert).unwrap();
        catalog.rollback_transaction().unwrap();

        drop(catalog);
        let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();
        let root_page = catalog.get_table("items").unwrap().root_page;
        let mut bt = BTree::open_root(&mut catalog.pager, root_page).unwrap();
        assert!(bt.find(1).unwrap().is_none());
    }

    #[test]
    fn transaction_rollback_in_session() {
        let filename = "test_tx_rollback_mem.db";
        let _ = fs::remove_file(filename);
        let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();

        catalog
            .create_table(
                "items",
                vec![("id".into(), ColumnType::Integer, false)],
            )
            .unwrap();

        catalog.begin_transaction(None).unwrap();
        let insert = Statement::Insert { table_name: "items".into(), values: vec!["1".into()] };
        handle_statement(&mut catalog, insert).unwrap();
        catalog.rollback_transaction().unwrap();

        // verify without reopening file
        let root_page = catalog.get_table("items").unwrap().root_page;
        let mut bt = BTree::open_root(&mut catalog.pager, root_page).unwrap();
        assert!(bt.find(1).unwrap().is_none());
    }

    #[test]
    fn transaction_rollback_new_pages() {
        let filename = "test_tx_newpage.db";
        let _ = fs::remove_file(filename);
        let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();

        catalog
            .create_table(
                "items",
                vec![("id".into(), ColumnType::Integer, false)],
            )
            .unwrap();

        catalog.begin_transaction(None).unwrap();
        // insert many rows so new pages are allocated during the transaction
        for i in 0..150 {
            let insert = Statement::Insert {
                table_name: "items".into(),
                values: vec![i.to_string()],
            };
            handle_statement(&mut catalog, insert).unwrap();
        }
        // Should not error even though new pages were allocated
        assert!(catalog.rollback_transaction().is_ok());
    }
}
