// src/main.rs

mod storage;
mod sql;
mod catalog;

use std::io::{self, Write};
use log::{debug, info, warn};

use crate::storage::pager::Pager;
use crate::storage::btree::BTree;
use crate::storage::row::{RowData, ColumnValue, ColumnType, build_row_data};
use crate::catalog::Catalog;
use crate::sql::parser::parse_statement;
use crate::sql::ast::{Statement, Expr};

// const DATABASE_FILE: &str = "data.aerodb";
const DATABASE_FILE: &str = "data.aerodb";

fn execute_delete(catalog: &mut Catalog, table_name: &str, selection: Option<Expr>) -> io::Result<()> {
    if let Ok(table_info) = catalog.get_table(table_name) {
        let root_page = table_info.root_page;
        let columns = table_info.columns.clone();
        let rows_to_delete = {
            let mut scan_tree = BTree::open_root(&mut catalog.pager, root_page)?;
            let mut cursor = scan_tree.scan_all_rows();
            let mut collected = Vec::new();
            while let Some(row) = cursor.next() {
                if let Some(ref expr) = selection {
                    let mut values = std::collections::HashMap::new();
                    for ((col, _), val) in columns.iter().zip(row.data.0.iter()) {
                        let v = match val {
                            ColumnValue::Integer(i) => i.to_string(),
                            ColumnValue::Text(s) => s.clone(),
                            ColumnValue::Boolean(b) => b.to_string(),
                        };
                        values.insert(col.clone(), v);
                    }
                    if crate::sql::ast::evaluate_expression(expr, &values) {
                        collected.push(row);
                    }
                } else {
                    collected.push(row);
                }
            }
            drop(cursor);
            collected
        };

        if !rows_to_delete.is_empty() {
            let mut table_btree = BTree::open_root(&mut catalog.pager, root_page)?;
            for r in &rows_to_delete {
                table_btree.delete(r.key)?;
            }
            let new_root = table_btree.root_page();
            drop(table_btree);
            if new_root != root_page {
                if let Ok(t) = catalog.get_table_mut(table_name) {
                    t.root_page = new_root;
                }
            }

            for r in rows_to_delete {
                catalog.remove_from_indexes(table_name, &r.data, r.key)?;
            }
        }
    }
    Ok(())
}

fn execute_select_with_indexes(
    catalog: &mut Catalog,
    table_name: &str,
    selection: Option<Expr>,
    out: &mut Vec<crate::storage::row::Row>,
) -> io::Result<bool> {
    let table_info = catalog.get_table(table_name)?;
    let root_page = table_info.root_page;
    let columns = table_info.columns.clone();

    // Try to use index for simple equality
    if let Some(Expr::Equals { left, right }) = selection.clone() {
        let (col_name, value) = if columns.iter().any(|(c, _)| c == &left) {
            (left, right)
        } else if columns.iter().any(|(c, _)| c == &right) {
            (right, left)
        } else {
            ("".into(), String::new())
        };
        if !col_name.is_empty() {
            if let Some(index) = catalog.find_index(table_name, &col_name).cloned() {
                let mut index_tree = BTree::open_root(&mut catalog.pager, index.root_page)?;
                let val_cv = ColumnValue::Text(value.clone());
                let hash = Catalog::hash_value(&val_cv);
                if let Some(row) = index_tree.find(hash)? {
                    if let ColumnValue::Text(ref stored) = row.data.0[0] {
                        if stored == &value {
                            for val in row.data.0.iter().skip(1) {
                                if let ColumnValue::Integer(k) = val {
                                    let mut table_tree = BTree::open_root(&mut catalog.pager, root_page)?;
                                    if let Some(r) = table_tree.find(*k)? {
                                        out.push(r);
                                    }
                                }
                            }
                            return Ok(true);
                        }
                    }
                }
                return Ok(true);
            }
        }
    }

    let mut table_btree = BTree::open_root(&mut catalog.pager, root_page)?;
    let mut cursor = table_btree.scan_all_rows();
    while let Some(row) = cursor.next() {
        if let Some(ref expr) = selection {
            let mut values = std::collections::HashMap::new();
            for ((col, _), val) in columns.iter().zip(row.data.0.iter()) {
                let v = match val {
                    ColumnValue::Integer(i) => i.to_string(),
                    ColumnValue::Text(s) => s.clone(),
                    ColumnValue::Boolean(b) => b.to_string(),
                };
                values.insert(col.clone(), v);
            }
            if crate::sql::ast::evaluate_expression(expr, &values) {
                out.push(row);
            }
        } else {
            out.push(row);
        }
    }
    Ok(false)
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
                Statement::CreateTable { table_name, columns, if_not_exists } => {
                    debug!("CREATE TABLE {} {:?}", table_name, columns);
                    match catalog.create_table(&table_name, columns.clone()) {
                        Ok(()) => info!("Table '{}' created", table_name),
                        Err(e) => {
                            if if_not_exists && e.to_string().contains("already exists") {
                                info!("Table '{}' already exists", table_name);
                            } else {
                                warn!("Error creating table {}: {}", table_name, e);
                            }
                        }
                    }
                }
                Statement::CreateIndex { index_name, table_name, column_name } => {
                    debug!("CREATE INDEX {} ON {}({})", index_name, table_name, column_name);
                    if let Err(e) = catalog.create_index(&index_name, &table_name, &column_name) {
                        warn!("Error creating index {}: {}", index_name, e);
                    } else {
                        info!("Index '{}' created", index_name);
                    }
                }
                Statement::Insert { table_name, values } => {
                    debug!("INSERT INTO {} VALUES {:?}", table_name, values);

                    // First, try to get the table’s metadata (immutable borrow of `catalog`)
                    match catalog.get_table(&table_name) {
                        Ok(table_info) => {
                            // Extract root_page and drop the borrow of `table_info` immediately.
                            let root_page = table_info.root_page;
                            let columns = table_info.columns.clone();
                            // Now the immutable borrow of `catalog` ends here,
                            // so we can borrow `catalog.pager` mutably below.

                            let row_data = match build_row_data(&values, &columns) {
                                Ok(d) => d,
                                Err(msg) => {
                                    warn!("{}", msg);
                                    continue;
                                }
                            };
                            let key = match row_data.0.get(0) {
                                Some(ColumnValue::Integer(i)) => *i,
                                _ => {
                                    warn!("First column must be an INTEGER key");
                                    continue;
                                }
                            };
                            {
                                let mut table_btree = BTree::open_root(&mut catalog.pager, root_page)?;
                                let row_copy = row_data.clone();
                                if let Err(e) = table_btree.insert(key, row_data) {
                                    warn!("Error inserting into {}: {}", table_name, e);
                                } else {
                                    info!("Row inserted into '{}'", table_name);
                                    let new_root = table_btree.root_page();
                                    if new_root != root_page {
                                        if let Ok(t) = catalog.get_table_mut(&table_name) {
                                            t.root_page = new_root;
                                        }
                                    }
                                    catalog.insert_into_indexes(&table_name, &row_copy)?;
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Table '{}' not found: {}", table_name, e);
                        }
                    }
                }
                Statement::Select { table_name, selection, limit, offset, order_by } => {
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

                                println!("-- Contents of table '{}':", table_name);
                                let header: Vec<String> = columns
                                    .iter()
                                    .map(|(c, t)| format!("{} ({})", c, t.as_str()))
                                    .collect();
                                println!("{:?}", header);

                                if let Some(ob) = order_by {
                                    if ob.descending {
                                        let rows = table_btree
                                            .scan_rows_desc_with_bounds(offset.unwrap_or(0), limit);
                                        for row in rows {
                                            let vals: Vec<String> = row
                                                .data
                                                .0
                                                .iter()
                                                .map(|c| match c {
                                                    ColumnValue::Integer(i) => i.to_string(),
                                                    ColumnValue::Text(s) => s.clone(),
                                                    ColumnValue::Boolean(b) => b.to_string(),
                                                })
                                                .collect();
                                            if selection.is_none() {
                                                println!("{:?}", vals);
                                            } else {
                                                let mut map = std::collections::HashMap::new();
                                                for ((col, _), val) in columns.iter().zip(vals.iter()) {
                                                    map.insert(col.clone(), val.clone());
                                                }
                                                if crate::sql::ast::evaluate_expression(selection.as_ref().unwrap(), &map) {
                                                    println!("{:?}", vals);
                                                }
                                            }
                                        }
                                    } else {
                                        let mut cursor = table_btree.scan_rows_with_bounds(offset.unwrap_or(0), limit);
                                        while let Some(row) = cursor.next() {
                                            let vals: Vec<String> = row
                                                .data
                                                .0
                                                .iter()
                                                .map(|c| match c {
                                                    ColumnValue::Integer(i) => i.to_string(),
                                                    ColumnValue::Text(s) => s.clone(),
                                                    ColumnValue::Boolean(b) => b.to_string(),
                                                })
                                                .collect();
                                            if selection.is_none() {
                                                println!("{:?}", vals);
                                            } else {
                                                let mut map = std::collections::HashMap::new();
                                                for ((col, _), val) in columns.iter().zip(vals.iter()) {
                                                    map.insert(col.clone(), val.clone());
                                                }
                                                if crate::sql::ast::evaluate_expression(selection.as_ref().unwrap(), &map) {
                                                    println!("{:?}", vals);
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    let mut cursor = table_btree.scan_rows_with_bounds(offset.unwrap_or(0), limit);

                                    while let Some(row) = cursor.next() {
                                        let vals: Vec<String> = row
                                            .data
                                            .0
                                            .iter()
                                            .map(|c| match c {
                                                ColumnValue::Integer(i) => i.to_string(),
                                                ColumnValue::Text(s) => s.clone(),
                                                ColumnValue::Boolean(b) => b.to_string(),
                                            })
                                            .collect();
                                        if selection.is_none() {
                                            println!("{:?}", vals);
                                        } else {
                                            let mut map = std::collections::HashMap::new();
                                            for ((col, _), val) in columns.iter().zip(vals.iter()) {
                                                map.insert(col.clone(), val.clone());
                                            }
                                            if crate::sql::ast::evaluate_expression(selection.as_ref().unwrap(), &map) {
                                                println!("{:?}", vals);
                                            }
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
                Statement::DropTable { table_name, if_exists } => {
                    debug!("DROP TABLE {}", table_name);
                    match catalog.drop_table(&table_name) {
                        Ok(true) => info!("Table '{}' dropped", table_name),
                        Ok(false) => {
                            if if_exists {
                                info!("Table '{}' does not exist", table_name);
                            } else {
                                warn!("Table '{}' does not exist", table_name);
                            }
                        }
                        Err(e) => warn!("Error dropping table {}: {}", table_name, e),
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
                    ("id".into(), ColumnType::Integer),
                    ("name".into(), ColumnType::Text),
                    ("email".into(), ColumnType::Text),
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
                    ("id".into(), ColumnType::Integer),
                    ("name".into(), ColumnType::Text),
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
                    let mut values = std::collections::HashMap::new();
                    for ((col, _), val) in columns.iter().zip(row.data.0.iter()) {
                        let v = match val {
                            ColumnValue::Integer(i) => i.to_string(),
                            ColumnValue::Text(s) => s.clone(),
                            ColumnValue::Boolean(b) => b.to_string(),
                        };
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
            .create_table(
                "users",
                vec![
                    ("id".into(), ColumnType::Integer),
                    ("name".into(), ColumnType::Text),
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
            .create_table(
                "users",
                vec![
                    ("id".into(), ColumnType::Integer),
                    ("name".into(), ColumnType::Text),
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
            .create_table(
                "nums",
                vec![("id".into(), ColumnType::Integer)],
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
            .create_table("nums", vec![("id".into(), ColumnType::Integer)])
            .unwrap();

        for i in 1..=600 {
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
            .create_table("nums", vec![("id".into(), ColumnType::Integer)])
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
            Statement::Select {
                table_name,
                selection,
                limit,
                offset,
                order_by: Some(ob),
            } => {
                assert_eq!(table_name, "nums");
                assert!(selection.is_none());
                assert_eq!(limit, Some(5));
                assert_eq!(offset, Some(2));
                assert_eq!(ob.column, "id");
                assert!(ob.descending);
            }
            _ => panic!("Expected select statement"),
        }
    }

    #[test]
    fn parse_order_by_variants() {
        let stmt = parse_statement("SELECT * FROM users ORDER BY id").unwrap();
        match stmt {
            Statement::Select { order_by: Some(ob), .. } => {
                assert_eq!(ob.column, "id");
                assert!(!ob.descending);
            }
            _ => panic!("Expected select"),
        }

        let stmt = parse_statement("SELECT * FROM users ORDER BY id ASC").unwrap();
        match stmt {
            Statement::Select { order_by: Some(ob), .. } => {
                assert_eq!(ob.column, "id");
                assert!(!ob.descending);
            }
            _ => panic!("Expected select"),
        }

        let stmt = parse_statement("SELECT * FROM users ORDER BY id DESC").unwrap();
        match stmt {
            Statement::Select { order_by: Some(ob), .. } => {
                assert_eq!(ob.column, "id");
                assert!(ob.descending);
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
            Statement::CreateTable { table_name, columns, if_not_exists } => {
                assert_eq!(table_name, "t");
                assert_eq!(columns,
                    vec![
                        ("id".into(), ColumnType::Integer),
                        ("name".into(), ColumnType::Text),
                        ("active".into(), ColumnType::Boolean),
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
            .create_table("nums", vec![("id".into(), ColumnType::Integer)])
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
            ("id".into(), ColumnType::Integer),
            ("name".into(), ColumnType::Text),
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
                vec![("id".into(), ColumnType::Integer)],
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
                    ("id".into(), ColumnType::Integer),
                    ("name".into(), ColumnType::Text),
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
            Statement::Select { table_name, selection, .. } => {
                let mut results = Vec::new();
                let used = crate::execute_select_with_indexes(&mut catalog, &table_name, selection, &mut results).unwrap();
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
                    ("id".into(), ColumnType::Integer),
                    ("name".into(), ColumnType::Text),
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
        execute_delete(&mut catalog, "users", Some(expr)).unwrap();

        let stmt = parse_statement("SELECT * FROM users WHERE name = user2").unwrap();
        match stmt {
            Statement::Select { table_name, selection, .. } => {
                let mut results = Vec::new();
                let used = crate::execute_select_with_indexes(&mut catalog, &table_name, selection, &mut results).unwrap();
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
                    ("id".into(), ColumnType::Integer),
                    ("name".into(), ColumnType::Text),
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
            Statement::Select { table_name, selection, .. } => {
                let mut results = Vec::new();
                let used = crate::execute_select_with_indexes(&mut catalog, &table_name, selection, &mut results).unwrap();
                assert!(used, "index should be used for duplicate values");
                let keys: Vec<i32> = results.iter().map(|r| r.key).collect();
                assert_eq!(keys, vec![1, 2, 3]);
            }
            _ => panic!("Expected select"),
        }
    }
}
