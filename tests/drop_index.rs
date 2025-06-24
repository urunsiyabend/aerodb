use aerodb::{catalog::Catalog, storage::pager::Pager, sql::{parser::parse_statement, ast::Statement}, execution::runtime::{handle_statement, execute_select_with_indexes}};
use std::fs;

fn setup_catalog(filename: &str) -> Catalog {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

#[test]
fn drop_index_basic() {
    let filename = "test_drop_index_basic.db";
    let mut catalog = setup_catalog(filename);

    handle_statement(&mut catalog, parse_statement("CREATE TABLE t (id INT, name TEXT)").unwrap()).unwrap();
    handle_statement(&mut catalog, parse_statement("CREATE INDEX idx1 ON t (name)").unwrap()).unwrap();
    handle_statement(&mut catalog, parse_statement("CREATE INDEX idx2 ON t (id)").unwrap()).unwrap();
    let names: Vec<String> = catalog.all_indexes().into_iter().map(|i| i.name).collect();
    assert!(names.contains(&"idx1".to_string()));
    assert!(names.contains(&"idx2".to_string()));

    handle_statement(&mut catalog, parse_statement("DROP INDEX idx1").unwrap()).unwrap();
    let names: Vec<String> = catalog.all_indexes().into_iter().map(|i| i.name).collect();
    assert!(!names.contains(&"idx1".to_string()));
    assert!(names.contains(&"idx2".to_string()));
}

#[test]
fn drop_table_cleans_indexes() {
    let filename = "test_drop_table_cleans_indexes.db";
    let mut catalog = setup_catalog(filename);

    handle_statement(&mut catalog, parse_statement("CREATE TABLE t (id INT, name TEXT)").unwrap()).unwrap();
    handle_statement(&mut catalog, parse_statement("CREATE INDEX idx1 ON t (name)").unwrap()).unwrap();
    handle_statement(&mut catalog, parse_statement("CREATE INDEX idx2 ON t (id)").unwrap()).unwrap();

    assert_eq!(catalog.all_indexes().len(), 2);
    handle_statement(&mut catalog, parse_statement("DROP TABLE t").unwrap()).unwrap();
    assert!(catalog.all_indexes().is_empty());

    handle_statement(&mut catalog, parse_statement("CREATE TABLE t (id INT, name TEXT)").unwrap()).unwrap();
    handle_statement(&mut catalog, parse_statement("CREATE INDEX idx1 ON t (name)").unwrap()).unwrap();
    let names: Vec<String> = catalog.all_indexes().into_iter().map(|i| i.name).collect();
    assert!(names.contains(&"idx1".to_string()));
}

#[test]
fn selects_use_full_scan_after_drop() {
    let filename = "test_drop_index_scan.db";
    let mut catalog = setup_catalog(filename);

    handle_statement(&mut catalog, parse_statement("CREATE TABLE t (id INT, name TEXT)").unwrap()).unwrap();
    handle_statement(&mut catalog, parse_statement("CREATE INDEX idx1 ON t (name)").unwrap()).unwrap();
    for i in 1..=3 {
        handle_statement(&mut catalog, parse_statement(&format!("INSERT INTO t VALUES ({}, 'u{}')", i, i)).unwrap()).unwrap();
    }

    handle_statement(&mut catalog, parse_statement("DROP INDEX idx1").unwrap()).unwrap();
    handle_statement(&mut catalog, parse_statement("INSERT INTO t VALUES (4, 'u2')").unwrap()).unwrap();

    let stmt = parse_statement("SELECT * FROM t WHERE name = u2").unwrap();
    if let Statement::Select { from, where_predicate, .. } = stmt {
        let table_name = match from.first().unwrap() {
            aerodb::sql::ast::TableRef::Named { name, .. } => name.clone(),
            _ => panic!("expected table"),
        };
        let mut rows = Vec::new();
        let used = execute_select_with_indexes(&mut catalog, &table_name, where_predicate, &mut rows).unwrap();
        assert!(!used);
        assert_eq!(rows.len(), 2);
    } else {
        panic!("expected select");
    }
}
