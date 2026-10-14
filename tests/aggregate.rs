use aerodb::{catalog::Catalog, storage::pager::Pager, sql::{parser::parse_statement, ast::Statement}, execution::{execute_query, runtime::format_result_header}, storage::row::ColumnType};
use std::fs;

fn setup_catalog(filename: &str) -> Catalog {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

/// Run a query through the unified executor; returns (header, rendered rows).
fn run(catalog: &mut Catalog, sql: &str) -> (String, Vec<Vec<String>>) {
    let Statement::Query(q) = parse_statement(sql).unwrap() else { panic!("expected select") };
    let r = execute_query(catalog, &q, None).unwrap();
    let rows = r.rows.iter().map(|row| row.iter().map(|v| v.to_string()).collect()).collect();
    (format_result_header(&r.columns), rows)
}

#[test]
fn basic_count() {
    let filename = "test_basic_count.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "employees".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), quoted: false, col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false }
        ],
        fks: Vec::new(), primary_key: None, if_not_exists: false, constraints: Vec::new(),
    }).unwrap();
    for i in 1..=3 {
        aerodb::execution::handle_statement(&mut catalog, parse_statement(&format!("INSERT INTO employees VALUES ({})", i)).unwrap()).unwrap();
    }
    let (header, out) = run(&mut catalog, "SELECT COUNT(*) FROM employees");
    assert_eq!(header, "COUNT(*) INTEGER");
    assert_eq!(out, vec![vec!["3".to_string()]]);
}

#[test]
fn simple_grouping() {
    let filename = "test_simple_grouping.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "employees".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), quoted: false, col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false },
            aerodb::sql::ast::ColumnDef { name: "department".into(), quoted: false, col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false, primary_key: false },
        ],
        fks: Vec::new(), primary_key: None, if_not_exists: false, constraints: Vec::new(),
    }).unwrap();
    let data = vec![
        (1, "d1"),
        (2, "d1"),
        (3, "d2"),
    ];
    for (id, dep) in data {
        aerodb::execution::handle_statement(&mut catalog, parse_statement(&format!("INSERT INTO employees VALUES ({}, '{}')", id, dep)).unwrap()).unwrap();
    }
    let (header, mut out) = run(&mut catalog, "SELECT department, COUNT(*) FROM employees GROUP BY department");
    assert_eq!(header, "department TEXT | COUNT(*) INTEGER");
    out.sort();
    assert_eq!(out, vec![vec!["d1".to_string(), "2".to_string()], vec!["d2".to_string(), "1".to_string()]]);
}

#[test]
fn sum_double_basic() {
    let filename = "test_sum_double_basic.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "orders".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), quoted: false, col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false },
            aerodb::sql::ast::ColumnDef { name: "user_id".into(), quoted: false, col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false },
            aerodb::sql::ast::ColumnDef { name: "total".into(), quoted: false, col_type: ColumnType::Double { precision: 10, scale: 2, unsigned: true }, not_null: false, default_value: None, auto_increment: false, primary_key: false },
        ],
        fks: Vec::new(), primary_key: None, if_not_exists: false, constraints: Vec::new(),
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO orders VALUES (1, 2, 20)").unwrap()).unwrap();
    let (header, out) = run(&mut catalog, "SELECT user_id, SUM(total) FROM orders GROUP BY user_id");
    // SUM over a DOUBLE column is DOUBLE (0.5.1 labelled every aggregate INTEGER).
    assert_eq!(header, "user_id INTEGER | SUM(total) DOUBLE");
    assert_eq!(out, vec![vec!["2".to_string(), "20".to_string()]]);
}
