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
fn having_basic_sum() {
    let filename = "test_having_sum.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "sales".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false},
            aerodb::sql::ast::ColumnDef { name: "region".into(), col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false, primary_key: false},
            aerodb::sql::ast::ColumnDef { name: "amount".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false},
        ],
        fks: Vec::new(), primary_key: None, if_not_exists: false,
    }).unwrap();
    let rows = vec![
        (1, "north", 50),
        (2, "north", 60),
        (3, "south", 40),
        (4, "south", 20),
    ];
    for (id, region, amt) in rows {
        aerodb::execution::handle_statement(
            &mut catalog,
            parse_statement(&format!("INSERT INTO sales VALUES ({}, '{}', {})", id, region, amt)).unwrap(),
        ).unwrap();
    }
    let (header, out) = run(&mut catalog, "SELECT region, SUM(amount) FROM sales GROUP BY region HAVING SUM(amount) > 100");
    assert_eq!(header, "region TEXT | SUM(amount) INTEGER");
    assert_eq!(out, vec![vec!["north".to_string(), "110".to_string()]]);
}

#[test]
fn having_with_where() {
    let filename = "test_having_where.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "employees".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false},
            aerodb::sql::ast::ColumnDef { name: "dept".into(), col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false, primary_key: false},
            aerodb::sql::ast::ColumnDef { name: "active".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false},
        ],
        fks: Vec::new(), primary_key: None, if_not_exists: false,
    }).unwrap();
    let rows = vec![
        (1, "a", 1),
        (2, "a", 1),
        (3, "a", 1),
        (4, "a", 1),
        (5, "a", 1),
        (6, "b", 1),
        (7, "b", 1),
        (8, "b", 1),
        (9, "b", 0),
        (10, "b", 0),
    ];
    for (id, dept, active) in rows {
        aerodb::execution::handle_statement(
            &mut catalog,
            parse_statement(&format!("INSERT INTO employees VALUES ({}, '{}', {})", id, dept, active)).unwrap(),
        ).unwrap();
    }
    let (_header, out) = run(&mut catalog, "SELECT dept, COUNT(*) FROM employees WHERE active = 1 GROUP BY dept HAVING COUNT(*) >= 5");
    assert_eq!(out.len(), 1);
    assert_eq!(out[0][0], "a");
    assert_eq!(out[0][1], "5");
}

#[test]
fn having_filters_all() {
    let filename = "test_having_none.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "t".into(),
        columns: vec![aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false}],
        fks: Vec::new(), primary_key: None, if_not_exists: false,
    }).unwrap();
    for i in 1..=3 {
        aerodb::execution::handle_statement(&mut catalog, parse_statement(&format!("INSERT INTO t VALUES ({})", i)).unwrap()).unwrap();
    }
    let (header, out) = run(&mut catalog, "SELECT COUNT(*) FROM t HAVING COUNT(*) > 10");
    assert_eq!(header, "COUNT(*) INTEGER");
    assert!(out.is_empty());
}
#[test]
fn having_sum_double() {
    let filename = "test_having_double.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "orders".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false},
            aerodb::sql::ast::ColumnDef { name: "user_id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false},
            aerodb::sql::ast::ColumnDef { name: "total".into(), col_type: ColumnType::Double { precision: 10, scale: 2, unsigned: true }, not_null: false, default_value: None, auto_increment: false, primary_key: false},
        ],
        fks: Vec::new(), primary_key: None, if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO orders VALUES (2, 2, 20)").unwrap()).unwrap();
    let (header, out) = run(&mut catalog, "SELECT user_id, SUM(TOTAL) FROM orders GROUP BY user_id HAVING SUM(TOTAL) > 100;");
    // SUM over a DOUBLE column is DOUBLE (0.5.1 labelled every aggregate INTEGER).
    assert_eq!(header, "user_id INTEGER | SUM(TOTAL) DOUBLE");
    assert!(out.is_empty());
}

#[test]
fn having_ignores_alias() {
    let filename = "test_having_alias.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "orders".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false},
            aerodb::sql::ast::ColumnDef { name: "user_id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false},
            aerodb::sql::ast::ColumnDef { name: "total".into(), col_type: ColumnType::Double { precision: 10, scale: 2, unsigned: true }, not_null: false, default_value: None, auto_increment: false, primary_key: false},
        ],
        fks: Vec::new(), primary_key: None, if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO orders VALUES (1, 2, 20)").unwrap()).unwrap();
    let (header, out) = run(&mut catalog, "SELECT user_id, SUM(total) AS total_spent FROM orders GROUP BY user_id HAVING SUM(total) > 3");
    // SUM over a DOUBLE column is DOUBLE (0.5.1 labelled every aggregate INTEGER).
    assert_eq!(header, "user_id INTEGER | total_spent DOUBLE");
    assert_eq!(out, vec![vec!["2".to_string(), "20".to_string()]]);
}
