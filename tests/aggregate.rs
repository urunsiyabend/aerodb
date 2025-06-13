use aerodb::{catalog::Catalog, storage::pager::Pager, sql::{parser::parse_statement, ast::Statement}, execution::runtime::{format_header, execute_group_query}, storage::row::ColumnType};
use std::fs;

fn setup_catalog(filename: &str) -> Catalog {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

#[test]
fn basic_count() {
    let filename = "test_basic_count.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "employees".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false }
        ],
        fks: Vec::new(), primary_key: None, if_not_exists: false,
    }).unwrap();
    for i in 1..=3 {
        aerodb::execution::handle_statement(&mut catalog, parse_statement(&format!("INSERT INTO employees VALUES ({})", i)).unwrap()).unwrap();
    }
    let stmt = parse_statement("SELECT COUNT(*) FROM employees").unwrap();
    if let Statement::Select { columns, from, group_by, .. } = stmt {
        let table = match from.first().unwrap() {
            aerodb::sql::ast::TableRef::Named { name, .. } => name,
            _ => panic!("expected table"),
        };
        let mut out = Vec::new();
        let header = aerodb::execution::runtime::execute_group_query(&mut catalog, table, &columns, group_by.as_deref(), None, None, &mut out, None).unwrap();
        assert_eq!(format_header(&header), "COUNT(*) INTEGER");
        assert_eq!(out, vec![vec!["3".to_string()]]);
    } else { panic!("expected select"); }
}

#[test]
fn simple_grouping() {
    let filename = "test_simple_grouping.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "employees".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false },
            aerodb::sql::ast::ColumnDef { name: "department".into(), col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false, primary_key: false },
        ],
        fks: Vec::new(), primary_key: None, if_not_exists: false,
    }).unwrap();
    let data = vec![
        (1, "d1"),
        (2, "d1"),
        (3, "d2"),
    ];
    for (id, dep) in data {
        aerodb::execution::handle_statement(&mut catalog, parse_statement(&format!("INSERT INTO employees VALUES ({}, '{}')", id, dep)).unwrap()).unwrap();
    }
    let stmt = parse_statement("SELECT department, COUNT(*) FROM employees GROUP BY department").unwrap();
    if let Statement::Select { columns, from, group_by, .. } = stmt {
        let table = match from.first().unwrap() {
            aerodb::sql::ast::TableRef::Named { name, .. } => name,
            _ => panic!("expected table"),
        };
        let mut out = Vec::new();
        let header = aerodb::execution::runtime::execute_group_query(&mut catalog, table, &columns, group_by.as_deref(), None, None, &mut out, None).unwrap();
        assert_eq!(format_header(&header), "department TEXT | COUNT(*) INTEGER");
        out.sort();
        assert_eq!(out, vec![vec!["d1".to_string(), "2".to_string()], vec!["d2".to_string(), "1".to_string()]]);
    } else { panic!("expected select") }
}

#[test]
fn sum_double_basic() {
    let filename = "test_sum_double_basic.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "orders".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false },
            aerodb::sql::ast::ColumnDef { name: "user_id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false },
            aerodb::sql::ast::ColumnDef { name: "total".into(), col_type: ColumnType::Double { precision: 10, scale: 2, unsigned: true }, not_null: false, default_value: None, auto_increment: false, primary_key: false },
        ],
        fks: Vec::new(), primary_key: None, if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO orders VALUES (1, 2, 20)").unwrap()).unwrap();
    let stmt = parse_statement("SELECT user_id, SUM(total) FROM orders GROUP BY user_id").unwrap();
    if let Statement::Select { columns, from, group_by, .. } = stmt {
        let table = match from.first().unwrap() {
            aerodb::sql::ast::TableRef::Named { name, .. } => name,
            _ => panic!("expected table"),
        };
        let mut out = Vec::new();
        let header = aerodb::execution::runtime::execute_group_query(&mut catalog, table, &columns, group_by.as_deref(), None, None, &mut out, None).unwrap();
        assert_eq!(format_header(&header), "user_id INTEGER | SUM(total) INTEGER");
        assert_eq!(out, vec![vec!["2".to_string(), "20".to_string()]]);
    } else { panic!("expected select") }
}

