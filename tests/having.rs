use aerodb::{catalog::Catalog, storage::pager::Pager, sql::{parser::parse_statement, ast::Statement}, execution::runtime::{execute_group_query, format_header}, storage::row::ColumnType};
use std::fs;

fn setup_catalog(filename: &str) -> Catalog {
    let _ = fs::remove_file(filename);
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

#[test]
fn having_basic_sum() {
    let filename = "test_having_sum.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "sales".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None },
            aerodb::sql::ast::ColumnDef { name: "region".into(), col_type: ColumnType::Text, not_null: false, default_value: None },
            aerodb::sql::ast::ColumnDef { name: "amount".into(), col_type: ColumnType::Integer, not_null: false, default_value: None },
        ],
        fks: Vec::new(),
        if_not_exists: false,
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
    let stmt = parse_statement("SELECT region, SUM(amount) FROM sales GROUP BY region HAVING SUM(amount) > 100").unwrap();
    if let Statement::Select { columns, from, group_by, having: Some(have), .. } = stmt {
        let table = match from.first().unwrap() {
            aerodb::sql::ast::TableRef::Named { name, .. } => name,
            _ => panic!("expected table"),
        };
        let mut out = Vec::new();
        let header = execute_group_query(&mut catalog, table, &columns, group_by.as_deref(), Some(have), None, &mut out, None).unwrap();
        assert_eq!(format_header(&header), "region TEXT | SUM(amount) INTEGER");
        assert_eq!(out, vec![vec!["north".to_string(), "110".to_string()]]);
    } else { panic!("expected select") }
}

#[test]
fn having_with_where() {
    let filename = "test_having_where.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "employees".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None },
            aerodb::sql::ast::ColumnDef { name: "dept".into(), col_type: ColumnType::Text, not_null: false, default_value: None },
            aerodb::sql::ast::ColumnDef { name: "active".into(), col_type: ColumnType::Integer, not_null: false, default_value: None },
        ],
        fks: Vec::new(),
        if_not_exists: false,
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
    let stmt = parse_statement("SELECT dept, COUNT(*) FROM employees WHERE active = 1 GROUP BY dept HAVING COUNT(*) >= 5").unwrap();
    if let Statement::Select { columns, from, group_by, having: Some(have), where_predicate, .. } = stmt {
        let table = match from.first().unwrap() {
            aerodb::sql::ast::TableRef::Named { name, .. } => name,
            _ => panic!("expected table"),
        };
        let mut out = Vec::new();
        let header = execute_group_query(&mut catalog, table, &columns, group_by.as_deref(), Some(have), where_predicate, &mut out, None).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0][0], "a");
        assert_eq!(out[0][1], "5");
    } else { panic!("expected select") }
}

#[test]
fn having_filters_all() {
    let filename = "test_having_none.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "t".into(),
        columns: vec![aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None }],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    for i in 1..=3 {
        aerodb::execution::handle_statement(&mut catalog, parse_statement(&format!("INSERT INTO t VALUES ({})", i)).unwrap()).unwrap();
    }
    let stmt = parse_statement("SELECT COUNT(*) FROM t HAVING COUNT(*) > 10").unwrap();
    if let Statement::Select { columns, from, group_by, having: Some(have), .. } = stmt {
        let table = match from.first().unwrap() { aerodb::sql::ast::TableRef::Named { name, .. } => name, _ => panic!("expected table") };
        let mut out = Vec::new();
        let header = execute_group_query(&mut catalog, table, &columns, group_by.as_deref(), Some(have), None, &mut out, None).unwrap();
        assert_eq!(format_header(&header), "COUNT(*) INTEGER");
        assert!(out.is_empty());
    } else { panic!("expected select") }
}
