use aerodb::sql::{parser::parse_statement, ast::{Statement, TableRef, SelectExpr}};

#[test]
fn parse_from_subquery() {
    let stmt = parse_statement("SELECT * FROM (SELECT id FROM t1) AS sub").unwrap();
    if let Statement::Select { from, .. } = stmt {
        assert!(matches!(from[0], TableRef::Subquery { .. }));
    } else { panic!("expected select"); }
}

// TODO: support subquery parsing in SELECT list

use aerodb::{catalog::Catalog, storage::pager::Pager, execution::runtime::{execute_select_statement, format_header}};
use aerodb::storage::row::ColumnType;
use std::fs;

fn setup_catalog(filename: &str) -> Catalog {
    let _ = fs::remove_file(filename);
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

#[test]
fn execute_from_subquery_simple() {
    let filename = "test_from_subquery_exec.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "t1".into(),
        columns: vec![("id".into(), ColumnType::Integer)],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::Insert { table_name: "t1".into(), values: vec!["1".into()] }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::Insert { table_name: "t1".into(), values: vec!["2".into()] }).unwrap();

    let stmt = parse_statement("SELECT * FROM (SELECT id FROM t1) AS sub").unwrap();
    if let Statement::Select { .. } = stmt {
        let mut out = Vec::new();
        let header = execute_select_statement(&mut catalog, &stmt, &mut out).unwrap();
        assert_eq!(format_header(&header), "id INTEGER");
        out.sort();
        assert_eq!(out, vec![vec![String::from("1")], vec![String::from("2")]]);
    } else { panic!("expected select"); }
}

#[test]
fn parse_where_in_subquery() {
    let stmt = parse_statement("SELECT id FROM users WHERE id IN (SELECT id FROM admins)").unwrap();
    if let Statement::Select { where_predicate: Some(pred), .. } = stmt {
        match pred {
            aerodb::sql::ast::Expr::InSubquery { ref left, .. } => assert_eq!(left, "id"),
            _ => panic!("expected InSubquery"),
        }
    } else { panic!("expected select with predicate"); }
}

#[test]
fn execute_where_in_subquery() {
    let filename = "test_where_in_subquery.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "users".into(),
        columns: vec![("id".into(), ColumnType::Integer)],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "admins".into(),
        columns: vec![("id".into(), ColumnType::Integer)],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    for id in 1..=3 {
        aerodb::execution::handle_statement(&mut catalog, Statement::Insert { table_name: "users".into(), values: vec![id.to_string()] }).unwrap();
    }
    for id in [2,3] {
        aerodb::execution::handle_statement(&mut catalog, Statement::Insert { table_name: "admins".into(), values: vec![id.to_string()] }).unwrap();
    }

    let stmt = parse_statement("SELECT id FROM users WHERE id IN (SELECT id FROM admins)").unwrap();
    if let Statement::Select { .. } = stmt {
        let mut out = Vec::new();
        let header = execute_select_statement(&mut catalog, &stmt, &mut out).unwrap();
        assert_eq!(format_header(&header), "id INTEGER");
        out.sort();
        assert_eq!(out, vec![vec!["2".to_string()], vec!["3".to_string()]]);
    } else { panic!("expected select"); }
}

