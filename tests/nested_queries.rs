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
        let header = execute_select_statement(&mut catalog, &stmt, &mut out, None).unwrap();
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
        let header = execute_select_statement(&mut catalog, &stmt, &mut out, None).unwrap();
        assert_eq!(format_header(&header), "id INTEGER");
        out.sort();
        assert_eq!(out, vec![vec!["2".to_string()], vec!["3".to_string()]]);
    } else { panic!("expected select"); }
}


#[test]
fn parse_exists_subquery() {
    let stmt = parse_statement("SELECT name FROM users WHERE EXISTS (SELECT user_id FROM orders WHERE orders.user_id = users.id)").unwrap();
    if let Statement::Select { where_predicate: Some(pred), .. } = stmt {
        if !matches!(pred, aerodb::sql::ast::Expr::ExistsSubquery { .. }) {
            panic!("expected ExistsSubquery");
        }
    } else {
        panic!("expected select with predicate");
    }
}

#[test]
fn execute_exists_correlated() {
    let filename = "test_exists_correlated.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "users".into(),
        columns: vec![("id".into(), ColumnType::Integer), ("name".into(), ColumnType::Text)],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "orders".into(),
        columns: vec![("id".into(), ColumnType::Integer), ("user_id".into(), ColumnType::Integer), ("product".into(), ColumnType::Text)],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::Insert { table_name: "users".into(), values: vec!["1".into(), "Alice".into()] }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::Insert { table_name: "users".into(), values: vec!["2".into(), "Bob".into()] }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::Insert { table_name: "users".into(), values: vec!["3".into(), "Cason".into()] }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::Insert { table_name: "orders".into(), values: vec!["10".into(), "1".into(), "Widget".into()] }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::Insert { table_name: "orders".into(), values: vec!["11".into(), "2".into(), "Phone".into()] }).unwrap();

    let stmt = parse_statement("SELECT name FROM users WHERE EXISTS (SELECT user_id FROM orders WHERE orders.user_id = users.id)").unwrap();
    if let Statement::Select { .. } = stmt {
        let mut out = Vec::new();
        let header = execute_select_statement(&mut catalog, &stmt, &mut out, None).unwrap();
        assert_eq!(format_header(&header), "name TEXT");
        out.sort();
        assert_eq!(out, vec![vec!["Alice".to_string()], vec!["Bob".to_string()]]);
    } else {
        panic!("expected select");
    }
}

