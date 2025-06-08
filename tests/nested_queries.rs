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
    let _ = fs::remove_file(format!("{}.wal", filename));
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

#[test]
fn execute_from_subquery_simple() {
    let filename = "test_from_subquery_exec.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "t1".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false}
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO t1 VALUES (1)").unwrap()).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO t1 VALUES (2)").unwrap()).unwrap();

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
        columns: vec![aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false}],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "admins".into(),
        columns: vec![aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false}],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    for id in 1..=3 {
        aerodb::execution::handle_statement(&mut catalog, parse_statement(&format!("INSERT INTO users VALUES ({})", id)).unwrap()).unwrap();
    }
    for id in [2,3] {
        aerodb::execution::handle_statement(&mut catalog, parse_statement(&format!("INSERT INTO admins VALUES ({})", id)).unwrap()).unwrap();
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
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false},
            aerodb::sql::ast::ColumnDef { name: "name".into(), col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false},
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "orders".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false},
            aerodb::sql::ast::ColumnDef { name: "user_id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false},
            aerodb::sql::ast::ColumnDef { name: "product".into(), col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false},
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (1, 'Alice')").unwrap()).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (2, 'Bob')").unwrap()).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (3, 'Cason')").unwrap()).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO orders VALUES (10, 1, 'Widget')").unwrap()).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO orders VALUES (11, 2, 'Phone')").unwrap()).unwrap();

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

#[test]
fn execute_exists_constant() {
    let filename = "test_exists_constant.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "users".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false},
            aerodb::sql::ast::ColumnDef { name: "name".into(), col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false},
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "orders".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false},
            aerodb::sql::ast::ColumnDef { name: "user_id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false},
            aerodb::sql::ast::ColumnDef { name: "product".into(), col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false},
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (1, 'Alice')").unwrap()).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (2, 'Bob')").unwrap()).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (3, 'Cason')").unwrap()).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO orders VALUES (10, 1, 'Widget')").unwrap()).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO orders VALUES (11, 2, 'Phone')").unwrap()).unwrap();

    let stmt = parse_statement("SELECT name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)").unwrap();
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

#[test]
fn parse_select_subquery() {
    let stmt = parse_statement("SELECT name, (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) FROM users").unwrap();
    if let Statement::Select { columns, .. } = stmt {
        assert!(matches!(columns[1], SelectExpr::Subquery(_)));
    } else { panic!("expected select"); }
}

#[test]
fn execute_scalar_subquery() {
    let filename = "test_scalar_subquery.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "users".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false},
            aerodb::sql::ast::ColumnDef { name: "name".into(), col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false},
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "orders".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false},
            aerodb::sql::ast::ColumnDef { name: "user_id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false},
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (1, 'Alice')").unwrap()).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (2, 'Bob')").unwrap()).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO orders VALUES (10, 1)").unwrap()).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO orders VALUES (11, 1)").unwrap()).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO orders VALUES (12, 2)").unwrap()).unwrap();
    let stmt = parse_statement("SELECT name, (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) FROM users").unwrap();
    if let Statement::Select { .. } = stmt {
        let mut out = Vec::new();
        let header = execute_select_statement(&mut catalog, &stmt, &mut out, None).unwrap();
        assert_eq!(format_header(&header), "name TEXT | SUBQUERY TEXT");
        assert_eq!(out.len(), 2);
        out.sort();
        assert_eq!(out, vec![vec!["Alice".to_string(), "2".to_string()], vec!["Bob".to_string(), "1".to_string()]]);
    } else { panic!("expected select"); }
}
