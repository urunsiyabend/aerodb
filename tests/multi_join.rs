use aerodb::{catalog::Catalog, storage::pager::Pager, sql::{parser::parse_statement, ast::Statement}, storage::row::ColumnType};
use aerodb::execution::runtime::{execute_multi_join};
use std::fs;

fn setup_catalog(filename: &str) -> Catalog {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

#[test]
fn join_two_tables() {
    let filename = "test_join_two.db";
    let mut catalog = setup_catalog(filename);
    // create tables a(id INTEGER, v TEXT) and b(id INTEGER, a_id INTEGER, w TEXT)
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "a".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false},
            aerodb::sql::ast::ColumnDef { name: "v".into(), col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false},
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "b".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false},
            aerodb::sql::ast::ColumnDef { name: "a_id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false},
            aerodb::sql::ast::ColumnDef { name: "w".into(), col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false},
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    // insert rows
    for (id, v) in &[(1,"av1"),(2,"av2")] {
        aerodb::execution::handle_statement(&mut catalog, parse_statement(&format!("INSERT INTO a VALUES ({}, '{}')", id, v)).unwrap()).unwrap();
    }
    let b_rows = vec![
        (1,1,"bw1"),
        (2,1,"bw2"),
        (3,2,"bw3"),
    ];
    for (id,a_id,w) in b_rows {
        aerodb::execution::handle_statement(&mut catalog, parse_statement(&format!("INSERT INTO b VALUES ({}, {}, '{}')", id, a_id, w)).unwrap()).unwrap();
    }

    let stmt = parse_statement("SELECT a.v, b.w FROM a JOIN b ON a.id = b.a_id").unwrap();
    if let Statement::Select { columns, from, joins, where_predicate, .. } = stmt {
        let base_table = match from.first().unwrap() { aerodb::sql::ast::TableRef::Named { name, .. } => name.clone(), _ => panic!("expected table") };
        let plan = aerodb::execution::plan::MultiJoinPlan { base_table, joins, projections: columns, where_predicate };
        let mut results = Vec::new();
        execute_multi_join(&plan, &mut catalog, &mut results).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], vec![String::from("av1"), String::from("bw1")]);
    } else { panic!("expected select") }
}

#[test]
fn join_three_tables() {
    let filename = "test_join_three.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "a".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false},
            aerodb::sql::ast::ColumnDef { name: "v".into(), col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false},
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "b".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false},
            aerodb::sql::ast::ColumnDef { name: "a_id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false},
            aerodb::sql::ast::ColumnDef { name: "w".into(), col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false},
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "c".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false},
            aerodb::sql::ast::ColumnDef { name: "b_id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false},
            aerodb::sql::ast::ColumnDef { name: "x".into(), col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false},
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO a VALUES (1, 'av1')").unwrap()).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO a VALUES (2, 'av2')").unwrap()).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO b VALUES (1, 1, 'bw1')").unwrap()).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO b VALUES (3, 2, 'bw3')").unwrap()).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO c VALUES (1, 1, 'cx1')").unwrap()).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO c VALUES (2, 3, 'cx2')").unwrap()).unwrap();
    let stmt = parse_statement("SELECT a.v, b.w, c.x FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id").unwrap();
    if let Statement::Select { columns, from, joins, where_predicate, .. } = stmt {
        let base_table = match from.first().unwrap() { aerodb::sql::ast::TableRef::Named { name, .. } => name.clone(), _ => panic!("expected table") };
        let plan = aerodb::execution::plan::MultiJoinPlan { base_table, joins, projections: columns, where_predicate };
        let mut results = Vec::new();
        execute_multi_join(&plan, &mut catalog, &mut results).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], vec![String::from("av1"), String::from("bw1"), String::from("cx1")]);
    } else { panic!("expected select") }
}

#[test]
fn join_with_where() {
    let filename = "test_join_where.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "a".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false},
            aerodb::sql::ast::ColumnDef { name: "v".into(), col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false},
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "b".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false},
            aerodb::sql::ast::ColumnDef { name: "a_id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false},
            aerodb::sql::ast::ColumnDef { name: "w".into(), col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false},
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO a VALUES (1, 'av1')").unwrap()).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO a VALUES (2, 'av2')").unwrap()).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO b VALUES (1, 1, 'bw1')").unwrap()).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO b VALUES (2, 2, 'bw2')").unwrap()).unwrap();
    let stmt = parse_statement("SELECT a.v, b.w FROM a JOIN b ON a.id = b.a_id WHERE a.v = av1").unwrap();
    if let Statement::Select { columns, from, joins, where_predicate, .. } = stmt {
        let base_table = match from.first().unwrap() { aerodb::sql::ast::TableRef::Named { name, .. } => name.clone(), _ => panic!("expected table") };
        let plan = aerodb::execution::plan::MultiJoinPlan { base_table, joins, projections: columns, where_predicate };
        let mut results = Vec::new();
        execute_multi_join(&plan, &mut catalog, &mut results).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], vec![String::from("av1"), String::from("bw1")]);
    } else { panic!("expected select") }
}

