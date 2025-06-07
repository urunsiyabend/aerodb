use aerodb::{catalog::Catalog, storage::pager::Pager, sql::{parser::parse_statement, ast::Statement}, storage::row::ColumnType};
use aerodb::execution::runtime::{execute_multi_join};
use std::fs;

fn setup_catalog(filename: &str) -> Catalog {
    let _ = fs::remove_file(filename);
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

#[test]
fn join_two_tables() {
    let filename = "test_join_two.db";
    let mut catalog = setup_catalog(filename);
    // create tables a(id INTEGER, v TEXT) and b(id INTEGER, a_id INTEGER, w TEXT)
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "a".into(),
        columns: vec![("id".into(), ColumnType::Integer), ("v".into(), ColumnType::Text)],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "b".into(),
        columns: vec![("id".into(), ColumnType::Integer), ("a_id".into(), ColumnType::Integer), ("w".into(), ColumnType::Text)],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    // insert rows
    for (id, v) in &[(1,"av1"),(2,"av2")] {
        aerodb::execution::handle_statement(&mut catalog, Statement::Insert { table_name: "a".into(), values: vec![id.to_string(), (*v).into()] }).unwrap();
    }
    let b_rows = vec![
        (1,1,"bw1"),
        (2,1,"bw2"),
        (3,2,"bw3"),
    ];
    for (id,a_id,w) in b_rows {
        aerodb::execution::handle_statement(&mut catalog, Statement::Insert { table_name: "b".into(), values: vec![id.to_string(), a_id.to_string(), w.into()] }).unwrap();
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
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable { table_name: "a".into(), columns: vec![("id".into(), ColumnType::Integer), ("v".into(), ColumnType::Text)], fks: Vec::new(), if_not_exists: false }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable { table_name: "b".into(), columns: vec![("id".into(), ColumnType::Integer), ("a_id".into(), ColumnType::Integer), ("w".into(), ColumnType::Text)], fks: Vec::new(), if_not_exists: false }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable { table_name: "c".into(), columns: vec![("id".into(), ColumnType::Integer), ("b_id".into(), ColumnType::Integer), ("x".into(), ColumnType::Text)], fks: Vec::new(), if_not_exists: false }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::Insert { table_name: "a".into(), values: vec!["1".into(), "av1".into()] }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::Insert { table_name: "a".into(), values: vec!["2".into(), "av2".into()] }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::Insert { table_name: "b".into(), values: vec!["1".into(), "1".into(), "bw1".into()] }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::Insert { table_name: "b".into(), values: vec!["3".into(), "2".into(), "bw3".into()] }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::Insert { table_name: "c".into(), values: vec!["1".into(), "1".into(), "cx1".into()] }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::Insert { table_name: "c".into(), values: vec!["2".into(), "3".into(), "cx2".into()] }).unwrap();
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
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable { table_name: "a".into(), columns: vec![("id".into(), ColumnType::Integer), ("v".into(), ColumnType::Text)], fks: Vec::new(), if_not_exists: false }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable { table_name: "b".into(), columns: vec![("id".into(), ColumnType::Integer), ("a_id".into(), ColumnType::Integer), ("w".into(), ColumnType::Text)], fks: Vec::new(), if_not_exists: false }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::Insert { table_name: "a".into(), values: vec!["1".into(), "av1".into()] }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::Insert { table_name: "a".into(), values: vec!["2".into(), "av2".into()] }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::Insert { table_name: "b".into(), values: vec!["1".into(), "1".into(), "bw1".into()] }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::Insert { table_name: "b".into(), values: vec!["2".into(), "2".into(), "bw2".into()] }).unwrap();
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

