use aerodb::{catalog::Catalog, storage::pager::Pager, sql::{parser::parse_statement, ast::Statement}, execution::runtime::{execute_group_query, format_header}, storage::row::ColumnType};
use std::fs;

fn setup_catalog(filename: &str) -> Catalog {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

fn create_matches_table(catalog: &mut Catalog) {
    aerodb::execution::handle_statement(catalog, Statement::CreateTable {
        table_name: "matches".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false },
            aerodb::sql::ast::ColumnDef { name: "team".into(), col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false, primary_key: false },
            aerodb::sql::ast::ColumnDef { name: "league".into(), col_type: ColumnType::Text, not_null: false, default_value: None, auto_increment: false, primary_key: false },
            aerodb::sql::ast::ColumnDef { name: "score".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false },
        ],
        fks: Vec::new(),
        primary_key: None,
        if_not_exists: false,
    }).unwrap();
    let rows = vec![(1, "a", "l1", 10), (2, "a", "l2", 20), (3, "b", "l1", 15)];
    for (id, team, league, score) in rows {
        aerodb::execution::handle_statement(catalog, parse_statement(&format!("INSERT INTO matches VALUES ({}, '{}', '{}', {})", id, team, league, score)).unwrap()).unwrap();
    }
}

#[test]
fn grouped_column_ok() {
    let filename = "test_group_ok.db";
    let mut catalog = setup_catalog(filename);
    create_matches_table(&mut catalog);
    let stmt = parse_statement("SELECT team, SUM(score) FROM matches GROUP BY team").unwrap();
    if let Statement::Select { columns, from, group_by, .. } = stmt {
        let table = match from.first().unwrap() { aerodb::sql::ast::TableRef::Named { name, .. } => name, _ => panic!("expected table") };
        let mut out = Vec::new();
        let header = execute_group_query(&mut catalog, table, &columns, group_by.as_deref(), None, None, &mut out, None).unwrap();
        assert_eq!(format_header(&header), "team TEXT | SUM(score) INTEGER");
        out.sort();
        assert_eq!(out.len(), 2);
    } else { panic!("expected select") }
}

#[test]
fn all_aggregated_ok() {
    let filename = "test_group_all_agg.db";
    let mut catalog = setup_catalog(filename);
    create_matches_table(&mut catalog);
    let stmt = parse_statement("SELECT SUM(score) FROM matches GROUP BY team").unwrap();
    if let Statement::Select { columns, from, group_by, .. } = stmt {
        let table = match from.first().unwrap() { aerodb::sql::ast::TableRef::Named { name, .. } => name, _ => panic!("expected table") };
        let mut out = Vec::new();
        let header = execute_group_query(&mut catalog, table, &columns, group_by.as_deref(), None, None, &mut out, None).unwrap();
        assert_eq!(format_header(&header), "SUM(score) INTEGER");
    } else { panic!("expected select") }
}

#[test]
fn having_uses_aggregate() {
    let filename = "test_group_having.db";
    let mut catalog = setup_catalog(filename);
    create_matches_table(&mut catalog);
    let stmt = parse_statement("SELECT team, COUNT(*) FROM matches GROUP BY team HAVING COUNT(*) > 1").unwrap();
    if let Statement::Select { columns, from, group_by, having: Some(have), .. } = stmt {
        let table = match from.first().unwrap() { aerodb::sql::ast::TableRef::Named { name, .. } => name, _ => panic!("expected table") };
        let mut out = Vec::new();
        let header = execute_group_query(&mut catalog, table, &columns, group_by.as_deref(), Some(have), None, &mut out, None).unwrap();
        assert_eq!(format_header(&header), "team TEXT | COUNT(*) INTEGER");
        assert_eq!(out.len(), 1);
    } else { panic!("expected select") }
}

#[test]
fn reject_non_grouped_select() {
    let filename = "test_group_fail1.db";
    let mut catalog = setup_catalog(filename);
    create_matches_table(&mut catalog);
    let stmt = parse_statement("SELECT id, SUM(score) FROM matches GROUP BY team").unwrap();
    let res = aerodb::execution::handle_statement(&mut catalog, stmt);
    assert!(matches!(res, Err(aerodb::error::DbError::GroupByMismatch(_))));
}

#[test]
fn reject_missing_group_column() {
    let filename = "test_group_fail2.db";
    let mut catalog = setup_catalog(filename);
    create_matches_table(&mut catalog);
    let stmt = parse_statement("SELECT id FROM matches GROUP BY team").unwrap();
    let res = aerodb::execution::handle_statement(&mut catalog, stmt);
    assert!(matches!(res, Err(aerodb::error::DbError::GroupByMismatch(_))));
}

#[test]
fn reject_extra_group_column() {
    let filename = "test_group_fail3.db";
    let mut catalog = setup_catalog(filename);
    create_matches_table(&mut catalog);
    let stmt = parse_statement("SELECT team FROM matches GROUP BY team, league").unwrap();
    let res = aerodb::execution::handle_statement(&mut catalog, stmt);
    assert!(matches!(res, Err(aerodb::error::DbError::GroupByMismatch(_))));
}

