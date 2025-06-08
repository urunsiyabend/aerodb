use aerodb::{catalog::Catalog, storage::pager::Pager, sql::{parser::parse_statement, ast::Statement}, execution::runtime::{format_header, execute_group_query}, storage::row::ColumnType};
use std::fs;

fn setup_catalog(filename: &str) -> Catalog {
    let _ = fs::remove_file(filename);
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

#[test]
fn basic_count() {
    let filename = "test_basic_count.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "employees".into(),
        columns: vec![("id".into(), ColumnType::Integer, false)],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    for i in 1..=3 {
        aerodb::execution::handle_statement(&mut catalog, Statement::Insert { table_name: "employees".into(), values: vec![i.to_string()] }).unwrap();
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
            ("id".into(), ColumnType::Integer, false),
            ("department".into(), ColumnType::Text, false),
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    let data = vec![
        (1, "d1"),
        (2, "d1"),
        (3, "d2"),
    ];
    for (id, dep) in data {
        aerodb::execution::handle_statement(&mut catalog, Statement::Insert { table_name: "employees".into(), values: vec![id.to_string(), dep.into()] }).unwrap();
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

