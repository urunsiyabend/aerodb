use aerodb::{catalog::Catalog, storage::pager::Pager, sql::{parser::parse_statement, ast::Statement}, execution::runtime::{select_projection_indices, row_to_strings, format_header, Projection}, storage::row::ColumnType};
use std::fs;

fn setup_catalog(filename: &str) -> Catalog {
    let _ = fs::remove_file(filename);
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

#[test]
fn select_single_column() {
    let filename = "test_select_single.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "users".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None },
            aerodb::sql::ast::ColumnDef { name: "name".into(), col_type: ColumnType::Text, not_null: false, default_value: None },
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::Insert { table_name: "users".into(), values: vec!["1".into(), "bob".into()] }).unwrap();
    let stmt = parse_statement("SELECT name FROM users").unwrap();
    if let Statement::Select { columns, from, .. } = stmt {
        let table = match from.first().unwrap() { aerodb::sql::ast::TableRef::Named { name, .. } => name, _ => panic!("expected table") };
        let info = catalog.get_table(table).unwrap();
        let (idxs, meta) = select_projection_indices(&info.columns, &columns).unwrap();
        assert_eq!(format_header(&meta), "name TEXT");
        let mut rows = Vec::new();
        aerodb::execution::execute_select_with_indexes(&mut catalog, table, None, &mut rows).unwrap();
        let vals = row_to_strings(&rows[0]);
        let proj: Vec<_> = idxs
            .iter()
            .map(|p| match p {
                aerodb::execution::runtime::Projection::Index(i) => vals[*i].clone(),
                aerodb::execution::runtime::Projection::Literal(s) => s.clone(),
                aerodb::execution::runtime::Projection::Subquery(_) => String::new(),
            })
            .collect();
        assert_eq!(proj, vec!["bob"]);
    } else { panic!("expected select"); }
}

#[test]
fn select_two_columns() {
    let filename = "test_select_two.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "users".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None },
            aerodb::sql::ast::ColumnDef { name: "name".into(), col_type: ColumnType::Text, not_null: false, default_value: None },
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::Insert { table_name: "users".into(), values: vec!["1".into(), "bob".into()] }).unwrap();
    let stmt = parse_statement("SELECT id, name FROM users").unwrap();
    if let Statement::Select { columns, from, .. } = stmt {
        let table = match from.first().unwrap() { aerodb::sql::ast::TableRef::Named { name, .. } => name, _ => panic!("expected table") };
        let info = catalog.get_table(table).unwrap();
        let (idxs, meta) = select_projection_indices(&info.columns, &columns).unwrap();
        assert_eq!(format_header(&meta), "id INTEGER | name TEXT");
        let mut rows = Vec::new();
        aerodb::execution::execute_select_with_indexes(&mut catalog, table, None, &mut rows).unwrap();
        let vals = row_to_strings(&rows[0]);
        let proj: Vec<_> = idxs
            .iter()
            .map(|p| match p {
                aerodb::execution::runtime::Projection::Index(i) => vals[*i].clone(),
                aerodb::execution::runtime::Projection::Literal(s) => s.clone(),
                aerodb::execution::runtime::Projection::Subquery(_) => String::new(),
            })
            .collect();
        assert_eq!(proj, vec!["1", "bob"]);
    } else { panic!("expected select"); }
}

