use aerodb::{catalog::Catalog, storage::pager::Pager, sql::{ast::{Statement, TableRef}, parser::parse_statement}, execution::runtime::{select_projection_indices, execute_select_with_indexes, row_to_strings, Projection, format_header}, storage::row::{ColumnType}};
use std::fs;

fn setup_catalog(filename: &str) -> Catalog {
    let _ = fs::remove_file(filename);
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

#[test]
fn char_column_basic() {
    let filename = "test_char_basic.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "items".into(),
        columns: vec![
            ("id".into(), ColumnType::Integer),
            ("code".into(), ColumnType::Char(3)),
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, Statement::Insert { table_name: "items".into(), values: vec!["1".into(), "A".into()] }).unwrap();
    let stmt = parse_statement("SELECT code FROM items").unwrap();
    if let Statement::Select { columns, from, .. } = stmt {
        let table = match from.first().unwrap() { TableRef::Named { name, .. } => name, _ => panic!("expected table") };
        let info = catalog.get_table(table).unwrap();
        let (idxs, meta) = select_projection_indices(&info.columns, &columns).unwrap();
        assert_eq!(format_header(&meta), "code CHAR(3)");
        let mut rows = Vec::new();
        execute_select_with_indexes(&mut catalog, table, None, &mut rows).unwrap();
        let vals = row_to_strings(&rows[0]);
        let proj: Vec<_> = idxs.iter().map(|p| match p { Projection::Index(i) => vals[*i].clone(), _ => String::new() }).collect();
        assert_eq!(proj, vec!["A  "]);
    } else { panic!("expected select"); }
}
