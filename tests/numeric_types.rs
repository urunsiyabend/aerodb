use aerodb::{catalog::Catalog, storage::pager::Pager, sql::{parser::parse_statement, ast::Statement}, execution::runtime::handle_statement, storage::row::ColumnType};
use std::fs;

fn setup_catalog(filename: &str) -> Catalog {
    let _ = fs::remove_file(filename);
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

#[test]
fn parse_numeric_types() {
    let stmt = parse_statement("CREATE TABLE nums (a SMALLINT(5) UNSIGNED, b MEDIUMINT(6), c DOUBLE(8,2) UNSIGNED, d DATE)").unwrap();
    if let Statement::CreateTable { columns, .. } = stmt {
        assert_eq!(columns[0].1, ColumnType::SmallInt { width: 5, unsigned: true });
        assert_eq!(columns[1].1, ColumnType::MediumInt { width: 6, unsigned: false });
        assert_eq!(columns[2].1, ColumnType::Double { precision: 8, scale: 2, unsigned: true });
        assert_eq!(columns[3].1, ColumnType::Date);
    } else { panic!("expected create table"); }
}

#[test]
fn smallint_range() {
    let filename = "test_smallint_range.db";
    let mut catalog = setup_catalog(filename);
    handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "t".into(),
        columns: vec![("id".into(), ColumnType::SmallInt { width: 5, unsigned: true })],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    let res = handle_statement(&mut catalog, Statement::Insert { table_name: "t".into(), values: vec!["-1".into()] });
    assert!(res.is_err());
    let res = handle_statement(&mut catalog, Statement::Insert { table_name: "t".into(), values: vec!["70000".into()] });
    assert!(res.is_err());
    handle_statement(&mut catalog, Statement::Insert { table_name: "t".into(), values: vec!["123".into()] }).unwrap();
}

#[test]
fn mediumint_range() {
    let filename = "test_mediumint_range.db";
    let mut catalog = setup_catalog(filename);
    handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "t".into(),
        columns: vec![("val".into(), ColumnType::MediumInt { width: 6, unsigned: false })],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    let res = handle_statement(&mut catalog, Statement::Insert { table_name: "t".into(), values: vec!["-9000000".into()] });
    assert!(res.is_err());
    let res = handle_statement(&mut catalog, Statement::Insert { table_name: "t".into(), values: vec!["9000000".into()] });
    assert!(res.is_err());
    handle_statement(&mut catalog, Statement::Insert { table_name: "t".into(), values: vec!["100".into()] }).unwrap();
}

#[test]
fn double_unsigned() {
    let filename = "test_double_unsigned.db";
    let mut catalog = setup_catalog(filename);
    handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "t".into(),
        columns: vec![
            ("id".into(), ColumnType::Integer),
            ("price".into(), ColumnType::Double { precision: 8, scale: 2, unsigned: true })
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    let res = handle_statement(&mut catalog, Statement::Insert { table_name: "t".into(), values: vec!["1".into(), "-1".into()] });
    assert!(res.is_err());
    handle_statement(&mut catalog, Statement::Insert { table_name: "t".into(), values: vec!["2".into(), "12.34".into()] }).unwrap();
}
#[test]
fn parse_datetime_types() {
    let stmt = parse_statement("CREATE TABLE t (a DATETIME, b TIMESTAMP, c TIME, d YEAR)").unwrap();
    if let Statement::CreateTable { columns, .. } = stmt {
        assert_eq!(columns[0].1, ColumnType::DateTime);
        assert_eq!(columns[1].1, ColumnType::Timestamp);
        assert_eq!(columns[2].1, ColumnType::Time);
        assert_eq!(columns[3].1, ColumnType::Year);
    } else { panic!("expected create table"); }
}

#[test]
fn date_validation() {
    let filename = "test_date_validate.db";
    let mut catalog = setup_catalog(filename);
    handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "t".into(),
        columns: vec![
            ("id".into(), ColumnType::Integer),
            ("d".into(), ColumnType::Date),
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    let res = handle_statement(&mut catalog, Statement::Insert { table_name: "t".into(), values: vec!["1".into(), "2025-13-01".into()] });
    assert!(res.is_err());
    handle_statement(&mut catalog, Statement::Insert { table_name: "t".into(), values: vec!["2".into(), "2025-12-01".into()] }).unwrap();
}

#[test]
fn datetime_validation() {
    let filename = "test_datetime_validate.db";
    let mut catalog = setup_catalog(filename);
    handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "t".into(),
        columns: vec![
            ("id".into(), ColumnType::Integer),
            ("ts".into(), ColumnType::DateTime),
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    let res = handle_statement(&mut catalog, Statement::Insert { table_name: "t".into(), values: vec!["1".into(), "2025-02-30 10:00:00".into()] });
    assert!(res.is_err());
    handle_statement(&mut catalog, Statement::Insert { table_name: "t".into(), values: vec!["2".into(), "2025-06-08 12:34:56".into()] }).unwrap();
}

#[test]
fn time_validation() {
    let filename = "test_time_validate.db";
    let mut catalog = setup_catalog(filename);
    handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "t".into(),
        columns: vec![
            ("id".into(), ColumnType::Integer),
            ("t".into(), ColumnType::Time),
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    let res = handle_statement(&mut catalog, Statement::Insert { table_name: "t".into(), values: vec!["1".into(), "839:00:00".into()] });
    assert!(res.is_err());
    handle_statement(&mut catalog, Statement::Insert { table_name: "t".into(), values: vec!["2".into(), "12:30:45".into()] }).unwrap();
}

#[test]
fn year_validation() {
    let filename = "test_year_validate.db";
    let mut catalog = setup_catalog(filename);
    handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "t".into(),
        columns: vec![
            ("id".into(), ColumnType::Integer),
            ("y".into(), ColumnType::Year),
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    let res = handle_statement(&mut catalog, Statement::Insert { table_name: "t".into(), values: vec!["1".into(), "1900".into()] });
    assert!(res.is_err());
    handle_statement(&mut catalog, Statement::Insert { table_name: "t".into(), values: vec!["2".into(), "2020".into()] }).unwrap();
}
