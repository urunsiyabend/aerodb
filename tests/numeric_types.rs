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
        assert_eq!(columns[0].col_type, ColumnType::SmallInt { width: 5, unsigned: true });
        assert_eq!(columns[1].col_type, ColumnType::MediumInt { width: 6, unsigned: false });
        assert_eq!(columns[2].col_type, ColumnType::Double { precision: 8, scale: 2, unsigned: true });
        assert_eq!(columns[3].col_type, ColumnType::Date);
    } else { panic!("expected create table"); }
}

#[test]
fn smallint_range() {
    let filename = "test_smallint_range.db";
    let mut catalog = setup_catalog(filename);
    handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "t".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::SmallInt { width: 5, unsigned: true }, not_null: false, default_value: None, auto_increment: false}
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    let res = handle_statement(&mut catalog, parse_statement("INSERT INTO t VALUES (-1)").unwrap());
    assert!(res.is_err());
    let res = handle_statement(&mut catalog, parse_statement("INSERT INTO t VALUES (70000)").unwrap());
    assert!(res.is_err());
    handle_statement(&mut catalog, parse_statement("INSERT INTO t VALUES (123)").unwrap()).unwrap();
}

#[test]
fn mediumint_range() {
    let filename = "test_mediumint_range.db";
    let mut catalog = setup_catalog(filename);
    handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "t".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "val".into(), col_type: ColumnType::MediumInt { width: 6, unsigned: false }, not_null: false, default_value: None, auto_increment: false}
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    let res = handle_statement(&mut catalog, parse_statement("INSERT INTO t VALUES (-9000000)").unwrap());
    assert!(res.is_err());
    let res = handle_statement(&mut catalog, parse_statement("INSERT INTO t VALUES (9000000)").unwrap());
    assert!(res.is_err());
    handle_statement(&mut catalog, parse_statement("INSERT INTO t VALUES (100)").unwrap()).unwrap();
}

#[test]
fn double_unsigned() {
    let filename = "test_double_unsigned.db";
    let mut catalog = setup_catalog(filename);
    handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "t".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false},
            aerodb::sql::ast::ColumnDef { name: "price".into(), col_type: ColumnType::Double { precision: 8, scale: 2, unsigned: true }, not_null: false, default_value: None, auto_increment: false}
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    let res = handle_statement(&mut catalog, parse_statement("INSERT INTO t VALUES (1, -1)").unwrap());
    assert!(res.is_err());
    handle_statement(&mut catalog, parse_statement("INSERT INTO t VALUES (2, 12.34)").unwrap()).unwrap();
}
#[test]
fn parse_datetime_types() {
    let stmt = parse_statement("CREATE TABLE t (a DATETIME, b TIMESTAMP, c TIME, d YEAR)").unwrap();
    if let Statement::CreateTable { columns, .. } = stmt {
        assert_eq!(columns[0].col_type, ColumnType::DateTime);
        assert_eq!(columns[1].col_type, ColumnType::Timestamp);
        assert_eq!(columns[2].col_type, ColumnType::Time);
        assert_eq!(columns[3].col_type, ColumnType::Year);
    } else { panic!("expected create table"); }
}

#[test]
fn date_validation() {
    let filename = "test_date_validate.db";
    let mut catalog = setup_catalog(filename);
    handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "t".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false},
            aerodb::sql::ast::ColumnDef { name: "d".into(), col_type: ColumnType::Date, not_null: false, default_value: None, auto_increment: false},
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    let res = handle_statement(&mut catalog, parse_statement("INSERT INTO t VALUES (1, '2025-13-01')").unwrap());
    assert!(res.is_err());
    handle_statement(&mut catalog, parse_statement("INSERT INTO t VALUES (2, '2025-12-01')").unwrap()).unwrap();
}

#[test]
fn datetime_validation() {
    let filename = "test_datetime_validate.db";
    let mut catalog = setup_catalog(filename);
    handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "t".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false},
            aerodb::sql::ast::ColumnDef { name: "ts".into(), col_type: ColumnType::DateTime, not_null: false, default_value: None, auto_increment: false},
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    let res = handle_statement(&mut catalog, parse_statement("INSERT INTO t VALUES (1, '2025-02-30 10:00:00')").unwrap());
    assert!(res.is_err());
    handle_statement(&mut catalog, parse_statement("INSERT INTO t VALUES (2, '2025-06-08 12:34:56')").unwrap()).unwrap();
}

#[test]
fn time_validation() {
    let filename = "test_time_validate.db";
    let mut catalog = setup_catalog(filename);
    handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "t".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false},
            aerodb::sql::ast::ColumnDef { name: "t".into(), col_type: ColumnType::Time, not_null: false, default_value: None, auto_increment: false},
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    let res = handle_statement(&mut catalog, parse_statement("INSERT INTO t VALUES (1, '839:00:00')").unwrap());
    assert!(res.is_err());
    handle_statement(&mut catalog, parse_statement("INSERT INTO t VALUES (2, '12:30:45')").unwrap()).unwrap();
}

#[test]
fn year_validation() {
    let filename = "test_year_validate.db";
    let mut catalog = setup_catalog(filename);
    handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "t".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false},
            aerodb::sql::ast::ColumnDef { name: "y".into(), col_type: ColumnType::Year, not_null: false, default_value: None, auto_increment: false},
        ],
        fks: Vec::new(),
        if_not_exists: false,
    }).unwrap();
    let res = handle_statement(&mut catalog, parse_statement("INSERT INTO t VALUES (1, '1900')").unwrap());
    assert!(res.is_err());
    handle_statement(&mut catalog, parse_statement("INSERT INTO t VALUES (2, '2020')").unwrap()).unwrap();
}
