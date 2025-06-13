use aerodb::{catalog::Catalog, storage::pager::Pager, sql::{parser::parse_statement, ast::Statement}, execution::runtime::{execute_select_statement, format_header}, storage::row::ColumnType};
use std::fs;

fn setup_catalog(filename: &str) -> Catalog {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

#[test]
fn select_literal() {
    let filename = "test_select_literal.db";
    let mut catalog = setup_catalog(filename);
    let stmt = parse_statement("SELECT 1").unwrap();
    if let Statement::Select { .. } = stmt {
        let mut out = Vec::new();
        let header = execute_select_statement(&mut catalog, &stmt, &mut out, None).unwrap();
        assert_eq!(out, vec![vec!["1".to_string()]]);
        assert_eq!(format_header(&header), "1 INTEGER");
    } else { panic!("expected select") }
}

#[test]
fn select_expression_and_text() {
    let filename = "test_select_expr_text.db";
    let mut catalog = setup_catalog(filename);
    let stmt = parse_statement("SELECT 2 + 3, 'hi'").unwrap();
    if let Statement::Select { .. } = stmt {
        let mut out = Vec::new();
        let _ = execute_select_statement(&mut catalog, &stmt, &mut out, None).unwrap();
        assert_eq!(out, vec![vec!["5".to_string(), "hi".to_string()]]);
    } else { panic!("expected select") }
}

#[test]
fn select_current_timestamp() {
    let filename = "test_select_now.db";
    let mut catalog = setup_catalog(filename);
    let stmt = parse_statement("SELECT CURRENT_TIMESTAMP").unwrap();
    if let Statement::Select { .. } = stmt {
        let mut out = Vec::new();
        let _header = execute_select_statement(&mut catalog, &stmt, &mut out, None).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].len(), 1);
    } else { panic!("expected select") }
}

#[test]
fn select_with_from() {
    let filename = "test_select_with_from.db";
    let mut catalog = setup_catalog(filename);
    aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "employees".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false },
        ],
        fks: Vec::new(), primary_key: None, if_not_exists: false,
    }).unwrap();
    aerodb::execution::handle_statement(&mut catalog, parse_statement("INSERT INTO employees VALUES (1)").unwrap()).unwrap();
    let stmt = parse_statement("SELECT 1 FROM employees").unwrap();
    if let Statement::Select { .. } = stmt {
        let mut out = Vec::new();
        let _ = execute_select_statement(&mut catalog, &stmt, &mut out, None).unwrap();
        assert_eq!(out, vec![vec!["1".to_string()]]);
    } else { panic!("expected select") }
}

#[test]
fn select_column_without_from() {
    let res = parse_statement("SELECT id");
    assert!(res.is_err());
}

#[test]
fn repl_output_includes_header() {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let tmp = tempfile::tempdir().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_aerodb"))
        .current_dir(&tmp)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    {
        let input = b"SELECT 1;\n.exit\n";
        child.stdin.as_mut().unwrap().write_all(input).unwrap();
    }
    let out = child.wait_with_output().unwrap();
    let output = String::from_utf8_lossy(&out.stdout);
    assert!(output.contains("1 INTEGER"));
    assert!(output.lines().any(|l| l.trim() == "1"));
}
