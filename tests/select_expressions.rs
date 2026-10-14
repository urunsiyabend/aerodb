use aerodb::{catalog::Catalog, storage::pager::Pager, sql::{parser::parse_statement, ast::{BinaryOp, Statement, Expr, SelectItem, SetExpr, ColumnDef}, value::Value}, execution::runtime::{execute_select_statement, handle_statement}, storage::row::{ColumnType}};
use std::fs;

fn setup_catalog(filename: &str) -> Catalog {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

#[test]
fn parse_select_add_expr() {
    let stmt = parse_statement("SELECT val + 5 FROM numbers").unwrap();
    let Statement::Query(q) = stmt else { panic!("expected select") };
    let SetExpr::Select(sel) = q.body else { panic!("expected select") };
    match &sel.projection[0] {
        SelectItem::Expr { expr: Expr::Binary { op: BinaryOp::Add, left, right }, .. } => {
            assert_eq!(**left, Expr::Column { table: None, name: "val".into() });
            assert_eq!(**right, Expr::Literal(Value::Int(5)));
        }
        _ => panic!("expected Add expression"),
    }
}

#[test]
fn execute_select_add_expr() {
    let filename = "test_select_add_expr.db";
    let mut catalog = setup_catalog(filename);
    handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "numbers".into(),
        columns: vec![
            ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false },
            ColumnDef { name: "val".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false },
        ],
        fks: Vec::new(), primary_key: None, if_not_exists: false, constraints: Vec::new(),
    }).unwrap();
    handle_statement(&mut catalog, parse_statement("INSERT INTO numbers VALUES (1, 10)").unwrap()).unwrap();
    let stmt = parse_statement("SELECT val + 5 FROM numbers WHERE id = 1").unwrap();
    if let Statement::Query(_) = stmt {
        let mut out = Vec::new();
        let _header = execute_select_statement(&mut catalog, &stmt, &mut out, None).unwrap();
        assert_eq!(out, vec![vec![String::from("15")]]);
    } else { panic!("expected select") }
}
#[test]
fn execute_select_mul_double_expr() {
    let filename = "test_select_mul_double_expr.db";
    let mut catalog = setup_catalog(filename);
    handle_statement(&mut catalog, Statement::CreateTable {
        table_name: "orders".into(),
        columns: vec![
            ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, default_value: None, auto_increment: false, primary_key: false },
            ColumnDef { name: "total".into(), col_type: ColumnType::Double { precision: 8, scale: 2, unsigned: true }, not_null: false, default_value: None, auto_increment: false, primary_key: false },
        ],
        fks: Vec::new(), primary_key: None, if_not_exists: false, constraints: Vec::new(),
    }).unwrap();
    handle_statement(&mut catalog, parse_statement("INSERT INTO orders VALUES (1, 20.0)").unwrap()).unwrap();
    let stmt = parse_statement("SELECT id, total * 1.05 FROM orders WHERE id = 1").unwrap();
    if let Statement::Query(_) = stmt {
        let mut out = Vec::new();
        let _header = execute_select_statement(&mut catalog, &stmt, &mut out, None).unwrap();
        assert_eq!(out, vec![vec![String::from("1"), String::from("21")]]);
    } else { panic!("expected select") }
}
