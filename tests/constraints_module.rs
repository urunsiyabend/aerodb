use aerodb::constraints::{Constraint, not_null::NotNullConstraint, default::DefaultConstraint, foreign_key::ForeignKeyConstraint};
use aerodb::storage::row::{RowData, ColumnValue, ColumnType};
use aerodb::catalog::{Catalog, TableInfo};
use aerodb::sql::ast::ForeignKey;
use aerodb::storage::pager::Pager;
use std::fs;

fn setup_catalog(filename: &str) -> Catalog {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

#[test]
fn not_null_constraint_fails_on_null() {
    let table = TableInfo {
        name: "t".into(),
        root_page: 0,
        columns: vec![("id".into(), ColumnType::Integer)],
        not_null: vec![true],
        default_values: vec![None],
        auto_increment: vec![false],
        fks: vec![],
        primary_key: None,
    };
    let mut row = RowData(vec![ColumnValue::Null]);
    let mut catalog = setup_catalog("nn_fail.db");
    let constraint = NotNullConstraint;
    let res = constraint.validate_insert(&mut catalog, &table, &mut row);
    assert!(res.is_err());
}

#[test]
fn default_constraint_evaluates_literal() {
    use aerodb::sql::ast::Expr;
    let val = DefaultConstraint::evaluate(&Expr::Literal("42".into())).unwrap();
    assert_eq!(val, "42");
}

#[test]
fn foreign_key_constraint_detects_missing_parent() {
    let mut catalog = setup_catalog("fk_fail.db");
    // parent table
    let parent = TableInfo {
        name: "p".into(),
        root_page: 1,
        columns: vec![("id".into(), ColumnType::Integer)],
        not_null: vec![false],
        default_values: vec![None],
        auto_increment: vec![false],
        fks: vec![],
        primary_key: None,
    };
    catalog
        .create_table_with_fks(
            &parent.name,
            vec![("id".into(), ColumnType::Integer, false, None, false)],
            vec![],
            None,
        )
        .unwrap();
    // child table info
    let child = TableInfo {
        name: "c".into(),
        root_page: 2,
        columns: vec![("pid".into(), ColumnType::Integer)],
        not_null: vec![false],
        default_values: vec![None],
        auto_increment: vec![false],
        fks: vec![ForeignKey { columns: vec!["pid".into()], parent_table: "p".into(), parent_columns: vec!["id".into()], on_delete: None, on_update: None }],
        primary_key: None,
    };
    catalog
        .create_table_with_fks(
            &child.name,
            vec![("pid".into(), ColumnType::Integer, false, None, false)],
            child.fks.clone(),
            None,
        )
        .unwrap();
    let mut row = RowData(vec![ColumnValue::Integer(1)]);
    let constraint = ForeignKeyConstraint { fks: &child.fks };
    let res = constraint.validate_insert(&mut catalog, &child, &mut row);
    assert!(res.is_err());
}
