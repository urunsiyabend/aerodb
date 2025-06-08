use aerodb::{catalog::Catalog, storage::pager::Pager, sql::parser::parse_statement, sql::ast::{Statement}};
use aerodb::storage::row::{ColumnType};
use std::fs;

#[test]
fn foreign_key_basic() {
    let filename = "test_fk.db";
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();

    // create users table
    let create_users = Statement::CreateTable {
        table_name: "users".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, primary_key: false, default_value: None, auto_increment: false}
        ],
        fks: Vec::new(),
        if_not_exists: false,
    };
    aerodb::execution::handle_statement(&mut catalog, create_users).unwrap();

    // create orders table with FK
    let stmt = parse_statement(
        "CREATE TABLE orders (id INTEGER, user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users (id))",
    )
    .unwrap();
    if let Statement::CreateTable { table_name, columns, fks, if_not_exists } = stmt {
        assert_eq!(fks.len(), 1);
        aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable { table_name, columns, fks, if_not_exists }).unwrap();
    } else { panic!("expected create table") }

    // insert a user id=1
    let insert_user = parse_statement("INSERT INTO users VALUES (1)").unwrap();
    aerodb::execution::handle_statement(&mut catalog, insert_user).unwrap();

    // insert order with user_id=2 should fail
    let bad_order = parse_statement("INSERT INTO orders VALUES (1, 2)").unwrap();
    let res = aerodb::execution::handle_statement(&mut catalog, bad_order);
    assert!(res.is_err());

    // insert order with user_id=1 should succeed
    let good_order = parse_statement("INSERT INTO orders VALUES (2, 1)").unwrap();
    aerodb::execution::handle_statement(&mut catalog, good_order).unwrap();

    // attempt delete user id=1 without cascade should fail
    let del = Statement::Delete { table_name: "users".into(), selection: Some(aerodb::sql::ast::Expr::Equals { left: "id".into(), right: "1".into() }) };
    let res = aerodb::execution::handle_statement(&mut catalog, del);
    assert!(res.is_err());
}

#[test]
fn foreign_key_on_delete_cascade() {
    let filename = "test_fk_cascade.db";
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();

    // users
    let create_users = Statement::CreateTable {
        table_name: "users".into(),
        columns: vec![
            aerodb::sql::ast::ColumnDef { name: "id".into(), col_type: ColumnType::Integer, not_null: false, primary_key: false, default_value: None, auto_increment: false}
        ],
        fks: Vec::new(),
        if_not_exists: false,
    };
    aerodb::execution::handle_statement(&mut catalog, create_users).unwrap();

    // orders with cascade
    let stmt = parse_statement(
        "CREATE TABLE orders (id INTEGER, user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE)"
    ).unwrap();
    if let Statement::CreateTable { table_name, columns, fks, if_not_exists } = stmt {
        aerodb::execution::handle_statement(&mut catalog, Statement::CreateTable { table_name, columns, fks, if_not_exists }).unwrap();
    } else { panic!("expected create") }

    let insert_user = parse_statement("INSERT INTO users VALUES (1)").unwrap();
    aerodb::execution::handle_statement(&mut catalog, insert_user).unwrap();
    let insert_order = parse_statement("INSERT INTO orders VALUES (1, 1)").unwrap();
    aerodb::execution::handle_statement(&mut catalog, insert_order).unwrap();

    // delete user should cascade
    let del = Statement::Delete { table_name: "users".into(), selection: Some(aerodb::sql::ast::Expr::Equals { left: "id".into(), right: "1".into() }) };
    aerodb::execution::handle_statement(&mut catalog, del).unwrap();

    // verify orders table empty
    let mut rows = Vec::new();
    aerodb::execution::execute_select_with_indexes(&mut catalog, "orders", None, &mut rows).unwrap();
    assert!(rows.is_empty());
}

