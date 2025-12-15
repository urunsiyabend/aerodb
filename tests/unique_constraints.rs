use aerodb::{catalog::Catalog, storage::pager::Pager, execution::runtime::handle_statement, sql::parser::parse_statement, sql::ast::Statement};
use std::fs;

fn setup_catalog(filename: &str) -> Catalog {
    let _ = fs::remove_file(filename);
    let _ = fs::remove_file(format!("{}.wal", filename));
    Catalog::open(Pager::new(filename).unwrap()).unwrap()
}

#[test]
fn parse_unique_column_constraint() {
    let stmt = parse_statement("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, username TEXT NOT NULL UNIQUE)").unwrap();
    if let Statement::CreateTable { columns, unique_constraints, .. } = stmt {
        assert_eq!(columns.len(), 3);
        assert!(!columns[0].unique); // id is primary key, not unique
        assert!(columns[1].unique);  // email is unique
        assert!(columns[2].unique);  // username is unique
        assert_eq!(unique_constraints.len(), 2); // email and username
    } else {
        panic!("expected create table");
    }
}

#[test]
fn parse_unique_table_constraint() {
    let stmt = parse_statement("CREATE TABLE orders (id INTEGER, customer_id INTEGER, order_number TEXT, UNIQUE(order_number))").unwrap();
    if let Statement::CreateTable { unique_constraints, .. } = stmt {
        assert_eq!(unique_constraints.len(), 1);
        assert_eq!(unique_constraints[0], vec!["order_number"]);
    } else {
        panic!("expected create table");
    }
}

#[test]
fn parse_multi_column_unique_constraint() {
    let stmt = parse_statement("CREATE TABLE enrollment (student_id INTEGER, course_id INTEGER, semester TEXT, UNIQUE(student_id, course_id, semester))").unwrap();
    if let Statement::CreateTable { unique_constraints, .. } = stmt {
        assert_eq!(unique_constraints.len(), 1);
        assert_eq!(unique_constraints[0], vec!["student_id", "course_id", "semester"]);
    } else {
        panic!("expected create table");
    }
}

#[test]
fn enforce_unique_constraint_insert() {
    let filename = "test_unique_insert.db";
    let mut catalog = setup_catalog(filename);

    let create_stmt = parse_statement("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE)").unwrap();
    handle_statement(&mut catalog, create_stmt).unwrap();

    // First insert should succeed
    handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (1, 'user1@example.com')").unwrap()).unwrap();

    // Second insert with different email should succeed
    handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (2, 'user2@example.com')").unwrap()).unwrap();

    // Insert with duplicate email should fail
    let res = handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (3, 'user1@example.com')").unwrap());
    assert!(matches!(res, Err(aerodb::error::DbError::UniqueViolation(_))));
}

#[test]
fn enforce_unique_constraint_update() {
    let filename = "test_unique_update.db";
    let mut catalog = setup_catalog(filename);

    let create_stmt = parse_statement("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE)").unwrap();
    handle_statement(&mut catalog, create_stmt).unwrap();

    // Insert two rows
    handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (1, 'user1@example.com')").unwrap()).unwrap();
    handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (2, 'user2@example.com')").unwrap()).unwrap();

    // Update to a unique email should succeed
    handle_statement(&mut catalog, parse_statement("UPDATE users SET email = 'user1_updated@example.com' WHERE id = 1").unwrap()).unwrap();

    // Update to a duplicate email should fail
    let res = handle_statement(&mut catalog, parse_statement("UPDATE users SET email = 'user2@example.com' WHERE id = 1").unwrap());
    assert!(matches!(res, Err(aerodb::error::DbError::UniqueViolation(_))));
}

#[test]
fn unique_allows_null_values() {
    let filename = "test_unique_null.db";
    let mut catalog = setup_catalog(filename);

    let create_stmt = parse_statement("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE)").unwrap();
    handle_statement(&mut catalog, create_stmt).unwrap();

    // Multiple NULL values should be allowed for UNIQUE columns
    handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (1, NULL)").unwrap()).unwrap();
    handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (2, NULL)").unwrap()).unwrap();
    handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (3, NULL)").unwrap()).unwrap();

    // But non-NULL duplicates should still fail
    handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (4, 'test@example.com')").unwrap()).unwrap();
    let res = handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (5, 'test@example.com')").unwrap());
    assert!(matches!(res, Err(aerodb::error::DbError::UniqueViolation(_))));
}

#[test]
fn enforce_multi_column_unique_constraint() {
    let filename = "test_multi_unique.db";
    let mut catalog = setup_catalog(filename);

    let create_stmt = parse_statement("CREATE TABLE enrollment (id INTEGER PRIMARY KEY, student_id INTEGER, course_id INTEGER, UNIQUE(student_id, course_id))").unwrap();
    handle_statement(&mut catalog, create_stmt).unwrap();

    // First enrollment
    handle_statement(&mut catalog, parse_statement("INSERT INTO enrollment VALUES (1, 100, 200)").unwrap()).unwrap();

    // Same student, different course - should succeed
    handle_statement(&mut catalog, parse_statement("INSERT INTO enrollment VALUES (2, 100, 201)").unwrap()).unwrap();

    // Different student, same course - should succeed
    handle_statement(&mut catalog, parse_statement("INSERT INTO enrollment VALUES (3, 101, 200)").unwrap()).unwrap();

    // Same student AND same course - should fail
    let res = handle_statement(&mut catalog, parse_statement("INSERT INTO enrollment VALUES (4, 100, 200)").unwrap());
    assert!(matches!(res, Err(aerodb::error::DbError::UniqueViolation(_))));
}

#[test]
fn multi_column_unique_with_null() {
    let filename = "test_multi_unique_null.db";
    let mut catalog = setup_catalog(filename);

    let create_stmt = parse_statement("CREATE TABLE enrollment (id INTEGER PRIMARY KEY, student_id INTEGER, course_id INTEGER, UNIQUE(student_id, course_id))").unwrap();
    handle_statement(&mut catalog, create_stmt).unwrap();

    // Insert with NULL in one of the unique columns
    handle_statement(&mut catalog, parse_statement("INSERT INTO enrollment VALUES (1, 100, NULL)").unwrap()).unwrap();

    // Another row with same student_id but NULL course_id should be allowed
    handle_statement(&mut catalog, parse_statement("INSERT INTO enrollment VALUES (2, 100, NULL)").unwrap()).unwrap();

    // NULL in first column
    handle_statement(&mut catalog, parse_statement("INSERT INTO enrollment VALUES (3, NULL, 200)").unwrap()).unwrap();
    handle_statement(&mut catalog, parse_statement("INSERT INTO enrollment VALUES (4, NULL, 200)").unwrap()).unwrap();
}

#[test]
fn multiple_unique_constraints() {
    let filename = "test_multiple_unique.db";
    let mut catalog = setup_catalog(filename);

    let create_stmt = parse_statement("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE, username TEXT UNIQUE)").unwrap();
    handle_statement(&mut catalog, create_stmt).unwrap();

    // First insert
    handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (1, 'user1@example.com', 'user1')").unwrap()).unwrap();

    // Duplicate email should fail
    let res = handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (2, 'user1@example.com', 'user2')").unwrap());
    assert!(matches!(res, Err(aerodb::error::DbError::UniqueViolation(_))));

    // Duplicate username should fail
    let res = handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (3, 'user3@example.com', 'user1')").unwrap());
    assert!(matches!(res, Err(aerodb::error::DbError::UniqueViolation(_))));

    // Both unique should succeed
    handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (4, 'user4@example.com', 'user4')").unwrap()).unwrap();
}

#[test]
fn unique_with_column_list_syntax() {
    let filename = "test_unique_column_list.db";
    let mut catalog = setup_catalog(filename);

    let create_stmt = parse_statement("CREATE TABLE products (id INTEGER PRIMARY KEY, sku TEXT, UNIQUE(sku))").unwrap();
    handle_statement(&mut catalog, create_stmt).unwrap();

    handle_statement(&mut catalog, parse_statement("INSERT INTO products VALUES (1, 'SKU-001')").unwrap()).unwrap();

    let res = handle_statement(&mut catalog, parse_statement("INSERT INTO products VALUES (2, 'SKU-001')").unwrap());
    assert!(matches!(res, Err(aerodb::error::DbError::UniqueViolation(_))));
}

#[test]
fn unique_and_not_null_together() {
    let filename = "test_unique_not_null.db";
    let mut catalog = setup_catalog(filename);

    let create_stmt = parse_statement("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE)").unwrap();
    handle_statement(&mut catalog, create_stmt).unwrap();

    // NULL should fail due to NOT NULL
    let res = handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (1, NULL)").unwrap());
    assert!(matches!(res, Err(aerodb::error::DbError::NullViolation(_))));

    // Valid insert
    handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (2, 'test@example.com')").unwrap()).unwrap();

    // Duplicate should fail due to UNIQUE
    let res = handle_statement(&mut catalog, parse_statement("INSERT INTO users VALUES (3, 'test@example.com')").unwrap());
    assert!(matches!(res, Err(aerodb::error::DbError::UniqueViolation(_))));
}

#[test]
fn unique_constraint_with_text_type() {
    let filename = "test_unique_text.db";
    let mut catalog = setup_catalog(filename);

    let create_stmt = parse_statement("CREATE TABLE documents (id INTEGER PRIMARY KEY, title TEXT UNIQUE)").unwrap();
    handle_statement(&mut catalog, create_stmt).unwrap();

    handle_statement(&mut catalog, parse_statement("INSERT INTO documents VALUES (1, 'First Document')").unwrap()).unwrap();
    handle_statement(&mut catalog, parse_statement("INSERT INTO documents VALUES (2, 'Second Document')").unwrap()).unwrap();

    let res = handle_statement(&mut catalog, parse_statement("INSERT INTO documents VALUES (3, 'First Document')").unwrap());
    assert!(matches!(res, Err(aerodb::error::DbError::UniqueViolation(_))));
}
