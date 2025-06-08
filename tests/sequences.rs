use aerodb::sql::parser::parse_statement;
use aerodb::sql::ast::{Statement, CreateSequence};

#[test]
fn parse_create_sequence_defaults() {
    let stmt = parse_statement("CREATE SEQUENCE seq1").unwrap();
    if let Statement::CreateSequence(CreateSequence { name, start, increment }) = stmt {
        assert_eq!(name, "seq1");
        assert_eq!(start, 1);
        assert_eq!(increment, 1);
    } else { panic!("expected create sequence"); }
}

#[test]
fn parse_create_sequence_with_options() {
    let stmt = parse_statement("CREATE SEQUENCE seq2 START WITH 5 INCREMENT BY 2").unwrap();
    if let Statement::CreateSequence(CreateSequence { name, start, increment }) = stmt {
        assert_eq!(name, "seq2");
        assert_eq!(start, 5);
        assert_eq!(increment, 2);
    } else { panic!("expected create sequence"); }
}

use aerodb::{catalog::Catalog, storage::pager::Pager, execution::handle_statement};
use std::fs;

#[test]
fn create_sequence_and_next_values() {
    let filename = "test_sequence_basic.db";
    let _ = fs::remove_file(filename);
    let mut catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();

    let stmt = parse_statement("CREATE SEQUENCE my_seq START WITH 100 INCREMENT BY 5").unwrap();
    if let Statement::CreateSequence(seq) = stmt {
        handle_statement(&mut catalog, Statement::CreateSequence(seq)).unwrap();
    }

    let v1 = catalog.next_sequence_value("my_seq").unwrap();
    let v2 = catalog.next_sequence_value("my_seq").unwrap();
    assert_eq!(v1, 100);
    assert_eq!(v2, 105);
}
