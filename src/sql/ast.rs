// src/sql/ast.rs

#[derive(Debug)]
pub enum Statement {
    CreateTable {
        table_name: String,
        columns: Vec<String>,
    },
    Insert {
        table_name: String,
        values: Vec<String>, // all literal values as strings
    },
    Select {
        table_name: String,
    },
    Exit,
}
