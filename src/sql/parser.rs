use crate::sql::ast::Statement;

pub fn parse_statement(input: &str) -> Result<Statement, String> {
    let tokens: Vec<&str> = input.split_whitespace().collect();
    if tokens.is_empty() {
        return Err("Empty input".to_string());
    }
    match tokens[0].to_uppercase().as_str() {
        "CREATE" => {
            // Expect: CREATE TABLE table_name (col1, col2, col3)
            if tokens.len() < 4 || !tokens[1].eq_ignore_ascii_case("TABLE") {
                return Err("Usage: CREATE TABLE <name> (col1, col2, ...)".to_string());
            }
            let name = tokens[2].to_string();
            // The rest is "(col1,col2,...)". Rejoin and strip parens.
            let rest = input[input.find('(').ok_or("Missing '('")?..].trim();
            if !rest.starts_with('(') || !rest.ends_with(')') {
                return Err("Columns must be in parentheses".to_string());
            }
            let inner = &rest[1..rest.len() - 1];
            let cols: Vec<String> = inner
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if cols.is_empty() {
                return Err("At least one column required".to_string());
            }
            Ok(Statement::CreateTable {
                table_name: name,
                columns: cols,
            })
        }
        "INSERT" => {
            // Expect: INSERT INTO table_name VALUES (v1, v2, v3)
            if tokens.len() < 4 || !tokens[1].eq_ignore_ascii_case("INTO") {
                return Err("Usage: INSERT INTO <table> VALUES (v1, v2, ...)".to_string());
            }
            let table = tokens[2].to_string();
            let rest = input[input.find('(').ok_or("Missing '('")?..].trim();
            if !rest.starts_with('(') || !rest.ends_with(')') {
                return Err("Values must be in parentheses".to_string());
            }
            let inner = &rest[1..rest.len() - 1];
            // For simplicity, treat each comma‐separated chunk as a literal string (no quotes).
            let vals: Vec<String> = inner
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if vals.is_empty() {
                return Err("At least one value required".to_string());
            }
            Ok(Statement::Insert {
                table_name: table,
                values: vals,
            })
        }
        "SELECT" => {
            // Only support: SELECT * FROM table_name
            if tokens.len() != 4 || tokens[1] != "*" || !tokens[2].eq_ignore_ascii_case("FROM") {
                return Err("Usage: SELECT * FROM <table>".to_string());
            }
            let table = tokens[3].trim_end_matches(';').to_string();
            Ok(Statement::Select { table_name: table })
        }
        "EXIT" | ".EXIT" | ".exit" => Ok(Statement::Exit),
        _ => Err(format!("Unrecognized command: {}", tokens[0])),
    }
}
