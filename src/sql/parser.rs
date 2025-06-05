use crate::sql::ast::{Expr, Statement};

/// Parse a simple boolean expression consisting of identifiers, =, !=, AND, OR.
/// Returns the expression and the number of tokens consumed.
fn parse_expression(tokens: &[&str]) -> Result<(Expr, usize), String> {
    if tokens.len() < 3 {
        return Err("Incomplete expression".into());
    }
    let left = tokens[0].to_string();
    let op = tokens[1];
    let right = tokens[2].trim_end_matches(';').to_string();
    let mut expr = match op {
        "=" => Expr::Equals { left, right },
        "!=" => Expr::NotEquals { left, right },
        _ => return Err(format!("Unknown operator '{}', expected = or !=", op)),
    };
    let mut consumed = 3;
    while tokens.len() > consumed {
        let logic = tokens[consumed].to_uppercase();
        if logic != "AND" && logic != "OR" {
            break;
        }
        consumed += 1;
        if tokens.len() < consumed + 3 {
            return Err("Incomplete expression after AND/OR".into());
        }
        let l = tokens[consumed].to_string();
        let op = tokens[consumed + 1];
        let r = tokens[consumed + 2].trim_end_matches(';').to_string();
        let next = match op {
            "=" => Expr::Equals { left: l, right: r },
            "!=" => Expr::NotEquals { left: l, right: r },
            _ => return Err(format!("Unknown operator '{}', expected = or !=", op)),
        };
        expr = if logic == "AND" {
            Expr::And(Box::new(expr), Box::new(next))
        } else {
            Expr::Or(Box::new(expr), Box::new(next))
        };
        consumed += 3;
    }
    Ok((expr, consumed))
}

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
            if tokens.len() < 4 || tokens[1] != "*" || !tokens[2].eq_ignore_ascii_case("FROM") {
                return Err("Usage: SELECT * FROM <table> [WHERE <expr>]".to_string());
            }
            let mut table = tokens[3].trim_end_matches(';').to_string();
            let mut selection = None;
            if tokens.len() > 4 {
                if !tokens[4].eq_ignore_ascii_case("WHERE") {
                    return Err("Usage: SELECT * FROM <table> WHERE <expr>".to_string());
                }
                let (expr, _) = parse_expression(&tokens[5..])?;
                selection = Some(expr);
                // remove trailing semicolon from table if not already
                table = table.trim_end_matches(';').to_string();
            }
            Ok(Statement::Select { table_name: table, selection })
        }
        "EXIT" | ".EXIT" | ".exit" => Ok(Statement::Exit),
        _ => Err(format!("Unrecognized command: {}", tokens[0])),
    }
}
