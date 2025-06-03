use crate::sql::ast::Statement;

pub fn parse_statement(input: &str) -> Result<Statement, String> {
    let tokens: Vec<&str> = input.split_whitespace().collect();
    if tokens.is_empty() {
        return Err("Empty input".to_string());
    }

    match tokens[0].to_uppercase().as_str() {
        "INSERT" => {
            if tokens.len() < 3 {
                return Err("Usage: INSERT <key> <payload>".to_string());
            }
            let key: i32 = tokens[1]
                .parse()
                .map_err(|_| "INSERT: key must be an integer".to_string())?;
            let payload = tokens[2..].join(" ");
            Ok(Statement::Insert { key, payload })
        }
        "SELECT" => {
            if tokens.len() != 2 {
                return Err("Usage: SELECT <key>".to_string());
            }
            let key: i32 = tokens[1]
                .parse()
                .map_err(|_| "SELECT: key must be an integer".to_string())?;
            Ok(Statement::Select { key })
        }
        "EXIT" | ".EXIT" | ".exit" => Ok(Statement::Exit),
        _ => Err(format!("Unrecognized command: {}", tokens[0])),
    }
}
