use crate::sql::ast::{Expr, Statement, OrderBy, ForeignKey, Action};
use crate::storage::row::ColumnType;

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
        "BEGIN" => {
            // Support BEGIN [TRANSACTION] [name]
            let mut idx = 1;
            if tokens.get(idx).map(|s| s.eq_ignore_ascii_case("TRANSACTION")) == Some(true) {
                idx += 1;
            }
            let name = tokens.get(idx).map(|s| s.trim_end_matches(';').to_string());
            Ok(Statement::BeginTransaction { name })
        }
        "COMMIT" => Ok(Statement::Commit),
        "ROLLBACK" => Ok(Statement::Rollback),
        "CREATE" => {
            if tokens.len() >= 3 && tokens[1].eq_ignore_ascii_case("INDEX") {
                if tokens.len() < 6 || !tokens[3].eq_ignore_ascii_case("ON") {
                    return Err("Usage: CREATE INDEX <name> ON <table>(<column>)".to_string());
                }
                let index_name = tokens[2].to_string();
                let table_name = tokens[4].trim_end_matches(';').to_string();
                let rest = input[input.find('(').ok_or("Missing '('")?..].trim();
                if !rest.starts_with('(') || !rest.ends_with(')') {
                    return Err("Column must be in parentheses".to_string());
                }
                let col = rest[1..rest.len() - 1].trim().to_string();
                return Ok(Statement::CreateIndex { index_name, table_name, column_name: col });
            }
            // Expect: CREATE TABLE [IF NOT EXISTS] table_name (col1 TYPE, ...)
            if tokens.len() < 4 || !tokens[1].eq_ignore_ascii_case("TABLE") {
                return Err("Usage: CREATE TABLE <name> (col1, col2, ...)".to_string());
            }
            let mut idx = 2;
            let mut if_not_exists = false;
            if tokens.get(idx).map(|s| s.to_uppercase()) == Some("IF".to_string())
                && tokens.get(idx + 1).map(|s| s.to_uppercase()) == Some("NOT".to_string())
                && tokens.get(idx + 2).map(|s| s.to_uppercase()) == Some("EXISTS".to_string())
            {
                if_not_exists = true;
                idx += 3;
            }
            if idx >= tokens.len() {
                return Err("Usage: CREATE TABLE <name> (col1, col2, ...)".to_string());
            }
            let name = tokens[idx].to_string();
            // The rest is "(col1,col2,...)". Rejoin and strip parens.
            let rest = input[input.find('(').ok_or("Missing '('")?..].trim();
            if !rest.starts_with('(') || !rest.ends_with(')') {
                return Err("Columns must be in parentheses".to_string());
            }
            let inner = &rest[1..rest.len() - 1];

            fn split_top_level(s: &str) -> Vec<String> {
                let mut parts = Vec::new();
                let mut current = String::new();
                let mut depth = 0;
                for ch in s.chars() {
                    match ch {
                        '(' => { depth += 1; current.push(ch); }
                        ')' => { depth -= 1; current.push(ch); }
                        ',' if depth == 0 => {
                            parts.push(current.trim().to_string());
                            current.clear();
                        }
                        _ => current.push(ch),
                    }
                }
                if !current.trim().is_empty() {
                    parts.push(current.trim().to_string());
                }
                parts
            }

            let mut columns = Vec::new();
            let mut fks = Vec::new();
            for chunk in split_top_level(inner) {
                if chunk.to_uppercase().starts_with("FOREIGN KEY") {
                    let mut rest = chunk[11..].trim();
                    if !rest.starts_with('(') {
                        return Err("Expected ( after FOREIGN KEY".into());
                    }
                    let end = rest.find(')').ok_or("Missing ) in FOREIGN KEY")?;
                    let cols_part = &rest[1..end];
                    let cols: Vec<String> = cols_part.split(',').map(|s| s.trim().to_string()).collect();
                    rest = rest[end + 1..].trim();
                    if !rest.to_uppercase().starts_with("REFERENCES") {
                        return Err("Expected REFERENCES".into());
                    }
                    rest = rest[10..].trim();
                    let mut parts = rest.splitn(2, '(');
                    let parent_table = parts.next().ok_or("Missing parent table")?.trim().to_string();
                    let remainder = parts.next().ok_or("Missing ( after parent table")?;
                    let end2 = remainder.find(')').ok_or("Missing ) after parent columns")?;
                    let pcols_part = &remainder[..end2];
                    let parent_columns: Vec<String> = pcols_part.split(',').map(|s| s.trim().to_string()).collect();
                    let mut rest2 = remainder[end2 + 1..].trim();
                    let mut on_delete = None;
                    let mut on_update = None;
                    while !rest2.is_empty() {
                        if rest2.to_uppercase().starts_with("ON DELETE") {
                            rest2 = rest2[9..].trim();
                            if rest2.to_uppercase().starts_with("CASCADE") {
                                on_delete = Some(Action::Cascade);
                                rest2 = rest2[7..].trim();
                            } else {
                                on_delete = Some(Action::NoAction);
                                rest2 = rest2.trim_start_matches("NO ACTION").trim();
                            }
                        } else if rest2.to_uppercase().starts_with("ON UPDATE") {
                            rest2 = rest2[9..].trim();
                            if rest2.to_uppercase().starts_with("CASCADE") {
                                on_update = Some(Action::Cascade);
                                rest2 = rest2[7..].trim();
                            } else {
                                on_update = Some(Action::NoAction);
                                rest2 = rest2.trim_start_matches("NO ACTION").trim();
                            }
                        } else {
                            break;
                        }
                    }
                    fks.push(ForeignKey { columns: cols, parent_table, parent_columns, on_delete, on_update });
                } else {
                    let parts: Vec<&str> = chunk.split_whitespace().collect();
                    if parts.len() != 2 {
                        return Err("Column definitions must be <name> <type>".to_string());
                    }
                    let ctype = ColumnType::from_str(parts[1])
                        .ok_or_else(|| format!("Unknown type {}", parts[1]))?;
                    columns.push((parts[0].to_string(), ctype));
                }
            }

            if columns.is_empty() {
                return Err("At least one column required".to_string());
            }

            Ok(Statement::CreateTable { table_name: name, columns, fks, if_not_exists })
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
            let vals: Vec<String> = inner
                .split(',')
                .map(|s| {
                    let v = s.trim();
                    if (v.starts_with('"') && v.ends_with('"')) || (v.starts_with('\'') && v.ends_with('\'')) {
                        v[1..v.len() - 1].to_string()
                    } else {
                        v.to_string()
                    }
                })
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
            if tokens.len() < 4 {
                return Err("Incomplete SELECT".into());
            }

            let mut idx = 1;
            let mut columns = Vec::new();
            while idx < tokens.len() {
                if tokens[idx].eq_ignore_ascii_case("FROM") {
                    break;
                }
                let token = tokens[idx].trim_end_matches(',');
                let upper = token.to_uppercase();
                if token == "*" {
                    columns.push(crate::sql::ast::SelectExpr::All);
                } else if upper.starts_with("COUNT(") {
                    let inner = token[6..token.len() - 1].trim();
                    let col = if inner == "*" { None } else { Some(inner.to_string()) };
                    columns.push(crate::sql::ast::SelectExpr::Aggregate { func: crate::sql::ast::AggFunc::Count, column: col });
                } else if upper.starts_with("SUM(") {
                    let inner = token[4..token.len() - 1].trim().to_string();
                    columns.push(crate::sql::ast::SelectExpr::Aggregate { func: crate::sql::ast::AggFunc::Sum, column: Some(inner) });
                } else if upper.starts_with("AVG(") {
                    let inner = token[4..token.len() - 1].trim().to_string();
                    columns.push(crate::sql::ast::SelectExpr::Aggregate { func: crate::sql::ast::AggFunc::Avg, column: Some(inner) });
                } else if upper.starts_with("MIN(") {
                    let inner = token[4..token.len() - 1].trim().to_string();
                    columns.push(crate::sql::ast::SelectExpr::Aggregate { func: crate::sql::ast::AggFunc::Min, column: Some(inner) });
                } else if upper.starts_with("MAX(") {
                    let inner = token[4..token.len() - 1].trim().to_string();
                    columns.push(crate::sql::ast::SelectExpr::Aggregate { func: crate::sql::ast::AggFunc::Max, column: Some(inner) });
                } else {
                    columns.push(crate::sql::ast::SelectExpr::Column(token.to_string()));
                }
                idx += 1;
            }
            if idx >= tokens.len() || !tokens[idx].eq_ignore_ascii_case("FROM") {
                return Err("Expected FROM".into());
            }
            idx += 1;
            if idx >= tokens.len() {
                return Err("Missing table after FROM".into());
            }
            let mut from_table = tokens[idx].trim_end_matches(';').to_string();
            let mut from_subquery = None;
            if from_table.starts_with('(') {
                let mut depth: i32 = from_table.chars().filter(|c| *c == '(').count() as i32 - from_table.chars().filter(|c| *c == ')').count() as i32;
                let mut sub_parts = vec![from_table.clone()];
                while depth > 0 {
                    idx += 1;
                    if idx >= tokens.len() { return Err("Unclosed subquery".into()); }
                    let t = tokens[idx];
                    depth += t.chars().filter(|c| *c == '(').count() as i32 - t.chars().filter(|c| *c == ')').count() as i32;
                    sub_parts.push(t.to_string());
                }
                let joined = sub_parts.join(" ");
                let inner = joined.trim();
                if !inner.ends_with(')') { return Err("Invalid subquery".into()); }
                let inner_query = &inner[1..inner.len()-1];
                from_subquery = Some(Box::new(parse_statement(inner_query)?));
                idx += 1;
                if idx >= tokens.len() || !tokens[idx].eq_ignore_ascii_case("AS") { return Err("Expected AS after subquery".into()); }
                idx += 1;
                if idx >= tokens.len() { return Err("Missing alias after subquery".into()); }
                from_table = tokens[idx].trim_end_matches(';').to_string();
                idx += 1;
            } else {
                idx += 1;
            }
            let mut joins = Vec::new();
            while idx < tokens.len() && tokens[idx].eq_ignore_ascii_case("JOIN") {
                idx += 1;
                if idx >= tokens.len() {
                    return Err("Expected table after JOIN".into());
                }
                let table = tokens[idx].trim_end_matches(';').to_string();
                idx += 1;
                let mut alias = None;
                if idx + 1 < tokens.len() && tokens[idx].eq_ignore_ascii_case("AS") {
                    alias = Some(tokens[idx + 1].trim_end_matches(';').to_string());
                    idx += 2;
                }
                if idx >= tokens.len() || !tokens[idx].eq_ignore_ascii_case("ON") {
                    return Err("Expected ON in JOIN".into());
                }
                idx += 1;
                if idx + 2 >= tokens.len() {
                    return Err("Incomplete JOIN condition".into());
                }
                let left = tokens[idx];
                idx += 1;
                if tokens[idx] != "=" {
                    return Err("Expected '=' in JOIN".into());
                }
                idx += 1;
                let right = tokens[idx].trim_end_matches(';');
                idx += 1;

                let mut lp = left.split('.');
                let left_table = lp.next().ok_or("Invalid left side in JOIN")?.to_string();
                let left_column = lp.next().ok_or("Invalid left side in JOIN")?.to_string();
                let mut rp = right.split('.');
                let _right_table = rp.next().ok_or("Invalid right side in JOIN")?;
                let right_column = rp.next().ok_or("Invalid right side in JOIN")?.to_string();

                joins.push(crate::sql::ast::JoinClause { table, alias, left_table, left_column, right_column });
            }

            let mut where_predicate = None;
            if idx < tokens.len() && tokens[idx].eq_ignore_ascii_case("WHERE") {
                let (expr, consumed) = parse_expression(&tokens[idx + 1..])?;
                where_predicate = Some(expr);
                idx += consumed + 1;
            }

            let mut group_by = None;
            if idx + 1 < tokens.len() && tokens[idx].eq_ignore_ascii_case("GROUP") && tokens[idx + 1].eq_ignore_ascii_case("BY") {
                idx += 2;
                let mut cols = Vec::new();
                while idx < tokens.len() {
                    let token = tokens[idx].trim_end_matches(',').trim_end_matches(';');
                    if token.eq_ignore_ascii_case("ORDER") || token.eq_ignore_ascii_case("WHERE") { break; }
                    cols.push(token.to_string());
                    idx += 1;
                    if idx >= tokens.len() { break; }
                    if tokens[idx - 1].ends_with(';') { break; }
                }
                group_by = Some(cols);
            }

            Ok(Statement::Select { columns, from_table, from_subquery, joins, where_predicate, group_by })
        }
        "DROP" => {
            if tokens.len() < 3 || !tokens[1].eq_ignore_ascii_case("TABLE") {
                return Err("Usage: DROP TABLE <name>".to_string());
            }
            let mut idx = 2;
            let mut if_exists = false;
            if tokens.get(idx).map(|s| s.to_uppercase()) == Some("IF".to_string())
                && tokens.get(idx + 1).map(|s| s.to_uppercase()) == Some("EXISTS".to_string())
            {
                if_exists = true;
                idx += 2;
            }
            if idx >= tokens.len() {
                return Err("Usage: DROP TABLE <name>".to_string());
            }
            let table = tokens[idx].trim_end_matches(';').to_string();
            Ok(Statement::DropTable { table_name: table, if_exists })
        }
        "DELETE" => {
            if tokens.len() < 5 || !tokens[1].eq_ignore_ascii_case("FROM") || !tokens[3].eq_ignore_ascii_case("WHERE") {
                return Err("Usage: DELETE FROM <table> WHERE <expr>".to_string());
            }
            let table = tokens[2].trim_end_matches(';').to_string();
            let (expr, _) = parse_expression(&tokens[4..])?;
            Ok(Statement::Delete { table_name: table, selection: Some(expr) })
        }
        "UPDATE" => {
            if tokens.len() < 4 || !tokens[2].eq_ignore_ascii_case("SET") {
                return Err("Usage: UPDATE <table> SET col = val [, ...] [WHERE <expr>]".to_string());
            }
            let table = tokens[1].to_string();
            let mut idx = 3;
            let mut assignments = Vec::new();
            while idx < tokens.len() {
                if tokens[idx].eq_ignore_ascii_case("WHERE") {
                    break;
                }
                let col = tokens[idx].trim_end_matches(',').to_string();
                idx += 1;
                if idx >= tokens.len() || tokens[idx] != "=" {
                    return Err("Expected '=' in assignment".into());
                }
                idx += 1;
                if idx >= tokens.len() {
                    return Err("Expected value after '='".into());
                }
                let mut val = tokens[idx].trim_end_matches(',').trim_end_matches(';').to_string();
                if (val.starts_with('"') && val.ends_with('"')) || (val.starts_with('\'') && val.ends_with('\'')) {
                    val = val[1..val.len() - 1].to_string();
                }
                assignments.push((col, val));
                idx += 1;
            }
            let selection = if idx < tokens.len() && tokens[idx].eq_ignore_ascii_case("WHERE") {
                let (expr, _) = parse_expression(&tokens[idx + 1..])?;
                Some(expr)
            } else {
                None
            };
            Ok(Statement::Update { table_name: table, assignments, selection })
        }
        "EXIT" | ".EXIT" | ".exit" => Ok(Statement::Exit),
        _ => Err(format!("Unrecognized command: {}", tokens[0])),
    }
}
