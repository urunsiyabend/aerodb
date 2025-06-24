use crate::sql::ast::{Expr, Statement, OrderBy, ForeignKey, Action, ColumnDef, Literal};
use crate::storage::row::ColumnType;

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

fn parse_column_def(chunk: &str) -> Result<ColumnDef, String> {
    let mut parts: Vec<&str> = chunk.split_whitespace().collect();
    if parts.len() < 2 {
        return Err("Column definitions must be <name> <type>".to_string());
    }
    let name = parts.remove(0);
    let mut not_null = false;
    if let Some(pos) = parts.iter().position(|s| s.eq_ignore_ascii_case("NOT")) {
        if pos + 1 < parts.len() && parts[pos + 1].eq_ignore_ascii_case("NULL") {
            not_null = true;
            parts.remove(pos + 1);
            parts.remove(pos);
        }
    } else if let Some(pos) = parts.iter().position(|s| s.eq_ignore_ascii_case("NULL")) {
        parts.remove(pos);
    }
    let mut default_value = None;
    if let Some(pos) = parts.iter().position(|s| s.eq_ignore_ascii_case("DEFAULT")) {
        if pos + 1 >= parts.len() {
            return Err("DEFAULT requires a literal".into());
        }
        let literal = parts[pos+1..].join(" ");
        let lit = literal.trim();
        let lit = if (lit.starts_with('"') && lit.ends_with('"')) || (lit.starts_with('\'') && lit.ends_with('\'')) {
            &lit[1..lit.len()-1]
        } else {
            lit
        };
        default_value = Some(crate::sql::ast::parse_default_expr(lit));
        parts.truncate(pos);
    }
    let mut auto_increment = false;
    if let Some(pos) = parts.iter().position(|s| s.eq_ignore_ascii_case("AUTO_INCREMENT")) {
        auto_increment = true;
        parts.remove(pos);
    }
    let mut primary_key = false;
    if let Some(pos) = parts.iter().position(|s| s.eq_ignore_ascii_case("PRIMARY")) {
        if pos + 1 < parts.len() && parts[pos + 1].eq_ignore_ascii_case("KEY") {
            primary_key = true;
            parts.remove(pos + 1);
            parts.remove(pos);
        }
    }
    let type_str = parts.join(" ");
    let ctype = ColumnType::from_str(&type_str).ok_or_else(|| format!("Unknown type {}", type_str))?;
    if auto_increment {
        let is_int = matches!(ctype, ColumnType::Integer | ColumnType::SmallInt { .. } | ColumnType::MediumInt { .. });
        if !is_int {
            return Err("AUTO_INCREMENT can only be used with integer columns".into());
        }
        if !not_null {
            return Err("AUTO_INCREMENT columns must be NOT NULL".into());
        }
    }
    Ok(ColumnDef { name: name.to_string(), col_type: ctype, not_null, default_value, auto_increment, primary_key })
}

/// Parse a simple boolean expression consisting of identifiers, =, !=, AND, OR.
/// Returns the expression and the number of tokens consumed.
fn parse_expression(tokens: &[&str]) -> Result<(Expr, usize), String> {
    if tokens.is_empty() {
        return Err("Incomplete expression".into());
    }
    if tokens[0].eq_ignore_ascii_case("EXISTS") {
        if tokens.len() < 2 || !tokens[1].starts_with('(') {
            return Err("Expected '(' after EXISTS".into());
        }
        let mut depth = tokens[1].matches('(').count() as i32 - tokens[1].matches(')').count() as i32;
        let mut end = 1;
        while depth > 0 {
            end += 1;
            if end >= tokens.len() { return Err("Unclosed subquery".into()); }
            depth += tokens[end].matches('(').count() as i32 - tokens[end].matches(')').count() as i32;
        }
        let sub_tokens = tokens[1..=end].join(" ");
        let inner = sub_tokens.trim_start_matches('(').trim_end_matches(')');
        let substmt = parse_statement(inner)?;
        let mut expr = Expr::ExistsSubquery { query: Box::new(substmt) };
        let mut consumed = end + 1;
        while tokens.len() > consumed {
            let logic = tokens[consumed].to_uppercase();
            if logic != "AND" && logic != "OR" { break; }
            consumed += 1;
            let (next, used) = parse_expression(&tokens[consumed..])?;
            expr = if logic == "AND" { Expr::And(Box::new(expr), Box::new(next)) } else { Expr::Or(Box::new(expr), Box::new(next)) };
            consumed += used;
        }
        return Ok((expr, consumed));
    }
    if tokens.len() < 3 {
        return Err("Incomplete expression".into());
    }
    let left = tokens[0].to_string();
    let op = tokens[1];
    let mut consumed;
    let mut expr = match op.to_uppercase().as_str() {
        "IN" => {
            if !tokens[2].starts_with('(') {
                return Err("Expected '(' after IN".into());
            }
            let mut depth = tokens[2].matches('(').count() as i32 - tokens[2].matches(')').count() as i32;
            let mut end = 2;
            while depth > 0 {
                end += 1;
                if end >= tokens.len() { return Err("Unclosed subquery".into()); }
                depth += tokens[end].matches('(').count() as i32 - tokens[end].matches(')').count() as i32;
            }
            let sub_tokens = tokens[2..=end].join(" ");
            let inner = sub_tokens.trim_start_matches('(').trim_end_matches(')');
            let substmt = parse_statement(inner)?;
            consumed = end + 1;
            Expr::InSubquery { left, query: Box::new(substmt) }
        }
        "=" => {
            let right = tokens[2].trim_end_matches(';').to_string();
            consumed = 3;
            Expr::Equals { left, right }
        }
        "!=" => {
            let right = tokens[2].trim_end_matches(';').to_string();
            consumed = 3;
            Expr::NotEquals { left, right }
        }
        "<>" => {
            let right = tokens[2].trim_end_matches(';').to_string();
            consumed = 3;
            Expr::NotEquals { left, right }
        }
        "+" => {
            let right = tokens[2].trim_end_matches(';').to_string();
            consumed = 3;
            Expr::Add { left, right }
        }
        "-" => {
            let right = tokens[2].trim_end_matches(';').to_string();
            consumed = 3;
            Expr::Subtract { left, right }
        }
        "*" => {
            let right = tokens[2].trim_end_matches(';').to_string();
            consumed = 3;
            Expr::Multiply { left, right }
        }
        "/" => {
            let right = tokens[2].trim_end_matches(';').to_string();
            consumed = 3;
            Expr::Divide { left, right }
        }
        "%" => {
            let right = tokens[2].trim_end_matches(';').to_string();
            consumed = 3;
            Expr::Modulo { left, right }
        }
        "&" => {
            let right = tokens[2].trim_end_matches(';').to_string();
            consumed = 3;
            Expr::BitwiseAnd { left, right }
        }
        "|" => {
            let right = tokens[2].trim_end_matches(';').to_string();
            consumed = 3;
            Expr::BitwiseOr { left, right }
        }
        "^" => {
            let right = tokens[2].trim_end_matches(';').to_string();
            consumed = 3;
            Expr::BitwiseXor { left, right }
        }
        "BETWEEN" => {
            if tokens.len() < 5 || !tokens[3].eq_ignore_ascii_case("AND") {
                return Err("BETWEEN requires syntax: <expr> BETWEEN <low> AND <high>".into());
            }
            let low = tokens[2].to_string();
            let high = tokens[4].trim_end_matches(';').to_string();
            consumed = 5;
            Expr::Between { expr: left, low, high }
        }
        ">" => {
            let right = tokens[2].trim_end_matches(';').to_string();
            consumed = 3;
            Expr::GreaterThan { left, right }
        }
        ">=" => {
            let right = tokens[2].trim_end_matches(';').to_string();
            consumed = 3;
            Expr::GreaterOrEquals { left, right }
        }
        "<" => {
            let right = tokens[2].trim_end_matches(';').to_string();
            consumed = 3;
            Expr::LessThan { left, right }
        }
        "<=" => {
            let right = tokens[2].trim_end_matches(';').to_string();
            consumed = 3;
            Expr::LessOrEquals { left, right }
        }
        _ => return Err(format!("Unknown operator '{}'", op)),
    };
    while tokens.len() > consumed {
        let logic = tokens[consumed].to_uppercase();
        if logic != "AND" && logic != "OR" {
            break;
        }
        consumed += 1;
        let (next, used) = parse_expression(&tokens[consumed..])?;
        expr = if logic == "AND" {
            Expr::And(Box::new(expr), Box::new(next))
        } else {
            Expr::Or(Box::new(expr), Box::new(next))
        };
        consumed += used;
    }
    Ok((expr, consumed))
}

fn parse_create_sequence(tokens: &[&str]) -> Result<Statement, String> {
    if tokens.len() < 2 {
        return Err("Usage: CREATE SEQUENCE <name> [START WITH n] [INCREMENT BY m]".into());
    }
    let mut idx = 1; // tokens[0] is SEQUENCE
    let name = tokens[idx].to_string();
    idx += 1;
    let mut start = 1i64;
    let mut increment = 1i64;
    while idx < tokens.len() {
        if idx + 1 < tokens.len() && tokens[idx].eq_ignore_ascii_case("START") && tokens[idx + 1].eq_ignore_ascii_case("WITH") {
            idx += 2;
            if idx >= tokens.len() { return Err("Missing value after START WITH".into()); }
            start = tokens[idx].trim_end_matches(';').parse::<i64>().map_err(|_| "Invalid START value".to_string())?;
            idx += 1;
        } else if idx + 1 < tokens.len() && tokens[idx].eq_ignore_ascii_case("INCREMENT") && tokens[idx + 1].eq_ignore_ascii_case("BY") {
            idx += 2;
            if idx >= tokens.len() { return Err("Missing value after INCREMENT BY".into()); }
            increment = tokens[idx].trim_end_matches(';').parse::<i64>().map_err(|_| "Invalid INCREMENT value".to_string())?;
            if increment == 0 { return Err("INCREMENT BY cannot be zero".into()); }
            idx += 1;
        } else {
            break;
        }
    }
    Ok(Statement::CreateSequence(crate::sql::ast::CreateSequence { name, start, increment }))
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
            if tokens.len() >= 3 && tokens[1].eq_ignore_ascii_case("SEQUENCE") {
                return parse_create_sequence(&tokens[1..]);
            }
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


            let mut columns: Vec<ColumnDef> = Vec::new();
            let mut fks = Vec::new();
            let mut primary_key: Option<Vec<String>> = None;
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
                } else if chunk.to_uppercase().starts_with("PRIMARY KEY") {
                    let rest = chunk[11..].trim();
                    if !rest.starts_with('(') || !rest.ends_with(')') {
                        return Err("Expected column list after PRIMARY KEY".into());
                    }
                    let inner = &rest[1..rest.len()-1];
                    let cols: Vec<String> = inner.split(',').map(|c| c.trim().to_string()).collect();
                    if primary_key.is_some() {
                        return Err("Multiple primary keys defined".into());
                    }
                    primary_key = Some(cols);
                } else {
                    let col = parse_column_def(&chunk)?;
                    columns.push(col);
                }
            }

            if columns.is_empty() {
                return Err("At least one column required".to_string());
            }

            let mut inline_pk: Vec<String> = columns.iter().filter(|c| c.primary_key).map(|c| c.name.clone()).collect();
            if !inline_pk.is_empty() {
                if primary_key.is_some() {
                    return Err("Multiple primary keys defined".into());
                }
                primary_key = Some(inline_pk);
            }

            Ok(Statement::CreateTable { table_name: name, columns, fks, primary_key, if_not_exists })
        }
        "INSERT" => {
            if tokens.len() < 4 || !tokens[1].eq_ignore_ascii_case("INTO") {
                return Err("Usage: INSERT INTO <table> [ (cols) ] VALUES (...)".to_string());
            }
            let table = tokens[2].trim_end_matches(',').to_string();
            let mut idx = 3;
            let mut columns = None;
            if idx < tokens.len() && tokens[idx].starts_with('(') && !tokens[idx].eq_ignore_ascii_case("VALUES") {
                let mut depth = tokens[idx].matches('(').count() as i32 - tokens[idx].matches(')').count() as i32;
                let mut col_tokens = vec![tokens[idx]];
                idx += 1;
                while depth > 0 {
                    if idx >= tokens.len() { return Err("Unclosed column list".into()); }
                    depth += tokens[idx].matches('(').count() as i32 - tokens[idx].matches(')').count() as i32;
                    col_tokens.push(tokens[idx]);
                    idx += 1;
                }
                let joined = col_tokens.join(" ");
                let inner = joined.trim();
                if !inner.starts_with('(') || !inner.ends_with(')') {
                    return Err("Column list must be in parentheses".into());
                }
                let cols_str = &inner[1..inner.len() - 1];
                let cols: Vec<String> = cols_str.split(',').map(|c| c.trim().to_string()).collect();
                columns = Some(cols);
            }
            if idx >= tokens.len() || !tokens[idx].eq_ignore_ascii_case("VALUES") {
                return Err("Expected VALUES".into());
            }
            idx += 1;
            if idx >= tokens.len() { return Err("Missing values".into()); }
            let rest_tokens = tokens[idx..].join(" ");
            let rest = rest_tokens.trim();
            if !rest.starts_with('(') || !rest.ends_with(')') {
                return Err("Values must be in parentheses".to_string());
            }
            let inner = &rest[1..rest.len() - 1];
            let vals: Vec<Expr> = split_top_level(inner)
                .into_iter()
                .map(|s| {
                    let v = s.trim();
                    if v.eq_ignore_ascii_case("DEFAULT") {
                        Expr::DefaultValue
                    } else if (v.starts_with('"') && v.ends_with('"')) || (v.starts_with('\'') && v.ends_with('\'')) {
                        Expr::Literal(v[1..v.len()-1].to_string())
                    } else {
                        Expr::Literal(v.to_string())
                    }
                })
                .collect();
            if vals.is_empty() {
                return Err("At least one value required".to_string());
            }
            Ok(Statement::Insert { table_name: table, columns, values: vals })
        }
        "SELECT" => {
            if tokens.len() < 2 {
                return Err("Incomplete SELECT".into());
            }

            let mut idx = 1;
            let mut columns = Vec::new();
            let mut col_tokens = Vec::new();
            let mut depth = 0i32;
            while idx < tokens.len() {
                if depth == 0 && tokens[idx].eq_ignore_ascii_case("FROM") {
                    break;
                }
                depth += tokens[idx].matches('(').count() as i32 - tokens[idx].matches(')').count() as i32;
                col_tokens.push(tokens[idx]);
                idx += 1;
            }
            let col_str = col_tokens.join(" ");
            for part in split_top_level(&col_str) {
                let token = part.trim().trim_end_matches(',').trim_end_matches(';');
                let mut expr_part = token.trim();
                let mut alias: Option<String> = None;
                if let Some(pos) = token.to_uppercase().rfind(" AS ") {
                    expr_part = token[..pos].trim();
                    alias = Some(token[pos + 4..].trim().to_string());
                } else if let Some(pos) = token.rfind(' ') {
                    let potential = token[pos + 1..].trim();
                    if !potential.contains('(')
                        && !token.trim_end().ends_with(')')
                        && !token[..pos].chars().any(|ch| "+-*/%".contains(ch))
                    {
                        alias = Some(potential.trim_end_matches(';').to_string());
                        expr_part = token[..pos].trim();
                    }
                }
                let upper = expr_part.to_uppercase();
                let item = if expr_part == "*" {
                    crate::sql::ast::SelectItem::All
                } else if expr_part.starts_with('(') {
                    let end = expr_part.rfind(')').ok_or("Unclosed subquery")?;
                    let inner = &expr_part[1..end];
                    let sub = parse_statement(inner)?;
                    crate::sql::ast::SelectItem::Subquery(Box::new(sub))
                } else if upper.starts_with("SELECT") {
                    let sub = parse_statement(expr_part)?;
                    crate::sql::ast::SelectItem::Subquery(Box::new(sub))
                } else if upper.starts_with("COUNT(") {
                    let inner = expr_part[6..expr_part.len() - 1].trim();
                    let col = if inner == "*" { None } else { Some(inner.to_string()) };
                    crate::sql::ast::SelectItem::Aggregate { func: crate::sql::ast::AggFunc::Count, column: col }
                } else if upper.starts_with("SUM(") {
                    let inner = expr_part[4..expr_part.len() - 1].trim().to_string();
                    crate::sql::ast::SelectItem::Aggregate { func: crate::sql::ast::AggFunc::Sum, column: Some(inner) }
                } else if upper.starts_with("AVG(") {
                    let inner = expr_part[4..expr_part.len() - 1].trim().to_string();
                    crate::sql::ast::SelectItem::Aggregate { func: crate::sql::ast::AggFunc::Avg, column: Some(inner) }
                } else if upper.starts_with("MIN(") {
                    let inner = expr_part[4..expr_part.len() - 1].trim().to_string();
                    crate::sql::ast::SelectItem::Aggregate { func: crate::sql::ast::AggFunc::Min, column: Some(inner) }
                } else if upper.starts_with("MAX(") {
                    let inner = expr_part[4..expr_part.len() - 1].trim().to_string();
                    crate::sql::ast::SelectItem::Aggregate { func: crate::sql::ast::AggFunc::Max, column: Some(inner) }
                } else if upper == "CURRENT_TIMESTAMP" || upper == "CURRENT_TIMESTAMP()" {
                    crate::sql::ast::SelectItem::Expr(Box::new(crate::sql::ast::Expr::FunctionCall { name: "CURRENT_TIMESTAMP".into(), args: Vec::new() }))
                } else if expr_part.starts_with("'") && expr_part.ends_with("'") {
                    crate::sql::ast::SelectItem::Literal(expr_part[1..expr_part.len()-1].to_string())
                } else if expr_part.chars().all(|c| c.is_ascii_digit()) {
                    crate::sql::ast::SelectItem::Literal(expr_part.to_string())
                } else {
                    let parts: Vec<&str> = expr_part.split_whitespace().collect();
                    if parts.len() >= 3 {
                        if let Ok((expr, used)) = parse_expression(&parts) {
                            if used == parts.len() {
                                crate::sql::ast::SelectItem::Expr(Box::new(expr))
                            } else {
                                crate::sql::ast::SelectItem::Column(expr_part.to_string())
                            }
                        } else {
                            crate::sql::ast::SelectItem::Column(expr_part.to_string())
                        }
                    } else {
                        crate::sql::ast::SelectItem::Column(expr_part.to_string())
                    }
                };
                columns.push(crate::sql::ast::SelectExpr { expr: item, alias });
            }
            if idx >= tokens.len() {
                if columns.iter().any(|c| matches!(c.expr, crate::sql::ast::SelectItem::Column(_) | crate::sql::ast::SelectItem::All | crate::sql::ast::SelectItem::Aggregate { .. })) {
                    return Err("Column without table".into());
                }
                return Ok(Statement::Select { columns, from: Vec::new(), joins: Vec::new(), where_predicate: None, group_by: None, having: None });
            }
            if !tokens[idx].eq_ignore_ascii_case("FROM") {
                return Err("Expected FROM".into());
            }
            idx += 1;
            if idx >= tokens.len() {
                return Err("Missing table after FROM".into());
            }
            let mut from = Vec::new();
            if tokens[idx].starts_with('(') {
                let mut depth = tokens[idx].matches('(').count() as i32 - tokens[idx].matches(')').count() as i32;
                let mut end = idx;
                while depth > 0 {
                    end += 1;
                    if end >= tokens.len() { return Err("Unclosed subquery".into()); }
                    depth += tokens[end].matches('(').count() as i32 - tokens[end].matches(')').count() as i32;
                }
                let sub_tokens = tokens[idx..=end].join(" ");
                let inner = sub_tokens.trim_start_matches('(').trim_end_matches(')');
                let substmt = parse_statement(inner)?;
                idx = end + 1;
                let mut alias = None;
                if idx < tokens.len() && tokens[idx].eq_ignore_ascii_case("AS") {
                    if idx + 1 >= tokens.len() { return Err("Subquery in FROM requires alias".into()); }
                    alias = Some(tokens[idx + 1].trim_end_matches(';').to_string());
                    idx += 2;
                } else if idx < tokens.len() {
                    alias = Some(tokens[idx].trim_end_matches(';').to_string());
                    idx += 1;
                } else {
                    return Err("Subquery in FROM requires alias".into());
                }
                let alias = alias.unwrap();
                from.push(crate::sql::ast::TableRef::Subquery { query: Box::new(substmt), alias });
            } else {
                let table = tokens[idx].trim_end_matches(';').to_string();
                idx += 1;
                let mut alias = None;
                if idx < tokens.len() && tokens[idx].eq_ignore_ascii_case("AS") {
                    if idx + 1 < tokens.len() {
                        alias = Some(tokens[idx + 1].trim_end_matches(';').to_string());
                        idx += 2;
                    } else {
                        return Err("Expected alias after AS".into());
                    }
                } else if idx < tokens.len()
                    && !tokens[idx].eq_ignore_ascii_case("JOIN")
                    && !tokens[idx].eq_ignore_ascii_case("WHERE")
                    && !tokens[idx].eq_ignore_ascii_case("GROUP")
                    && !tokens[idx].eq_ignore_ascii_case("ORDER")
                    && !tokens[idx].eq_ignore_ascii_case("HAVING")
                {
                    alias = Some(tokens[idx].trim_end_matches(';').to_string());
                    idx += 1;
                }
                from.push(crate::sql::ast::TableRef::Named { name: table, alias });
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
                if idx < tokens.len() && tokens[idx].eq_ignore_ascii_case("AS") {
                    if idx + 1 < tokens.len() {
                        alias = Some(tokens[idx + 1].trim_end_matches(';').to_string());
                        idx += 2;
                    } else { return Err("Expected alias after AS".into()); }
                } else if idx < tokens.len() && !tokens[idx].eq_ignore_ascii_case("ON") {
                    alias = Some(tokens[idx].trim_end_matches(';').to_string());
                    idx += 1;
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
                    if token.eq_ignore_ascii_case("ORDER") || token.eq_ignore_ascii_case("WHERE") || token.eq_ignore_ascii_case("HAVING") { break; }
                    cols.push(token.to_string());
                    idx += 1;
                    if idx >= tokens.len() { break; }
                    if tokens[idx - 1].ends_with(';') { break; }
                }
                group_by = Some(cols);
            }

            let mut having = None;
            if idx < tokens.len() && tokens[idx].eq_ignore_ascii_case("HAVING") {
                let (expr, consumed) = parse_expression(&tokens[idx + 1..])?;
                having = Some(expr);
                idx += consumed + 1;
            }

            Ok(Statement::Select { columns, from, joins, where_predicate, group_by, having })
        }
        "DROP" => {
            if tokens.len() < 3 {
                return Err("Usage: DROP TABLE <name> | DROP INDEX <name>".to_string());
            }
            if tokens[1].eq_ignore_ascii_case("TABLE") {
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
            } else if tokens[1].eq_ignore_ascii_case("INDEX") {
                if tokens.len() < 3 {
                    return Err("Usage: DROP INDEX <name>".to_string());
                }
                let name = tokens[2].trim_end_matches(';').to_string();
                Ok(Statement::DropIndex { name })
            } else {
                Err("Usage: DROP TABLE <name>".to_string())
            }
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
