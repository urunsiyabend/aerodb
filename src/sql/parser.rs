use crate::sql::ast::{Expr, Statement, OrderBy, ForeignKey, Action, ColumnDef};
use crate::storage::row::ColumnType;

fn tokenize(input: &str) -> Result<Vec<String>, String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_quote: Option<char> = None;
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if let Some(quote) = in_quote {
            current.push(ch);
            if ch == quote {
                in_quote = None;
                tokens.push(current.clone());
                current.clear();
            }
            continue;
        }

        match ch {
            '\'' | '"' => {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
                in_quote = Some(ch);
                current.push(ch);
            }
            c if c.is_whitespace() => {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
            }
            ';' => {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
            }
            '(' | ')' | ',' => {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
                tokens.push(ch.to_string());
            }
            '<' | '>' | '!' => {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
                let mut op = ch.to_string();
                if let Some(&next) = chars.peek() {
                    if (ch == '<' && (next == '=' || next == '>'))
                        || (ch == '>' && next == '=')
                        || (ch == '!' && next == '=')
                    {
                        op.push(next);
                        chars.next();
                    }
                }
                tokens.push(op);
            }
            '+' | '-' => {
                if current.is_empty() {
                    if let Some(&next) = chars.peek() {
                        if next.is_ascii_digit() {
                            current.push(ch);
                            continue;
                        }
                    }
                }
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
                tokens.push(ch.to_string());
            }
            '=' | '*' | '/' | '%' | '&' | '|' | '^' => {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
                tokens.push(ch.to_string());
            }
            _ => current.push(ch),
        }
    }

    if in_quote.is_some() {
        return Err("Unterminated quoted string or identifier".to_string());
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    Ok(tokens)
}

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

fn split_top_level_tokens(tokens: &[String]) -> Vec<Vec<String>> {
    let mut parts = Vec::new();
    let mut current = Vec::new();
    let mut depth = 0;
    for token in tokens {
        match token.as_str() {
            "(" => {
                depth += 1;
                current.push(token.clone());
            }
            ")" => {
                depth -= 1;
                current.push(token.clone());
            }
            "," if depth == 0 => {
                if !current.is_empty() {
                    parts.push(current);
                    current = Vec::new();
                }
            }
            _ => current.push(token.clone()),
        }
    }
    if !current.is_empty() {
        parts.push(current);
    }
    parts
}

fn unquote_token(token: &str) -> &str {
    if (token.starts_with('"') && token.ends_with('"')) || (token.starts_with('\'') && token.ends_with('\'')) {
        &token[1..token.len() - 1]
    } else {
        token
    }
}

fn is_identifier_token(token: &str) -> bool {
    if token.is_empty() {
        return false;
    }
    if token.starts_with('"') {
        return true;
    }
    if token.starts_with('\'') {
        return false;
    }
    token.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.')
}

fn is_operator_token(token: &str) -> bool {
    matches!(
        token.to_uppercase().as_str(),
        "=" | "!=" | "<>" | "<" | "<=" | ">" | ">=" | "+" | "-" | "*" | "/" | "%" | "&" | "|" | "^" | "IN" | "BETWEEN"
    )
}

fn join_tokens(tokens: &[String]) -> String {
    let mut out = String::new();
    let mut prev: Option<&str> = None;
    for token in tokens {
        let token_str = token.as_str();
        let needs_space = match prev {
            None => false,
            Some(prev_token) => {
                if prev_token == "(" || token_str == "(" {
                    false
                } else if token_str == ")" || token_str == "," {
                    false
                } else if prev_token == "," {
                    true
                } else if is_operator_token(prev_token) || is_operator_token(token_str) {
                    true
                } else {
                    true
                }
            }
        };
        if needs_space {
            out.push(' ');
        }
        out.push_str(token_str);
        prev = Some(token_str);
    }
    out
}

fn join_type_tokens(tokens: &[String]) -> String {
    let mut out = String::new();
    let mut prev: Option<&str> = None;
    for token in tokens {
        let token_str = token.as_str();
        let needs_space = match prev {
            None => false,
            Some(prev_token) => {
                if token_str == ")" || token_str == "," {
                    false
                } else if prev_token == "(" || prev_token == "," {
                    false
                } else {
                    true
                }
            }
        };
        if needs_space {
            out.push(' ');
        }
        out.push_str(token_str);
        prev = Some(token_str);
    }
    out
}

fn parse_column_def(chunk: &str) -> Result<ColumnDef, String> {
    let mut parts: Vec<String> = tokenize(chunk)?;
    if parts.len() < 2 {
        return Err("Column definitions must be <name> <type>".to_string());
    }
    let name = unquote_token(&parts.remove(0)).to_string();
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
        let literal = join_tokens(&parts[pos+1..]);
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
    let type_str = join_type_tokens(&parts);
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
fn parse_expression(tokens: &[String]) -> Result<(Expr, usize), String> {
    if tokens.is_empty() {
        return Err("Incomplete expression".into());
    }
    fn parse_operand(tokens: &[String]) -> Result<(String, usize), String> {
        if tokens.is_empty() {
            return Err("Incomplete expression".into());
        }
        if tokens.len() > 1 && tokens[1] == "(" {
            let mut depth = 0i32;
            for (idx, token) in tokens.iter().enumerate().skip(1) {
                if token == "(" {
                    depth += 1;
                } else if token == ")" {
                    depth -= 1;
                    if depth == 0 {
                        let combined = join_tokens(&tokens[..=idx]);
                        return Ok((combined, idx + 1));
                    }
                }
            }
            return Err("Unclosed function call".into());
        }
        Ok((unquote_token(&tokens[0]).to_string(), 1))
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
        let sub_tokens = join_tokens(&tokens[1..=end]);
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
    let (left, mut idx) = parse_operand(tokens)?;
    if idx >= tokens.len() {
        return Err("Incomplete expression".into());
    }
    let op = tokens[idx].as_str();
    idx += 1;
    let mut consumed;
    let mut expr = match op.to_uppercase().as_str() {
        "IN" => {
            if idx >= tokens.len() || !tokens[idx].starts_with('(') {
                return Err("Expected '(' after IN".into());
            }
            let mut depth = tokens[idx].matches('(').count() as i32 - tokens[idx].matches(')').count() as i32;
            let mut end = idx;
            while depth > 0 {
                end += 1;
                if end >= tokens.len() { return Err("Unclosed subquery".into()); }
                depth += tokens[end].matches('(').count() as i32 - tokens[end].matches(')').count() as i32;
            }
            let sub_tokens = join_tokens(&tokens[idx..=end]);
            let inner = sub_tokens.trim_start_matches('(').trim_end_matches(')');
            let substmt = parse_statement(inner)?;
            consumed = end + 1;
            Expr::InSubquery { left, query: Box::new(substmt) }
        }
        "=" => {
            let right = unquote_token(&tokens[idx]).trim_end_matches(';').to_string();
            consumed = idx + 1;
            Expr::Equals { left, right }
        }
        "!=" => {
            let right = unquote_token(&tokens[idx]).trim_end_matches(';').to_string();
            consumed = idx + 1;
            Expr::NotEquals { left, right }
        }
        "<>" => {
            let right = unquote_token(&tokens[idx]).trim_end_matches(';').to_string();
            consumed = idx + 1;
            Expr::NotEquals { left, right }
        }
        "+" => {
            let right = unquote_token(&tokens[idx]).trim_end_matches(';').to_string();
            consumed = idx + 1;
            Expr::Add { left, right }
        }
        "-" => {
            let right = unquote_token(&tokens[idx]).trim_end_matches(';').to_string();
            consumed = idx + 1;
            Expr::Subtract { left, right }
        }
        "*" => {
            let right = unquote_token(&tokens[idx]).trim_end_matches(';').to_string();
            consumed = idx + 1;
            Expr::Multiply { left, right }
        }
        "/" => {
            let right = unquote_token(&tokens[idx]).trim_end_matches(';').to_string();
            consumed = idx + 1;
            Expr::Divide { left, right }
        }
        "%" => {
            let right = unquote_token(&tokens[idx]).trim_end_matches(';').to_string();
            consumed = idx + 1;
            Expr::Modulo { left, right }
        }
        "&" => {
            let right = unquote_token(&tokens[idx]).trim_end_matches(';').to_string();
            consumed = idx + 1;
            Expr::BitwiseAnd { left, right }
        }
        "|" => {
            let right = unquote_token(&tokens[idx]).trim_end_matches(';').to_string();
            consumed = idx + 1;
            Expr::BitwiseOr { left, right }
        }
        "^" => {
            let right = unquote_token(&tokens[idx]).trim_end_matches(';').to_string();
            consumed = idx + 1;
            Expr::BitwiseXor { left, right }
        }
        "BETWEEN" => {
            if idx + 2 >= tokens.len() || !tokens[idx + 1].eq_ignore_ascii_case("AND") {
                return Err("BETWEEN requires syntax: <expr> BETWEEN <low> AND <high>".into());
            }
            let low = unquote_token(&tokens[idx]).to_string();
            let high = unquote_token(&tokens[idx + 2]).trim_end_matches(';').to_string();
            consumed = idx + 3;
            Expr::Between { expr: left, low, high }
        }
        ">" => {
            let right = unquote_token(&tokens[idx]).trim_end_matches(';').to_string();
            consumed = idx + 1;
            Expr::GreaterThan { left, right }
        }
        ">=" => {
            let right = unquote_token(&tokens[idx]).trim_end_matches(';').to_string();
            consumed = idx + 1;
            Expr::GreaterOrEquals { left, right }
        }
        "<" => {
            let right = unquote_token(&tokens[idx]).trim_end_matches(';').to_string();
            consumed = idx + 1;
            Expr::LessThan { left, right }
        }
        "<=" => {
            let right = unquote_token(&tokens[idx]).trim_end_matches(';').to_string();
            consumed = idx + 1;
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

fn parse_create_sequence(tokens: &[String]) -> Result<Statement, String> {
    if tokens.len() < 2 {
        return Err("Usage: CREATE SEQUENCE <name> [START WITH n] [INCREMENT BY m]".into());
    }
    let mut idx = 1; // tokens[0] is SEQUENCE
    let name = unquote_token(&tokens[idx]).to_string();
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
    let tokens = tokenize(input)?;
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
            let name = tokens.get(idx).map(|s| unquote_token(s).trim_end_matches(';').to_string());
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
                let table_name = unquote_token(&tokens[4]).trim_end_matches(';').to_string();
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
            let name = unquote_token(&tokens[idx]).to_string();
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
            let table = unquote_token(&tokens[2]).trim_end_matches(',').to_string();
            let mut idx = 3;
            let mut columns = None;
            if idx < tokens.len() && tokens[idx].starts_with('(') && !tokens[idx].eq_ignore_ascii_case("VALUES") {
                let mut depth = tokens[idx].matches('(').count() as i32 - tokens[idx].matches(')').count() as i32;
                let mut col_tokens = vec![tokens[idx].clone()];
                idx += 1;
                while depth > 0 {
                    if idx >= tokens.len() { return Err("Unclosed column list".into()); }
                    depth += tokens[idx].matches('(').count() as i32 - tokens[idx].matches(')').count() as i32;
                    col_tokens.push(tokens[idx].clone());
                    idx += 1;
                }
                let joined = col_tokens.join(" ");
                let inner = joined.trim();
                if !inner.starts_with('(') || !inner.ends_with(')') {
                    return Err("Column list must be in parentheses".into());
                }
                let cols_str = &inner[1..inner.len() - 1];
                let cols: Vec<String> = cols_str
                    .split(',')
                    .map(|c| unquote_token(c.trim()).to_string())
                    .collect();
                columns = Some(cols);
            }
            if idx >= tokens.len() || !tokens[idx].eq_ignore_ascii_case("VALUES") {
                return Err("Expected VALUES".into());
            }
            idx += 1;
            if idx >= tokens.len() { return Err("Missing values".into()); }
            let rest_tokens = tokens[idx..].join(" ");
            let rest = rest_tokens.trim().trim_end_matches(';');
            let tuple_strs = split_top_level(rest);
            if tuple_strs.is_empty() { return Err("Missing values".into()); }

            let mut rows = Vec::new();
            for tup in tuple_strs {
                let tup = tup.trim();
                if !tup.starts_with('(') || !tup.ends_with(')') {
                    return Err("Values must be in parentheses".to_string());
                }
                let inner = &tup[1..tup.len()-1];
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
                rows.push(vals);
            }
            Ok(Statement::Insert { table_name: table, columns, rows })
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
                col_tokens.push(tokens[idx].clone());
                idx += 1;
            }
            for item_tokens in split_top_level_tokens(&col_tokens) {
                if item_tokens.is_empty() {
                    continue;
                }
                let mut alias: Option<String> = None;
                let mut expr_tokens = item_tokens.clone();
                let mut depth = 0i32;
                let mut as_pos = None;
                for (i, token) in item_tokens.iter().enumerate() {
                    match token.as_str() {
                        "(" => depth += 1,
                        ")" => depth -= 1,
                        _ => {}
                    }
                    if depth == 0 && token.eq_ignore_ascii_case("AS") {
                        as_pos = Some(i);
                        break;
                    }
                }
                if let Some(pos) = as_pos {
                    if pos + 1 >= item_tokens.len() {
                        return Err("Expected alias after AS".into());
                    }
                    alias = Some(unquote_token(&item_tokens[pos + 1]).to_string());
                    expr_tokens = item_tokens[..pos].to_vec();
                } else if item_tokens.len() >= 2 {
                    let last = item_tokens.last().unwrap();
                    let has_operator = item_tokens[..item_tokens.len() - 1]
                        .iter()
                        .any(|tok| is_operator_token(tok.as_str()));
                    if !has_operator && is_identifier_token(last) {
                        alias = Some(unquote_token(last).to_string());
                        expr_tokens = item_tokens[..item_tokens.len() - 1].to_vec();
                    }
                }
                if expr_tokens.is_empty() {
                    return Err("Expected expression in SELECT list".into());
                }
                let upper = expr_tokens[0].to_uppercase();
                let item = if expr_tokens.len() == 1 && expr_tokens[0] == "*" {
                    crate::sql::ast::SelectItem::All
                } else if expr_tokens[0] == "(" && expr_tokens.last().map(|t| t.as_str()) == Some(")") {
                    let inner = join_tokens(&expr_tokens[1..expr_tokens.len() - 1]);
                    let sub = parse_statement(&inner)?;
                    crate::sql::ast::SelectItem::Subquery(Box::new(sub))
                } else if upper.starts_with("SELECT") {
                    let sub = parse_statement(&join_tokens(&expr_tokens))?;
                    crate::sql::ast::SelectItem::Subquery(Box::new(sub))
                } else if expr_tokens.len() >= 3
                    && expr_tokens[1] == "("
                    && expr_tokens.last().map(|t| t.as_str()) == Some(")")
                    && matches!(upper.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
                {
                    let inner_tokens = &expr_tokens[2..expr_tokens.len() - 1];
                    let inner = join_tokens(inner_tokens);
                    let inner_trim = inner.trim();
                    let column = if inner_trim == "*" {
                        None
                    } else {
                        Some(unquote_token(inner_trim).to_string())
                    };
                    let func = match upper.as_str() {
                        "COUNT" => crate::sql::ast::AggFunc::Count,
                        "SUM" => crate::sql::ast::AggFunc::Sum,
                        "AVG" => crate::sql::ast::AggFunc::Avg,
                        "MIN" => crate::sql::ast::AggFunc::Min,
                        _ => crate::sql::ast::AggFunc::Max,
                    };
                    crate::sql::ast::SelectItem::Aggregate { func, column }
                } else if upper == "CURRENT_TIMESTAMP"
                    && (expr_tokens.len() == 1
                        || (expr_tokens.len() == 3 && expr_tokens[1] == "(" && expr_tokens[2] == ")"))
                {
                    crate::sql::ast::SelectItem::Expr(Box::new(crate::sql::ast::Expr::FunctionCall { name: "CURRENT_TIMESTAMP".into(), args: Vec::new() }))
                } else if expr_tokens.len() == 1 && expr_tokens[0].starts_with('\'') && expr_tokens[0].ends_with('\'') {
                    crate::sql::ast::SelectItem::Literal(unquote_token(&expr_tokens[0]).to_string())
                } else if expr_tokens.len() == 1 && expr_tokens[0].starts_with('"') && expr_tokens[0].ends_with('"') {
                    crate::sql::ast::SelectItem::Column(unquote_token(&expr_tokens[0]).to_string())
                } else if expr_tokens.len() == 1 && expr_tokens[0].chars().all(|c| c.is_ascii_digit()) {
                    crate::sql::ast::SelectItem::Literal(expr_tokens[0].to_string())
                } else {
                    if expr_tokens.len() >= 3 {
                        if let Ok((expr, used)) = parse_expression(&expr_tokens) {
                            if used == expr_tokens.len() {
                                crate::sql::ast::SelectItem::Expr(Box::new(expr))
                            } else {
                                crate::sql::ast::SelectItem::Column(join_tokens(&expr_tokens))
                            }
                        } else {
                            crate::sql::ast::SelectItem::Column(join_tokens(&expr_tokens))
                        }
                    } else {
                        crate::sql::ast::SelectItem::Column(join_tokens(&expr_tokens))
                    }
                };
                columns.push(crate::sql::ast::SelectExpr { expr: item, alias });
            }
            if idx >= tokens.len() {
                if columns.iter().any(|c| matches!(c.expr, crate::sql::ast::SelectItem::Column(_) | crate::sql::ast::SelectItem::All | crate::sql::ast::SelectItem::Aggregate { .. })) {
                    return Err("Column without table".into());
                }
                return Ok(Statement::Select {
                    columns,
                    from: Vec::new(),
                    joins: Vec::new(),
                    where_predicate: None,
                    group_by: None,
                    having: None,
                    order_by: None,
                    limit: None,
                    offset: None,
                });
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
                let sub_tokens = join_tokens(&tokens[idx..=end]);
                let inner = sub_tokens.trim_start_matches('(').trim_end_matches(')');
                let substmt = parse_statement(inner)?;
                idx = end + 1;
                let mut alias = None;
                if idx < tokens.len() && tokens[idx].eq_ignore_ascii_case("AS") {
                    if idx + 1 >= tokens.len() { return Err("Subquery in FROM requires alias".into()); }
                    alias = Some(unquote_token(&tokens[idx + 1]).trim_end_matches(';').to_string());
                    idx += 2;
                } else if idx < tokens.len() {
                    alias = Some(unquote_token(&tokens[idx]).trim_end_matches(';').to_string());
                    idx += 1;
                } else {
                    return Err("Subquery in FROM requires alias".into());
                }
                let alias = alias.unwrap();
                from.push(crate::sql::ast::TableRef::Subquery { query: Box::new(substmt), alias });
            } else {
                let table = unquote_token(&tokens[idx]).trim_end_matches(';').to_string();
                idx += 1;
                let mut alias = None;
                if idx < tokens.len() && tokens[idx].eq_ignore_ascii_case("AS") {
                    if idx + 1 < tokens.len() {
                        alias = Some(unquote_token(&tokens[idx + 1]).trim_end_matches(';').to_string());
                        idx += 2;
                    } else {
                        return Err("Expected alias after AS".into());
                    }
                } else if idx < tokens.len()
                    && !tokens[idx].eq_ignore_ascii_case("JOIN")
                    && !tokens[idx].eq_ignore_ascii_case("INNER")
                    && !tokens[idx].eq_ignore_ascii_case("LEFT")
                    && !tokens[idx].eq_ignore_ascii_case("RIGHT")
                    && !tokens[idx].eq_ignore_ascii_case("FULL")
                    && !tokens[idx].eq_ignore_ascii_case("CROSS")
                    && !tokens[idx].eq_ignore_ascii_case("WHERE")
                    && !tokens[idx].eq_ignore_ascii_case("GROUP")
                    && !tokens[idx].eq_ignore_ascii_case("ORDER")
                    && !tokens[idx].eq_ignore_ascii_case("HAVING")
                {
                    alias = Some(unquote_token(&tokens[idx]).trim_end_matches(';').to_string());
                    idx += 1;
                }
                from.push(crate::sql::ast::TableRef::Named { name: table, alias });
            }
            let mut joins = Vec::new();
            let is_join_boundary = |token: &str| {
                token.eq_ignore_ascii_case("JOIN")
                    || token.eq_ignore_ascii_case("INNER")
                    || token.eq_ignore_ascii_case("LEFT")
                    || token.eq_ignore_ascii_case("RIGHT")
                    || token.eq_ignore_ascii_case("FULL")
                    || token.eq_ignore_ascii_case("CROSS")
                    || token.eq_ignore_ascii_case("ON")
                    || token.eq_ignore_ascii_case("WHERE")
                    || token.eq_ignore_ascii_case("GROUP")
                    || token.eq_ignore_ascii_case("ORDER")
                    || token.eq_ignore_ascii_case("HAVING")
                    || token.eq_ignore_ascii_case("LIMIT")
                    || token.eq_ignore_ascii_case("OFFSET")
            };
            while idx < tokens.len() {
                let join_type = if tokens[idx].eq_ignore_ascii_case("JOIN") {
                    idx += 1;
                    crate::sql::ast::JoinType::Inner
                } else if tokens[idx].eq_ignore_ascii_case("INNER") {
                    idx += 1;
                    if idx >= tokens.len() || !tokens[idx].eq_ignore_ascii_case("JOIN") {
                        return Err("Expected JOIN after INNER".into());
                    }
                    idx += 1;
                    crate::sql::ast::JoinType::Inner
                } else if tokens[idx].eq_ignore_ascii_case("LEFT") {
                    idx += 1;
                    if idx < tokens.len() && tokens[idx].eq_ignore_ascii_case("OUTER") {
                        idx += 1;
                    }
                    if idx >= tokens.len() || !tokens[idx].eq_ignore_ascii_case("JOIN") {
                        return Err("Expected JOIN after LEFT".into());
                    }
                    idx += 1;
                    crate::sql::ast::JoinType::Left
                } else if tokens[idx].eq_ignore_ascii_case("RIGHT") {
                    idx += 1;
                    if idx < tokens.len() && tokens[idx].eq_ignore_ascii_case("OUTER") {
                        idx += 1;
                    }
                    if idx >= tokens.len() || !tokens[idx].eq_ignore_ascii_case("JOIN") {
                        return Err("Expected JOIN after RIGHT".into());
                    }
                    idx += 1;
                    crate::sql::ast::JoinType::Right
                } else if tokens[idx].eq_ignore_ascii_case("FULL") {
                    idx += 1;
                    if idx < tokens.len() && tokens[idx].eq_ignore_ascii_case("OUTER") {
                        idx += 1;
                    }
                    if idx >= tokens.len() || !tokens[idx].eq_ignore_ascii_case("JOIN") {
                        return Err("Expected JOIN after FULL".into());
                    }
                    idx += 1;
                    crate::sql::ast::JoinType::Full
                } else if tokens[idx].eq_ignore_ascii_case("CROSS") {
                    idx += 1;
                    if idx >= tokens.len() || !tokens[idx].eq_ignore_ascii_case("JOIN") {
                        return Err("Expected JOIN after CROSS".into());
                    }
                    idx += 1;
                    crate::sql::ast::JoinType::Cross
                } else {
                    break;
                };

                if idx >= tokens.len() {
                    return Err("Expected table after JOIN".into());
                }
                let table = unquote_token(&tokens[idx]).trim_end_matches(';').to_string();
                idx += 1;
                let mut alias = None;
                if idx < tokens.len() && tokens[idx].eq_ignore_ascii_case("AS") {
                    if idx + 1 < tokens.len() {
                        alias = Some(unquote_token(&tokens[idx + 1]).trim_end_matches(';').to_string());
                        idx += 2;
                    } else { return Err("Expected alias after AS".into()); }
                } else if idx < tokens.len() && !is_join_boundary(&tokens[idx]) {
                    alias = Some(unquote_token(&tokens[idx]).trim_end_matches(';').to_string());
                    idx += 1;
                }

                let predicate = if matches!(join_type, crate::sql::ast::JoinType::Cross) {
                    if idx < tokens.len() && tokens[idx].eq_ignore_ascii_case("ON") {
                        return Err("CROSS JOIN does not support ON".into());
                    }
                    None
                } else {
                    if idx >= tokens.len() || !tokens[idx].eq_ignore_ascii_case("ON") {
                        return Err("Expected ON in JOIN".into());
                    }
                    idx += 1;
                    let mut end = idx;
                    let mut depth = 0i32;
                    while end < tokens.len() {
                        let token = tokens[end].as_str();
                        depth += token.matches('(').count() as i32;
                        depth -= token.matches(')').count() as i32;
                        if depth == 0 && is_join_boundary(token) {
                            break;
                        }
                        end += 1;
                    }
                    if end == idx {
                        return Err("Incomplete JOIN condition".into());
                    }
                    let slice = &tokens[idx..end];
                    let (expr, consumed) = parse_expression(slice)?;
                    if consumed != slice.len() {
                        return Err("Invalid JOIN condition".into());
                    }
                    idx = end;
                    Some(expr)
                };

                joins.push(crate::sql::ast::JoinClause { join_type, table, alias, predicate });
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
                    if tokens[idx] == "," {
                        idx += 1;
                        continue;
                    }
                    let token = tokens[idx].trim_end_matches(',').trim_end_matches(';');
                    if token.eq_ignore_ascii_case("ORDER") || token.eq_ignore_ascii_case("WHERE") || token.eq_ignore_ascii_case("HAVING") { break; }
                    cols.push(unquote_token(token).to_string());
                    idx += 1;
                    if idx >= tokens.len() { break; }
                }
                group_by = Some(cols);
            }

            let mut having = None;
            if idx < tokens.len() && tokens[idx].eq_ignore_ascii_case("HAVING") {
                let (expr, consumed) = parse_expression(&tokens[idx + 1..])?;
                having = Some(expr);
                idx += consumed + 1;
            }

            let mut order_by = None;
            if idx + 1 < tokens.len()
                && tokens[idx].eq_ignore_ascii_case("ORDER")
                && tokens[idx + 1].eq_ignore_ascii_case("BY")
            {
                idx += 2;
                if idx >= tokens.len() {
                    return Err("Expected column after ORDER BY".into());
                }
                if tokens[idx] == "," {
                    return Err("Expected column after ORDER BY".into());
                }
                let column = tokens[idx].trim_end_matches(',').trim_end_matches(';');
                if idx + 1 < tokens.len() && tokens[idx + 1] == "," {
                    return Err("Only one ORDER BY column is supported".into());
                }
                let mut descending = false;
                idx += 1;
                if idx < tokens.len() {
                    let keyword = tokens[idx].trim_end_matches(';');
                    if keyword.eq_ignore_ascii_case("DESC") {
                        descending = true;
                        idx += 1;
                    } else if keyword.eq_ignore_ascii_case("ASC") {
                        idx += 1;
                    }
                }
                if idx < tokens.len() {
                    let keyword = tokens[idx].trim_end_matches(';');
                    if !keyword.eq_ignore_ascii_case("LIMIT") && !keyword.eq_ignore_ascii_case("OFFSET") {
                        return Err("Unexpected token after ORDER BY clause".into());
                    }
                }
                if column.is_empty() {
                    return Err("Expected column after ORDER BY".into());
                }
                if column.contains(',') {
                    return Err("Unexpected token after ORDER BY clause".into());
                }
                order_by = Some(OrderBy { column: unquote_token(column).to_string(), descending });
            }

            let mut limit = None;
            if idx < tokens.len() && tokens[idx].trim_end_matches(';').eq_ignore_ascii_case("LIMIT") {
                idx += 1;
                if idx >= tokens.len() {
                    return Err("Expected value after LIMIT".into());
                }
                let raw = tokens[idx].trim_end_matches(';');
                let value = raw.parse::<usize>().map_err(|_| "Invalid LIMIT value".to_string())?;
                limit = Some(value);
                idx += 1;
            }

            let mut offset = None;
            if idx < tokens.len() && tokens[idx].trim_end_matches(';').eq_ignore_ascii_case("OFFSET") {
                idx += 1;
                if idx >= tokens.len() {
                    return Err("Expected value after OFFSET".into());
                }
                let raw = tokens[idx].trim_end_matches(';');
                let value = raw.parse::<usize>().map_err(|_| "Invalid OFFSET value".to_string())?;
                offset = Some(value);
                idx += 1;
            }

            Ok(Statement::Select { columns, from, joins, where_predicate, group_by, having, order_by, limit, offset })
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
                let table = unquote_token(&tokens[idx]).trim_end_matches(';').to_string();
                Ok(Statement::DropTable { table_name: table, if_exists })
            } else if tokens[1].eq_ignore_ascii_case("INDEX") {
                if tokens.len() < 3 {
                    return Err("Usage: DROP INDEX <name>".to_string());
                }
                let name = unquote_token(&tokens[2]).trim_end_matches(';').to_string();
                Ok(Statement::DropIndex { name })
            } else {
                Err("Usage: DROP TABLE <name>".to_string())
            }
        }
        "DELETE" => {
            if tokens.len() < 5 || !tokens[1].eq_ignore_ascii_case("FROM") || !tokens[3].eq_ignore_ascii_case("WHERE") {
                return Err("Usage: DELETE FROM <table> WHERE <expr>".to_string());
            }
            let table = unquote_token(&tokens[2]).trim_end_matches(';').to_string();
            let (expr, _) = parse_expression(&tokens[4..])?;
            Ok(Statement::Delete { table_name: table, selection: Some(expr) })
        }
        "UPDATE" => {
            if tokens.len() < 4 || !tokens[2].eq_ignore_ascii_case("SET") {
                return Err("Usage: UPDATE <table> SET col = val [, ...] [WHERE <expr>]".to_string());
            }
            let table = unquote_token(&tokens[1]).to_string();
            let mut idx = 3;
            let mut assignments = Vec::new();
            while idx < tokens.len() {
                if tokens[idx].eq_ignore_ascii_case("WHERE") {
                    break;
                }
                if tokens[idx] == "," {
                    idx += 1;
                    continue;
                }
                let col = unquote_token(&tokens[idx]).trim_end_matches(',').to_string();
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
