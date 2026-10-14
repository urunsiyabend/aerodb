//! SQL tokenizer. Every token carries the byte offset where it starts so the
//! parser can report precise syntax errors.

use crate::error::{DbError, DbResult};

#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    /// Unquoted identifier or keyword, original spelling preserved.
    Ident(String),
    /// `"quoted identifier"` with `""` unescaped.
    QuotedIdent(String),
    /// `'string literal'` with `''` unescaped.
    String(String),
    /// Integer literal text (digits only).
    Integer(String),
    /// Exact numeric literal with a decimal point (`1.5`, `.5`, `1.`).
    Decimal(String),
    /// Approximate numeric literal with an exponent (`1e3`, `1.5E-2`).
    Float(String),
    /// Operator or punctuation symbol.
    Symbol(&'static str),
    Eof,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub kind: TokenKind,
    pub offset: usize,
}

impl Token {
    /// True when this token is the unquoted keyword `kw` (case-insensitive).
    pub fn is_keyword(&self, kw: &str) -> bool {
        matches!(&self.kind, TokenKind::Ident(s) if s.eq_ignore_ascii_case(kw))
    }

    pub fn is_symbol(&self, sym: &str) -> bool {
        matches!(&self.kind, TokenKind::Symbol(s) if *s == sym)
    }

    /// Human-readable rendering used in error messages.
    pub fn describe(&self) -> String {
        match &self.kind {
            TokenKind::Ident(s) => s.clone(),
            TokenKind::QuotedIdent(s) => format!("\"{}\"", s),
            TokenKind::String(s) => format!("'{}'", s),
            TokenKind::Integer(s) | TokenKind::Decimal(s) | TokenKind::Float(s) => s.clone(),
            TokenKind::Symbol(s) => s.to_string(),
            TokenKind::Eof => "end of input".into(),
        }
    }
}

const SYMBOLS: [&str; 21] = [
    "<>", "!=", "<=", ">=", "||", "=", "<", ">", "+", "-", "*", "/", "%", "&", "|", "^", "(", ")",
    ",", ".", ";",
];

fn syntax(offset: usize, msg: &str) -> DbError {
    DbError::Syntax(format!("{} at byte {}", msg, offset))
}

/// Tokenize `input`. The returned vector always ends with an `Eof` token.
pub fn tokenize(input: &str) -> DbResult<Vec<Token>> {
    let bytes = input.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        // Comments.
        if c == b'-' && bytes.get(i + 1) == Some(&b'-') {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if c == b'/' && bytes.get(i + 1) == Some(&b'*') {
            let start = i;
            i += 2;
            loop {
                if i + 1 >= bytes.len() {
                    return Err(syntax(start, "unterminated block comment"));
                }
                if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                    i += 2;
                    break;
                }
                i += 1;
            }
            continue;
        }
        let start = i;
        // Quoted strings and identifiers.
        if c == b'\'' || c == b'"' {
            let quote = c;
            i += 1;
            let mut text = String::new();
            let mut seg_start = i;
            loop {
                if i >= bytes.len() {
                    let what = if quote == b'\'' {
                        "string literal"
                    } else {
                        "quoted identifier"
                    };
                    return Err(syntax(start, &format!("unterminated {}", what)));
                }
                if bytes[i] == quote {
                    text.push_str(&input[seg_start..i]);
                    if bytes.get(i + 1) == Some(&quote) {
                        text.push(quote as char);
                        i += 2;
                        seg_start = i;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            let kind = if quote == b'\'' {
                TokenKind::String(text)
            } else {
                if text.is_empty() {
                    return Err(syntax(start, "zero-length quoted identifier"));
                }
                TokenKind::QuotedIdent(text)
            };
            tokens.push(Token {
                kind,
                offset: start,
            });
            continue;
        }
        // Numbers: digits, optional fraction, optional exponent; or `.digits`.
        if c.is_ascii_digit() || (c == b'.' && bytes.get(i + 1).is_some_and(|b| b.is_ascii_digit()))
        {
            let mut has_point = false;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            if i < bytes.len() && bytes[i] == b'.' {
                has_point = true;
                i += 1;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
            }
            let mut has_exp = false;
            if i < bytes.len() && (bytes[i] == b'e' || bytes[i] == b'E') {
                let mut j = i + 1;
                if j < bytes.len() && (bytes[j] == b'+' || bytes[j] == b'-') {
                    j += 1;
                }
                if j < bytes.len() && bytes[j].is_ascii_digit() {
                    has_exp = true;
                    i = j;
                    while i < bytes.len() && bytes[i].is_ascii_digit() {
                        i += 1;
                    }
                } else {
                    return Err(syntax(start, "malformed numeric literal"));
                }
            }
            // A number immediately followed by an identifier character is malformed (e.g. `12abc`).
            if i < bytes.len() && (bytes[i].is_ascii_alphabetic() || bytes[i] == b'_') {
                return Err(syntax(start, "malformed numeric literal"));
            }
            let text = input[start..i].to_string();
            let kind = if has_exp {
                TokenKind::Float(text)
            } else if has_point {
                TokenKind::Decimal(text)
            } else {
                TokenKind::Integer(text)
            };
            tokens.push(Token {
                kind,
                offset: start,
            });
            continue;
        }
        // Identifiers / keywords.
        let ch = input[i..].chars().next().unwrap();
        if ch.is_alphabetic() || ch == '_' {
            let mut end = i;
            for (off, ch) in input[i..].char_indices() {
                if ch.is_alphanumeric() || ch == '_' {
                    end = i + off + ch.len_utf8();
                } else {
                    break;
                }
            }
            tokens.push(Token {
                kind: TokenKind::Ident(input[i..end].to_string()),
                offset: start,
            });
            i = end;
            continue;
        }
        // Symbols (longest match first).
        let rest = &input[i..];
        if let Some(sym) = SYMBOLS.iter().find(|s| rest.starts_with(**s)) {
            tokens.push(Token {
                kind: TokenKind::Symbol(sym),
                offset: start,
            });
            i += sym.len();
            continue;
        }
        return Err(syntax(start, &format!("unexpected character '{}'", ch)));
    }
    tokens.push(Token {
        kind: TokenKind::Eof,
        offset: input.len(),
    });
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(s: &str) -> Vec<TokenKind> {
        tokenize(s).unwrap().into_iter().map(|t| t.kind).collect()
    }

    #[test]
    fn numbers_by_kind() {
        assert_eq!(
            kinds("12 1.5 .5 1. 1e3 1.5E-2"),
            vec![
                TokenKind::Integer("12".into()),
                TokenKind::Decimal("1.5".into()),
                TokenKind::Decimal(".5".into()),
                TokenKind::Decimal("1.".into()),
                TokenKind::Float("1e3".into()),
                TokenKind::Float("1.5E-2".into()),
                TokenKind::Eof,
            ]
        );
    }

    #[test]
    fn quotes_and_escapes() {
        assert_eq!(
            kinds("'it''s' \"a \"\"b\"\"\""),
            vec![
                TokenKind::String("it's".into()),
                TokenKind::QuotedIdent("a \"b\"".into()),
                TokenKind::Eof
            ]
        );
    }

    #[test]
    fn comments_are_skipped() {
        assert_eq!(
            kinds("-- x\n/* y */ a"),
            vec![TokenKind::Ident("a".into()), TokenKind::Eof]
        );
        assert!(tokenize("/* open").is_err());
        assert!(tokenize("'open").is_err());
    }

    #[test]
    fn offsets_and_operators() {
        let toks = tokenize("a<>b || c").unwrap();
        assert_eq!(toks[1].kind, TokenKind::Symbol("<>"));
        assert_eq!(toks[1].offset, 1);
        assert_eq!(toks[3].kind, TokenKind::Symbol("||"));
        assert!(tokenize("a @ b").is_err());
    }
}
