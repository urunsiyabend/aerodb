//! Eagerly-evaluated scalar SQL functions over typed values. COALESCE and
//! NULLIF are evaluated lazily by the expression evaluator instead.

use chrono::{Local, Utc};

use crate::error::{DbError, DbResult};
use crate::sql::value::Value;

fn text_arg<'a>(name: &str, v: &'a Value) -> DbResult<&'a str> {
    match v {
        Value::Text(s) => Ok(s),
        // CHAR values behave as their trimmed text (PAD SPACE semantics).
        Value::Char(s) => Ok(s.trim_end_matches(' ')),
        other => Err(DbError::TypeMismatch(format!(
            "function {} is not defined for {}",
            name,
            other.type_name()
        ))),
    }
}

/// Evaluate a scalar function whose arguments are already computed.
pub fn call_scalar(name: &str, args: &[Value]) -> DbResult<Value> {
    let arity = |n: usize| -> DbResult<()> {
        if args.len() == n {
            Ok(())
        } else {
            Err(DbError::UndefinedFunction(format!("{} with {} argument(s)", name, args.len())))
        }
    };
    match name {
        "CURRENT_TIMESTAMP" | "GETDATE" => {
            arity(0)?;
            Ok(Value::DateTime(Local::now().timestamp()))
        }
        "GETUTCDATE" => {
            arity(0)?;
            Ok(Value::DateTime(Utc::now().timestamp()))
        }
        "UPPER" | "LOWER" | "CHAR_LENGTH" | "CHARACTER_LENGTH" | "ABS" => {
            arity(1)?;
            let v = &args[0];
            if v.is_null() {
                return Ok(Value::Null);
            }
            match name {
                "UPPER" => Ok(Value::Text(text_arg(name, v)?.to_uppercase())),
                "LOWER" => Ok(Value::Text(text_arg(name, v)?.to_lowercase())),
                "CHAR_LENGTH" | "CHARACTER_LENGTH" => {
                    Ok(Value::Int(text_arg(name, v)?.chars().count() as i64))
                }
                _ => match v {
                    Value::Int(i) => i
                        .checked_abs()
                        .map(Value::Int)
                        .ok_or_else(|| DbError::NumericOverflow("ABS of the minimum integer".into())),
                    Value::Decimal(d) => Ok(Value::Decimal(if d.mantissa() < 0 { d.neg() } else { *d })),
                    Value::Double(f) => Ok(Value::Double(f.abs())),
                    other => Err(DbError::TypeMismatch(format!(
                        "function ABS is not defined for {}",
                        other.type_name()
                    ))),
                },
            }
        }
        _ => Err(DbError::UndefinedFunction(name.to_string())),
    }
}
