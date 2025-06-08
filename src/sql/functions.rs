use chrono::{Local, Utc};
use crate::storage::row::ColumnValue;

pub enum EvalError {
    UnknownFunction(String),
    InvalidArgumentCount,
}

pub struct FunctionEvaluator;

impl FunctionEvaluator {
    pub fn evaluate_function(name: &str, args: &[ColumnValue]) -> Result<ColumnValue, EvalError> {
        match name.to_uppercase().as_str() {
            "CURRENT_TIMESTAMP" | "GETDATE" => {
                if !args.is_empty() {
                    return Err(EvalError::InvalidArgumentCount);
                }
                Ok(ColumnValue::DateTime(Local::now().timestamp()))
            }
            "GETUTCDATE" => {
                if !args.is_empty() {
                    return Err(EvalError::InvalidArgumentCount);
                }
                Ok(ColumnValue::DateTime(Utc::now().timestamp()))
            }
            _ => Err(EvalError::UnknownFunction(name.to_string())),
        }
    }
}
