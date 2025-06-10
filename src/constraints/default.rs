use std::io;
use crate::sql::ast::Expr;
use crate::storage::row::ColumnValue;
use crate::sql::functions::FunctionEvaluator;

pub struct DefaultConstraint;

impl DefaultConstraint {
    pub fn evaluate(expr: &Expr) -> io::Result<String> {
        match expr {
            Expr::Literal(s) => Ok(s.clone()),
            Expr::FunctionCall { name, args } => {
                let arg_vals: Vec<ColumnValue> = args
                    .iter()
                    .map(|e| match Self::evaluate(e) {
                        Ok(s) => Ok(ColumnValue::Text(s)),
                        Err(e) => Err(e),
                    })
                    .collect::<Result<_, _>>()?;
                match FunctionEvaluator::evaluate_function(name, &arg_vals) {
                    Ok(val) => Ok(val.to_string_value()),
                    Err(_) => Err(io::Error::new(io::ErrorKind::Other, "function error")),
                }
            }
            _ => Err(io::Error::new(io::ErrorKind::Other, "unsupported default expression")),
        }
    }
}

use crate::catalog::{Catalog, TableInfo};
use crate::storage::row::RowData;
use super::Constraint;

impl Constraint for DefaultConstraint {
    fn validate_insert(&self, _catalog: &mut Catalog, _table: &TableInfo, _row: &mut RowData) -> io::Result<()> {
        Ok(())
    }
}
