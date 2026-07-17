use crate::error::{DbError, DbResult};
use crate::sql::ast::Expr;
use crate::sql::functions::FunctionEvaluator;
use crate::storage::row::ColumnValue;

pub struct DefaultConstraint;

impl DefaultConstraint {
    pub fn evaluate(expr: &Expr) -> DbResult<String> {
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
                    Err(_) => Err(DbError::InvalidValue("function error".into())),
                }
            }
            _ => Err(DbError::InvalidValue(
                "unsupported default expression".into(),
            )),
        }
    }
}

use super::Constraint;
use crate::catalog::{Catalog, TableInfo};
use crate::storage::row::RowData;
use crate::transaction::Snapshot;

impl Constraint for DefaultConstraint {
    fn validate_insert(
        &self,
        _catalog: &mut Catalog,
        _table: &TableInfo,
        _row: &mut RowData,
        _snapshot: &Snapshot,
    ) -> DbResult<()> {
        Ok(())
    }
}
