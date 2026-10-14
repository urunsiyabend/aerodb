use crate::error::DbResult;
use crate::execution::eval::evaluate_constant;
use crate::sql::ast::Expr;
use crate::sql::value::Value;

pub struct DefaultConstraint;

impl DefaultConstraint {
    /// Evaluate a column DEFAULT expression. The caller coerces the result to
    /// the column type with `Value::to_column_value`.
    pub fn evaluate(expr: &Expr) -> DbResult<Value> {
        evaluate_constant(expr)
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
