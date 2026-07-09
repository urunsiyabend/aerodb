use crate::{catalog::Catalog, error::DbResult, sql::ast::Statement};
use std::io;

use super::{statement_requires_transaction, TransactionMode};

/// Coordinates SQL transaction boundaries for the engine while keeping statement
/// execution itself outside the transaction module.
#[derive(Debug, Default)]
pub struct TransactionManager {
    mode: TransactionMode,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn execute<F>(
        &mut self,
        catalog: &mut Catalog,
        stmt: Statement,
        execute_stmt: F,
    ) -> DbResult<()>
    where
        F: FnOnce(&mut Catalog, Statement) -> DbResult<()>,
    {
        if self.handle_transaction_control(catalog, &stmt)? {
            return Ok(());
        }

        let implicit = self.begin_implicit_if_needed(catalog, &stmt)?;
        let result = execute_stmt(catalog, stmt);
        self.finish_implicit_if_needed(catalog, implicit, result)
    }

    fn handle_transaction_control(
        &mut self,
        catalog: &mut Catalog,
        stmt: &Statement,
    ) -> io::Result<bool> {
        match stmt {
            Statement::BeginTransaction { name } => {
                if self.mode.is_implicit() && catalog.transaction_active() {
                    catalog.commit_transaction()?;
                }
                catalog.begin_transaction(name.clone())?;
                self.mode = TransactionMode::Explicit;
                Ok(true)
            }
            Statement::Commit => {
                catalog.commit_transaction()?;
                self.mode = TransactionMode::None;
                Ok(true)
            }
            Statement::Rollback => {
                catalog.rollback_transaction()?;
                self.mode = TransactionMode::None;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn begin_implicit_if_needed(
        &mut self,
        catalog: &mut Catalog,
        stmt: &Statement,
    ) -> io::Result<bool> {
        if statement_requires_transaction(stmt) && !catalog.transaction_active() {
            catalog.begin_transaction(None)?;
            self.mode = TransactionMode::Implicit;
            return Ok(true);
        }

        Ok(false)
    }

    fn finish_implicit_if_needed(
        &mut self,
        catalog: &mut Catalog,
        implicit: bool,
        result: DbResult<()>,
    ) -> DbResult<()> {
        if !implicit {
            return result;
        }

        if result.is_ok() {
            catalog.commit_transaction()?;
        } else {
            catalog.rollback_transaction()?;
        }
        self.mode = TransactionMode::None;
        result
    }
}
