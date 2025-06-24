use crate::{catalog::Catalog, storage::pager::Pager, sql::ast::Statement, execution::runtime::handle_statement, error::DbResult};

#[derive(PartialEq)]
enum TransactionMode {
    None,
    Implicit,
    Explicit,
}

pub struct Engine {
    pub catalog: Catalog,
    tx_mode: TransactionMode,
}

impl Engine {
    pub fn new(filename: &str) -> Self {
        let catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();
        Engine { catalog, tx_mode: TransactionMode::None }
    }

    pub fn execute(&mut self, stmt: Statement) -> DbResult<()> {
        use Statement::*;

        match &stmt {
            BeginTransaction { name } => {
                if self.tx_mode == TransactionMode::Implicit && self.catalog.transaction_active() {
                    self.catalog.commit_transaction()?;
                }
                self.catalog.begin_transaction(name.clone())?;
                self.tx_mode = TransactionMode::Explicit;
                return Ok(());
            }
            Commit => {
                self.catalog.commit_transaction()?;
                self.tx_mode = TransactionMode::None;
                return Ok(());
            }
            Rollback => {
                self.catalog.rollback_transaction()?;
                self.tx_mode = TransactionMode::None;
                return Ok(());
            }
            _ => {}
        }

        let mut implicit = false;
        let is_mutating = matches!(
            stmt,
            Insert { .. }
                | Update { .. }
                | Delete { .. }
                | CreateTable { .. }
                | DropTable { .. }
                | CreateIndex { .. }
                | DropIndex { .. }
                | CreateSequence(_)
        );

        if is_mutating && !self.catalog.transaction_active() {
            self.catalog.begin_transaction(None)?;
            self.tx_mode = TransactionMode::Implicit;
            implicit = true;
        }

        let res = handle_statement(&mut self.catalog, stmt);

        if implicit {
            if res.is_ok() {
                self.catalog.commit_transaction()?;
            } else {
                self.catalog.rollback_transaction()?;
            }
            self.tx_mode = TransactionMode::None;
        }

        res
    }
}
