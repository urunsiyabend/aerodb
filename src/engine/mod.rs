use crate::{
    catalog::Catalog, error::DbResult, execution::runtime::handle_statement, sql::ast::Statement,
    storage::pager::Pager, storage::vacuum::VacuumReport, transaction::TransactionManager,
};

pub struct Engine {
    pub catalog: Catalog,
    transaction_manager: TransactionManager,
}

impl Engine {
    pub fn new(filename: &str) -> Self {
        let catalog = Catalog::open(Pager::new(filename).unwrap()).unwrap();
        Engine {
            catalog,
            transaction_manager: TransactionManager::new(),
        }
    }

    pub fn execute(&mut self, stmt: Statement) -> DbResult<()> {
        self.transaction_manager
            .execute(&mut self.catalog, stmt, handle_statement)
    }

    /// Physically prune obsolete MVCC versions from `table_name`. The vacuum
    /// cutoff (`global_xmin`) comes from the transaction manager, which owns the
    /// set of currently-live transactions.
    pub fn vacuum_table(&mut self, table_name: &str) -> std::io::Result<VacuumReport> {
        let global_xmin = self.transaction_manager.global_xmin(&self.catalog);
        self.catalog.vacuum_table(table_name, global_xmin)
    }
}
