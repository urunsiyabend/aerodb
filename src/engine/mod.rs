use crate::{
    catalog::Catalog, error::DbResult, execution::runtime::handle_statement, sql::ast::Statement,
    storage::pager::Pager, transaction::TransactionManager,
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
}
