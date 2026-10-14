use crate::{
    catalog::Catalog,
    error::DbResult,
    execution::query::{QueryResult, execute_query},
    execution::runtime::handle_statement,
    sql::ast::Statement,
    sql::parser::parse_sql,
    storage::pager::Pager,
    storage::vacuum::VacuumReport,
    transaction::TransactionManager,
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

    /// Open (or create) the database at `filename`, reporting open errors.
    pub fn open(filename: &str) -> DbResult<Self> {
        let catalog = Catalog::open(Pager::new(filename)?)?;
        Ok(Engine {
            catalog,
            transaction_manager: TransactionManager::new(),
        })
    }

    pub fn execute(&mut self, stmt: Statement) -> DbResult<()> {
        self.transaction_manager
            .execute(&mut self.catalog, stmt, handle_statement)
    }

    /// Parse and execute one SQL statement (printing query results like the REPL).
    ///
    /// This is an embedded, typed Rust API: it is not the SQL standard's
    /// SQL/CLI binding, nor an ODBC or JDBC driver. Errors are
    /// [`crate::error::DbError`] values; [`crate::error::DbError::sqlstate`]
    /// gives their SQLSTATE class.
    pub fn execute_sql(&mut self, sql: &str) -> DbResult<()> {
        let stmt = self.parse(sql)?;
        self.execute(stmt)
    }

    /// Parse one statement; a syntax error inside an explicit transaction
    /// fails the transaction like any other statement error (so callers
    /// that parse and [`Engine::execute`] separately, like the REPL, keep
    /// the 25P02 rule).
    pub fn parse(&mut self, sql: &str) -> DbResult<Statement> {
        parse_sql(sql).inspect_err(|_| self.transaction_manager.statement_failed(&self.catalog))
    }

    /// Parse and run one statement inside the transaction manager, returning
    /// the typed result rows of a query without printing them. Non-query
    /// statements are executed and yield an empty result.
    ///
    /// Like [`Engine::execute_sql`], a typed Rust API (not SQL/CLI, ODBC or
    /// JDBC): values come back as [`crate::sql::value::Value`]s with result
    /// column types, errors as [`crate::error::DbError`] with a SQLSTATE.
    pub fn query(&mut self, sql: &str) -> DbResult<QueryResult> {
        let stmt = self.parse(sql)?;
        let mut result = QueryResult::default();
        self.transaction_manager
            .execute(&mut self.catalog, stmt, |catalog, stmt| match stmt {
                Statement::Query(q) => {
                    result = execute_query(catalog, &q, None)?;
                    Ok(())
                }
                other => handle_statement(catalog, other),
            })?;
        Ok(result)
    }

    /// Physically prune obsolete MVCC versions from `table_name`. The vacuum
    /// cutoff (`global_xmin`) comes from the transaction manager, which owns the
    /// set of currently-live transactions.
    pub fn vacuum_table(&mut self, table_name: &str) -> std::io::Result<VacuumReport> {
        let global_xmin = self.transaction_manager.global_xmin(&self.catalog);
        self.catalog.vacuum_table(table_name, global_xmin)
    }

    /// Vacuum every table in one pass and advance the durable frozen watermark,
    /// reclaiming the clog statuses below it. Use this rather than repeated
    /// [`Engine::vacuum_table`] calls when the goal is to bound clog growth: the
    /// watermark can only be advanced from an all-tables sweep.
    pub fn vacuum_all(&mut self) -> std::io::Result<VacuumReport> {
        let global_xmin = self.transaction_manager.global_xmin(&self.catalog);
        self.catalog.vacuum_all(global_xmin)
    }

    /// The durable frozen watermark: transaction ids below it have had their clog
    /// status reclaimed and are unconditionally treated as committed.
    pub fn frozen_xid(&self) -> crate::transaction::TransactionId {
        self.catalog.frozen_xid()
    }
}
