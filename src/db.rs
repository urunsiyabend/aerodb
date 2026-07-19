//! Concurrent, multi-session database facade.
//!
//! [`Database`] shares one storage backend across many simultaneously-live
//! transactions, each snapshot-isolated, with first-committer-wins write/write
//! conflict resolution. It is `Send + Sync + Clone` (an `Arc<Mutex<..>>`), so a
//! clone can be handed to each thread.
//!
//! Concurrency model: MVCC provides *logical* isolation (each transaction reads
//! its own snapshot; aborted/uncommitted versions are hidden via the durable
//! clog). *Physical* access to the shared pager/buffer-pool is currently guarded
//! by a single storage latch (the `Mutex` below), so operations are serialized
//! at the physical layer even though they interleave at the transaction layer.
//! Replacing that coarse latch with per-page latches (so readers and writers run
//! in parallel) is a performance refinement; correctness does not depend on it.
//!
//! The single-session [`crate::engine::Engine`] remains for the auto-commit /
//! implicit-transaction REPL and test surface; `Database` is the concurrent path.

use std::sync::{Arc, Mutex, MutexGuard};

use crate::{
    catalog::Catalog,
    error::DbResult,
    execution::runtime::{execute_select_with_indexes, handle_statement},
    sql::ast::Statement,
    storage::pager::Pager,
    storage::row::Row,
    transaction::{IsolationLevel, TransactionId, TransactionManager},
};

/// The shared, latched core: schema/storage plus the transaction manager that
/// owns the live-transaction set and id/commit-ts allocation.
struct Core {
    catalog: Catalog,
    manager: TransactionManager,
}

/// A handle to a live transaction. Cheap to copy and `Send`, so it can be moved
/// to the thread that drives its transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxHandle {
    id: TransactionId,
}

impl TxHandle {
    pub fn id(&self) -> TransactionId {
        self.id
    }
}

/// A concurrent handle to an AeroDB database.
#[derive(Clone)]
pub struct Database {
    core: Arc<Mutex<Core>>,
}

impl Database {
    /// Open (or create) the database at `filename`.
    pub fn open(filename: &str) -> DbResult<Self> {
        let catalog = Catalog::open(Pager::new(filename)?)?;
        Ok(Self {
            core: Arc::new(Mutex::new(Core {
                catalog,
                manager: TransactionManager::new(),
            })),
        })
    }

    /// Take the storage latch. A poisoned lock (a thread panicked mid-operation)
    /// is recovered rather than propagated: MVCC state is consistent because a
    /// panic leaves any in-flight transaction uncommitted (invisible).
    fn lock(&self) -> MutexGuard<'_, Core> {
        self.core.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Begin a new transaction with snapshot isolation.
    pub fn begin(&self) -> DbResult<TxHandle> {
        let mut core = self.lock();
        let Core { catalog, manager } = &mut *core;
        let id = manager.begin_detached(catalog, None, IsolationLevel::Snapshot)?;
        Ok(TxHandle { id })
    }

    /// Execute one DML/DDL statement within transaction `tx`.
    pub fn execute(&self, tx: &TxHandle, stmt: Statement) -> DbResult<()> {
        let mut core = self.lock();
        let Core { catalog, manager } = &mut *core;
        manager.run_with(catalog, tx.id, |catalog| handle_statement(catalog, stmt))
    }

    /// Read every row of `table` visible to `tx`'s snapshot.
    pub fn query_all(&self, tx: &TxHandle, table: &str) -> DbResult<Vec<Row>> {
        let mut core = self.lock();
        let Core { catalog, manager } = &mut *core;
        manager.run_with(catalog, tx.id, |catalog| {
            let mut rows = Vec::new();
            execute_select_with_indexes(catalog, table, None, &mut rows)?;
            Ok(rows)
        })
    }

    /// Commit `tx`. Re-validates write/write conflicts first; the loser of a
    /// first-committer-wins race is aborted and gets [`crate::error::DbError::WriteConflict`].
    pub fn commit(&self, tx: TxHandle) -> DbResult<()> {
        let mut core = self.lock();
        let Core { catalog, manager } = &mut *core;
        manager.commit_detached(catalog, tx.id)
    }

    /// Abort `tx` (logical abort: its versions are left hidden for vacuum).
    pub fn abort(&self, tx: TxHandle) -> DbResult<()> {
        let mut core = self.lock();
        let Core { catalog, manager } = &mut *core;
        manager.abort_detached(catalog, tx.id)?;
        Ok(())
    }

    /// Run one statement in its own transaction, committing on success and
    /// aborting on error.
    pub fn autocommit(&self, stmt: Statement) -> DbResult<()> {
        let tx = self.begin()?;
        match self.execute(&tx, stmt) {
            Ok(()) => self.commit(tx),
            Err(err) => {
                let _ = self.abort(tx);
                Err(err)
            }
        }
    }

    /// Physically reclaim obsolete MVCC versions of `table` (committed-deleted
    /// below `global_xmin`, and aborted-creator versions).
    pub fn vacuum_table(&self, table: &str) -> DbResult<crate::storage::vacuum::VacuumReport> {
        let mut core = self.lock();
        let Core { catalog, manager } = &mut *core;
        let global_xmin = manager.global_xmin(catalog);
        Ok(catalog.vacuum_table(table, global_xmin)?)
    }
}
