use crate::storage::btree::BTree;
use crate::{catalog::Catalog, error::DbError, error::DbResult, sql::ast::Statement};
use log::debug;
use std::collections::{BTreeSet, HashMap};
use std::io;

use super::{
    IsolationLevel, Snapshot, Transaction, TransactionId, TransactionMode,
    statement_requires_transaction,
};

/// Owns the transaction lifecycle for the engine: the set of currently live
/// transactions, the begin/commit/abort mechanics, and the auto-commit vs
/// user-managed mode. Statement execution itself stays outside this module.
///
/// The durable id/commit-ts counters and the `TransactionTable` still live in
/// the pager (their home is page-0 meta and the WAL); the manager drives them
/// through the pager. A later phase hoists the whole thing into a shared,
/// thread-safe storage layer so several sessions can be live at once.
#[derive(Debug, Default)]
pub struct TransactionManager {
    mode: TransactionMode,
    /// Transactions that are currently live. Replaces the former
    /// `Catalog.active_tx_ids`; used to build each new snapshot's concurrent set
    /// and to compute `global_xmin` for vacuum.
    active: BTreeSet<TransactionId>,
    /// Live transactions detached from the pager's single execution slot. Under
    /// multi-session operation the manager owns each `Transaction` between
    /// statements and installs one into the pager for the duration of each
    /// operation (operations are serialized by the shared-storage lock). The
    /// single-session `Engine` path leaves this empty.
    detached: HashMap<TransactionId, Transaction>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Begin a transaction: allocate an id, capture its snapshot over the
    /// currently-live set, start it in the pager, and record it as active.
    pub fn begin(
        &mut self,
        catalog: &mut Catalog,
        name: Option<String>,
        isolation_level: IsolationLevel,
    ) -> io::Result<()> {
        if catalog.transaction_active() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "transaction already active",
            ));
        }

        let transaction_id = catalog.pager.allocate_transaction_id();
        let snapshot = Snapshot::new_for_transaction(
            transaction_id,
            catalog.pager.peek_next_transaction_id(),
            self.active.iter().copied().collect(),
        );

        debug!(
            "Transaction started with id: {}, snapshot: {:?}, name: {:?}, isolation: {:?}",
            transaction_id, snapshot, name, isolation_level
        );
        catalog
            .pager
            .begin_transaction(transaction_id, snapshot, name, isolation_level)?;
        self.active.insert(transaction_id);
        // Capture the index map so ROLLBACK can undo DDL that mutated it.
        catalog.capture_pre_tx_indexes();
        Ok(())
    }

    /// Commit the live transaction: recheck deferred constraints, flush its
    /// pages durably, then drop it from the active set.
    pub fn commit(&mut self, catalog: &mut Catalog) -> io::Result<()> {
        let transaction_id = catalog.transaction_id();
        debug!("Transaction committed: {:?}", transaction_id);
        if let Some(transaction_id) = transaction_id {
            catalog
                .recheck_constraints_for_commit(transaction_id)
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
        }
        catalog.pager.commit_transaction()?;
        if let Some(transaction_id) = transaction_id {
            self.active.remove(&transaction_id);
        }
        // Committed DDL keeps its index changes; drop the rollback snapshot.
        catalog.clear_pre_tx_indexes();
        Ok(())
    }

    /// Roll back the live transaction: revert its pages, drop it from the active
    /// set, then rebuild the in-memory catalog/index maps captured at BEGIN.
    pub fn rollback(&mut self, catalog: &mut Catalog) -> io::Result<()> {
        let transaction_id = catalog.transaction_id();
        catalog.pager.rollback_transaction()?;
        if let Some(transaction_id) = transaction_id {
            self.active.remove(&transaction_id);
        }
        // Page-level rollback restores table/sequence catalog pages; rebuild the
        // in-memory maps from them, then restore the (non-persisted) index map
        // captured at BEGIN so aborted DDL leaves no trace.
        catalog.reload_tables()?;
        catalog.restore_pre_tx_indexes();
        Ok(())
    }

    /// Oldest snapshot boundary that can still observe deleted versions.
    ///
    /// With no live transactions the next transaction id is the safe cutoff;
    /// otherwise the oldest live transaction id protects all versions deleted by
    /// that transaction or any newer one.
    pub fn global_xmin(&self, catalog: &Catalog) -> TransactionId {
        self.active
            .iter()
            .copied()
            .min()
            .unwrap_or_else(|| catalog.pager.peek_next_transaction_id())
    }

    // ---- Multi-session API --------------------------------------------------
    //
    // These support several concurrently-live transactions sharing one storage
    // backend, each identified by its id. A transaction is begun detached (its
    // `Transaction` is held here, not in the pager), and each operation installs
    // it into the pager's single execution slot for the duration of the call.
    // Callers serialize access with the shared-storage lock.

    /// Begin a transaction and hold it detached, returning its id.
    pub fn begin_detached(
        &mut self,
        catalog: &mut Catalog,
        name: Option<String>,
        isolation_level: IsolationLevel,
    ) -> io::Result<TransactionId> {
        self.begin(catalog, name, isolation_level)?;
        let transaction = catalog
            .pager
            .uninstall_transaction()
            .expect("begin installed a transaction");
        let id = transaction.id();
        self.detached.insert(id, transaction);
        Ok(id)
    }

    /// Run `f` with transaction `tx_id` installed in the pager, then detach it
    /// again. Panics if `tx_id` is not a live detached transaction.
    pub fn run_with<F, R>(&mut self, catalog: &mut Catalog, tx_id: TransactionId, f: F) -> R
    where
        F: FnOnce(&mut Catalog) -> R,
    {
        let transaction = self
            .detached
            .remove(&tx_id)
            .expect("run_with called for a live transaction");
        catalog.pager.install_transaction(transaction);
        let result = f(catalog);
        let transaction = catalog
            .pager
            .uninstall_transaction()
            .expect("transaction still installed");
        self.detached.insert(tx_id, transaction);
        result
    }

    /// Commit detached transaction `tx_id`, first re-validating write/write
    /// conflicts (first-committer-wins). On conflict the transaction is aborted
    /// and `WriteConflict` returned.
    pub fn commit_detached(
        &mut self,
        catalog: &mut Catalog,
        tx_id: TransactionId,
    ) -> DbResult<()> {
        let transaction = self
            .detached
            .remove(&tx_id)
            .expect("commit_detached called for a live transaction");
        catalog.pager.install_transaction(transaction);

        if let Some(conflict_key) = self.first_conflicting_key(catalog) {
            // Loser of first-committer-wins: abort and report the conflict.
            self.commit_cleanup_after_abort(catalog);
            return Err(DbError::WriteConflict(conflict_key));
        }

        self.commit(catalog)?;
        Ok(())
    }

    /// Abort detached transaction `tx_id`.
    pub fn abort_detached(&mut self, catalog: &mut Catalog, tx_id: TransactionId) -> io::Result<()> {
        let transaction = self
            .detached
            .remove(&tx_id)
            .expect("abort_detached called for a live transaction");
        catalog.pager.install_transaction(transaction);
        self.rollback(catalog)
    }

    /// Re-check every recorded write intent of the installed transaction against
    /// the current version chains. Returns the first logical key that another,
    /// non-visible transaction has changed since this transaction's snapshot.
    fn first_conflicting_key(&self, catalog: &mut Catalog) -> Option<i32> {
        let snapshot = catalog.transaction_snapshot().cloned()?;
        let intents = catalog.pager.transaction_write_set();
        for intent in intents {
            let conflict = BTree::open_root(&mut catalog.pager, intent.table_root)
                .and_then(|mut tree| {
                    tree.has_write_conflict(intent.key, intent.visible_created_tx, &snapshot)
                })
                .unwrap_or(false);
            if conflict {
                return Some(intent.key);
            }
        }
        None
    }

    /// Abort the installed transaction as part of losing a commit conflict.
    fn commit_cleanup_after_abort(&mut self, catalog: &mut Catalog) {
        let _ = self.rollback(catalog);
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
                    self.commit(catalog)?;
                }
                self.begin(catalog, name.clone(), IsolationLevel::default())?;
                self.mode = TransactionMode::Explicit;
                Ok(true)
            }
            Statement::Commit => {
                self.commit(catalog)?;
                self.mode = TransactionMode::None;
                Ok(true)
            }
            Statement::Rollback => {
                self.rollback(catalog)?;
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
            self.begin(catalog, None, IsolationLevel::default())?;
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
            self.commit(catalog)?;
        } else {
            self.rollback(catalog)?;
        }
        self.mode = TransactionMode::None;
        result
    }
}
