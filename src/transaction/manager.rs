use crate::storage::btree::BTree;
use crate::{catalog::Catalog, error::DbError, error::DbResult, sql::ast::Statement};
use log::{debug, error};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::io;

use super::{
    IsolationLevel, Snapshot, Transaction, TransactionId, TransactionMode, TransactionStatus,
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
    /// The explicit (Engine) transaction had a failed statement: every later
    /// statement except COMMIT/ROLLBACK is refused with `InFailedTransaction`
    /// and COMMIT rolls back (PostgreSQL's aborted-transaction state).
    failed: bool,
    /// Detached (Database) transactions in that same failed state.
    failed_detached: HashSet<TransactionId>,
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
    /// pages durably, then drop it from the active set. A constraint failing
    /// the recheck rolls the transaction back and is returned as-is.
    pub fn commit(&mut self, catalog: &mut Catalog) -> DbResult<()> {
        let transaction_id = catalog.transaction_id();
        debug!("Transaction committed: {:?}", transaction_id);
        if let Some(transaction_id) = transaction_id {
            if let Err(err) = catalog.recheck_constraints_for_commit(transaction_id) {
                let rollback = self.rollback(catalog);
                self.mode = TransactionMode::None;
                return Err(commit_failure(err, rollback));
            }
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
        // Every page of the (non-versioned) catalog trees as the aborted
        // transaction left them; the pager reverts the touched ones.
        let catalog_pages = catalog.catalog_tree_pages()?;
        catalog.pager.rollback_transaction_reverting(&catalog_pages)?;
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

    /// The error for an operation on `tx_id` that is not a live detached
    /// transaction: it already committed or aborted (a reused, copied
    /// handle), or never existed. SQLSTATE 25P01.
    fn not_active(&self, catalog: &Catalog, tx_id: TransactionId) -> DbError {
        let status = catalog.pager.with_transaction_table(|t| t.get(&tx_id).cloned());
        DbError::NoActiveTransaction(match status {
            Some(TransactionStatus::Committed(_)) => format!("transaction {} already committed", tx_id),
            Some(TransactionStatus::Aborted) => format!("transaction {} already aborted", tx_id),
            _ => format!("transaction {} is not active", tx_id),
        })
    }

    fn is_aborted(catalog: &Catalog, tx_id: TransactionId) -> bool {
        catalog
            .pager
            .with_transaction_table(|t| matches!(t.get(&tx_id), Some(TransactionStatus::Aborted)))
    }

    /// Run `f` with transaction `tx_id` installed in the pager, then detach it
    /// again. A `tx_id` that is not a live detached transaction is refused
    /// with `NoActiveTransaction` (25P01) without running `f`.
    pub fn run_with<F, R>(&mut self, catalog: &mut Catalog, tx_id: TransactionId, f: F) -> DbResult<R>
    where
        F: FnOnce(&mut Catalog) -> R,
    {
        let Some(transaction) = self.detached.remove(&tx_id) else {
            return Err(self.not_active(catalog, tx_id));
        };
        catalog.pager.install_transaction(transaction);
        let result = f(catalog);
        if let Some(transaction) = catalog.pager.uninstall_transaction() {
            self.detached.insert(tx_id, transaction);
        }
        Ok(result)
    }

    /// Run one statement `f` of detached transaction `tx_id` under the
    /// failed-transaction policy: a transaction with an earlier failed
    /// statement refuses it with `InFailedTransaction`; an error marks the
    /// transaction failed (its partial effects can then never commit). An
    /// ended or unknown transaction is refused with `NoActiveTransaction`.
    pub fn run_statement<F, R>(&mut self, catalog: &mut Catalog, tx_id: TransactionId, f: F) -> DbResult<R>
    where
        F: FnOnce(&mut Catalog) -> DbResult<R>,
    {
        if self.failed_detached.contains(&tx_id) {
            return Err(DbError::InFailedTransaction);
        }
        let result = self.run_with(catalog, tx_id, f)?;
        if result.is_err() {
            self.failed_detached.insert(tx_id);
        }
        result
    }

    /// Commit detached transaction `tx_id`, first re-validating write/write
    /// conflicts (first-committer-wins). On conflict the transaction is aborted
    /// and `WriteConflict` returned. A failed transaction (see
    /// [`TransactionManager::run_statement`]) is rolled back instead and
    /// `InFailedTransaction` returned. An ended or unknown transaction is
    /// refused with `NoActiveTransaction` (25P01).
    pub fn commit_detached(
        &mut self,
        catalog: &mut Catalog,
        tx_id: TransactionId,
    ) -> DbResult<()> {
        if self.failed_detached.contains(&tx_id) {
            // A rollback failure is reported, never swallowed.
            self.abort_detached(catalog, tx_id)?;
            return Err(DbError::InFailedTransaction);
        }
        let Some(transaction) = self.detached.remove(&tx_id) else {
            return Err(self.not_active(catalog, tx_id));
        };
        catalog.pager.install_transaction(transaction);

        if let Some(conflict_key) = self.first_conflicting_key(catalog) {
            // Loser of first-committer-wins: abort and report the conflict.
            if let Err(e) = self.abort_installed(catalog, tx_id) {
                error!("rollback of conflicting transaction {} failed: {}", tx_id, e);
            }
            return Err(DbError::WriteConflict(conflict_key));
        }

        let result = self.commit(catalog);
        if result.is_err() {
            // Never leave a transaction in the execution slot: if it is still
            // there (the commit failed before ending it), keep it live but
            // failed, so it can only be aborted.
            if let Some(transaction) = catalog.pager.uninstall_transaction() {
                self.detached.insert(tx_id, transaction);
                self.failed_detached.insert(tx_id);
            }
        }
        result
    }

    /// Abort detached transaction `tx_id`. Aborting an already aborted
    /// transaction is a no-op (`Ok`); a committed or unknown one is refused
    /// with `NoActiveTransaction` (25P01).
    ///
    /// If the rollback fails, the error is returned and the state stays
    /// consistent: when the failure happened before the pager took the
    /// transaction, it remains live (detached) and is marked failed, so it
    /// can never commit and a later abort retries; once the pager has taken
    /// it, it is aborted (status recorded in the shared table, so its versions
    /// are invisible; a reopen treats an unfinished transaction as aborted)
    /// and is dropped from the live set, so a repeated abort is a no-op.
    pub fn abort_detached(&mut self, catalog: &mut Catalog, tx_id: TransactionId) -> DbResult<()> {
        let Some(transaction) = self.detached.remove(&tx_id) else {
            if Self::is_aborted(catalog, tx_id) {
                self.failed_detached.remove(&tx_id);
                return Ok(());
            }
            return Err(self.not_active(catalog, tx_id));
        };
        catalog.pager.install_transaction(transaction);
        self.abort_installed(catalog, tx_id).map_err(DbError::from)
    }

    /// Roll back the installed transaction `tx_id` (see
    /// [`TransactionManager::abort_detached`] for the failure handling).
    fn abort_installed(&mut self, catalog: &mut Catalog, tx_id: TransactionId) -> io::Result<()> {
        match self.rollback(catalog) {
            Ok(()) => {
                self.failed_detached.remove(&tx_id);
                Ok(())
            }
            Err(e) => {
                if let Some(transaction) = catalog.pager.uninstall_transaction() {
                    // Not taken: still live; it must never commit now.
                    self.detached.insert(tx_id, transaction);
                    self.failed_detached.insert(tx_id);
                } else {
                    // Taken and marked aborted: the transaction is over.
                    // Finish the in-memory side of the rollback as far as
                    // possible (best effort; the I/O error is what we report).
                    self.active.remove(&tx_id);
                    self.failed_detached.remove(&tx_id);
                    if let Err(reload) = catalog.reload_tables() {
                        error!("reloading the catalog after a failed rollback of {}: {}", tx_id, reload);
                    }
                    catalog.restore_pre_tx_indexes();
                }
                Err(e)
            }
        }
    }

    /// Re-check every recorded write intent of the installed transaction against
    /// the current version chains. Returns the first logical key that another,
    /// non-visible transaction has changed since this transaction's snapshot.
    fn first_conflicting_key(&self, catalog: &mut Catalog) -> Option<i32> {
        let snapshot = catalog.transaction_snapshot()?;
        let intents = catalog.pager.transaction_write_set();
        for intent in intents {
            let conflict = BTree::open_root(&catalog.pager, intent.table_root)
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

    pub fn execute<F>(
        &mut self,
        catalog: &mut Catalog,
        stmt: Statement,
        execute_stmt: F,
    ) -> DbResult<()>
    where
        F: FnOnce(&mut Catalog, Statement) -> DbResult<()>,
    {
        if self.failed {
            match stmt {
                // ROLLBACK ends the failed transaction normally. The flag is
                // cleared only once the rollback succeeded; if it fails the
                // transaction stays failed and the error is returned.
                Statement::Rollback => {
                    let rollback = self.rollback(catalog);
                    settle_failed(&mut self.failed, rollback)?;
                    self.mode = TransactionMode::None;
                    return Ok(());
                }
                // COMMIT of a failed transaction rolls it back and reports it
                // (or reports the rollback failure, staying failed).
                Statement::Commit => {
                    let rollback = self.rollback(catalog);
                    settle_failed(&mut self.failed, rollback)?;
                    self.mode = TransactionMode::None;
                    return Err(DbError::InFailedTransaction);
                }
                _ => return Err(DbError::InFailedTransaction),
            }
        }
        if self.handle_transaction_control(catalog, &stmt)? {
            return Ok(());
        }

        let explicit = self.in_explicit_transaction(catalog);
        let implicit = self.begin_implicit_if_needed(catalog, &stmt)?;
        let result = execute_stmt(catalog, stmt);
        if explicit && result.is_err() {
            // Statement atomicity inside BEGIN ... COMMIT: the statement may
            // have left partial effects, so the transaction can no longer
            // commit (PostgreSQL's 25P02 policy).
            self.failed = true;
        }
        self.finish_implicit_if_needed(catalog, implicit, result)
    }

    fn in_explicit_transaction(&self, catalog: &Catalog) -> bool {
        self.mode == TransactionMode::Explicit && catalog.transaction_active()
    }

    /// Record that a statement of the current explicit transaction failed
    /// before reaching [`TransactionManager::execute`] (e.g. a parse error).
    /// No effect outside an explicit transaction.
    pub fn statement_failed(&mut self, catalog: &Catalog) {
        if self.in_explicit_transaction(catalog) {
            self.failed = true;
        }
    }

    fn handle_transaction_control(
        &mut self,
        catalog: &mut Catalog,
        stmt: &Statement,
    ) -> DbResult<bool> {
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

/// End a failed transaction with the result of its rollback: the failed flag
/// is cleared only if the rollback succeeded; a rollback error leaves the
/// transaction failed and is returned.
fn settle_failed(failed: &mut bool, rollback: io::Result<()>) -> io::Result<()> {
    rollback?;
    *failed = false;
    Ok(())
}

/// The error a commit reports after its constraint recheck failed with
/// `original` and the transaction was rolled back with result `rollback`:
/// always the constraint error; a rollback failure is logged, not surfaced
/// in its place.
fn commit_failure(original: DbError, rollback: io::Result<()>) -> DbError {
    if let Err(e) = rollback {
        error!("rollback after failed commit recheck ({}) also failed: {}", original, e);
    }
    original
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::query::execute_query;
    use crate::execution::runtime::handle_statement;
    use crate::sql::ast::Statement;
    use crate::sql::parser::parse_sql;
    use crate::storage::pager::{Pager, rollback_fault};

    fn exec(m: &mut TransactionManager, c: &mut Catalog, tx: TransactionId, sql: &str) -> DbResult<()> {
        let stmt = parse_sql(sql).unwrap();
        m.run_statement(c, tx, |c| handle_statement(c, stmt))
    }

    fn count(m: &mut TransactionManager, c: &mut Catalog) -> usize {
        let tx = m.begin_detached(c, None, IsolationLevel::Snapshot).unwrap();
        let Statement::Query(q) = parse_sql("SELECT id FROM t").unwrap() else { unreachable!() };
        let n = m.run_statement(c, tx, |c| execute_query(c, &q, None)).unwrap().rows.len();
        m.commit_detached(c, tx).unwrap();
        n
    }

    #[test]
    fn failed_detached_rollbacks_leave_a_consistent_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("m.db");
        let mut c = Catalog::open(Pager::new(path.to_str().unwrap()).unwrap()).unwrap();
        let mut m = TransactionManager::new();
        let begin = |m: &mut TransactionManager, c: &mut Catalog| m.begin_detached(c, None, IsolationLevel::Snapshot).unwrap();
        let tx = begin(&mut m, &mut c);
        exec(&mut m, &mut c, tx, "CREATE TABLE t (id INTEGER PRIMARY KEY)").unwrap();
        m.commit_detached(&mut c, tx).unwrap();

        // 1. The pager took the transaction (and marked it aborted), then
        //    failed: the transaction is over; aborting again is a no-op and
        //    every other use of the handle is refused (no panic).
        let tx = begin(&mut m, &mut c);
        exec(&mut m, &mut c, tx, "INSERT INTO t VALUES (1)").unwrap();
        rollback_fault::arm(rollback_fault::AFTER_TAKE);
        assert_eq!(m.abort_detached(&mut c, tx).unwrap_err().sqlstate(), "58030");
        assert!(!m.active.contains(&tx) && !m.detached.contains_key(&tx) && !m.failed_detached.contains(&tx));
        assert!(!c.transaction_active());
        m.abort_detached(&mut c, tx).unwrap();
        assert_eq!(m.commit_detached(&mut c, tx).unwrap_err().sqlstate(), "25P01");
        assert_eq!(exec(&mut m, &mut c, tx, "INSERT INTO t VALUES (2)").unwrap_err().sqlstate(), "25P01");
        assert_eq!(count(&mut m, &mut c), 0);

        // 2. COMMIT of a failed transaction whose rollback fails before the
        //    pager took it: still live and failed; a retried COMMIT rolls it
        //    back (25P02), a third one is refused (25P01).
        let tx = begin(&mut m, &mut c);
        exec(&mut m, &mut c, tx, "INSERT INTO t VALUES (3)").unwrap();
        assert!(exec(&mut m, &mut c, tx, "INSERT INTO t VALUES (3)").is_err());
        rollback_fault::arm(rollback_fault::BEFORE_TAKE);
        assert_eq!(m.commit_detached(&mut c, tx).unwrap_err().sqlstate(), "58030");
        assert!(m.detached.contains_key(&tx) && m.failed_detached.contains(&tx));
        assert!(!c.transaction_active(), "the execution slot is free again");
        assert_eq!(exec(&mut m, &mut c, tx, "INSERT INTO t VALUES (4)").unwrap_err().sqlstate(), "25P02");
        assert_eq!(m.commit_detached(&mut c, tx).unwrap_err().sqlstate(), "25P02");
        assert_eq!(m.commit_detached(&mut c, tx).unwrap_err().sqlstate(), "25P01");
        m.abort_detached(&mut c, tx).unwrap();
        assert_eq!(count(&mut m, &mut c), 0);

        // 3. A plain ABORT failing before the take: the transaction stays
        //    live but can never commit any more.
        let tx = begin(&mut m, &mut c);
        exec(&mut m, &mut c, tx, "INSERT INTO t VALUES (5)").unwrap();
        rollback_fault::arm(rollback_fault::BEFORE_TAKE);
        assert!(m.abort_detached(&mut c, tx).is_err());
        assert_eq!(m.commit_detached(&mut c, tx).unwrap_err().sqlstate(), "25P02");
        assert_eq!(count(&mut m, &mut c), 0);

        // The manager still works normally.
        let tx = begin(&mut m, &mut c);
        exec(&mut m, &mut c, tx, "INSERT INTO t VALUES (6)").unwrap();
        m.commit_detached(&mut c, tx).unwrap();
        assert_eq!(count(&mut m, &mut c), 1);
        assert!(m.active.is_empty() && m.detached.is_empty() && m.failed_detached.is_empty());
    }

    #[test]
    fn failed_flag_is_cleared_only_after_a_successful_rollback() {
        let mut failed = true;
        let e = settle_failed(&mut failed, Err(io::Error::other("disk gone"))).unwrap_err();
        assert_eq!(e.to_string(), "disk gone");
        assert!(failed, "a failed rollback must leave the transaction failed");
        settle_failed(&mut failed, Ok(())).unwrap();
        assert!(!failed);
    }

    #[test]
    fn failed_rollback_keeps_the_constraint_error() {
        let original = DbError::ForeignKeyViolation("p 1".into());
        let rollback = Err(io::Error::other("disk gone"));
        assert!(matches!(
            commit_failure(original, rollback),
            DbError::ForeignKeyViolation(ref m) if m == "p 1"
        ));
        let ok = commit_failure(DbError::DuplicateKey("1".into()), Ok(()));
        assert!(matches!(ok, DbError::DuplicateKey(ref k) if k == "1"));
    }
}
