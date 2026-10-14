//! Logical PRIMARY KEY / FOREIGN KEY checks.
//!
//! SQL keys are compared by value (typed equality through
//! [`Value::compare`]: numeric equality across INTEGER/DECIMAL/DOUBLE, CHAR
//! PAD SPACE), never by the B-tree's physical row id. The same functions run
//! at statement time (with a [`Pending`] overlay describing the statement's
//! not-yet-applied changes) and at commit recheck (empty overlay, against the
//! commit-time view).

use crate::catalog::{Catalog, CheckConstraint, TableInfo, UniqueConstraint};
use crate::error::{DbError, DbResult};
use crate::execution::eval::{Ctx, Env, Scope, display_column, eval, truth};
use crate::sql::ast::{Action, Expr, ForeignKey, Ident, TableConstraint};
use crate::sql::value::{SqlType, Value};
use crate::storage::btree::BTree;
use crate::storage::row::{ColumnType, ColumnValue, Row, RowData};
use crate::transaction::{Snapshot, TransactionId};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};

/// Typed equality of two key tuples. A tuple containing NULL equals nothing.
pub fn key_values_equal(a: &[ColumnValue], b: &[ColumnValue]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|(x, y)| {
            matches!(
                Value::from_column_value(x).compare(&Value::from_column_value(y)),
                Ok(Some(Ordering::Equal))
            )
        })
}

/// Render a key tuple for error messages: `1`, `(1, 2)`.
pub fn render_key(values: &[ColumnValue]) -> String {
    let parts: Vec<String> = values
        .iter()
        .map(|v| Value::from_column_value(v).to_string())
        .collect();
    if parts.len() == 1 {
        parts[0].clone()
    } else {
        format!("({})", parts.join(", "))
    }
}

fn has_null(values: &[ColumnValue]) -> bool {
    values.iter().any(|v| matches!(v, ColumnValue::Null))
}

/// Position of the declared column `name` (exact match: constraint metadata
/// stores declared names).
pub fn column_position(table: &TableInfo, name: &str) -> DbResult<usize> {
    table
        .columns
        .iter()
        .position(|(c, _)| c == name)
        .ok_or_else(|| DbError::ColumnNotFound(format!("{}.{}", table.name, name)))
}

fn positions(table: &TableInfo, names: &[Ident]) -> DbResult<Vec<usize>> {
    names.iter().map(|n| column_position(table, n)).collect()
}

/// Column positions of the table's PRIMARY KEY, if it has one.
pub fn primary_key_positions(table: &TableInfo) -> DbResult<Option<Vec<usize>>> {
    match &table.primary_key {
        Some(cols) if !cols.is_empty() => cols
            .iter()
            .map(|c| column_position(table, c))
            .collect::<DbResult<Vec<_>>>()
            .map(Some),
        _ => Ok(None),
    }
}

/// The values of `row` at `idx`.
pub fn key_of(row: &RowData, idx: &[usize]) -> Vec<ColumnValue> {
    idx.iter()
        .map(|&i| row.0.get(i).cloned().unwrap_or(ColumnValue::Null))
        .collect()
}

/// Changes of the running statement that are not in the B-tree yet: row
/// versions about to be replaced/deleted, and new row images about to be
/// written. Commit recheck uses an empty overlay.
#[derive(Debug, Default)]
pub struct Pending {
    removed: HashSet<(String, i32, TransactionId)>,
    added: Vec<(String, RowData)>,
}

impl Pending {
    pub fn new() -> Self {
        Self::default()
    }

    /// `row` (a visible version of `table`) is being deleted or replaced.
    pub fn remove(&mut self, table: &str, row: &Row) {
        self.removed
            .insert((table.to_string(), row.key, row.created_tx));
    }

    /// `data` is about to become a visible row of `table`.
    pub fn add(&mut self, table: &str, data: RowData) {
        self.added.push((table.to_string(), data));
    }

    pub fn is_removed(&self, table: &str, row: &Row) -> bool {
        self.removed
            .contains(&(table.to_string(), row.key, row.created_tx))
    }

    fn added_rows<'a>(&'a self, table: &'a str) -> impl Iterator<Item = &'a RowData> + 'a {
        self.added
            .iter()
            .filter(move |(t, _)| t == table)
            .map(|(_, d)| d)
    }
}

/// Rows of `table` visible to `snapshot`, minus the overlay's removed versions.
pub fn visible_rows(
    catalog: &Catalog,
    table: &TableInfo,
    snapshot: &Snapshot,
    pending: &Pending,
) -> DbResult<Vec<Row>> {
    let root = catalog
        .get_table(&table.name)
        .map(|t| t.root_page)
        .unwrap_or(table.root_page);
    let tree = BTree::open_root(&catalog.pager, root)?;
    Ok(tree
        .scan_visible(snapshot)?
        .into_iter()
        .filter(|r| !pending.is_removed(&table.name, r))
        .map(|mut r| {
            table.pad_row(&mut r.data);
            r
        })
        .collect())
}

/// PRIMARY KEY check for `candidates`, the row images a statement (or a
/// committing transaction) makes visible in `table`. Each candidate must have
/// no NULL key column and a key distinct from every other candidate and from
/// every visible row not removed by `pending`.
pub fn check_primary_key(
    catalog: &Catalog,
    table: &TableInfo,
    snapshot: &Snapshot,
    candidates: &[RowData],
    pending: &Pending,
) -> DbResult<()> {
    let Some(pk) = primary_key_positions(table)? else {
        return Ok(());
    };
    for c in candidates {
        for &i in &pk {
            if matches!(c.0.get(i), None | Some(ColumnValue::Null)) {
                return Err(DbError::NullViolation(table.columns[i].0.clone()));
            }
        }
    }
    if candidates.is_empty() {
        return Ok(());
    }
    let existing: Vec<Vec<ColumnValue>> = visible_rows(catalog, table, snapshot, pending)?
        .iter()
        .map(|r| key_of(&r.data, &pk))
        .collect();
    let mut seen: Vec<Vec<ColumnValue>> = Vec::with_capacity(candidates.len());
    for c in candidates {
        let key = key_of(c, &pk);
        if existing
            .iter()
            .chain(seen.iter())
            .any(|k| key_values_equal(k, &key))
        {
            return Err(DbError::DuplicateKey(render_key(&key)));
        }
        seen.push(key);
    }
    Ok(())
}

/// UNIQUE check for `candidates` (row images a statement or committing
/// transaction makes visible in `table`): per UNIQUE constraint, a candidate
/// key without NULLs must differ from every other candidate and from every
/// visible row not removed by `pending`. Keys with a NULL never conflict.
pub fn check_unique(
    catalog: &Catalog,
    table: &TableInfo,
    snapshot: &Snapshot,
    candidates: &[RowData],
    pending: &Pending,
) -> DbResult<()> {
    if table.uniques.is_empty() || candidates.is_empty() {
        return Ok(());
    }
    let visible = visible_rows(catalog, table, snapshot, pending)?;
    for u in &table.uniques {
        let idx: Vec<usize> = u
            .columns
            .iter()
            .map(|c| column_position(table, c))
            .collect::<DbResult<_>>()?;
        let existing: Vec<Vec<ColumnValue>> = visible
            .iter()
            .map(|r| key_of(&r.data, &idx))
            .filter(|k| !has_null(k))
            .collect();
        let mut seen: Vec<Vec<ColumnValue>> = Vec::new();
        for c in candidates {
            let key = key_of(c, &idx);
            if has_null(&key) {
                continue;
            }
            if existing
                .iter()
                .chain(seen.iter())
                .any(|k| key_values_equal(k, &key))
            {
                return Err(unique_violation(u, &key));
            }
            seen.push(key);
        }
    }
    Ok(())
}

fn unique_violation(u: &UniqueConstraint, key: &[ColumnValue]) -> DbError {
    let values: Vec<String> = key
        .iter()
        .map(|v| Value::from_column_value(v).to_string())
        .collect();
    DbError::UniqueViolation {
        constraint: u.name.clone(),
        key: format!("({})=({})", u.columns.join(", "), values.join(", ")),
    }
}

/// Expression scope over the columns of `table`, qualified by its name.
pub fn table_scope(table: &TableInfo) -> Scope {
    column_scope(&table.name, &table.columns)
}

fn column_scope(table_name: &str, columns: &[(String, ColumnType)]) -> Scope {
    let mut scope = Scope::new();
    for (name, ty) in columns {
        scope.push(
            Some(table_name.to_string()),
            name.clone(),
            SqlType::from_column_type(ty),
        );
    }
    scope
}

/// CHECK constraints of `table` for one row image: FALSE is a violation,
/// TRUE and UNKNOWN (NULL) pass.
pub fn check_checks(table: &TableInfo, row: &RowData) -> DbResult<()> {
    if table.checks.is_empty() {
        return Ok(());
    }
    let scope = table_scope(table);
    let vals: Vec<Value> = row.0.iter().map(Value::from_column_value).collect();
    let env = Env::new(&scope, &vals, None);
    for c in &table.checks {
        let v = eval(&c.expr, &env, &Ctx { catalog: None })?;
        if truth(&v)? == Some(false) {
            return Err(DbError::CheckViolation {
                constraint: c.name.clone(),
            });
        }
    }
    Ok(())
}

fn check_not_null(table: &TableInfo, row: &RowData) -> DbResult<()> {
    for (i, (name, _)) in table.columns.iter().enumerate() {
        let nn = table.not_null.get(i).copied().unwrap_or(false);
        if nn && matches!(row.0.get(i), None | Some(ColumnValue::Null)) {
            return Err(DbError::NullViolation(name.clone()));
        }
    }
    Ok(())
}

/// Every constraint on new row images `images` of `table`: NOT NULL and
/// CHECK per row, then PRIMARY KEY and UNIQUE across the images and the
/// remaining rows, then FOREIGN KEY existence. `pending` describes the
/// statement's (or committing transaction's) final state and must list
/// `images` as added rows of `table`. Used by INSERT, UPDATE (including
/// cascaded rows) and the commit recheck alike.
pub fn check_new_rows(
    catalog: &Catalog,
    table: &TableInfo,
    snapshot: &Snapshot,
    images: &[RowData],
    pending: &Pending,
) -> DbResult<()> {
    for image in images {
        check_not_null(table, image)?;
        check_checks(table, image)?;
    }
    check_primary_key(catalog, table, snapshot, images, pending)?;
    check_unique(catalog, table, snapshot, images, pending)?;
    for image in images {
        check_foreign_keys(catalog, table, image, snapshot, pending)?;
    }
    Ok(())
}

/// The parent table of `fk` declared on `child` (a self-reference resolves to
/// `child` itself).
fn parent_of(catalog: &Catalog, child: &TableInfo, fk: &ForeignKey) -> DbResult<TableInfo> {
    if fk.parent_table.value == child.name {
        return Ok(child.clone());
    }
    catalog
        .get_table(&fk.parent_table)
        .cloned()
        .map_err(|_| DbError::TableNotFound(fk.parent_table.value.clone()))
}

/// Whether some row of `table` (visible and not removed, or added by the
/// overlay) has `key` in the columns `idx`.
fn key_provided(
    catalog: &Catalog,
    table: &TableInfo,
    idx: &[usize],
    key: &[ColumnValue],
    snapshot: &Snapshot,
    pending: &Pending,
) -> DbResult<bool> {
    if pending
        .added_rows(&table.name)
        .any(|d| key_values_equal(&key_of(d, idx), key))
    {
        return Ok(true);
    }
    Ok(visible_rows(catalog, table, snapshot, pending)?
        .iter()
        .any(|r| key_values_equal(&key_of(&r.data, idx), key)))
}

/// FOREIGN KEY check (MATCH SIMPLE) for one row image of `table`: for every
/// foreign key whose columns are all non-NULL, a parent row with equal
/// referenced columns must exist.
pub fn check_foreign_keys(
    catalog: &Catalog,
    table: &TableInfo,
    row: &RowData,
    snapshot: &Snapshot,
    pending: &Pending,
) -> DbResult<()> {
    for fk in &table.fks {
        let child_idx = positions(table, &fk.columns)?;
        let key = key_of(row, &child_idx);
        if key.is_empty() || has_null(&key) {
            continue;
        }
        let parent = parent_of(catalog, table, fk)?;
        let parent_idx = positions(&parent, &fk.parent_columns)?;
        if !key_provided(catalog, &parent, &parent_idx, &key, snapshot, pending)? {
            return Err(DbError::ForeignKeyViolation(format!(
                "insert or update on table \"{}\" violates foreign key constraint \"{}\": \
                 key ({}) = {} is not present in table \"{}\"",
                table.name,
                fk_name(table, fk),
                names(&fk.columns),
                render_key(&key),
                parent.name
            )));
        }
    }
    Ok(())
}

fn names(cols: &[Ident]) -> String {
    cols.iter()
        .map(|c| c.value.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Why rows are leaving a parent table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Removal {
    /// DELETE: ON DELETE CASCADE children are handled by [`plan_delete`];
    /// other references are violations.
    Delete,
    /// UPDATE: the old row images are replaced. ON UPDATE CASCADE children
    /// were rewritten by [`plan_update`] (their old versions are removed in
    /// the overlay), so every remaining reference is a violation (NO ACTION;
    /// RESTRICT references were already checked by [`check_restrict`]).
    Update,
    /// Commit recheck: no action can run any more; every remaining reference
    /// is a violation.
    Commit,
}

/// Every (foreign key, child table, referencing child rows) for keys of the
/// `removed` row images of `table` that no remaining row still provides.
fn orphaned_references(
    catalog: &Catalog,
    table: &TableInfo,
    removed: &[RowData],
    snapshot: &Snapshot,
    pending: &Pending,
) -> DbResult<Vec<(ForeignKey, TableInfo, Vec<Row>)>> {
    let mut out = Vec::new();
    if removed.is_empty() {
        return Ok(out);
    }
    let mut children: Vec<TableInfo> = catalog.all_tables();
    children.sort_by(|a, b| a.name.cmp(&b.name));
    for child in children {
        for fk in child
            .fks
            .iter()
            .filter(|fk| fk.parent_table.value == table.name)
        {
            let parent_idx = positions(table, &fk.parent_columns)?;
            let child_idx = positions(&child, &fk.columns)?;
            // Keys still provided by the remaining/added parent rows, and the
            // candidate child rows; both loaded at most once per foreign key.
            let mut provided: Option<Vec<Vec<ColumnValue>>> = None;
            let mut child_rows: Option<Vec<Row>> = None;
            let mut referencing: Vec<Row> = Vec::new();
            for old in removed {
                let key = key_of(old, &parent_idx);
                if has_null(&key) {
                    continue;
                }
                let provided = match &mut provided {
                    Some(p) => p,
                    slot => {
                        let mut keys: Vec<Vec<ColumnValue>> =
                            visible_rows(catalog, table, snapshot, pending)?
                                .iter()
                                .map(|r| key_of(&r.data, &parent_idx))
                                .collect();
                        keys.extend(
                            pending
                                .added_rows(&table.name)
                                .map(|d| key_of(d, &parent_idx)),
                        );
                        slot.insert(keys)
                    }
                };
                if provided.iter().any(|k| key_values_equal(k, &key)) {
                    continue;
                }
                let child_rows = match &mut child_rows {
                    Some(c) => c,
                    slot => slot.insert(visible_rows(catalog, &child, snapshot, pending)?),
                };
                for r in child_rows.iter() {
                    if key_values_equal(&key_of(&r.data, &child_idx), &key)
                        && !referencing
                            .iter()
                            .any(|x| x.key == r.key && x.created_tx == r.created_tx)
                    {
                        referencing.push(r.clone());
                    }
                }
            }
            if !referencing.is_empty() {
                out.push((fk.clone(), child.clone(), referencing));
            }
        }
    }
    Ok(out)
}

/// Referential check for row images leaving `table` (deleted, or replaced by
/// UPDATE): a key that is still referenced by a visible child row and no
/// longer provided by any remaining row is a violation, except for ON DELETE
/// CASCADE references on [`Removal::Delete`] (cascaded by [`plan_delete`]).
pub fn check_removed_keys(
    catalog: &Catalog,
    table: &TableInfo,
    removed: &[RowData],
    snapshot: &Snapshot,
    pending: &Pending,
    removal: Removal,
) -> DbResult<()> {
    for (fk, child, _rows) in orphaned_references(catalog, table, removed, snapshot, pending)? {
        if removal == Removal::Delete && fk.on_delete == Some(Action::Cascade) {
            continue;
        }
        let what = if removal == Removal::Update {
            "update"
        } else {
            "delete"
        };
        return Err(DbError::ForeignKeyViolation(format!(
            "{} on table \"{}\" violates foreign key constraint \"{}\" on table \"{}\": \
             rows still reference ({}) without a parent",
            what,
            table.name,
            fk_name(&child, &fk),
            child.name,
            names(&fk.parent_columns)
        )));
    }
    Ok(())
}

/// Immediate referential check for `ON DELETE RESTRICT` / `ON UPDATE
/// RESTRICT` foreign keys referencing `table`. `changes` are the parent rows
/// leaving their key: (old image, new image), the new image `None` for a
/// DELETE. A changed (UPDATE: old key not equal to the new key) or deleted
/// non-NULL referenced key is a violation if a child row still references
/// it after the statement's own changes to the *referencing* rows: children
/// the statement deletes, or updates to point elsewhere, do not count
/// (`statement` holds the statement's removed old versions and new images);
/// other parent rows re-providing the key do not help (unlike NO ACTION,
/// which is checked against the statement's end state by
/// [`check_removed_keys`]).
pub fn check_restrict(
    catalog: &Catalog,
    table: &TableInfo,
    changes: &[(RowData, Option<RowData>)],
    snapshot: &Snapshot,
    statement: &Pending,
) -> DbResult<()> {
    if changes.is_empty() {
        return Ok(());
    }
    let mut children: Vec<TableInfo> = catalog.all_tables();
    children.sort_by(|a, b| a.name.cmp(&b.name));
    for child in &children {
        for fk in child
            .fks
            .iter()
            .filter(|fk| fk.parent_table.value == table.name)
        {
            let parent_idx = positions(table, &fk.parent_columns)?;
            let child_idx = positions(child, &fk.columns)?;
            // Child references after the statement's own changes to the
            // child rows: untouched rows plus new images of updated ones.
            let mut child_keys: Option<Vec<Vec<ColumnValue>>> = None;
            for (old, new) in changes {
                let action = if new.is_some() {
                    &fk.on_update
                } else {
                    &fk.on_delete
                };
                if *action != Some(Action::Restrict) {
                    continue;
                }
                let key = key_of(old, &parent_idx);
                if has_null(&key) {
                    continue;
                }
                if let Some(new) = new
                    && key_values_equal(&key, &key_of(new, &parent_idx))
                {
                    continue;
                }
                let refs = match &mut child_keys {
                    Some(k) => k,
                    slot => {
                        let mut keys: Vec<Vec<ColumnValue>> =
                            visible_rows(catalog, child, snapshot, statement)?
                                .iter()
                                .map(|r| key_of(&r.data, &child_idx))
                                .collect();
                        keys.extend(
                            statement
                                .added_rows(&child.name)
                                .map(|d| key_of(d, &child_idx)),
                        );
                        slot.insert(keys)
                    }
                };
                if refs.iter().any(|k| key_values_equal(k, &key)) {
                    let (what, event) = if new.is_some() {
                        ("update", "UPDATE")
                    } else {
                        ("delete", "DELETE")
                    };
                    return Err(DbError::ForeignKeyViolation(format!(
                        "{} on table \"{}\" violates foreign key constraint \"{}\" on table \"{}\" \
                         (ON {} RESTRICT): key ({}) = {} is referenced",
                        what,
                        table.name,
                        fk_name(child, fk),
                        child.name,
                        event,
                        names(&fk.parent_columns),
                        render_key(&key)
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Plan a DELETE of `rows` from `table`: follow ON DELETE CASCADE references
/// recursively, then check ON DELETE RESTRICT references ([`check_restrict`]:
/// any reference by a child row this statement does not delete) and every
/// other reference to the deleted keys (NO ACTION) against the final set of
/// deleted rows. Returns the rows to delete per table, `table` first.
pub fn plan_delete(
    catalog: &Catalog,
    table: &TableInfo,
    rows: Vec<Row>,
    snapshot: &Snapshot,
) -> DbResult<Vec<(TableInfo, Vec<Row>)>> {
    let mut pending = Pending::new();
    for r in &rows {
        pending.remove(&table.name, r);
    }
    let mut batches: Vec<(TableInfo, Vec<Row>)> = vec![(table.clone(), rows)];
    let mut next = 0;
    while next < batches.len() {
        let (t, batch) = batches[next].clone();
        next += 1;
        let data: Vec<RowData> = batch.iter().map(|r| r.data.clone()).collect();
        for (fk, child, refs) in orphaned_references(catalog, &t, &data, snapshot, &pending)? {
            if fk.on_delete != Some(Action::Cascade) {
                continue;
            }
            let fresh: Vec<Row> = refs
                .into_iter()
                .filter(|r| !pending.is_removed(&child.name, r))
                .collect();
            if fresh.is_empty() {
                continue;
            }
            for r in &fresh {
                pending.remove(&child.name, r);
            }
            batches.push((child, fresh));
        }
    }
    for (t, batch) in &batches {
        let changes: Vec<(RowData, Option<RowData>)> =
            batch.iter().map(|r| (r.data.clone(), None)).collect();
        check_restrict(catalog, t, &changes, snapshot, &pending)?;
    }
    for (t, batch) in &batches {
        let data: Vec<RowData> = batch.iter().map(|r| r.data.clone()).collect();
        check_removed_keys(catalog, t, &data, snapshot, &pending, Removal::Delete)?;
    }
    Ok(batches)
}

/// Row replacements of one UPDATE, grouped by table: (old visible version,
/// new row image).
pub type UpdatePlan = Vec<(TableInfo, Vec<(Row, RowData)>)>;

/// Upper bound on cascade steps of one statement (guards against a cascade
/// cycle that never settles).
const MAX_CASCADE_STEPS: usize = 1_000_000;

/// Plan an UPDATE of `table` replacing each old row with its new image
/// (`ops`): follow ON UPDATE CASCADE references recursively, rewriting the
/// referencing columns of child rows (in the state before the statement) that
/// matched a parent row's old key to that row's new key. A child row reached
/// more than once (or also updated directly) gets one combined new image.
/// Returns the replacements per table, `table` first; constraints are not
/// checked here (see [`check_new_rows`] / [`check_removed_keys`]).
pub fn plan_update(
    catalog: &Catalog,
    table: &TableInfo,
    ops: Vec<(Row, RowData)>,
    snapshot: &Snapshot,
) -> DbResult<UpdatePlan> {
    let mut tables: Vec<TableInfo> = vec![table.clone()];
    // (table index, old version, new image)
    let mut plan: Vec<(usize, Row, RowData)> = Vec::with_capacity(ops.len());
    let mut index: HashMap<(String, i32, TransactionId), usize> = HashMap::new();
    for (old, new) in ops {
        index.insert((table.name.clone(), old.key, old.created_tx), plan.len());
        plan.push((0, old, new));
    }
    let mut all = catalog.all_tables();
    all.sort_by(|a, b| a.name.cmp(&b.name));
    let mut child_rows: HashMap<String, Vec<Row>> = HashMap::new();
    let mut queue: VecDeque<usize> = (0..plan.len()).collect();
    let mut steps = 0usize;
    while let Some(i) = queue.pop_front() {
        steps += 1;
        if steps > MAX_CASCADE_STEPS {
            return Err(DbError::FeatureNotSupported(
                "ON UPDATE CASCADE cycle that does not settle".into(),
            ));
        }
        let parent = tables[plan[i].0].clone();
        let (old, new) = (plan[i].1.data.clone(), plan[i].2.clone());
        for child in &all {
            for fk in child.fks.iter().filter(|fk| {
                fk.parent_table.value == parent.name && fk.on_update == Some(Action::Cascade)
            }) {
                let parent_idx = positions(&parent, &fk.parent_columns)?;
                let old_key = key_of(&old, &parent_idx);
                let new_key = key_of(&new, &parent_idx);
                if has_null(&old_key) || key_values_equal(&old_key, &new_key) {
                    continue;
                }
                let child_idx = positions(child, &fk.columns)?;
                if !child_rows.contains_key(&child.name) {
                    let rows = visible_rows(catalog, child, snapshot, &Pending::new())?;
                    child_rows.insert(child.name.clone(), rows);
                }
                let referencing: Vec<Row> = child_rows[&child.name]
                    .iter()
                    .filter(|r| key_values_equal(&key_of(&r.data, &child_idx), &old_key))
                    .cloned()
                    .collect();
                for r in referencing {
                    let id = (child.name.clone(), r.key, r.created_tx);
                    let j = match index.get(&id) {
                        Some(&j) => j,
                        None => {
                            let t = match tables.iter().position(|t| t.name == child.name) {
                                Some(t) => t,
                                None => {
                                    tables.push(child.clone());
                                    tables.len() - 1
                                }
                            };
                            let data = r.data.clone();
                            plan.push((t, r, data));
                            index.insert(id, plan.len() - 1);
                            plan.len() - 1
                        }
                    };
                    let mut image = plan[j].2.clone();
                    for (k, &ci) in child_idx.iter().enumerate() {
                        let (name, ty) = &child.columns[ci];
                        image.0[ci] = Value::from_column_value(&new_key[k])
                            .to_column_value(ty, name)
                            .map_err(DbError::from)?;
                    }
                    if image != plan[j].2 {
                        plan[j].2 = image;
                        queue.push_back(j);
                    }
                }
            }
        }
    }
    let mut out: UpdatePlan = tables.into_iter().map(|t| (t, Vec::new())).collect();
    for (t, old, new) in plan {
        out[t].1.push((old, new));
    }
    Ok(out)
}

/// Resolve `ident` against declared column names with the identifier rule.
fn resolve_column(columns: &[(String, ColumnType)], ident: &Ident, table: &str) -> DbResult<usize> {
    let hits: Vec<usize> = columns
        .iter()
        .enumerate()
        .filter(|(_, (n, _))| ident.matches(n))
        .map(|(i, _)| i)
        .collect();
    match hits.as_slice() {
        [i] => Ok(*i),
        [] => Err(DbError::ColumnNotFound(format!(
            "{}.{}",
            table, ident.value
        ))),
        _ => Err(DbError::AmbiguousColumn(ident.value.clone())),
    }
}

fn comparable(a: &ColumnType, b: &ColumnType) -> bool {
    let (x, y) = (SqlType::from_column_type(a), SqlType::from_column_type(b));
    (x.is_numeric() && y.is_numeric())
        || (x.is_text() && y.is_text())
        || std::mem::discriminant(&x) == std::mem::discriminant(&y)
}

/// Resolve a constraint's column list to declared names (no duplicates).
fn resolve_column_list(
    columns: &[(String, ColumnType)],
    idents: &[Ident],
    table_name: &str,
    what: &str,
) -> DbResult<Vec<String>> {
    let mut out: Vec<String> = Vec::new();
    for c in idents {
        let name = columns[resolve_column(columns, c, table_name)?].0.clone();
        if out.contains(&name) {
            return Err(DbError::Syntax(format!(
                "column \"{}\" appears twice in {} constraint",
                name, what
            )));
        }
        out.push(name);
    }
    Ok(out)
}

/// Validate a CHECK expression of table `table_name` and canonicalise its
/// column references to the declared names (quoted, unqualified): only
/// columns of the table itself (42703 otherwise); no subqueries or aggregates
/// (0A000).
fn resolve_check_expr(
    expr: &Expr,
    table_name: &str,
    columns: &[(String, ColumnType)],
) -> DbResult<Expr> {
    let mut unsupported: Option<&str> = None;
    expr.walk(&mut |e| match e {
        Expr::ScalarSubquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {
            unsupported.get_or_insert("subqueries in CHECK constraints");
        }
        Expr::Aggregate { .. } => {
            unsupported.get_or_insert("aggregate functions in CHECK constraints");
        }
        Expr::Default => {
            unsupported.get_or_insert("DEFAULT in CHECK constraints");
        }
        _ => {}
    });
    if let Some(what) = unsupported {
        return Err(DbError::FeatureNotSupported(what.into()));
    }
    let scope = column_scope(table_name, columns);
    let mut failure: Option<DbError> = None;
    let canonical = expr.transform(&mut |e| match e {
        Expr::Column { table, name } => match scope.resolve(table.as_ref(), &name) {
            Ok(Some(i)) => Expr::Column {
                table: None,
                name: Ident::quoted(columns[i].0.clone()),
            },
            Ok(None) => {
                failure.get_or_insert(DbError::ColumnNotFound(display_column(
                    table.as_ref().map(Ident::as_str),
                    &name,
                )));
                Expr::Column { table, name }
            }
            Err(err) => {
                failure.get_or_insert(err);
                Expr::Column { table, name }
            }
        },
        other => other,
    });
    if let Some(err) = failure {
        return Err(err);
    }
    // The expression must be BOOLEAN (TypeMismatch 42804 otherwise).
    crate::execution::bind::bind_check(&scope, &canonical)?;
    Ok(canonical)
}

/// `base`, or `base1`, `base2`, ... — the first name not in `used`.
pub fn fresh_name(base: String, used: &HashSet<String>) -> String {
    if !used.contains(&base) {
        return base;
    }
    (1..)
        .map(|n| format!("{}{}", base, n))
        .find(|c| !used.contains(c))
        .expect("unbounded")
}

/// The validated, canonical constraints of a CREATE TABLE.
#[derive(Debug, Default)]
pub struct ResolvedConstraints {
    pub primary_key: Option<Vec<String>>,
    /// Given or generated (`{table}_pkey`) name of the PRIMARY KEY.
    pub primary_key_name: Option<String>,
    pub fks: Vec<ForeignKey>,
    pub uniques: Vec<UniqueConstraint>,
    pub checks: Vec<CheckConstraint>,
}

/// Validate and canonicalise the constraints of a CREATE TABLE: column names
/// resolve with the identifier rule and are stored as the declared (quoted)
/// names. Each foreign key references exactly the parent's PRIMARY KEY or one
/// of its UNIQUE column sets (no column list = the PRIMARY KEY) with the same
/// arity and comparable types. CHECK expressions must be BOOLEAN (42804).
/// Every constraint gets its given name, else `{table}_pkey` (PRIMARY KEY),
/// `{table}_{cols}_key` (UNIQUE), `{table}_{col}_check` (column CHECK),
/// `{table}_check{n}` (n-th unnamed table CHECK) or `{table}_{cols}_fkey`
/// (FOREIGN KEY); a generated name that is taken gets a numeric suffix, a
/// given name used twice is `DuplicateConstraint` (42710).
pub fn resolve_table_constraints(
    catalog: &Catalog,
    table_name: &str,
    columns: &[(String, ColumnType)],
    primary_key: Option<Vec<Ident>>,
    fks: Vec<ForeignKey>,
    constraints: Vec<TableConstraint>,
) -> DbResult<ResolvedConstraints> {
    let pk = match primary_key {
        Some(cols) => Some(resolve_column_list(
            columns,
            &cols,
            table_name,
            "primary key",
        )?),
        None => None,
    };

    // Given constraint names (any kind) must be distinct within the table.
    let mut used: HashSet<String> = HashSet::new();
    let given = constraints
        .iter()
        .filter_map(|c| match c {
            TableConstraint::Unique { name, .. } | TableConstraint::Check { name, .. } => {
                name.as_ref()
            }
            TableConstraint::PrimaryKeyName(n) => Some(n),
        })
        .chain(fks.iter().filter_map(|fk| fk.name.as_ref()));
    for n in given {
        if !used.insert(n.clone()) {
            return Err(DbError::DuplicateConstraint {
                name: n.clone(),
                table: table_name.to_string(),
            });
        }
    }
    let given_pk_name = constraints.iter().find_map(|c| match c {
        TableConstraint::PrimaryKeyName(n) => Some(n.clone()),
        _ => None,
    });
    let primary_key_name = match (&pk, given_pk_name) {
        (Some(_), Some(n)) => Some(n),
        (Some(_), None) => {
            let n = fresh_name(default_pk_name(table_name), &used);
            used.insert(n.clone());
            Some(n)
        }
        (None, _) => None,
    };
    let mut uniques: Vec<UniqueConstraint> = Vec::new();
    let mut checks: Vec<CheckConstraint> = Vec::new();
    let mut table_checks = 0usize;
    for c in constraints {
        match c {
            TableConstraint::PrimaryKeyName(_) => {}
            TableConstraint::Unique {
                name,
                columns: cols,
            } => {
                let cols = resolve_column_list(columns, &cols, table_name, "unique")?;
                let name = name.unwrap_or_else(|| {
                    let n = fresh_name(format!("{}_{}_key", table_name, cols.join("_")), &used);
                    used.insert(n.clone());
                    n
                });
                uniques.push(UniqueConstraint {
                    name,
                    columns: cols,
                });
            }
            TableConstraint::Check { name, expr, column } => {
                let expr = resolve_check_expr(&expr, table_name, columns)?;
                if column.is_none() {
                    table_checks += 1;
                }
                let name = name.unwrap_or_else(|| {
                    let base = match &column {
                        Some(col) => format!("{}_{}_check", table_name, col),
                        None => format!("{}_check{}", table_name, table_checks),
                    };
                    let n = fresh_name(base, &used);
                    used.insert(n.clone());
                    n
                });
                checks.push(CheckConstraint { name, expr });
            }
        }
    }

    let mut resolved = Vec::with_capacity(fks.len());
    for fk in fks {
        let child_idx: Vec<usize> = fk
            .columns
            .iter()
            .map(|c| resolve_column(columns, c, table_name))
            .collect::<DbResult<_>>()?;
        let self_ref = fk.parent_table.matches(table_name);
        let (parent_name, parent_cols, parent_pk, parent_uniques) = if self_ref {
            (
                table_name.to_string(),
                columns.to_vec(),
                pk.clone(),
                uniques.clone(),
            )
        } else {
            let p = catalog.resolve_table(&fk.parent_table)?;
            (
                p.name.clone(),
                p.columns.clone(),
                p.primary_key.clone(),
                p.uniques.clone(),
            )
        };
        let parent_idents: Vec<Ident> = if fk.parent_columns.is_empty() {
            match &parent_pk {
                Some(cols) => cols.iter().map(|c| Ident::quoted(c.clone())).collect(),
                None => {
                    return Err(DbError::InvalidForeignKey(format!(
                        "there is no primary key for referenced table \"{}\"",
                        parent_name
                    )));
                }
            }
        } else {
            fk.parent_columns.clone()
        };
        let parent_idx: Vec<usize> = parent_idents
            .iter()
            .map(|c| resolve_column(&parent_cols, c, &parent_name))
            .collect::<DbResult<_>>()?;
        if child_idx.len() != parent_idx.len() {
            return Err(DbError::InvalidForeignKey(format!(
                "number of referencing columns ({}) does not match referenced columns ({}) of table \"{}\"",
                child_idx.len(),
                parent_idx.len(),
                parent_name
            )));
        }
        let referenced: Vec<String> = parent_idx
            .iter()
            .map(|&i| parent_cols[i].0.clone())
            .collect();
        let as_set = |cols: &[String]| {
            let mut set: Vec<String> = cols.to_vec();
            set.sort();
            set
        };
        let ref_set = as_set(&referenced);
        let distinct = ref_set.windows(2).all(|w| w[0] != w[1]);
        let is_key = parent_pk.as_deref().is_some_and(|pk| as_set(pk) == ref_set)
            || parent_uniques.iter().any(|u| as_set(&u.columns) == ref_set);
        if !distinct || !is_key {
            return Err(DbError::InvalidForeignKey(format!(
                "referenced columns ({}) are not the primary key or a unique key of table \"{}\"",
                referenced.join(", "),
                parent_name
            )));
        }
        for (&c, &p) in child_idx.iter().zip(&parent_idx) {
            if !comparable(&columns[c].1, &parent_cols[p].1) {
                return Err(DbError::TypeMismatch(format!(
                    "foreign key column \"{}\" ({}) is not comparable with referenced column \"{}\" ({})",
                    columns[c].0,
                    columns[c].1.as_str(),
                    parent_cols[p].0,
                    parent_cols[p].1.as_str()
                )));
            }
        }
        let child_cols: Vec<String> = child_idx.iter().map(|&i| columns[i].0.clone()).collect();
        let name = match fk.name {
            Some(n) => n,
            None => {
                let n = fresh_name(default_fk_name(table_name, &child_cols), &used);
                used.insert(n.clone());
                n
            }
        };
        resolved.push(ForeignKey {
            columns: child_cols.into_iter().map(Ident::quoted).collect(),
            parent_table: Ident::quoted(parent_name),
            parent_columns: referenced.into_iter().map(Ident::quoted).collect(),
            on_delete: fk.on_delete,
            on_update: fk.on_update,
            name: Some(name),
        });
    }
    Ok(ResolvedConstraints {
        primary_key: pk,
        primary_key_name,
        fks: resolved,
        uniques,
        checks,
    })
}

/// Default name of a table's PRIMARY KEY constraint.
pub fn default_pk_name(table: &str) -> String {
    format!("{}_pkey", table)
}

/// Default name of a FOREIGN KEY constraint over the child columns `cols`.
pub fn default_fk_name(table: &str, cols: &[String]) -> String {
    format!("{}_{}_fkey", table, cols.join("_"))
}

/// The name of foreign key `fk` declared on table `child` (stored, else the
/// deterministic default).
pub fn fk_name(child: &TableInfo, fk: &ForeignKey) -> String {
    fk.name.clone().unwrap_or_else(|| {
        let cols: Vec<String> = fk.columns.iter().map(|c| c.value.clone()).collect();
        default_fk_name(&child.name, &cols)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::decimal::Decimal;

    #[test]
    fn typed_key_equality() {
        let d = |s: &str| ColumnValue::Decimal(Decimal::parse(s).unwrap());
        assert!(key_values_equal(&[ColumnValue::Integer(1)], &[d("1.00")]));
        assert!(key_values_equal(
            &[ColumnValue::Char("a  ".into())],
            &[ColumnValue::Text("a".into())]
        ));
        assert!(!key_values_equal(
            &[ColumnValue::Integer(1)],
            &[ColumnValue::Integer(2)]
        ));
        assert!(!key_values_equal(
            &[ColumnValue::Null],
            &[ColumnValue::Null]
        ));
        assert!(!key_values_equal(
            &[ColumnValue::Integer(1), ColumnValue::Null],
            &[ColumnValue::Integer(1), ColumnValue::Null]
        ));
        assert!(!key_values_equal(
            &[ColumnValue::Integer(1)],
            &[ColumnValue::Text("1".into())]
        ));
        assert_eq!(render_key(&[ColumnValue::Integer(1)]), "1");
        assert_eq!(
            render_key(&[ColumnValue::Integer(1), ColumnValue::Integer(2)]),
            "(1, 2)"
        );
    }
}
