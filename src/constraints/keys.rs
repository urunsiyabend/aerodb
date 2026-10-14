//! Logical PRIMARY KEY / FOREIGN KEY checks.
//!
//! SQL keys are compared by value (typed equality through
//! [`Value::compare`]: numeric equality across INTEGER/DECIMAL/DOUBLE, CHAR
//! PAD SPACE), never by the B-tree's physical row id. The same functions run
//! at statement time (with a [`Pending`] overlay describing the statement's
//! not-yet-applied changes) and at commit recheck (empty overlay, against the
//! commit-time view).

use crate::catalog::{Catalog, TableInfo};
use crate::error::{DbError, DbResult};
use crate::sql::ast::{Action, ForeignKey, Ident};
use crate::sql::value::{SqlType, Value};
use crate::storage::btree::BTree;
use crate::storage::row::{ColumnType, ColumnValue, Row, RowData};
use crate::transaction::{Snapshot, TransactionId};
use std::cmp::Ordering;
use std::collections::HashSet;

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
    let parts: Vec<String> =
        values.iter().map(|v| Value::from_column_value(v).to_string()).collect();
    if parts.len() == 1 { parts[0].clone() } else { format!("({})", parts.join(", ")) }
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
        Some(cols) if !cols.is_empty() => {
            cols.iter().map(|c| column_position(table, c)).collect::<DbResult<Vec<_>>>().map(Some)
        }
        _ => Ok(None),
    }
}

/// The values of `row` at `idx`.
pub fn key_of(row: &RowData, idx: &[usize]) -> Vec<ColumnValue> {
    idx.iter().map(|&i| row.0.get(i).cloned().unwrap_or(ColumnValue::Null)).collect()
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
        self.removed.insert((table.to_string(), row.key, row.created_tx));
    }

    /// `data` is about to become a visible row of `table`.
    pub fn add(&mut self, table: &str, data: RowData) {
        self.added.push((table.to_string(), data));
    }

    pub fn is_removed(&self, table: &str, row: &Row) -> bool {
        self.removed.contains(&(table.to_string(), row.key, row.created_tx))
    }

    fn added_rows<'a>(&'a self, table: &'a str) -> impl Iterator<Item = &'a RowData> + 'a {
        self.added.iter().filter(move |(t, _)| t == table).map(|(_, d)| d)
    }
}

/// Rows of `table` visible to `snapshot`, minus the overlay's removed versions.
pub fn visible_rows(
    catalog: &Catalog,
    table: &TableInfo,
    snapshot: &Snapshot,
    pending: &Pending,
) -> DbResult<Vec<Row>> {
    let root = catalog.get_table(&table.name).map(|t| t.root_page).unwrap_or(table.root_page);
    let tree = BTree::open_root(&catalog.pager, root)?;
    Ok(tree
        .scan_visible(snapshot)?
        .into_iter()
        .filter(|r| !pending.is_removed(&table.name, r))
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
        if existing.iter().chain(seen.iter()).any(|k| key_values_equal(k, &key)) {
            return Err(DbError::DuplicateKey(render_key(&key)));
        }
        seen.push(key);
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
    if pending.added_rows(&table.name).any(|d| key_values_equal(&key_of(d, idx), key)) {
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
                "key ({}) = {} of table \"{}\" is not present in table \"{}\"",
                names(&fk.columns),
                render_key(&key),
                table.name,
                parent.name
            )));
        }
    }
    Ok(())
}

fn names(cols: &[Ident]) -> String {
    cols.iter().map(|c| c.value.as_str()).collect::<Vec<_>>().join(", ")
}

/// Why rows are leaving a parent table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Removal {
    /// DELETE: ON DELETE CASCADE children are handled by [`plan_delete`];
    /// other references are violations.
    Delete,
    /// UPDATE: the old row images are replaced. ON UPDATE CASCADE is not
    /// implemented yet and behaves like NO ACTION.
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
        for fk in child.fks.iter().filter(|fk| fk.parent_table.value == table.name) {
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
                        let mut keys: Vec<Vec<ColumnValue>> = visible_rows(catalog, table, snapshot, pending)?
                            .iter()
                            .map(|r| key_of(&r.data, &parent_idx))
                            .collect();
                        keys.extend(pending.added_rows(&table.name).map(|d| key_of(d, &parent_idx)));
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
                        && !referencing.iter().any(|x| x.key == r.key && x.created_tx == r.created_tx)
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
        let what = if removal == Removal::Update { "update" } else { "delete" };
        return Err(DbError::ForeignKeyViolation(format!(
            "{} on table \"{}\" leaves rows of \"{}\" referencing ({}) without a parent",
            what,
            table.name,
            child.name,
            names(&fk.parent_columns)
        )));
    }
    Ok(())
}

/// Plan a DELETE of `rows` from `table`: follow ON DELETE CASCADE references
/// recursively, then check every other reference to the deleted keys
/// (NO ACTION / RESTRICT) against the final set of deleted rows. Returns the
/// rows to delete per table, `table` first.
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
            let fresh: Vec<Row> = refs.into_iter().filter(|r| !pending.is_removed(&child.name, r)).collect();
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
        let data: Vec<RowData> = batch.iter().map(|r| r.data.clone()).collect();
        check_removed_keys(catalog, t, &data, snapshot, &pending, Removal::Delete)?;
    }
    Ok(batches)
}

/// Resolve `ident` against declared column names with the identifier rule.
fn resolve_column(columns: &[(String, ColumnType)], ident: &Ident, table: &str) -> DbResult<usize> {
    let hits: Vec<usize> =
        columns.iter().enumerate().filter(|(_, (n, _))| ident.matches(n)).map(|(i, _)| i).collect();
    match hits.as_slice() {
        [i] => Ok(*i),
        [] => Err(DbError::ColumnNotFound(format!("{}.{}", table, ident.value))),
        _ => Err(DbError::AmbiguousColumn(ident.value.clone())),
    }
}

fn comparable(a: &ColumnType, b: &ColumnType) -> bool {
    let (x, y) = (SqlType::from_column_type(a), SqlType::from_column_type(b));
    (x.is_numeric() && y.is_numeric())
        || (x.is_text() && y.is_text())
        || std::mem::discriminant(&x) == std::mem::discriminant(&y)
}

/// Validate and canonicalise the PRIMARY KEY and FOREIGN KEY clauses of a
/// CREATE TABLE: names resolve with the identifier rule and are stored as
/// the declared (quoted) names; each foreign key references exactly the
/// parent's PRIMARY KEY column set with the same arity and comparable types.
pub fn resolve_table_constraints(
    catalog: &Catalog,
    table_name: &str,
    columns: &[(String, ColumnType)],
    primary_key: Option<Vec<Ident>>,
    fks: Vec<ForeignKey>,
) -> DbResult<(Option<Vec<String>>, Vec<ForeignKey>)> {
    let pk = match primary_key {
        Some(cols) => {
            let mut out: Vec<String> = Vec::new();
            for c in &cols {
                let name = columns[resolve_column(columns, c, table_name)?].0.clone();
                if out.contains(&name) {
                    return Err(DbError::Syntax(format!(
                        "column \"{}\" appears twice in primary key constraint",
                        name
                    )));
                }
                out.push(name);
            }
            Some(out)
        }
        None => None,
    };

    let mut resolved = Vec::with_capacity(fks.len());
    for fk in fks {
        let child_idx: Vec<usize> = fk
            .columns
            .iter()
            .map(|c| resolve_column(columns, c, table_name))
            .collect::<DbResult<_>>()?;
        let self_ref = fk.parent_table.matches(table_name);
        let (parent_name, parent_cols, parent_pk) = if self_ref {
            (table_name.to_string(), columns.to_vec(), pk.clone())
        } else {
            let p = catalog.resolve_table(&fk.parent_table)?;
            (p.name.clone(), p.columns.clone(), p.primary_key.clone())
        };
        let parent_idx: Vec<usize> = fk
            .parent_columns
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
        let referenced: Vec<String> = parent_idx.iter().map(|&i| parent_cols[i].0.clone()).collect();
        let mut ref_set: Vec<&String> = referenced.iter().collect();
        ref_set.sort();
        ref_set.dedup();
        let mut key_set: Vec<&String> = parent_pk.iter().flatten().collect();
        key_set.sort();
        if ref_set.len() != referenced.len() || ref_set != key_set {
            return Err(DbError::InvalidForeignKey(format!(
                "referenced columns ({}) are not the primary key of table \"{}\"",
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
        resolved.push(ForeignKey {
            columns: child_idx.iter().map(|&i| Ident::quoted(columns[i].0.clone())).collect(),
            parent_table: Ident::quoted(parent_name),
            parent_columns: referenced.into_iter().map(Ident::quoted).collect(),
            on_delete: fk.on_delete,
            on_update: fk.on_update,
        });
    }
    Ok((pk, resolved))
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
        assert!(!key_values_equal(&[ColumnValue::Integer(1)], &[ColumnValue::Integer(2)]));
        assert!(!key_values_equal(&[ColumnValue::Null], &[ColumnValue::Null]));
        assert!(!key_values_equal(
            &[ColumnValue::Integer(1), ColumnValue::Null],
            &[ColumnValue::Integer(1), ColumnValue::Null]
        ));
        assert!(!key_values_equal(&[ColumnValue::Integer(1)], &[ColumnValue::Text("1".into())]));
        assert_eq!(render_key(&[ColumnValue::Integer(1)]), "1");
        assert_eq!(render_key(&[ColumnValue::Integer(1), ColumnValue::Integer(2)]), "(1, 2)");
    }
}
