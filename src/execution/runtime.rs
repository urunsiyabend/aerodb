use crate::catalog::{Catalog, Relation, TableInfo, ViewInfo};
use crate::constraints::keys::{self, Pending, Removal};
use crate::constraints::default::DefaultConstraint;
use crate::error::{DbError, DbResult};
use crate::execution::eval::{Ctx, Env, Scope, eval, eval_predicate};
use crate::execution::query::{ResultColumn, execute_query};
use crate::sql::ast::{BinaryOp, Expr, Ident, Query, Statement};
use crate::sql::value::{SqlType, Value, ValueError};
use crate::storage::btree::BTree;
use crate::storage::row::{COMMITTED_BOOTSTRAP_TX, ColumnType, ColumnValue, Row, RowData};
use crate::transaction::{Snapshot, TransactionStatus};
use std::collections::{HashMap, HashSet};

/// The MVCC snapshot reads and DML scans use: the active transaction's
/// snapshot, or "everything committed" outside a transaction.
pub(crate) fn read_snapshot(catalog: &Catalog) -> Snapshot {
    catalog
        .current_snapshot()
        .unwrap_or_else(|| Snapshot::new(u64::MAX, Vec::new()))
}

fn current_tx_id(catalog: &Catalog) -> u64 {
    catalog
        .transaction_snapshot()
        .and_then(|snapshot| snapshot.current_tx_id)
        .unwrap_or(COMMITTED_BOOTSTRAP_TX)
}

fn ensure_no_write_conflict(
    table_btree: &mut BTree<'_>,
    key: i32,
    visible_created_tx: u64,
    snapshot: &Snapshot,
) -> DbResult<()> {
    if table_btree.has_write_conflict(key, visible_created_tx, snapshot)? {
        return Err(DbError::WriteConflict(key));
    }
    Ok(())
}

fn table_info(catalog: &Catalog, table_name: &str) -> DbResult<TableInfo> {
    catalog
        .get_table(table_name)
        .cloned()
        .map_err(|_| DbError::TableNotFound(table_name.to_string()))
}

/// Columns of a base table as an expression scope qualified by the table name.
fn table_scope(info: &TableInfo) -> Scope {
    keys::table_scope(info)
}

fn row_values(row: &RowData) -> Vec<Value> {
    row.0.iter().map(Value::from_column_value).collect()
}

/// Assignment (INSERT/UPDATE) coercion failure, classified like evaluator
/// errors: too long → `StringTooLong` (22001), out of range →
/// `NumericOverflow` (22003), unparseable → `InvalidCast` (22018).
pub(crate) fn assignment_error(column: &str, e: ValueError) -> DbError {
    match e {
        ValueError::NumericOverflow => {
            DbError::NumericOverflow(format!("value for column '{}' is out of range", column))
        }
        other => DbError::from(other),
    }
}

/// A row image must fit in one B-tree leaf page (rows never span pages):
/// checked for every new image before a statement writes anything, so an
/// oversized row fails cleanly with `RowTooLarge` (54000) and no effect.
fn ensure_rows_fit<'a>(images: impl IntoIterator<Item = &'a RowData>) -> DbResult<()> {
    let max = crate::storage::btree::max_row_data_size();
    for image in images {
        let size = image.serialize().len();
        if size > max {
            return Err(DbError::RowTooLarge(format!("{} bytes, max {}", size, max)));
        }
    }
    Ok(())
}

fn coerce(v: &Value, ty: &ColumnType, column: &str) -> DbResult<ColumnValue> {
    v.to_column_value(ty, column).map_err(|e| assignment_error(column, e))
}

/// Position of the column `ident` names in `info`, with the identifier rule
/// of column references (unquoted: case-insensitive, quoted: exact).
fn resolve_column(info: &TableInfo, ident: &Ident) -> DbResult<usize> {
    let hits: Vec<usize> = info
        .columns
        .iter()
        .enumerate()
        .filter(|(_, (n, _))| ident.matches(n))
        .map(|(i, _)| i)
        .collect();
    match hits.as_slice() {
        [i] => Ok(*i),
        [] => Err(DbError::ColumnNotFound(ident.value.clone())),
        _ => Err(DbError::AmbiguousColumn(ident.value.clone())),
    }
}

/// Resolve INSERT/UPDATE target columns; a column may be named only once.
pub(crate) fn resolve_targets<'a>(info: &TableInfo, idents: impl Iterator<Item = &'a Ident>) -> DbResult<Vec<usize>> {
    let mut out: Vec<usize> = Vec::new();
    for ident in idents {
        let idx = resolve_column(info, ident)?;
        if out.contains(&idx) {
            return Err(DbError::Syntax(format!(
                "column \"{}\" specified more than once",
                info.columns[idx].0
            )));
        }
        out.push(idx);
    }
    Ok(out)
}

/// Rows of `info` visible to the current snapshot for which `selection` is TRUE.
fn matching_rows(catalog: &Catalog, info: &TableInfo, selection: Option<&Expr>) -> DbResult<Vec<Row>> {
    let snapshot = read_snapshot(catalog);
    let tree = BTree::open_root(&catalog.pager, info.root_page)?;
    let mut rows = tree.scan_visible(&snapshot)?;
    for row in &mut rows {
        info.pad_row(&mut row.data);
    }
    let Some(pred) = selection else {
        return Ok(rows);
    };
    let scope = table_scope(info);
    let cx = Ctx::with_catalog(catalog);
    let mut out = Vec::new();
    for row in rows {
        let vals = row_values(&row.data);
        if eval_predicate(pred, &Env::new(&scope, &vals, None), &cx)? {
            out.push(row);
        }
    }
    Ok(out)
}

/// Mark the visible versions `rows` of `table_name` deleted by the current
/// transaction (after first-committer-wins conflict checks on all of them).
/// Callers record the write intents with [`record_write_intents`] once all
/// of the statement's writes to the table are done.
fn delete_versions(catalog: &mut Catalog, table_name: &str, rows: &[Row]) -> DbResult<()> {
    let snapshot = read_snapshot(catalog);
    let tx_id = current_tx_id(catalog);
    let root = catalog.get_table(table_name)?.root_page;
    let mut tree = BTree::open_root(&catalog.pager, root)?;
    for r in rows {
        ensure_no_write_conflict(&mut tree, r.key, r.created_tx, &snapshot)?;
    }
    for r in rows {
        if !tree.mark_deleted_visible(r.key, &snapshot, tx_id)? {
            return Err(std::io::Error::other(format!(
                "internal error: version of row {} in {table_name} vanished before it was deleted",
                r.key
            ))
            .into());
        }
    }
    // Index entries intentionally remain until vacuum: indexed lookups
    // re-check base-table visibility with find_visible().
    Ok(())
}

/// Record write intents for the replaced/deleted versions `rows` against the
/// table's current root, so commit re-validation searches the right tree.
fn record_write_intents(catalog: &mut Catalog, table_name: &str, rows: &[Row]) -> DbResult<()> {
    let root = catalog.get_table(table_name)?.root_page;
    for r in rows {
        catalog.pager.record_write_intent(root, r.key, r.created_tx);
    }
    Ok(())
}

/// Physical key for a new row version of a table with a rowid alias (column
/// `alias_idx`) whose alias value is `alias`: the value itself, unless that
/// physical key holds any version (live or dead) of a row with a different
/// alias value (possible in files written by 0.5.1, whose key was the first
/// column, or after a fallback) — then a fresh row id. Dead versions of the
/// same alias value share the chain, as in 0.5.1.
fn alias_key(catalog: &mut Catalog, table_name: &str, alias_idx: usize, alias: i32) -> DbResult<i32> {
    let root = catalog.get_table(table_name)?.root_page;
    let versions = BTree::open_root(&catalog.pager, root)?.versions_of(alias)?;
    let foreign = versions
        .iter()
        .any(|r| r.data.0.get(alias_idx) != Some(&ColumnValue::Integer(alias)));
    // A same-alias version of another transaction that is or may become live
    // (committed and not deleted, or still in progress) must not be collided
    // with physically: the two versions would share one chain and hide each
    // other from scans. A fresh rowid lets the statement-time or commit-time
    // PRIMARY KEY check report DuplicateKey (23505) instead.
    let current = catalog.transaction_id();
    let occupied = catalog.pager.with_transaction_table(|tx_table| {
        let gone = |tx: u64| matches!(tx_table.get(&tx), Some(TransactionStatus::Aborted));
        let pending = |tx: u64| matches!(tx_table.get(&tx), Some(TransactionStatus::Active | TransactionStatus::Aborted));
        versions.iter().any(|r| {
            Some(r.created_tx) != current
                && !gone(r.created_tx)
                && match r.deleted_tx {
                    None => true,
                    Some(d) => Some(d) != current && pending(d),
                }
        })
    });
    if foreign || occupied { catalog.allocate_rowid(table_name) } else { Ok(alias) }
}

/// Write `data` as a new version with physical key `key` and index it.
fn write_version(catalog: &mut Catalog, table_name: &str, key: i32, data: RowData, fresh: bool) -> DbResult<()> {
    let root = catalog.get_table(table_name)?.root_page;
    let mut tree = BTree::open_root(&catalog.pager, root)?;
    if fresh {
        tree.insert(key, data.clone())?;
    } else {
        let mut row = Row::new(key, data.clone());
        row.created_tx = current_tx_id(catalog);
        row.deleted_tx = None;
        tree.insert_version(row)?;
    }
    // The root page never moves (`BTree::keep_root`), so the catalog row and
    // the in-memory TableInfo stay valid without a rewrite.
    debug_assert_eq!(tree.root_page(), root);
    drop(tree);
    catalog.note_rowid_used(table_name, key);
    catalog.insert_into_indexes(table_name, key, &data)?;
    Ok(())
}

pub fn execute_delete(
    catalog: &mut Catalog,
    table_name: &str,
    selection: Option<Expr>,
) -> DbResult<usize> {
    let info = table_info(catalog, table_name)?;
    let rows = matching_rows(catalog, &info, selection.as_ref())?;
    if rows.is_empty() {
        return Ok(0);
    }
    let count = rows.len();
    let snapshot = read_snapshot(catalog);
    // Referential actions: ON DELETE CASCADE closure, NO ACTION/RESTRICT check.
    let batches = keys::plan_delete(catalog, &info, rows, &snapshot)?;
    for (table, batch) in &batches {
        delete_versions(catalog, &table.name, batch)?;
    }
    for (table, batch) in &batches {
        record_write_intents(catalog, &table.name, batch)?;
    }
    Ok(count)
}

/// Value a column takes when no value (or the DEFAULT keyword) is supplied.
fn column_default(info: &TableInfo, idx: usize) -> DbResult<Option<Value>> {
    match info.default_values.get(idx).and_then(|d| d.as_ref()) {
        Some(expr) => Ok(Some(DefaultConstraint::evaluate(expr)?)),
        None => Ok(None),
    }
}

pub fn execute_update(
    catalog: &mut Catalog,
    table_name: &str,
    assignments: Vec<(Ident, Expr)>,
    selection: Option<Expr>,
) -> DbResult<usize> {
    let info = table_info(catalog, table_name)?;
    let target_idx = resolve_targets(&info, assignments.iter().map(|(c, _)| c))?;
    let targets: Vec<(usize, Expr)> =
        target_idx.into_iter().zip(assignments.into_iter().map(|(_, e)| e)).collect();

    // (old visible version, new row image)
    let mut ops: Vec<(Row, RowData)> = Vec::new();
    {
        let rows = matching_rows(catalog, &info, selection.as_ref())?;
        let scope = table_scope(&info);
        let cx = Ctx::with_catalog(catalog);
        for row in rows {
            let old = row_values(&row.data);
            let env = Env::new(&scope, &old, None);
            let mut new_data = row.data.clone();
            for (idx, expr) in &targets {
                let (name, ty) = &info.columns[*idx];
                let v = match expr {
                    Expr::Default => column_default(&info, *idx)?.unwrap_or(Value::Null),
                    e => eval(e, &env, &cx)?,
                };
                new_data.0[*idx] = coerce(&v, ty, name)?;
            }
            ops.push((row, new_data));
        }
    }
    if ops.is_empty() {
        return Ok(0);
    }
    let count = ops.len();

    // ON UPDATE CASCADE closure: child rows whose referenced parent key
    // changes are rewritten in the same statement.
    let snapshot = read_snapshot(catalog);
    let plan = keys::plan_update(catalog, &info, ops, &snapshot)?;
    ensure_rows_fit(plan.iter().flat_map(|(_, ops)| ops.iter().map(|(_, new)| new)))?;

    // The statement's overlay: every old version (direct and cascaded) is
    // gone and every new image is present.
    let mut pending = Pending::new();
    for (table, ops) in &plan {
        for (old, new) in ops {
            pending.remove(&table.name, old);
            pending.add(&table.name, new.clone());
        }
    }

    // ON UPDATE RESTRICT: a changed key still referenced by a child row the
    // statement does not re-point is a violation, even if another row of the
    // statement re-provides the key.
    for (table, ops) in &plan {
        let changes: Vec<(RowData, Option<RowData>)> =
            ops.iter().map(|(old, new)| (old.data.clone(), Some(new.clone()))).collect();
        keys::check_restrict(catalog, table, &changes, &snapshot, &pending)?;
    }

    // Constraint checks against the statement's final state.
    for (table, ops) in &plan {
        let images: Vec<RowData> = ops.iter().map(|(_, new)| new.clone()).collect();
        keys::check_new_rows(catalog, table, &snapshot, &images, &pending)?;
    }
    for (table, ops) in &plan {
        let olds: Vec<RowData> = ops.iter().map(|(old, _)| old.data.clone()).collect();
        keys::check_removed_keys(catalog, table, &olds, &snapshot, &pending, Removal::Update)?;
    }

    // Apply: retire every old version first, so a new image may take a
    // physical key another updated row is vacating (`SET id = id + 1`).
    for (table, ops) in &plan {
        let old_rows: Vec<Row> = ops.iter().map(|(old, _)| old.clone()).collect();
        delete_versions(catalog, &table.name, &old_rows)?;
    }
    for (table, ops) in &plan {
        let alias = Catalog::rowid_alias_column(table);
        for (old, new) in ops {
            let key = match alias {
                Some(i) if new.0[i] != old.data.0[i] => match new.0[i] {
                    ColumnValue::Integer(v) => alias_key(catalog, &table.name, i, v)?,
                    _ => return Err(DbError::NullViolation(table.columns[i].0.clone())),
                },
                _ => old.key,
            };
            write_version(catalog, &table.name, key, new.clone(), false)?;
        }
    }
    // After the new versions: their inserts may have split the root.
    for (table, ops) in &plan {
        let old_rows: Vec<Row> = ops.iter().map(|(old, _)| old.clone()).collect();
        record_write_intents(catalog, &table.name, &old_rows)?;
    }
    Ok(count)
}

/// Build the stored row for one VALUES tuple; `targets` are the resolved
/// positions of an explicit column list.
fn build_insert_row(
    catalog: &mut Catalog,
    info: &TableInfo,
    targets: Option<&[usize]>,
    row_vals: &[Expr],
) -> DbResult<RowData> {
    let meta = &info.columns;
    let expected = targets.map_or(meta.len(), <[usize]>::len);
    if row_vals.len() != expected {
        return Err(DbError::InvalidValue(match targets {
            Some(_) => format!(
                "INSERT column/value count mismatch: expected {} columns, got {} values",
                expected,
                row_vals.len()
            ),
            None => format!(
                "INSERT has wrong number of values: expected {}, got {}",
                expected,
                row_vals.len()
            ),
        }));
    }

    let mut out = Vec::with_capacity(meta.len());
    for (idx, (col_name, col_type)) in meta.iter().enumerate() {
        let supplied = match targets {
            Some(t) => t.iter().position(|&c| c == idx).map(|pos| &row_vals[pos]),
            None => Some(&row_vals[idx]),
        };
        let auto = info.auto_increment.get(idx).copied().unwrap_or(false);
        let seq = if auto { catalog.auto_increment_sequence(&info.name, col_name) } else { String::new() };
        let value = match supplied {
            Some(expr) if !matches!(expr, Expr::Default) => {
                let v = crate::execution::eval::evaluate_constant(expr)?;
                if auto && let Value::Int(i) = v {
                    catalog.update_sequence_current(&seq, i)?;
                }
                v
            }
            _ => {
                if auto {
                    Value::Int(catalog.next_sequence_value(&seq)?)
                } else if let Some(def) = column_default(info, idx)? {
                    def
                } else if !info.not_null[idx] {
                    Value::Null
                } else if supplied.is_some() {
                    return Err(DbError::InvalidValue(format!(
                        "Cannot use DEFAULT for column '{}' - no default value defined",
                        col_name
                    )));
                } else {
                    return Err(DbError::InvalidValue(format!(
                        "Column '{}' requires a value or DEFAULT",
                        col_name
                    )));
                }
            }
        };
        out.push(coerce(&value, col_type, col_name)?);
    }
    Ok(RowData(out))
}

pub fn execute_insert(
    catalog: &mut Catalog,
    table_name: &str,
    columns: Option<Vec<Ident>>,
    rows: Vec<Vec<Expr>>,
) -> DbResult<usize> {
    let info = table_info(catalog, table_name)?;
    let targets = match &columns {
        Some(cols) => Some(resolve_targets(&info, cols.iter())?),
        None => None,
    };
    let alias = Catalog::rowid_alias_column(&info);

    // Build every row first, then check them all against the statement's
    // final state (existing rows plus all new images), then write.
    let mut images: Vec<RowData> = Vec::with_capacity(rows.len());
    for row_vals in &rows {
        images.push(build_insert_row(catalog, &info, targets.as_deref(), row_vals)?);
    }
    ensure_rows_fit(&images)?;
    let snapshot = read_snapshot(catalog);
    let mut pending = Pending::new();
    for image in &images {
        pending.add(table_name, image.clone());
    }
    keys::check_new_rows(catalog, &info, &snapshot, &images, &pending)?;

    let inserted = images.len();
    for row_data in images {
        let key = match alias {
            Some(i) => match row_data.0[i] {
                ColumnValue::Integer(v) => alias_key(catalog, table_name, i, v)?,
                _ => return Err(DbError::NullViolation(info.columns[i].0.clone())),
            },
            None => catalog.allocate_rowid(table_name)?,
        };
        write_version(catalog, table_name, key, row_data, true)?;
    }
    Ok(inserted)
}

/// DROP ... RESTRICT: refuse to drop table or view `name` while another
/// object depends on it — a FOREIGN KEY of another table referencing the
/// table (a self-reference does not count), or a view whose stored query
/// reads the relation.
fn ensure_no_dependents(catalog: &Catalog, name: &str, is_view: bool) -> DbResult<()> {
    let fk_dependent = !is_view
        && catalog
            .all_tables()
            .iter()
            .any(|t| t.name != name && t.fks.iter().any(|fk| fk.parent_table.value == name));
    let view_dependent = catalog.all_views().iter().any(|v| {
        !(is_view && v.name == name) && v.query.referenced_relations().iter().any(|r| r.matches(name))
    });
    if fk_dependent || view_dependent {
        return Err(DbError::DependentObjects(name.to_string()));
    }
    Ok(())
}

/// CREATE VIEW: the name must be free, wildcards are expanded (the view keeps
/// its definition-time columns), the query must run against the current
/// schema, and the output names (column list, else the query's) must be
/// distinct. The stored text is the canonical `Query::to_sql`, re-parsed so
/// the cached query is exactly what a reopen will see.
fn create_view(catalog: &mut Catalog, name: Ident, columns: Option<Vec<String>>, query: Query) -> DbResult<()> {
    // Identifier rule, as for CREATE TABLE.
    if let Some(existing) = catalog.colliding_relation(&name) {
        return Err(DbError::DuplicateObject(existing));
    }
    let name = name.value;
    let mut query = query;
    crate::execution::query::expand_wildcards(catalog, &mut query)?;
    let sql = query.to_sql();
    let query = match crate::sql::parser::parse_sql(&sql)? {
        Statement::Query(q) => *q,
        _ => return Err(DbError::Syntax(format!("view query does not round-trip: {}", sql))),
    };
    let result = execute_query(catalog, &query, None)?;
    let names = match columns {
        Some(cols) => {
            if cols.len() != result.columns.len() {
                return Err(DbError::Syntax(format!(
                    "view \"{}\" names {} columns but its query returns {}",
                    name,
                    cols.len(),
                    result.columns.len()
                )));
            }
            cols
        }
        None => result.columns.iter().map(|c| c.name.clone()).collect(),
    };
    for (i, n) in names.iter().enumerate() {
        if names[..i].contains(n) {
            return Err(DbError::Syntax(format!(
                "column \"{}\" specified more than once in view \"{}\"",
                n, name
            )));
        }
    }
    catalog.create_view(ViewInfo { name: name.clone(), columns: names, sql, query })?;
    println!("View {} created", name);
    Ok(())
}

/// Would two column names collide under the identifier rule: if either was
/// written unquoted they compare case-insensitively, if both were quoted
/// exactly.
fn names_collide(a: &str, a_quoted: bool, b: &str, b_quoted: bool) -> bool {
    if a_quoted && b_quoted { a == b } else { a.eq_ignore_ascii_case(b) }
}

/// ALTER TABLE t ADD COLUMN: validate the new column against every visible
/// row (default value or NULL appended; NOT NULL / CHECK / UNIQUE and all
/// existing constraints re-checked on the new images), then persist the new
/// shape and rewrite each visible row as a new MVCC version in the current
/// transaction (write intents recorded like UPDATE). Nothing changes if any
/// check fails.
fn alter_add_column(
    catalog: &mut Catalog,
    table_name: &Ident,
    column: crate::sql::ast::ColumnDef,
    constraints: Vec<crate::sql::ast::TableConstraint>,
) -> DbResult<()> {
    use crate::sql::ast::TableConstraint;
    let info = catalog.resolve_table(table_name)?.clone();
    // Quotedness of existing columns is not stored: the new name is matched
    // per the identifier rule (unquoted: case-insensitive, quoted: exact).
    if info.columns.iter().any(|(n, _)| names_collide(n, true, &column.name, column.quoted)) {
        return Err(DbError::DuplicateColumn(column.name));
    }
    let mut columns = info.columns.clone();
    columns.push((column.name.clone(), column.col_type));

    // Constraint names must not collide with the table's existing ones.
    let mut used: HashSet<String> = info
        .uniques
        .iter()
        .map(|u| u.name.clone())
        .chain(info.checks.iter().map(|c| c.name.clone()))
        .chain(info.primary_key_name.clone())
        .chain(info.fks.iter().map(|fk| keys::fk_name(&info, fk)))
        .collect();
    for c in &constraints {
        if let TableConstraint::Unique { name: Some(n), .. } | TableConstraint::Check { name: Some(n), .. } = c
            && used.contains(n)
        {
            return Err(DbError::DuplicateConstraint { name: n.clone(), table: info.name.clone() });
        }
    }
    let resolved = keys::resolve_table_constraints(catalog, &info.name, &columns, None, Vec::new(), constraints)?;
    let mut new_info = info.clone();
    new_info.columns = columns;
    new_info.not_null.push(column.not_null);
    new_info.default_values.push(column.default_value.clone());
    new_info.auto_increment.push(false);
    for mut u in resolved.uniques {
        if used.contains(&u.name) {
            u.name = keys::fresh_name(u.name, &used);
        }
        used.insert(u.name.clone());
        new_info.uniques.push(u);
    }
    for mut c in resolved.checks {
        if used.contains(&c.name) {
            c.name = keys::fresh_name(c.name, &used);
        }
        used.insert(c.name.clone());
        new_info.checks.push(c);
    }

    // The value existing rows get: the default, evaluated once, or NULL.
    let fill = match &column.default_value {
        Some(expr) => coerce(&DefaultConstraint::evaluate(expr)?, &column.col_type, &column.name)?,
        None => ColumnValue::Null,
    };
    let rows = matching_rows(catalog, &info, None)?;
    let images: Vec<RowData> = rows
        .iter()
        .map(|r| {
            let mut data = r.data.clone();
            data.0.push(fill.clone());
            data
        })
        .collect();
    ensure_rows_fit(&images)?;
    let snapshot = read_snapshot(catalog);
    let mut pending = Pending::new();
    for (old, new) in rows.iter().zip(&images) {
        pending.remove(&info.name, old);
        pending.add(&info.name, new.clone());
    }
    keys::check_new_rows(catalog, &new_info, &snapshot, &images, &pending)?;

    // Apply: retire the old versions (write-conflict checks first), persist
    // the new shape, then write the widened versions under the same keys.
    delete_versions(catalog, &info.name, &rows)?;
    new_info.root_page = catalog.get_table(&info.name)?.root_page;
    catalog.replace_table_info(new_info)?;
    for (old, new) in rows.iter().zip(images) {
        write_version(catalog, &info.name, old.key, new, false)?;
    }
    record_write_intents(catalog, &info.name, &rows)?;
    println!("Table {} altered", info.name);
    Ok(())
}

fn same_category(v: &Value, ty: &ColumnType) -> bool {
    let st = SqlType::from_column_type(ty);
    match v {
        Value::Int(_) | Value::Decimal(_) | Value::Double(_) => st.is_numeric(),
        Value::Text(_) | Value::Char(_) => st.is_text(),
        Value::Bool(_) => st == SqlType::Boolean,
        other => other.sql_type() == st,
    }
}

/// Scan `table_name`, keeping rows for which `selection` is TRUE. An equality
/// predicate on an indexed column probes the index first; candidates are
/// re-checked against MVCC visibility and the full predicate. Returns whether
/// an index was used.
pub fn execute_select_with_indexes(
    catalog: &mut Catalog,
    table_name: &str,
    selection: Option<Expr>,
    out: &mut Vec<Row>,
) -> DbResult<bool> {
    let info = table_info(catalog, table_name)?;
    let snapshot = read_snapshot(catalog);
    let scope = table_scope(&info);
    let mut selection = selection;
    if let Some(pred) = &mut selection {
        crate::execution::bind::bind_predicate(catalog, &scope, pred, "WHERE")?;
    }

    if let Some(Expr::Binary { op: BinaryOp::Eq, left, right }) = &selection {
        let probe = match (left.as_ref(), right.as_ref()) {
            (Expr::Column { table, name }, Expr::Literal(v)) | (Expr::Literal(v), Expr::Column { table, name }) => {
                // Same identifier rules as the evaluator; a reference that
                // does not resolve (or is ambiguous) is left to the full scan.
                match scope.resolve(table.as_ref(), name) {
                    Ok(Some(i)) => Some((info.columns[i].0.clone(), v.clone())),
                    _ => None,
                }
            }
            _ => None,
        };
        if let Some((col_name, lit)) = probe {
            let col_type = info.columns.iter().find(|(c, _)| *c == col_name).map(|(_, ty)| *ty);
            let index = catalog.find_index(table_name, &col_name).cloned();
            if let (Some(index), Some(col_type)) = (index, col_type) {
                let lookup = if same_category(&lit, &col_type) {
                    lit.to_column_value(&col_type, &col_name).ok().filter(|cv| *cv != ColumnValue::Null)
                } else {
                    None
                };
                if let Some(val_cv) = lookup {
                    // Only the bucket group of exactly this value (hash
                    // collisions are skipped; NULL is never a candidate).
                    let candidate_keys = catalog.index_candidates(index.root_page, &val_cv)?;
                    let pred = selection.as_ref().expect("probe implies a predicate");
                    let cx = Ctx::with_catalog(catalog);
                    let mut table_tree = BTree::open_root(&catalog.pager, info.root_page)?;
                    let mut seen_keys = HashSet::new();
                    for key in candidate_keys {
                        if !seen_keys.insert(key) {
                            continue;
                        }
                        // Index entries store logical base-row keys and may be stale
                        // until vacuum. Always resolve each candidate through the
                        // base table's MVCC visibility rules before returning it.
                        if let Some(mut r) = table_tree.find_visible(key, &snapshot)? {
                            info.pad_row(&mut r.data);
                            let vals = row_values(&r.data);
                            if eval_predicate(pred, &Env::new(&scope, &vals, None), &cx)? {
                                out.push(r);
                            }
                        }
                    }
                    return Ok(true);
                }
            }
        }
    }

    out.extend(matching_rows(catalog, &info, selection.as_ref())?);
    Ok(false)
}

pub fn handle_statement(catalog: &mut Catalog, stmt: Statement) -> DbResult<()> {
    // Static binding of DML expressions before any row is touched (queries
    // are bound by `execute_query`); the bound form is what runs.
    let mut stmt = stmt;
    if !matches!(stmt, Statement::Query(_)) {
        crate::execution::bind::bind_statement(catalog, &mut stmt)?;
    }
    match stmt {
        Statement::CreateTable {
            table_name,
            columns,
            fks,
            primary_key,
            if_not_exists,
            constraints,
        } => {
            let auto_cols: Vec<_> = columns.iter().filter(|c| c.auto_increment).collect();
            if auto_cols.len() > 1 {
                return Err(DbError::InvalidValue(
                    "Only one AUTO_INCREMENT column allowed per table".into(),
                ));
            }
            // Identifier rule: an unquoted name collides with an existing
            // table or view case-insensitively (else every unquoted
            // reference to either would become ambiguous).
            if let Some(existing) = catalog.colliding_relation(&table_name) {
                if if_not_exists {
                    println!("Table {} already exists", existing);
                    return Ok(());
                }
                return Err(DbError::DuplicateObject(existing));
            }
            let table_name = table_name.value;
            // Same rule as ALTER TABLE ... ADD COLUMN: a declared name twice.
            for (i, c) in columns.iter().enumerate() {
                if columns[..i].iter().any(|p| names_collide(&p.name, p.quoted, &c.name, c.quoted)) {
                    return Err(DbError::DuplicateColumn(c.name.clone()));
                }
            }
            let declared: Vec<(String, ColumnType)> =
                columns.iter().map(|c| (c.name.clone(), c.col_type)).collect();
            let resolved = keys::resolve_table_constraints(
                catalog,
                &table_name,
                &declared,
                primary_key,
                fks,
                constraints,
            )?;
            let cols: Vec<_> = columns
                .into_iter()
                .map(|c| {
                    (
                        c.name.clone(),
                        c.col_type,
                        c.not_null,
                        c.default_value,
                        c.auto_increment,
                    )
                })
                .collect();
            match catalog.create_table_with_constraints(
                &table_name,
                cols.clone(),
                resolved.fks,
                resolved.primary_key,
                resolved.primary_key_name,
                resolved.uniques,
                resolved.checks,
            ) {
                Ok(()) => println!("Table {} created", table_name),
                Err(e) => {
                    if if_not_exists && e.to_string().contains("already exists") {
                        println!("Table {} already exists", table_name);
                        return Ok(());
                    }
                    return Err(DbError::from(e));
                }
            }
            for (name, _, _, _, ai) in cols {
                if ai {
                    // A fresh sequence under the unambiguous 0.6.0 name (a
                    // leftover of an earlier table of this name restarts).
                    let seq_name = Catalog::auto_increment_sequence_name(&table_name, &name);
                    catalog.drop_sequence(&seq_name)?;
                    catalog.create_sequence(&seq_name, 1, 1)?;
                }
            }
        }
        Statement::CreateIndex {
            index_name,
            table_name,
            column_name,
        } => {
            if let Ok(Relation::View(v)) = catalog.resolve_relation(&table_name) {
                return Err(DbError::FeatureNotSupported(format!("cannot create index on view \"{}\"", v.name)));
            }
            let info = catalog.resolve_table(&table_name)?.clone();
            let column = &info.columns[resolve_column(&info, &column_name)?].0;
            if let Some(existing) = catalog.colliding_index(&index_name) {
                return Err(DbError::DuplicateObject(existing));
            }
            catalog.create_index(&index_name.value, &info.name, column)?;
            println!("Index {} created", index_name);
        }
        Statement::DropIndex { name } => match catalog.resolve_index(&name)? {
            Some(declared) => {
                catalog.drop_index(&declared)?;
                println!("Index {} dropped", declared);
            }
            None => return Err(DbError::NotFound(format!("index \"{}\"", name))),
        },
        Statement::Insert {
            table_name,
            columns: col_list,
            rows,
        } => {
            let name = catalog.resolve_table(&table_name)?.name.clone();
            execute_insert(catalog, &name, col_list, rows)?;
        }
        Statement::Query(query) => {
            let result = execute_query(catalog, &query, None)?;
            println!("{}", format_result_header(&result.columns));
            for row in &result.rows {
                let vals: Vec<String> = row.iter().map(Value::to_string).collect();
                println!("{}", format_values(&vals));
            }
        }
        Statement::DropTable { table_name, if_exists } => {
            // Same identifier rule as every other reference.
            let table_name = match catalog.resolve_relation(&table_name) {
                Ok(Relation::Table(t)) => t.name.clone(),
                Ok(Relation::View(v)) => {
                    return Err(DbError::WrongObjectType(format!("{} is not a table", v.name)));
                }
                Err(DbError::TableNotFound(_)) if if_exists => return Ok(()),
                Err(e) => return Err(e),
            };
            ensure_no_dependents(catalog, &table_name, false)?;
            if catalog.drop_table(&table_name)? {
                println!("Table {} dropped", table_name);
            }
        }
        Statement::CreateView { name, columns, query } => {
            create_view(catalog, name, columns, *query)?;
        }
        Statement::DropView { name, if_exists } => {
            let name = match catalog.resolve_relation(&name) {
                Ok(Relation::View(v)) => v.name.clone(),
                Ok(Relation::Table(t)) => {
                    return Err(DbError::WrongObjectType(format!("{} is not a view", t.name)));
                }
                Err(DbError::TableNotFound(_)) if if_exists => return Ok(()),
                Err(e) => return Err(e),
            };
            ensure_no_dependents(catalog, &name, true)?;
            catalog.drop_view(&name)?;
            println!("View {} dropped", name);
        }
        Statement::AlterTableAddColumn { table_name, column, constraints } => {
            alter_add_column(catalog, &table_name, column, constraints)?;
        }
        Statement::Delete {
            table_name,
            selection,
        } => {
            let name = catalog.resolve_table(&table_name)?.name.clone();
            let count = execute_delete(catalog, &name, selection)?;
            println!("{} row(s) deleted", count);
        }
        Statement::Update {
            table_name,
            assignments,
            selection,
        } => {
            let name = catalog.resolve_table(&table_name)?.name.clone();
            let count = execute_update(catalog, &name, assignments, selection)?;
            println!("{} row(s) updated", count);
        }
        Statement::CreateSequence(seq) => {
            if let Some(existing) = catalog.colliding_sequence(&seq.name) {
                return Err(DbError::DuplicateObject(existing));
            }
            catalog.create_sequence(&seq.name, seq.start, seq.increment)?;
            println!("Sequence '{}' created successfully", seq.name);
        }
        Statement::BeginTransaction { .. } | Statement::Commit | Statement::Rollback => {
            // Transaction control is owned by `TransactionManager::execute`, which
            // intercepts these before dispatching here. Reaching this arm means a
            // caller bypassed the manager.
            return Err(DbError::InvalidValue(
                "transaction control statements must go through the transaction manager".into(),
            ));
        }
        Statement::Exit => {}
    }
    Ok(())
}

/// Closest storage column type for a result column (legacy header API).
fn sql_type_to_column_type(ty: &SqlType) -> ColumnType {
    match ty {
        SqlType::Boolean => ColumnType::Boolean,
        SqlType::Integer | SqlType::BigInt => ColumnType::Integer,
        SqlType::SmallInt => ColumnType::SmallInt { width: 0, unsigned: false },
        SqlType::Decimal { precision, scale } => ColumnType::Decimal { precision: *precision, scale: *scale },
        SqlType::Double => ColumnType::Double { precision: 10, scale: 0, unsigned: false },
        SqlType::Text | SqlType::Null => ColumnType::Text,
        SqlType::Char(n) => ColumnType::Char(*n),
        SqlType::Varchar(n) => ColumnType::Varchar(*n),
        SqlType::Date => ColumnType::Date,
        SqlType::DateTime => ColumnType::DateTime,
        SqlType::Timestamp => ColumnType::Timestamp,
        SqlType::Time => ColumnType::Time,
        SqlType::Year => ColumnType::Year,
    }
}

/// Legacy string-rendering wrapper over [`execute_query`]. Correlation
/// context maps are no longer supported (pass an outer `Env` to
/// `execute_query` instead); a non-empty map is rejected.
pub fn execute_select_statement(
    catalog: &mut Catalog,
    stmt: &Statement,
    out: &mut Vec<Vec<String>>,
    context: Option<&HashMap<String, String>>,
) -> DbResult<Vec<(String, ColumnType)>> {
    if context.is_some_and(|c| !c.is_empty()) {
        return Err(DbError::FeatureNotSupported(
            "string correlation context maps; use execute_query with an outer Env".into(),
        ));
    }
    let Statement::Query(query) = stmt else {
        return Err(DbError::InvalidValue("Not a SELECT".into()));
    };
    let result = execute_query(catalog, query, None)?;
    out.extend(
        result
            .rows
            .iter()
            .map(|row| row.iter().map(Value::to_string).collect::<Vec<_>>()),
    );
    Ok(result
        .columns
        .iter()
        .map(|c| (c.name.clone(), sql_type_to_column_type(&c.ty)))
        .collect())
}

pub fn row_to_strings(row: &Row) -> Vec<String> {
    row.data.0.iter().map(|v| v.to_string_value()).collect()
}

pub fn format_values(vals: &[String]) -> String {
    vals.join(" | ")
}

pub fn format_row(row: &Row) -> String {
    row.data
        .0
        .iter()
        .map(|v| v.to_string_value())
        .collect::<Vec<_>>()
        .join(" | ")
}

pub fn format_header(columns: &[(String, ColumnType)]) -> String {
    columns
        .iter()
        .map(|(name, ty)| format!("{} {}", name, ty.as_str()))
        .collect::<Vec<_>>()
        .join(" | ")
}

/// `name TYPE | ...` header for a query result (NULL-typed columns print as TEXT).
pub fn format_result_header(columns: &[ResultColumn]) -> String {
    columns
        .iter()
        .map(|c| {
            let ty = if c.ty == SqlType::Null { SqlType::Text } else { c.ty };
            format!("{} {}", c.name, ty.name())
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 0.5.1 file keyed `p(id INTEGER, n INTEGER PRIMARY KEY)` by its first
    /// column. The rowid alias is now `n`; a new row whose alias value equals
    /// an occupied physical key gets a fresh row id instead of colliding.
    #[test]
    fn alias_key_falls_back_when_legacy_key_is_occupied() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.db");
        let pager = crate::storage::pager::Pager::new(path.to_str().unwrap()).unwrap();
        let mut catalog = Catalog::open(pager).unwrap();
        let parse = |sql: &str| crate::sql::parser::parse_sql(sql).unwrap();
        handle_statement(&mut catalog, parse("CREATE TABLE p (id INTEGER, n INTEGER PRIMARY KEY)")).unwrap();
        let root = catalog.get_table("p").unwrap().root_page;
        // 0.5.1 layout: physical key = first column (id = 1), n = 100.
        BTree::open_root(&catalog.pager, root)
            .unwrap()
            .insert(1, RowData(vec![ColumnValue::Integer(1), ColumnValue::Integer(100)]))
            .unwrap();

        handle_statement(&mut catalog, parse("INSERT INTO p VALUES (2, 1)")).unwrap();
        assert!(matches!(
            handle_statement(&mut catalog, parse("INSERT INTO p VALUES (3, 100)")),
            Err(DbError::DuplicateKey(_))
        ));
        let mut rows = Vec::new();
        execute_select_with_indexes(&mut catalog, "p", None, &mut rows).unwrap();
        let mut seen: Vec<(i32, Vec<ColumnValue>)> = rows.into_iter().map(|r| (r.key, r.data.0)).collect();
        seen.sort_by_key(|(k, _)| *k);
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0], (1, vec![ColumnValue::Integer(1), ColumnValue::Integer(100)]));
        // Key 1 was taken by the legacy row, so (2, 1) got a fresh row id.
        assert_eq!(seen[1], (2, vec![ColumnValue::Integer(2), ColumnValue::Integer(1)]));
    }

    /// Catalog over a fresh file with the 0.5.1-style table
    /// `p(id INTEGER, n INTEGER PRIMARY KEY)` holding (1, 100) at physical key 1.
    fn legacy_p() -> (tempfile::TempDir, Catalog) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.db");
        let pager = crate::storage::pager::Pager::new(path.to_str().unwrap()).unwrap();
        let mut catalog = Catalog::open(pager).unwrap();
        run(&mut catalog, "CREATE TABLE p (id INTEGER, n INTEGER PRIMARY KEY)").unwrap();
        let root = catalog.get_table("p").unwrap().root_page;
        BTree::open_root(&catalog.pager, root)
            .unwrap()
            .insert(1, RowData(vec![ColumnValue::Integer(1), ColumnValue::Integer(100)]))
            .unwrap();
        (dir, catalog)
    }

    fn run(catalog: &mut Catalog, sql: &str) -> DbResult<()> {
        handle_statement(catalog, crate::sql::parser::parse_sql(sql).unwrap())
    }

    /// Every stored version of `p` as (physical key, n).
    fn p_versions(catalog: &Catalog) -> Vec<(i32, ColumnValue)> {
        let root = catalog.get_table("p").unwrap().root_page;
        let mut out: Vec<(i32, ColumnValue)> = BTree::open_root(&catalog.pager, root)
            .unwrap()
            .all_versions()
            .unwrap()
            .into_iter()
            .map(|r| (r.key, r.data.0[1].clone()))
            .collect();
        out.sort_by_key(|(k, _)| *k);
        out
    }

    /// Fallback row ids and alias keys interleave without colliding: alias
    /// writes advance the row id counter.
    #[test]
    fn fallback_and_alias_inserts_interleave_without_collision() {
        let (_dir, mut catalog) = legacy_p();
        run(&mut catalog, "INSERT INTO p VALUES (2, 1)").unwrap(); // key 1 taken → fallback 2
        run(&mut catalog, "INSERT INTO p VALUES (3, 3)").unwrap(); // alias key 3
        run(&mut catalog, "INSERT INTO p VALUES (4, 2)").unwrap(); // key 2 taken → fallback must skip 3
        run(&mut catalog, "INSERT INTO p VALUES (5, 5)").unwrap();
        let versions = p_versions(&catalog);
        let mut keys: Vec<i32> = versions.iter().map(|(k, _)| *k).collect();
        keys.dedup();
        assert_eq!(keys.len(), 5, "{versions:?}");
        let mut ns: Vec<ColumnValue> = versions.into_iter().map(|(_, n)| n).collect();
        ns.sort_by_key(|n| match n {
            ColumnValue::Integer(i) => *i,
            _ => i32::MIN,
        });
        assert_eq!(ns, [1, 2, 3, 5, 100].map(ColumnValue::Integer).to_vec());
    }

    /// A fallback row id never lands on a key that only holds dead versions
    /// of another logical row.
    #[test]
    fn fallback_rowid_skips_key_with_dead_versions() {
        let (_dir, mut catalog) = legacy_p();
        run(&mut catalog, "INSERT INTO p VALUES (2, 1)").unwrap(); // fallback key 2, counter 3
        run(&mut catalog, "INSERT INTO p VALUES (3, 3)").unwrap(); // alias key 3
        run(&mut catalog, "DELETE FROM p WHERE n = 3").unwrap(); // key 3: dead version only
        run(&mut catalog, "INSERT INTO p VALUES (4, 2)").unwrap(); // key 2 taken → fallback
        let versions = p_versions(&catalog);
        let at3: Vec<&ColumnValue> = versions.iter().filter(|(k, _)| *k == 3).map(|(_, n)| n).collect();
        assert_eq!(at3, vec![&ColumnValue::Integer(3)], "{versions:?}");
    }

    /// An alias key whose physical slot holds dead versions of a different
    /// logical row is not shared with that row's version chain.
    #[test]
    fn alias_key_skips_dead_versions_of_another_row() {
        let (_dir, mut catalog) = legacy_p();
        run(&mut catalog, "INSERT INTO p VALUES (2, 1)").unwrap(); // fallback key 2
        run(&mut catalog, "DELETE FROM p WHERE n = 1").unwrap(); // key 2: dead n=1
        run(&mut catalog, "INSERT INTO p VALUES (3, 2)").unwrap(); // alias 2 must not reuse key 2
        let versions = p_versions(&catalog);
        let at2: Vec<&ColumnValue> = versions.iter().filter(|(k, _)| *k == 2).map(|(_, n)| n).collect();
        assert_eq!(at2, vec![&ColumnValue::Integer(1)], "{versions:?}");
    }

    /// UPDATE write intents name the table's root, which stays the same page
    /// even when writing the new versions splits the root leaf.
    #[test]
    fn update_write_intents_use_final_root() {
        use crate::storage::page::{NODE_LEAF, get_node_type};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("intents.db");
        let pager = crate::storage::pager::Pager::new(path.to_str().unwrap()).unwrap();
        let mut catalog = Catalog::open(pager).unwrap();
        run(&mut catalog, "CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT)").unwrap();
        let root0 = catalog.get_table("t").unwrap().root_page;
        let root_is_leaf = |catalog: &Catalog| {
            let root = catalog.get_table("t").unwrap().root_page;
            get_node_type(&catalog.pager.read_page(root).unwrap()) == NODE_LEAF
        };
        let pad = "x".repeat(300);
        let mut n = 0;
        // Fill the root leaf as far as it goes without splitting.
        loop {
            let root = catalog.get_table("t").unwrap().root_page;
            let mut tree = BTree::open_root(&catalog.pager, root).unwrap();
            tree.insert(n + 1, RowData(vec![ColumnValue::Integer(n + 1), ColumnValue::Text(pad.clone())]))
                .unwrap();
            assert_eq!(tree.root_page(), root0, "the root page never moves");
            if !root_is_leaf(&catalog) {
                // Went one too far: start over with n rows.
                break;
            }
            n += 1;
        }
        assert!(n >= 2, "leaf holds {n} rows");
        run(&mut catalog, "DROP TABLE t").unwrap();
        run(&mut catalog, "CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT)").unwrap();
        for i in 1..=n {
            run(&mut catalog, &format!("INSERT INTO t VALUES ({i}, '{pad}')")).unwrap();
        }
        let before = catalog.get_table("t").unwrap().root_page;
        assert!(root_is_leaf(&catalog));

        let mut tm = crate::transaction::TransactionManager::new();
        tm.begin(&mut catalog, None, crate::transaction::IsolationLevel::default()).unwrap();
        run(&mut catalog, "UPDATE t SET s = 'y'").unwrap();
        let after = catalog.get_table("t").unwrap().root_page;
        assert!(!root_is_leaf(&catalog), "precondition: the UPDATE split the root");
        assert_eq!(before, after);
        let intents = catalog.pager.transaction_write_set();
        assert_eq!(intents.len(), n as usize);
        for intent in intents {
            assert_eq!(intent.table_root, after, "intent for key {} recorded against a stale root", intent.key);
        }
        tm.rollback(&mut catalog).unwrap();
    }

    #[test]
    fn format_row_simple() {
        let row = Row::new(
            1,
            RowData(vec![
                ColumnValue::Integer(1),
                ColumnValue::Text("bob".into()),
            ]),
        );
        assert_eq!(format_row(&row), "1 | bob");
    }

    #[test]
    fn format_header_simple() {
        let cols = vec![
            ("id".into(), ColumnType::Integer),
            ("name".into(), ColumnType::Text),
        ];
        assert_eq!(format_header(&cols), "id INTEGER | name TEXT");
    }

    #[test]
    fn format_result_header_uses_sql_types() {
        let cols = vec![
            ResultColumn { name: "n".into(), ty: SqlType::Decimal { precision: 10, scale: 2 } },
            ResultColumn { name: "x".into(), ty: SqlType::Null },
        ];
        assert_eq!(format_result_header(&cols), "n DECIMAL(10,2) | x TEXT");
    }
}
