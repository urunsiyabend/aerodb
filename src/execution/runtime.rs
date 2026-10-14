use crate::catalog::{Catalog, TableInfo};
use crate::constraints::{
    Constraint, default::DefaultConstraint, foreign_key::ForeignKeyConstraint,
    not_null::NotNullConstraint, primary_key::PrimaryKeyConstraint,
};
use crate::error::{DbError, DbResult};
use crate::execution::eval::{Ctx, Env, Scope, eval, eval_predicate};
use crate::execution::query::{ResultColumn, execute_query};
use crate::sql::ast::{BinaryOp, Expr, Statement};
use crate::sql::value::{SqlType, Value, ValueError};
use crate::storage::btree::BTree;
use crate::storage::row::{COMMITTED_BOOTSTRAP_TX, ColumnType, ColumnValue, Row, RowData};
use crate::transaction::Snapshot;
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
        .map(Clone::clone)
        .map_err(|_| DbError::TableNotFound(table_name.to_string()))
}

/// Columns of a base table as an expression scope qualified by the table name.
fn table_scope(info: &TableInfo) -> Scope {
    let mut scope = Scope::new();
    for (name, ty) in &info.columns {
        scope.push(Some(info.name.clone()), name.clone(), SqlType::from_column_type(ty));
    }
    scope
}

fn row_values(row: &RowData) -> Vec<Value> {
    row.0.iter().map(Value::from_column_value).collect()
}

/// Assignment (INSERT/UPDATE) coercion failure. Kept as `InvalidValue`, the
/// 0.5.1 error kind for rejected column values.
fn assignment_error(column: &str, e: ValueError) -> DbError {
    match e {
        ValueError::NumericOverflow => {
            DbError::InvalidValue(format!("value for column '{}' is out of range", column))
        }
        ValueError::DivisionByZero => DbError::DivisionByZero,
        ValueError::TypeMismatch(m) => DbError::TypeMismatch(m),
        ValueError::InvalidCast(m) | ValueError::StringTooLong(m) => DbError::InvalidValue(m),
    }
}

fn coerce(v: &Value, ty: &ColumnType, column: &str) -> DbResult<ColumnValue> {
    v.to_column_value(ty, column).map_err(|e| assignment_error(column, e))
}

/// Rows of `info` visible to the current snapshot for which `selection` is TRUE.
fn matching_rows(catalog: &Catalog, info: &TableInfo, selection: Option<&Expr>) -> DbResult<Vec<Row>> {
    let snapshot = read_snapshot(catalog);
    let tree = BTree::open_root(&catalog.pager, info.root_page)?;
    let rows = tree.scan_visible(&snapshot)?;
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

pub fn execute_delete(
    catalog: &mut Catalog,
    table_name: &str,
    selection: Option<Expr>,
) -> DbResult<usize> {
    let table_info = table_info(catalog, table_name)?;
    let root_page = table_info.root_page;
    let rows_to_delete = matching_rows(catalog, &table_info, selection.as_ref())?;
    if rows_to_delete.is_empty() {
        return Ok(0);
    }

    let fk_cons = ForeignKeyConstraint { fks: &table_info.fks };
    for row in &rows_to_delete {
        fk_cons.validate_delete(catalog, &table_info, &row.data, &read_snapshot(catalog))?;
    }

    let count = rows_to_delete.len();
    let snapshot = read_snapshot(catalog);
    let tx_id = current_tx_id(catalog);
    let mut table_btree = BTree::open_root(&catalog.pager, root_page)?;
    for r in &rows_to_delete {
        ensure_no_write_conflict(&mut table_btree, r.key, r.created_tx, &snapshot)?;
        table_btree.mark_deleted_visible(r.key, &snapshot, tx_id)?;
    }
    let new_root = table_btree.root_page();
    drop(table_btree);
    if new_root != root_page {
        if let Ok(t) = catalog.get_table_mut(table_name) {
            t.root_page = new_root;
        }
        catalog.update_catalog_root(table_name, new_root)?;
    }

    // Record write intents so commit can re-validate first-committer-wins.
    let final_root = catalog
        .get_table(table_name)
        .map(|t| t.root_page)
        .unwrap_or(new_root);
    for r in &rows_to_delete {
        catalog
            .pager
            .record_write_intent(final_root, r.key, r.created_tx);
    }

    // Index entries intentionally remain in place until a future vacuum pass.
    // Indexed lookups re-check base-table visibility with find_visible(), so
    // deleted base rows are filtered after the candidate key is read.
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
    assignments: Vec<(String, Expr)>,
    selection: Option<Expr>,
) -> DbResult<usize> {
    let table_info = table_info(catalog, table_name)?;
    let root_page = table_info.root_page;
    let columns = table_info.columns.clone();

    let mut targets: Vec<(usize, Expr)> = Vec::new();
    for (col, expr) in assignments {
        let idx = columns
            .iter()
            .position(|(c, _)| *c == col)
            .ok_or_else(|| DbError::ColumnNotFound(col.clone()))?;
        if targets.iter().any(|(i, _)| *i == idx) {
            return Err(DbError::InvalidValue(format!("multiple assignments to column '{}'", col)));
        }
        targets.push((idx, expr));
    }

    struct UpdateOp {
        old_key: i32,
        old_created_tx: u64,
        new_key: i32,
        new_data: RowData,
    }

    let mut ops = Vec::new();
    {
        let rows_to_update = matching_rows(catalog, &table_info, selection.as_ref())?;
        let scope = table_scope(&table_info);
        let cx = Ctx::with_catalog(catalog);
        for row in rows_to_update {
            let old = row_values(&row.data);
            let env = Env::new(&scope, &old, None);
            let mut new_data = row.data.clone();
            for (idx, expr) in &targets {
                let (name, ty) = &columns[*idx];
                let v = match expr {
                    Expr::Default => column_default(&table_info, *idx)?.unwrap_or(Value::Null),
                    e => eval(e, &env, &cx)?,
                };
                new_data.0[*idx] = coerce(&v, ty, name)?;
            }
            let new_key = match new_data.0[0] {
                ColumnValue::Integer(i) => i,
                _ => row.key,
            };
            ops.push(UpdateOp {
                old_key: row.key,
                old_created_tx: row.created_tx,
                new_key,
                new_data,
            });
        }
    }
    if ops.is_empty() {
        return Ok(0);
    }
    let count = ops.len();

    for op in &mut ops {
        let nn = NotNullConstraint;
        nn.validate_insert(catalog, &table_info, &mut op.new_data, &read_snapshot(catalog))?;

        let fk_cons = ForeignKeyConstraint { fks: &table_info.fks };
        fk_cons.validate_insert(catalog, &table_info, &mut op.new_data, &read_snapshot(catalog))?;
    }

    if let Some(ref pk_cols) = table_info.primary_key {
        let snapshot = read_snapshot(catalog);
        let mut existing_rows = {
            let scan_tree = BTree::open_root(&catalog.pager, root_page)?;
            scan_tree.scan_visible(&snapshot)?
        };

        let pk_equal = |a: &RowData, b: &RowData| {
            pk_cols.iter().all(|col| {
                table_info
                    .columns
                    .iter()
                    .position(|(c, _)| c == col)
                    .map(|pos| a.0[pos] == b.0[pos])
                    .unwrap_or(false)
            })
        };
        for (idx, op) in ops.iter().enumerate() {
            existing_rows.retain(|existing| {
                !(existing.key == op.old_key && existing.created_tx == op.old_created_tx)
            });
            for existing in &existing_rows {
                if pk_equal(&existing.data, &op.new_data) {
                    return Err(DbError::DuplicateKey(op.new_key));
                }
            }
            for other in ops.iter().skip(idx + 1) {
                if pk_equal(&other.new_data, &op.new_data) {
                    return Err(DbError::DuplicateKey(op.new_key));
                }
            }
        }
    }

    let snapshot = read_snapshot(catalog);
    let tx_id = current_tx_id(catalog);
    let mut table_btree = BTree::open_root(&catalog.pager, root_page)?;
    for op in &ops {
        ensure_no_write_conflict(&mut table_btree, op.old_key, op.old_created_tx, &snapshot)?;
        table_btree.mark_deleted_visible(op.old_key, &snapshot, tx_id)?;
        let mut new_row = Row::new(op.new_key, op.new_data.clone());
        new_row.created_tx = tx_id;
        new_row.deleted_tx = None;
        table_btree.insert_version(new_row)?;
    }
    let new_root = table_btree.root_page();
    drop(table_btree);
    if new_root != root_page {
        if let Ok(t) = catalog.get_table_mut(table_name) {
            t.root_page = new_root;
        }
        catalog.update_catalog_root(table_name, new_root)?;
    }

    // Record write intents so commit can re-validate first-committer-wins.
    let final_root = catalog
        .get_table(table_name)
        .map(|t| t.root_page)
        .unwrap_or(new_root);
    for op in &ops {
        catalog
            .pager
            .record_write_intent(final_root, op.old_key, op.old_created_tx);
    }

    for op in ops {
        // The old indexed value is left as a stale candidate for the logical
        // row key. The base-table old version was marked invisible above;
        // indexed lookup correctness is enforced by find_visible() plus the
        // original predicate check. Add the new value as another candidate.
        catalog.insert_into_indexes(table_name, &op.new_data)?;
    }
    Ok(count)
}

/// Build the stored row for one VALUES tuple.
fn build_insert_row(
    catalog: &mut Catalog,
    info: &TableInfo,
    columns: Option<&[String]>,
    row_vals: &[Expr],
) -> DbResult<RowData> {
    let meta = &info.columns;
    match columns {
        Some(cols) => {
            if row_vals.len() != cols.len() {
                return Err(DbError::InvalidValue(format!(
                    "INSERT column/value count mismatch: expected {} columns, got {} values",
                    cols.len(),
                    row_vals.len()
                )));
            }
            for c in cols {
                if !meta.iter().any(|(n, _)| n == c) {
                    return Err(DbError::ColumnNotFound(c.clone()));
                }
            }
        }
        None => {
            if row_vals.len() != meta.len() {
                return Err(DbError::InvalidValue(format!(
                    "INSERT has wrong number of values: expected {}, got {}",
                    meta.len(),
                    row_vals.len()
                )));
            }
        }
    }

    let mut out = Vec::with_capacity(meta.len());
    for (idx, (col_name, col_type)) in meta.iter().enumerate() {
        let supplied = match columns {
            Some(cols) => cols.iter().position(|c| c == col_name).map(|pos| &row_vals[pos]),
            None => Some(&row_vals[idx]),
        };
        let auto = info.auto_increment.get(idx).copied().unwrap_or(false);
        let seq = format!("{}_{}", info.name, col_name);
        let value = match supplied {
            Some(expr) if !matches!(expr, Expr::Default) => {
                let v = crate::execution::eval::evaluate_constant(expr)?;
                if auto {
                    if let Value::Int(i) = v {
                        catalog.update_sequence_current(&seq, i)?;
                    }
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
    columns: Option<Vec<String>>,
    rows: Vec<Vec<Expr>>,
) -> DbResult<usize> {
    let table_info = table_info(catalog, table_name)?;
    let root_page = table_info.root_page;
    let fks = table_info.fks.clone();

    let mut inserted = 0usize;
    for row_vals in rows {
        let mut row_data = build_insert_row(catalog, &table_info, columns.as_deref(), &row_vals)?;

        let nn = NotNullConstraint;
        nn.validate_insert(catalog, &table_info, &mut row_data, &read_snapshot(catalog))?;

        let fk_cons = ForeignKeyConstraint { fks: &fks };
        fk_cons.validate_insert(catalog, &table_info, &mut row_data, &read_snapshot(catalog))?;
        if let Some(ref pk_cols) = table_info.primary_key {
            let pk_cons = PrimaryKeyConstraint { columns: pk_cols };
            pk_cons.validate_insert(catalog, &table_info, &mut row_data, &read_snapshot(catalog))?;
        }
        let key = match row_data.0.first() {
            Some(ColumnValue::Integer(i)) => *i,
            _ => {
                return Err(DbError::InvalidValue("First column must be an INTEGER key".into()));
            }
        };
        let current_root = catalog.get_table(table_name).map(|t| t.root_page).unwrap_or(root_page);
        let mut table_btree = BTree::open_root(&catalog.pager, current_root)?;
        table_btree.insert(key, row_data.clone())?;
        let new_root = table_btree.root_page();
        drop(table_btree);
        if new_root != current_root {
            catalog.get_table_mut(table_name)?.root_page = new_root;
        }
        catalog.insert_into_indexes(table_name, &row_data)?;
        inserted += 1;
    }
    Ok(inserted)
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

    if let Some(Expr::Binary { op: BinaryOp::Eq, left, right }) = &selection {
        let probe = match (left.as_ref(), right.as_ref()) {
            (Expr::Column { table, name }, Expr::Literal(v)) | (Expr::Literal(v), Expr::Column { table, name })
                if table.as_deref().is_none_or(|t| t == table_name) =>
            {
                Some((name.clone(), v.clone()))
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
                    let expected = Catalog::value_to_string(&val_cv);
                    let hash = Catalog::hash_value(&val_cv);
                    let mut index_tree = BTree::open_root(&catalog.pager, index.root_page)?;
                    let mut candidate_keys = Vec::new();
                    if let Some(row) = index_tree.find(hash)? {
                        if let ColumnValue::Text(ref stored) = row.data.0[0] {
                            if stored == &expected {
                                candidate_keys = row
                                    .data
                                    .0
                                    .iter()
                                    .skip(1)
                                    .filter_map(|val| match val {
                                        ColumnValue::Integer(k) => Some(*k),
                                        _ => None,
                                    })
                                    .collect();
                            }
                        }
                    }
                    drop(index_tree);
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
                        if let Some(r) = table_tree.find_visible(key, &snapshot)? {
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
    match stmt {
        Statement::CreateTable {
            table_name,
            columns,
            fks,
            primary_key,
            if_not_exists,
        } => {
            let auto_cols: Vec<_> = columns.iter().filter(|c| c.auto_increment).collect();
            if auto_cols.len() > 1 {
                return Err(DbError::InvalidValue(
                    "Only one AUTO_INCREMENT column allowed per table".into(),
                ));
            }
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
            match catalog.create_table_with_fks(&table_name, cols.clone(), fks, primary_key.clone())
            {
                Ok(()) => println!("Table {} created", table_name),
                Err(e) => {
                    if if_not_exists && e.to_string().contains("already exists") {
                        println!("Table {} already exists", table_name);
                    } else {
                        return Err(DbError::from(e));
                    }
                }
            }
            for (name, _, _, _, ai) in cols {
                if ai {
                    let seq_name = format!("{}_{}", table_name, name);
                    catalog.create_sequence(&seq_name, 1, 1)?;
                }
            }
        }
        Statement::CreateIndex {
            index_name,
            table_name,
            column_name,
        } => {
            catalog.create_index(&index_name, &table_name, &column_name)?;
            println!("Index {} created", index_name);
        }
        Statement::DropIndex { name } => {
            if catalog.drop_index(&name)? {
                println!("Index {} dropped", name);
            } else {
                return Err(DbError::NotFound(format!("index '{}' not found", name)));
            }
        }
        Statement::Insert {
            table_name,
            columns: col_list,
            rows,
        } => {
            execute_insert(catalog, &table_name, col_list, rows)?;
        }
        Statement::Query(query) => {
            let result = execute_query(catalog, &query, None)?;
            println!("{}", format_result_header(&result.columns));
            for row in &result.rows {
                let vals: Vec<String> = row.iter().map(Value::to_string).collect();
                println!("{}", format_values(&vals));
            }
        }
        Statement::DropTable { table_name, .. } => {
            if catalog.drop_table(&table_name)? {
                println!("Table {} dropped", table_name);
            }
        }
        Statement::Delete {
            table_name,
            selection,
        } => {
            let count = execute_delete(catalog, &table_name, selection)?;
            println!("{} row(s) deleted", count);
        }
        Statement::Update {
            table_name,
            assignments,
            selection,
        } => {
            let count = execute_update(catalog, &table_name, assignments, selection)?;
            println!("{} row(s) updated", count);
        }
        Statement::CreateSequence(seq) => {
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
