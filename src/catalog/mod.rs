use crate::sql::ast::{Expr, Ident, Query};
use crate::storage::btree::BTree;
use crate::storage::page::PAGE_SIZE;
use crate::storage::pager::Pager;
use crate::storage::row::{ColumnType, ColumnValue, Row, RowData};
use crate::storage::vacuum::VacuumReport;
use crate::transaction::{Snapshot, TransactionId};
use std::collections::HashMap;
use std::io;

/// In‐memory representation of a table’s metadata.
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub name: String,
    pub root_page: u32,
    pub columns: Vec<(String, ColumnType)>, // column name and type
    pub not_null: Vec<bool>,
    pub default_values: Vec<Option<Expr>>,
    pub auto_increment: Vec<bool>,
    pub fks: Vec<crate::sql::ast::ForeignKey>,
    pub primary_key: Option<Vec<String>>,
    /// UNIQUE constraints (declared column names), in declaration order.
    pub uniques: Vec<UniqueConstraint>,
    /// CHECK constraints, in declaration order (column-level first as they
    /// appear in the column list, interleaved with table-level ones).
    pub checks: Vec<CheckConstraint>,
}

impl TableInfo {
    /// Pad a stored row image that predates `ALTER TABLE ... ADD COLUMN`
    /// (shorter than the table's column list) with NULLs. Every read path
    /// that hands row images to the executor or the constraint checks calls
    /// this, so a short version (e.g. one only an older snapshot can see)
    /// never causes an out-of-bounds access.
    pub fn pad_row(&self, data: &mut RowData) {
        if data.0.len() < self.columns.len() {
            data.0.resize(self.columns.len(), ColumnValue::Null);
        }
    }
}

/// A view: a stored query exposed under `columns` (one name per output
/// column of the query). `sql` is the persisted text (`Query::to_sql`, with
/// wildcards expanded at CREATE time); `query` is its parsed form.
#[derive(Debug, Clone)]
pub struct ViewInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub sql: String,
    pub query: Query,
}

/// First value of a view's catalog row on page 1. Table rows start with the
/// table name instead; no table name can contain NUL, so the two kinds never
/// collide. (A 0.5.1 reader fails on such a row with "root page not int".)
pub const VIEW_ROW_TAG: &str = "\u{0}VIEW";

/// A named relation: a base table or a view.
#[derive(Debug, Clone, Copy)]
pub enum Relation<'a> {
    Table(&'a TableInfo),
    View(&'a ViewInfo),
}

/// One decoded page-1 catalog row.
enum CatalogEntry {
    Table(TableInfo),
    View(ViewInfo),
}

impl CatalogEntry {
    fn is(&self, view: bool, name: &str) -> bool {
        match self {
            CatalogEntry::Table(t) => !view && t.name == name,
            CatalogEntry::View(v) => view && v.name == name,
        }
    }
}

/// A UNIQUE constraint: rows whose key (`columns`, declared names) has no
/// NULL must be pairwise distinct.
#[derive(Debug, Clone, PartialEq)]
pub struct UniqueConstraint {
    pub name: String,
    pub columns: Vec<String>,
}

/// A CHECK constraint: `expr` (column references canonicalised to the
/// declared, quoted names) must not evaluate to FALSE for any row.
#[derive(Debug, Clone, PartialEq)]
pub struct CheckConstraint {
    pub name: String,
    pub expr: Expr,
}

#[derive(Debug, Clone)]
pub struct SequenceInfo {
    pub key: i32,
    pub current_value: i64,
    pub start_value: i64,
    pub increment: i64,
}

#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub table_name: String,
    pub column_name: String,
    pub root_page: u32,
}

/// The Catalog holds all user tables. Internally, it also persists itself in B-Tree page 1.
pub struct Catalog {
    tables: HashMap<String, TableInfo>,
    /// Views, persisted on page 1 next to the tables (see [`VIEW_ROW_TAG`]).
    views: HashMap<String, ViewInfo>,
    indexes: HashMap<String, IndexInfo>,
    sequences: HashMap<String, SequenceInfo>,
    pub(crate) pager: Pager,
    /// In-memory index map captured at BEGIN so an aborted transaction's DDL
    /// (CREATE INDEX / DROP TABLE side effects) can be undone. Index metadata is
    /// not persisted on disk, so page-level rollback alone cannot restore it.
    pre_tx_indexes: Option<HashMap<String, IndexInfo>>,
    /// Next physical row key per table (see [`Catalog::allocate_rowid`]).
    rowid_counters: HashMap<String, i64>,
}

/// Prefix marking a persisted DEFAULT stored as SQL expression text
/// (`Expr::to_sql`). Unprefixed text is a 0.5.1 default.
pub const DEFAULT_SQL_MARKER: &str = "\u{1}sql:";

/// Decode a persisted DEFAULT. Marker-prefixed text is parsed as an SQL
/// expression; legacy (0.5.1) text keeps its old meaning: the system time
/// functions, or else the raw text as a literal coerced on insert.
pub fn decode_default_expr(stored: &str) -> io::Result<Expr> {
    if let Some(sql) = stored.strip_prefix(DEFAULT_SQL_MARKER) {
        return crate::sql::parser::parse_expression_sql(sql).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("bad stored DEFAULT '{}': {}", sql, e))
        });
    }
    let upper = stored.to_ascii_uppercase();
    let func = match upper.as_str() {
        "CURRENT_TIMESTAMP" | "CURRENT_TIMESTAMP()" => Some("CURRENT_TIMESTAMP"),
        "GETDATE()" => Some("GETDATE"),
        "GETUTCDATE()" => Some("GETUTCDATE"),
        // 0.5.1 stored `DEFAULT NULL` as the bare word NULL: SQL NULL.
        "NULL" => return Ok(Expr::Literal(crate::sql::value::Value::Null)),
        _ => None,
    };
    Ok(match func {
        Some(name) => Expr::Function { name: name.into(), args: Vec::new() },
        None => Expr::Literal(crate::sql::value::Value::Text(stored.to_string())),
    })
}

/// Initialize `page_num` as an empty leaf root and persist it.
///
/// Shared by every fresh-tree allocation (catalog pages, table roots, index
/// roots). Takes the page's exclusive latch only for the header write.
fn init_leaf_root(pager: &Pager, page_num: u32) -> io::Result<()> {
    pager.with_page_mut(page_num, |data| {
        crate::storage::page::set_node_type(data, crate::storage::page::NODE_LEAF);
        crate::storage::page::set_is_root(data, true);
        crate::storage::page::set_parent(data, 0);
        crate::storage::page::set_cell_count(data, 0);
    })?;
    pager.flush_page(page_num)
}

impl Catalog {
    /// Open (or create) a catalog. This ensures page 1 exists as a leaf root,
    /// then reads any existing rows from page 1 into `tables`.
    pub fn open(pager: Pager) -> io::Result<Self> {
        // If the file has ≤1 pages total, allocate page 1 and initialize it as a leaf.
        if pager.file_length_pages() <= 1 {
            // Allocate page 1
            pager.allocate_page()?;
            init_leaf_root(&pager, 1)?;
        }

        // Ensure page 2 exists for sequence catalog
        if pager.file_length_pages() <= 2 {
            pager.allocate_page()?;
            init_leaf_root(&pager, 2)?;
        }

        let (tables, views) = Self::load_relations(&pager)?;
        let sequences = Self::load_sequences(&pager)?;

        Ok(Catalog {
            tables,
            views,
            indexes: HashMap::new(),
            sequences,
            pager,
            pre_tx_indexes: None,
            rowid_counters: HashMap::new(),
        })
    }

    /// Every page-1 catalog row (key, decoded entry). The single scanner of
    /// page 1: table and view rows are told apart by [`VIEW_ROW_TAG`].
    fn catalog_entries(pager: &Pager) -> io::Result<Vec<(i32, CatalogEntry)>> {
        let catalog_btree = BTree::open_root(pager, 1)?;
        let mut cursor = catalog_btree.scan_all_rows();
        let mut out = Vec::new();
        while let Some(row) = cursor.next() {
            out.push((row.key, Self::decode_catalog_row(&row)?));
        }
        Ok(out)
    }

    #[allow(clippy::type_complexity)]
    fn load_relations(
        pager: &Pager,
    ) -> io::Result<(HashMap<String, TableInfo>, HashMap<String, ViewInfo>)> {
        let mut tables = HashMap::new();
        let mut views = HashMap::new();
        for (_, entry) in Self::catalog_entries(pager)? {
            match entry {
                CatalogEntry::Table(info) => {
                    tables.insert(info.name.clone(), info);
                }
                CatalogEntry::View(view) => {
                    views.insert(view.name.clone(), view);
                }
            }
        }
        Ok((tables, views))
    }

    fn load_sequences(pager: &Pager) -> io::Result<HashMap<String, SequenceInfo>> {
        let mut sequences = HashMap::new();
        let seq_btree = BTree::open_root(pager, 2)?;
        let mut cursor = seq_btree.scan_all_rows();
        while let Some(row) = cursor.next() {
            let (name, current, start, inc) = Self::deserialize_sequence_row(&row)?;
            sequences.insert(
                name.clone(),
                SequenceInfo {
                    key: row.key,
                    current_value: current,
                    start_value: start,
                    increment: inc,
                },
            );
        }
        Ok(sequences)
    }

    /// Rebuild the in-memory tables, views and sequences from pages 1 and 2
    /// (after a ROLLBACK reverted them).
    pub(crate) fn reload_tables(&mut self) -> io::Result<()> {
        let (tables, views) = Self::load_relations(&self.pager)?;
        self.tables = tables;
        self.views = views;
        self.sequences = Self::load_sequences(&self.pager)?;
        Ok(())
    }

    /// Every page of the table and sequence catalog trees (rooted at pages 1
    /// and 2). ROLLBACK reverts the touched ones: the catalog is not
    /// MVCC-versioned, so aborted DDL must be undone physically.
    pub(crate) fn catalog_tree_pages(&self) -> io::Result<Vec<u32>> {
        let mut pages = BTree::open_root(&self.pager, 1)?.page_numbers()?;
        pages.extend(BTree::open_root(&self.pager, 2)?.page_numbers()?);
        Ok(pages)
    }

    /// Page-1 key of the table (`view == false`) or view named `name`.
    fn catalog_key(&self, view: bool, name: &str) -> io::Result<Option<i32>> {
        Ok(Self::catalog_entries(&self.pager)?
            .into_iter()
            .find(|(_, e)| e.is(view, name))
            .map(|(k, _)| k))
    }

    /// A page-1 key not used by any table or view row.
    fn next_catalog_key(&self) -> io::Result<i32> {
        let max = Self::catalog_entries(&self.pager)?.iter().map(|(k, _)| *k).max().unwrap_or(0);
        Ok(max + 1)
    }

    /// Replace (`Some`) or delete (`None`) the page-1 row `key`, keeping the
    /// catalog tree rooted at page 1.
    fn write_catalog_row(&mut self, key: i32, data: Option<RowData>) -> io::Result<()> {
        let mut tree = BTree::open_root(&self.pager, 1)?;
        if tree.find(key)?.is_some() {
            tree.delete(key)?;
        }
        if let Some(data) = data {
            tree.insert(key, data)?;
        }
        // A root split moved the root off page 1: move it back (the catalog
        // tree is always opened at page 1).
        tree.pin_root(1)
    }

    pub(crate) fn update_catalog_root(&mut self, name: &str, new_root: u32) -> io::Result<()> {
        let target = Self::catalog_entries(&self.pager)?.into_iter().find_map(|(key, e)| match e {
            CatalogEntry::Table(info) if info.name == name => Some((key, info)),
            _ => None,
        });
        if let Some((key, mut info)) = target {
            info.root_page = new_root;
            self.write_catalog_row(key, Some(Self::serialize_catalog_row(&info)))?;
        }
        Ok(())
    }

    /// Persist new metadata for an existing table (ALTER TABLE): rewrites its
    /// page-1 row and the in-memory entry. Transactional like every catalog
    /// change (page 1 is reverted on ROLLBACK).
    pub fn replace_table_info(&mut self, info: TableInfo) -> io::Result<()> {
        let key = self.catalog_key(false, &info.name)?.ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("No such table: {}", info.name))
        })?;
        self.write_catalog_row(key, Some(Self::serialize_catalog_row(&info)))?;
        self.tables.insert(info.name.clone(), info);
        Ok(())
    }

    /// Persist a new view. The caller validates it (name free, query binds).
    pub fn create_view(&mut self, view: ViewInfo) -> io::Result<()> {
        if self.tables.contains_key(&view.name) || self.views.contains_key(&view.name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("relation {} already exists", view.name),
            ));
        }
        let key = self.next_catalog_key()?;
        self.write_catalog_row(key, Some(Self::serialize_view_row(&view)))?;
        self.views.insert(view.name.clone(), view);
        Ok(())
    }

    /// Drop a view if it exists. Returns true if it was removed.
    pub fn drop_view(&mut self, name: &str) -> io::Result<bool> {
        if !self.views.contains_key(name) {
            return Ok(false);
        }
        if let Some(key) = self.catalog_key(true, name)? {
            self.write_catalog_row(key, None)?;
        }
        self.views.remove(name);
        Ok(true)
    }

    pub fn get_view(&self, name: &str) -> Option<&ViewInfo> {
        self.views.get(name)
    }

    pub fn all_views(&self) -> Vec<ViewInfo> {
        self.views.values().cloned().collect()
    }

    /// Whether a table or a view is named exactly `name`.
    pub fn relation_exists(&self, name: &str) -> bool {
        self.tables.contains_key(name) || self.views.contains_key(name)
    }

    fn serialize_view_row(view: &ViewInfo) -> RowData {
        let mut vals = vec![
            ColumnValue::Text(VIEW_ROW_TAG.to_string()),
            ColumnValue::Text(view.name.clone()),
            ColumnValue::Integer(view.columns.len() as i32),
        ];
        vals.extend(view.columns.iter().map(|c| ColumnValue::Text(c.clone())));
        vals.push(ColumnValue::Text(view.sql.clone()));
        RowData(vals)
    }

    fn deserialize_view_row(row: &Row) -> io::Result<ViewInfo> {
        let bad = |what: &str| io::Error::new(io::ErrorKind::InvalidData, format!("view catalog row: bad {}", what));
        let values = &row.data.0;
        let text = |i: usize, what: &str| match values.get(i) {
            Some(ColumnValue::Text(s)) => Ok(s.clone()),
            _ => Err(bad(what)),
        };
        let name = text(1, "name")?;
        let n = match values.get(2) {
            Some(ColumnValue::Integer(n)) if *n >= 0 => *n as usize,
            _ => return Err(bad("column count")),
        };
        let columns = (0..n).map(|i| text(3 + i, "column name")).collect::<io::Result<Vec<_>>>()?;
        let sql = text(3 + n, "query text")?;
        let query = match crate::sql::parser::parse_sql(&sql) {
            Ok(crate::sql::ast::Statement::Query(q)) => *q,
            Ok(_) => return Err(bad("query text")),
            Err(e) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("bad stored view query '{}': {}", sql, e),
                ));
            }
        };
        Ok(ViewInfo { name, columns, sql, query })
    }

    fn decode_catalog_row(row: &Row) -> io::Result<CatalogEntry> {
        match row.data.0.first() {
            Some(ColumnValue::Text(tag)) if tag == VIEW_ROW_TAG => {
                Ok(CatalogEntry::View(Self::deserialize_view_row(row)?))
            }
            _ => Ok(CatalogEntry::Table(Self::deserialize_catalog_row(row)?)),
        }
    }

    /// Capture the in-memory index map at BEGIN so ROLLBACK can undo DDL that
    /// mutated it (index metadata is not persisted, so page rollback alone
    /// cannot restore it). Driven by [`crate::transaction::TransactionManager`].
    pub(crate) fn capture_pre_tx_indexes(&mut self) {
        self.pre_tx_indexes = Some(self.indexes.clone());
    }

    /// Drop the BEGIN-time index snapshot after a successful COMMIT.
    pub(crate) fn clear_pre_tx_indexes(&mut self) {
        self.pre_tx_indexes = None;
    }

    /// Restore the BEGIN-time index map during ROLLBACK so aborted DDL leaves no
    /// trace in the (non-persisted) index metadata.
    pub(crate) fn restore_pre_tx_indexes(&mut self) {
        if let Some(saved_indexes) = self.pre_tx_indexes.take() {
            self.indexes = saved_indexes;
        }
    }

    /// Deferred re-validation at COMMIT against everything committed so far
    /// plus the transaction's own writes. Uses the same logical key checks as
    /// statement execution ([`crate::constraints::keys::check_new_rows`]):
    /// NOT NULL, CHECK, PRIMARY KEY / UNIQUE uniqueness and FOREIGN KEY
    /// existence for rows the transaction created, and the referential check
    /// for row versions it
    /// deleted or replaced (no referential action can run at commit, so any
    /// orphaned reference is a violation).
    pub(crate) fn recheck_constraints_for_commit(
        &mut self,
        transaction_id: TransactionId,
    ) -> crate::error::DbResult<()> {
        use crate::constraints::keys::{Pending, Removal, check_new_rows, check_removed_keys};
        let snapshot =
            Snapshot::new_for_transaction(transaction_id, TransactionId::MAX, Vec::new());
        let mut tables = self.all_tables();
        tables.sort_by(|a, b| a.name.cmp(&b.name));

        for table in &tables {
            let tree = BTree::open_root(&self.pager, table.root_page)?;
            let created: Vec<Row> = tree
                .scan_visible(&snapshot)?
                .into_iter()
                .filter(|row| row.created_tx == transaction_id)
                .map(|mut row| {
                    table.pad_row(&mut row.data);
                    row
                })
                .collect();
            drop(tree);
            if !created.is_empty() {
                // The created rows are the candidates; key checks compare
                // them with every *other* visible row (the overlay removes
                // and re-adds them, so FK lookups still see them).
                let mut own = Pending::new();
                for row in &created {
                    own.remove(&table.name, row);
                    own.add(&table.name, row.data.clone());
                }
                let images: Vec<RowData> = created.iter().map(|r| r.data.clone()).collect();
                check_new_rows(self, table, &snapshot, &images, &own)?;
            }

            let mut tree = BTree::open_root(&self.pager, table.root_page)?;
            let removed: Vec<RowData> = tree
                .all_versions()?
                .into_iter()
                .filter(|row| row.deleted_tx == Some(transaction_id))
                .map(|mut row| {
                    table.pad_row(&mut row.data);
                    row.data
                })
                .collect();
            drop(tree);
            check_removed_keys(self, table, &removed, &snapshot, &Pending::new(), Removal::Commit)?;
        }

        Ok(())
    }

    /// The column whose value is a table's physical row key (a "rowid
    /// alias"): the sole PRIMARY KEY column when it is INTEGER, SMALLINT or
    /// MEDIUMINT. Other tables get keys from [`Catalog::allocate_rowid`].
    pub fn rowid_alias_column(table: &TableInfo) -> Option<usize> {
        let pk = table.primary_key.as_ref()?;
        if pk.len() != 1 {
            return None;
        }
        let idx = table.columns.iter().position(|(c, _)| *c == pk[0])?;
        match table.columns[idx].1 {
            ColumnType::Integer | ColumnType::SmallInt { .. } | ColumnType::MediumInt { .. } => {
                Some(idx)
            }
            _ => None,
        }
    }

    /// A fresh physical row key for `table_name`: strictly greater than every
    /// key present in any version of the table (visible or not) and than any
    /// key handed out before. The counter is cached in memory and initialised
    /// lazily from the table's versions, so it survives reopen without being
    /// persisted; it is never rewound (a rolled-back allocation leaves a gap).
    pub fn allocate_rowid(&mut self, table_name: &str) -> crate::error::DbResult<i32> {
        let root = self.get_table(table_name)?.root_page;
        let mut tree = BTree::open_root(&self.pager, root)?;
        let mut next: i64 = match self.rowid_counters.get(table_name) {
            Some(next) => *next,
            None => {
                let max = tree.all_versions()?.iter().map(|r| r.key).max();
                max.map_or(1, |m| m as i64 + 1)
            }
        };
        loop {
            let Ok(key) = i32::try_from(next) else {
                self.rowid_counters.insert(table_name.to_string(), next);
                return Err(crate::error::DbError::NumericOverflow(format!(
                    "row ids of table \"{}\" are exhausted",
                    table_name
                )));
            };
            next += 1;
            // Defence in depth: skip keys that already hold any version
            // (e.g. written without going through `note_rowid_used`).
            if tree.versions_of(key)?.is_empty() {
                self.rowid_counters.insert(table_name.to_string(), next);
                return Ok(key);
            }
        }
    }

    /// Record that physical key `key` of `table_name` now holds a version, so
    /// [`Catalog::allocate_rowid`] never hands it out. Alias-keyed writes
    /// call this; an uninitialised counter is left alone (its lazy scan sees
    /// the version).
    pub fn note_rowid_used(&mut self, table_name: &str, key: i32) {
        if let Some(next) = self.rowid_counters.get_mut(table_name) {
            *next = (*next).max(key as i64 + 1);
        }
    }

    /// Look up a table named by an SQL identifier: unquoted names match
    /// case-insensitively, quoted names exactly (the rule column references
    /// use). More than one case-insensitive match is ambiguous.
    ///
    /// Only base tables qualify: a view name is rejected with
    /// `FeatureNotSupported` (views are read-only).
    pub fn resolve_table(&self, ident: &Ident) -> crate::error::DbResult<&TableInfo> {
        match self.resolve_relation(ident)? {
            Relation::Table(t) => Ok(t),
            Relation::View(v) => Err(crate::error::DbError::FeatureNotSupported(format!(
                "cannot modify view \"{}\"",
                v.name
            ))),
        }
    }

    /// Look up a table or view named by an SQL identifier (same rule as
    /// [`Catalog::resolve_table`]; tables and views share one namespace).
    pub fn resolve_relation(&self, ident: &Ident) -> crate::error::DbResult<Relation<'_>> {
        let not_found = || crate::error::DbError::TableNotFound(ident.value.clone());
        if ident.quoted {
            if let Some(t) = self.tables.get(&ident.value) {
                return Ok(Relation::Table(t));
            }
            return self.views.get(&ident.value).map(Relation::View).ok_or_else(not_found);
        }
        let mut hits = self
            .tables
            .values()
            .filter(|t| ident.matches(&t.name))
            .map(Relation::Table)
            .chain(self.views.values().filter(|v| ident.matches(&v.name)).map(Relation::View));
        match (hits.next(), hits.next()) {
            (Some(r), None) => Ok(r),
            (None, _) => Err(not_found()),
            (Some(_), Some(_)) => Err(crate::error::DbError::Syntax(format!(
                "table name \"{}\" is ambiguous",
                ident.value
            ))),
        }
    }

    pub fn transaction_active(&self) -> bool {
        self.pager.transaction_active()
    }

    pub fn transaction_id(&self) -> Option<TransactionId> {
        self.pager.transaction_id()
    }

    /// The installed transaction's snapshot, cloned: it lives behind the pager's
    /// lock and cannot be borrowed out across page access.
    pub fn transaction_snapshot(&self) -> Option<Snapshot> {
        self.pager.transaction_snapshot()
    }

    pub fn current_snapshot(&self) -> Option<Snapshot> {
        self.transaction_snapshot()
    }

    /// Create a new table with `name` and `columns`. Allocates a fresh page for the table’s root,
    /// then inserts one catalog row into page 1 (the catalog B-Tree), and updates `tables`.
    pub fn create_table(
        &mut self,
        name: &str,
        columns: Vec<(String, ColumnType)>,
    ) -> io::Result<()> {
        let cols_with_nn: Vec<_> = columns
            .into_iter()
            .map(|(n, t)| (n, t, false, None, false))
            .collect();
        self.create_table_with_fks(name, cols_with_nn, Vec::new(), None)
    }

    /// Transactional DDL rule: when a transaction is active, catalog pages are
    /// written through the pager as dirty pages, so CREATE/DROP TABLE and CREATE
    /// INDEX commit or roll back atomically with the surrounding transaction
    /// (page rollback + `reload_tables` + index-snapshot restore on ROLLBACK).
    /// The catalog itself is intentionally NOT snapshot-versioned: a committed
    /// schema change is visible to any handle opened afterward — there is no
    /// per-snapshot schema isolation between concurrent handles.
    pub fn create_table_with_fks(
        &mut self,
        name: &str,
        columns: Vec<(String, ColumnType, bool, Option<Expr>, bool)>,
        fks: Vec<crate::sql::ast::ForeignKey>,
        primary_key: Option<Vec<String>>,
    ) -> io::Result<()> {
        self.create_table_with_constraints(name, columns, fks, primary_key, Vec::new(), Vec::new())
    }

    /// [`Catalog::create_table_with_fks`] plus UNIQUE and CHECK constraints.
    /// No validation: SQL CREATE TABLE validates through
    /// [`crate::constraints::keys::resolve_table_constraints`].
    pub fn create_table_with_constraints(
        &mut self,
        name: &str,
        columns: Vec<(String, ColumnType, bool, Option<Expr>, bool)>,
        fks: Vec<crate::sql::ast::ForeignKey>,
        primary_key: Option<Vec<String>>,
        uniques: Vec<UniqueConstraint>,
        checks: Vec<CheckConstraint>,
    ) -> io::Result<()> {
        if self.tables.contains_key(name) {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Table {} already exists", name),
            ));
        }
        if self.views.contains_key(name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("relation {} already exists", name),
            ));
        }

        // A new table starts a fresh row-id sequence (a dropped or rolled-back
        // table of the same name may have left a counter behind).
        self.rowid_counters.remove(name);

        // Allocate a new leaf page for this table’s data
        let new_root = self.pager.allocate_page()?;
        init_leaf_root(&self.pager, new_root)?;

        let mut info = TableInfo {
            name: name.to_string(),
            root_page: new_root,
            columns: Vec::new(),
            not_null: Vec::new(),
            default_values: Vec::new(),
            auto_increment: Vec::new(),
            fks,
            primary_key,
            uniques,
            checks,
        };
        for (n, t, nn, d, ai) in columns {
            info.columns.push((n, t));
            info.not_null.push(nn);
            info.default_values.push(d);
            info.auto_increment.push(ai);
        }

        // A synthetic key not used by any table or view row (a count-based
        // key could collide once a relation was dropped).
        let key = self.next_catalog_key()?;
        self.write_catalog_row(key, Some(Self::serialize_catalog_row(&info)))?;
        self.tables.insert(name.to_string(), info);
        Ok(())
    }

    /// See [`Catalog::create_table_with_fks`] for the transactional-DDL rule.
    /// Because the index map is not persisted, its rollback relies on the
    /// snapshot captured at BEGIN rather than on page rollback alone.
    pub fn create_index(
        &mut self,
        index_name: &str,
        table_name: &str,
        column_name: &str,
    ) -> io::Result<()> {
        if self.indexes.contains_key(index_name) {
            return Err(io::Error::new(io::ErrorKind::Other, "Index already exists"));
        }
        let (table_root, col_idx) = {
            let table = self.get_table(table_name)?;
            let idx = table
                .columns
                .iter()
                .position(|(c, _)| c == column_name)
                .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Column not found"))?;
            (table.root_page, idx)
        };

        let mut root_page = self.pager.allocate_page()?;
        init_leaf_root(&self.pager, root_page)?;

        // Build index from existing rows
        {
            let mut table_btree = BTree::open_root(&self.pager, table_root)?;
            let mut cursor = table_btree.scan_all_rows();
            let mut rows = Vec::new();
            while let Some(row) = cursor.next() {
                rows.push(row);
            }
            drop(cursor);
            let mut index_tree = BTree::open_root(&self.pager, root_page)?;
            for row in rows {
                if let Some(val) = row.data.0.get(col_idx).cloned() {
                    root_page = Catalog::insert_index_value(&mut index_tree, val, row.key)?;
                }
            }
            // update root_page in case tree changed
            self.pager.flush_page(root_page)?;
        }

        self.indexes.insert(
            index_name.to_string(),
            IndexInfo {
                name: index_name.to_string(),
                table_name: table_name.to_string(),
                column_name: column_name.to_string(),
                root_page,
            },
        );
        Ok(())
    }

    pub fn drop_index(&mut self, name: &str) -> io::Result<bool> {
        if self.indexes.remove(name).is_some() {
            // In the future pages belonging to the index would be freed here
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Append an index candidate for a logical base-row key.
    ///
    /// Index payloads are `[indexed_value_as_text, logical_row_key, ...]`; they do
    /// not own MVCC visibility. UPDATE and DELETE may leave stale candidates until
    /// vacuum, and indexed reads must call `find_visible` on the base table before
    /// returning a row.
    pub(crate) fn insert_index_value(
        index_tree: &mut BTree,
        value: ColumnValue,
        row_key: i32,
    ) -> io::Result<u32> {
        let hash = Catalog::hash_value(&value);
        if let Some(mut existing) = index_tree.find(hash)? {
            if let ColumnValue::Text(ref s) = existing.data.0[0] {
                if *s == Self::value_to_string(&value) {
                    existing.data.0.push(ColumnValue::Integer(row_key));
                    index_tree.delete(hash)?;
                    index_tree.insert(hash, existing.data)?;
                    return Ok(index_tree.root_page());
                }
            }
        }
        let data = RowData(vec![
            ColumnValue::Text(Self::value_to_string(&value)),
            ColumnValue::Integer(row_key),
        ]);
        index_tree.insert(hash, data)?;
        Ok(index_tree.root_page())
    }

    pub(crate) fn value_to_string(val: &ColumnValue) -> String {
        match val {
            ColumnValue::Null => "NULL".into(),
            ColumnValue::Integer(i) => i.to_string(),
            ColumnValue::Text(s) => s.clone(),
            ColumnValue::Boolean(b) => b.to_string(),
            ColumnValue::Char(s) => s.clone(),
            ColumnValue::Double(f) => f.to_string(),
            ColumnValue::Date(d) => ColumnValue::Date(*d).to_string_value(),
            ColumnValue::DateTime(ts) => ColumnValue::DateTime(*ts).to_string_value(),
            ColumnValue::Timestamp(ts) => ColumnValue::Timestamp(*ts).to_string_value(),
            ColumnValue::Time(t) => ColumnValue::Time(*t).to_string_value(),
            ColumnValue::Year(y) => ColumnValue::Year(*y).to_string_value(),
            ColumnValue::Decimal(d) => d.to_string(),
        }
    }

    pub fn hash_value(val: &ColumnValue) -> i32 {
        match val {
            ColumnValue::Null => 0,
            ColumnValue::Integer(i) => *i,
            ColumnValue::Text(s) => {
                use std::hash::{Hash, Hasher};
                let mut h = std::collections::hash_map::DefaultHasher::new();
                s.hash(&mut h);
                (h.finish() as i64 & 0x7FFF_FFFF) as i32
            }
            ColumnValue::Boolean(b) => {
                if *b {
                    1
                } else {
                    0
                }
            }
            ColumnValue::Char(s) => {
                use std::hash::{Hash, Hasher};
                let mut h = std::collections::hash_map::DefaultHasher::new();
                s.hash(&mut h);
                (h.finish() as i64 & 0x7FFF_FFFF) as i32
            }
            ColumnValue::Double(f) => *f as i32,
            ColumnValue::Date(d) => *d,
            ColumnValue::DateTime(ts) => (*ts % i32::MAX as i64) as i32,
            ColumnValue::Timestamp(ts) => (*ts % i32::MAX as i64) as i32,
            ColumnValue::Time(t) => *t,
            ColumnValue::Year(y) => *y as i32,
            ColumnValue::Decimal(d) => {
                use std::hash::{Hash, Hasher};
                // Normalize so numerically equal values (1.0, 1.00) hash alike.
                let mut h = std::collections::hash_map::DefaultHasher::new();
                d.normalize().hash(&mut h);
                (h.finish() as i64 & 0x7FFF_FFFF) as i32
            }
        }
    }

    pub fn create_sequence(&mut self, name: &str, start: i64, increment: i64) -> io::Result<()> {
        if self.sequences.contains_key(name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("Sequence '{}' already exists", name),
            ));
        }
        let key = (self.sequences.len() as i32) + 1;
        let current = start - increment;
        {
            let mut tree = BTree::open_root(&self.pager, 2)?;
            tree.insert(
                key,
                Self::serialize_sequence_row(name, current, start, increment),
            )?;
        }
        self.sequences.insert(
            name.to_string(),
            SequenceInfo {
                key,
                current_value: current,
                start_value: start,
                increment,
            },
        );
        Ok(())
    }

    pub fn next_sequence_value(&mut self, name: &str) -> io::Result<i64> {
        let info = self.sequences.get_mut(name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Sequence '{}' not found", name),
            )
        })?;
        info.current_value += info.increment;
        {
            let mut tree = BTree::open_root(&self.pager, 2)?;
            tree.delete(info.key)?;
            tree.insert(
                info.key,
                Self::serialize_sequence_row(
                    name,
                    info.current_value,
                    info.start_value,
                    info.increment,
                ),
            )?;
        }
        Ok(info.current_value)
    }

    pub fn update_sequence_current(&mut self, name: &str, new_current: i64) -> io::Result<()> {
        let info = self
            .sequences
            .get_mut(name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "sequence not found"))?;
        if new_current > info.current_value {
            info.current_value = new_current;
            let mut tree = BTree::open_root(&self.pager, 2)?;
            tree.delete(info.key)?;
            tree.insert(
                info.key,
                Self::serialize_sequence_row(
                    name,
                    info.current_value,
                    info.start_value,
                    info.increment,
                ),
            )?;
        }
        Ok(())
    }

    /// Add index candidates for the row with physical key `row_key`.
    pub fn insert_into_indexes(
        &mut self,
        table_name: &str,
        row_key: i32,
        row_data: &RowData,
    ) -> io::Result<()> {
        let indices: Vec<IndexInfo> = self.indexes.values().cloned().collect();
        for idx in indices {
            if idx.table_name == table_name {
                let col_pos = self
                    .tables
                    .get(table_name)
                    .and_then(|t| t.columns.iter().position(|(c, _)| c == &idx.column_name))
                    .unwrap();
                if let Some(val) = row_data.0.get(col_pos).cloned() {
                    let mut tree = BTree::open_root(&self.pager, idx.root_page)?;
                    let new_root = Catalog::insert_index_value(&mut tree, val, row_key)?;
                    if let Some(idx_info) = self.indexes.get_mut(&idx.name) {
                        idx_info.root_page = new_root;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn find_index(&self, table: &str, column: &str) -> Option<&IndexInfo> {
        self.indexes
            .values()
            .find(|idx| idx.table_name == table && idx.column_name == column)
    }

    pub fn remove_from_indexes(
        &mut self,
        table_name: &str,
        row_data: &RowData,
        row_key: i32,
    ) -> io::Result<()> {
        // Logical index entries are deliberately not removed immediately. DELETE
        // and UPDATE make old base-row versions invisible in the table B-tree;
        // indexed lookups then filter stale candidates by calling find_visible on
        // the base table. A future vacuum can physically prune these entries.
        let _ = (table_name, row_data, row_key);
        Ok(())
    }

    /// Look up a table’s metadata, or return an error if it doesn’t exist.
    pub fn get_table(&self, name: &str) -> io::Result<&TableInfo> {
        self.tables
            .get(name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, format!("No such table: {}", name)))
    }

    /// Mutable variant of `get_table` so callers can update the metadata.
    pub fn get_table_mut(&mut self, name: &str) -> io::Result<&mut TableInfo> {
        self.tables
            .get_mut(name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, format!("No such table: {}", name)))
    }

    pub fn all_tables(&self) -> Vec<TableInfo> {
        self.tables.values().cloned().collect()
    }

    pub fn all_indexes(&self) -> Vec<IndexInfo> {
        self.indexes.values().cloned().collect()
    }

    /// Explicit internal maintenance API for physically pruning obsolete MVCC
    /// versions from one table and lazily rebuilding that table's indexes to
    /// remove stale index candidates left behind by UPDATE/DELETE. `global_xmin`
    /// is supplied by the [`crate::transaction::TransactionManager`], which owns
    /// the live-transaction set; see [`crate::engine::Engine::vacuum_table`].
    pub fn vacuum_table(
        &mut self,
        table_name: &str,
        global_xmin: TransactionId,
    ) -> io::Result<VacuumReport> {
        let table = self.get_table(table_name)?.clone();
        let tx_table = self.pager.transaction_table().clone();

        let versions_removed = {
            let mut table_tree = BTree::open_root(&self.pager, table.root_page)?;
            table_tree.vacuum_deleted_versions(global_xmin, &tx_table)?
        };

        let indexes_cleaned = if versions_removed > 0 {
            self.rebuild_indexes_for_table(table_name)?
        } else {
            0
        };
        Ok(VacuumReport {
            versions_removed,
            indexes_cleaned,
        })
    }

    /// The durable frozen watermark; see [`Catalog::vacuum_all`].
    pub fn frozen_xid(&self) -> TransactionId {
        self.pager.frozen_xid()
    }

    /// Vacuum **every** table in one pass, then advance the durable frozen
    /// watermark to `global_xmin` and reclaim the clog statuses below it.
    ///
    /// The watermark is only advanced from here, never from the per-table
    /// [`Catalog::vacuum_table`]: a status entry may be dropped only once no
    /// remaining on-disk version needs it, and a single-table pass cannot know
    /// whether another table still holds versions created by those transactions.
    /// Sweeping all tables discharges that condition -- an aborted transaction's
    /// versions are reclaimable immediately (the aborted-creator rule) and its
    /// `deleted_tx` stamps on still-live versions are cleared (the
    /// aborted-deleter rule), so after a full sweep no version below
    /// `global_xmin` depends on being able to tell an aborted id from a frozen one.
    pub fn vacuum_all(&mut self, global_xmin: TransactionId) -> io::Result<VacuumReport> {
        let table_names: Vec<String> = self.all_tables().into_iter().map(|t| t.name).collect();

        let mut report = VacuumReport::default();
        for name in table_names {
            let table_report = self.vacuum_table(&name, global_xmin)?;
            report.versions_removed += table_report.versions_removed;
            report.indexes_cleaned += table_report.indexes_cleaned;
        }

        self.pager.advance_frozen_xid(global_xmin)?;
        Ok(report)
    }

    fn rebuild_indexes_for_table(&mut self, table_name: &str) -> io::Result<usize> {
        let table = self.get_table(table_name)?.clone();
        let index_names: Vec<String> = self
            .indexes
            .values()
            .filter(|index| index.table_name == table_name)
            .map(|index| index.name.clone())
            .collect();
        let mut rebuilt = 0;

        for index_name in index_names {
            let index = self.indexes.get(&index_name).cloned().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "index disappeared during vacuum")
            })?;
            let col_idx = table
                .columns
                .iter()
                .position(|(column, _)| column == &index.column_name)
                .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "index column not found"))?;

            let mut rows = Vec::new();
            {
                let mut table_tree = BTree::open_root(&self.pager, table.root_page)?;
                let mut cursor = table_tree.scan_all_rows();
                while let Some(row) = cursor.next() {
                    rows.push(row);
                }
            }

            let mut new_root = self.pager.allocate_page()?;
            init_leaf_root(&self.pager, new_root)?;
            {
                let mut index_tree = BTree::open_root(&self.pager, new_root)?;
                for row in rows {
                    if let Some(value) = row.data.0.get(col_idx).cloned() {
                        new_root = Catalog::insert_index_value(&mut index_tree, value, row.key)?;
                    }
                }
            }
            if let Some(index_info) = self.indexes.get_mut(&index_name) {
                index_info.root_page = new_root;
            }
            rebuilt += 1;
        }

        Ok(rebuilt)
    }

    /// Drop a table if it exists. Returns true if the table was removed.
    /// See [`Catalog::create_table_with_fks`] for the transactional-DDL rule:
    /// inside a transaction this rolls back with the surrounding transaction.
    pub fn drop_table(&mut self, name: &str) -> io::Result<bool> {
        if !self.tables.contains_key(name) {
            return Ok(false);
        }

        let index_names: Vec<String> = self
            .indexes
            .values()
            .filter(|idx| idx.table_name == name)
            .map(|idx| idx.name.clone())
            .collect();
        for idx in index_names {
            self.drop_index(&idx)?;
        }

        // Find catalog row key corresponding to this table
        if let Some(key) = self.catalog_key(false, name)? {
            self.write_catalog_row(key, None)?;
            self.tables.remove(name);
            self.rowid_counters.remove(name);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Serialize a catalog row into a UTF-8 string:
    ///
    /// [u32 name_len][name_bytes][u32 root_page][u16 num_columns]
    /// for each column: [u32 col_len][col_bytes]
    /// then [u16 num_fks] followed by each foreign key description, then the
    /// PRIMARY KEY columns, then (since 0.6.0; absent in 0.5.1 rows) the
    /// UNIQUE section `[n] { name, ncols, cols... }` and the CHECK section
    /// `[n] { name, marker + Expr::to_sql() }`.
    fn serialize_catalog_row(info: &TableInfo) -> RowData {
        let mut vals = Vec::new();
        vals.push(ColumnValue::Text(info.name.clone()));
        vals.push(ColumnValue::Integer(info.root_page as i32));
        vals.push(ColumnValue::Integer(info.columns.len() as i32));
        for (i, (name, ty)) in info.columns.iter().enumerate() {
            let nn = info.not_null.get(i).copied().unwrap_or(false);
            let default = info.default_values.get(i).cloned().flatten();
            let ai = info.auto_increment.get(i).copied().unwrap_or(false);
            vals.push(ColumnValue::Text(name.clone()));
            vals.push(ColumnValue::Integer(ty.to_code()));
            match ty {
                ColumnType::Char(size) => vals.push(ColumnValue::Integer(*size as i32)),
                ColumnType::Varchar(size) => vals.push(ColumnValue::Integer(*size as i32)),
                ColumnType::Decimal { precision, scale } => {
                    vals.push(ColumnValue::Integer(*precision as i32));
                    vals.push(ColumnValue::Integer(*scale as i32));
                }
                ColumnType::SmallInt { width, unsigned } => {
                    vals.push(ColumnValue::Integer(*width as i32));
                    vals.push(ColumnValue::Integer(if *unsigned { 1 } else { 0 }));
                }
                ColumnType::MediumInt { width, unsigned } => {
                    vals.push(ColumnValue::Integer(*width as i32));
                    vals.push(ColumnValue::Integer(if *unsigned { 1 } else { 0 }));
                }
                ColumnType::Double {
                    precision,
                    scale,
                    unsigned,
                } => {
                    vals.push(ColumnValue::Integer(*precision as i32));
                    vals.push(ColumnValue::Integer(*scale as i32));
                    vals.push(ColumnValue::Integer(if *unsigned { 1 } else { 0 }));
                }
                _ => {}
            }
            vals.push(ColumnValue::Integer(if nn { 1 } else { 0 }));
            match default {
                Some(expr) => {
                    vals.push(ColumnValue::Integer(1));
                    vals.push(ColumnValue::Text(format!(
                        "{}{}",
                        DEFAULT_SQL_MARKER,
                        expr.to_sql()
                    )));
                }
                None => {
                    vals.push(ColumnValue::Integer(0));
                }
            }
            vals.push(ColumnValue::Integer(if ai { 1 } else { 0 }));
        }
        vals.push(ColumnValue::Integer(info.fks.len() as i32));
        for fk in &info.fks {
            vals.push(ColumnValue::Integer(fk.columns.len() as i32));
            for c in &fk.columns {
                vals.push(ColumnValue::Text(c.value.clone()));
            }
            vals.push(ColumnValue::Text(fk.parent_table.value.clone()));
            vals.push(ColumnValue::Integer(fk.parent_columns.len() as i32));
            for pc in &fk.parent_columns {
                vals.push(ColumnValue::Text(pc.value.clone()));
            }
            let to_code = |a: &Option<crate::sql::ast::Action>| match a {
                Some(crate::sql::ast::Action::Cascade) => 1,
                Some(crate::sql::ast::Action::Restrict) => 2,
                _ => 0,
            };
            vals.push(ColumnValue::Integer(to_code(&fk.on_delete)));
            vals.push(ColumnValue::Integer(to_code(&fk.on_update)));
        }
        let pk = info.primary_key.as_deref().unwrap_or(&[]);
        vals.push(ColumnValue::Integer(pk.len() as i32));
        for c in pk {
            vals.push(ColumnValue::Text(c.clone()));
        }
        vals.push(ColumnValue::Integer(info.uniques.len() as i32));
        for u in &info.uniques {
            vals.push(ColumnValue::Text(u.name.clone()));
            vals.push(ColumnValue::Integer(u.columns.len() as i32));
            for c in &u.columns {
                vals.push(ColumnValue::Text(c.clone()));
            }
        }
        vals.push(ColumnValue::Integer(info.checks.len() as i32));
        for c in &info.checks {
            vals.push(ColumnValue::Text(c.name.clone()));
            vals.push(ColumnValue::Text(c.expr.to_sql()));
        }
        RowData(vals)
    }

    /// Deserialize a catalog row (see [`Catalog::serialize_catalog_row`]).
    /// Trailing sections missing in older rows decode as empty.
    fn deserialize_catalog_row(row: &Row) -> io::Result<TableInfo> {
        let values = &row.data.0;
        if values.len() < 3 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "catalog row too short",
            ));
        }
        let name = match &values[0] {
            ColumnValue::Text(s) => s.clone(),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "catalog name not text",
                ));
            }
        };
        let root_page = match values[1] {
            ColumnValue::Integer(i) => i as u32,
            _ => return Err(io::Error::new(io::ErrorKind::Other, "root page not int")),
        };
        let num_cols = match values[2] {
            ColumnValue::Integer(i) => i as usize,
            _ => return Err(io::Error::new(io::ErrorKind::Other, "num cols not int")),
        };
        let mut columns = Vec::new();
        let mut not_null = Vec::new();
        let mut defaults = Vec::new();
        let mut auto_inc: Vec<bool> = Vec::new();
        let mut idx = 3;
        for _ in 0..num_cols {
            let name = match &values[idx] {
                ColumnValue::Text(s) => s.clone(),
                _ => return Err(io::Error::new(io::ErrorKind::Other, "column name not text")),
            };
            idx += 1;
            let ty_code = match values.get(idx) {
                Some(ColumnValue::Integer(code)) => *code,
                _ => return Err(io::Error::new(io::ErrorKind::Other, "column type missing")),
            };
            idx += 1;
            let ty = match ColumnType::from_code(ty_code) {
                Some(ColumnType::Char(_)) => {
                    let size = match values.get(idx) {
                        Some(ColumnValue::Integer(sz)) => *sz as usize,
                        _ => return Err(io::Error::new(io::ErrorKind::Other, "char size")),
                    };
                    idx += 1;
                    ColumnType::Char(size)
                }
                Some(ColumnType::Varchar(_)) => {
                    let size = match values.get(idx) {
                        Some(ColumnValue::Integer(sz)) => *sz as usize,
                        _ => return Err(io::Error::new(io::ErrorKind::Other, "varchar size")),
                    };
                    idx += 1;
                    ColumnType::Varchar(size)
                }
                Some(ColumnType::Decimal { .. }) => {
                    let precision = match values.get(idx) {
                        Some(ColumnValue::Integer(p)) => *p as u8,
                        _ => return Err(io::Error::new(io::ErrorKind::Other, "decimal precision")),
                    };
                    idx += 1;
                    let scale = match values.get(idx) {
                        Some(ColumnValue::Integer(sc)) => *sc as u8,
                        _ => return Err(io::Error::new(io::ErrorKind::Other, "decimal scale")),
                    };
                    idx += 1;
                    ColumnType::Decimal { precision, scale }
                }
                Some(ColumnType::SmallInt { .. }) => {
                    let width = match values.get(idx) {
                        Some(ColumnValue::Integer(w)) => *w as usize,
                        _ => 0,
                    };
                    idx += 1;
                    let unsigned = match values.get(idx) {
                        Some(ColumnValue::Integer(u)) => *u == 1,
                        _ => false,
                    };
                    idx += 1;
                    ColumnType::SmallInt { width, unsigned }
                }
                Some(ColumnType::MediumInt { .. }) => {
                    let width = match values.get(idx) {
                        Some(ColumnValue::Integer(w)) => *w as usize,
                        _ => 0,
                    };
                    idx += 1;
                    let unsigned = match values.get(idx) {
                        Some(ColumnValue::Integer(u)) => *u == 1,
                        _ => false,
                    };
                    idx += 1;
                    ColumnType::MediumInt { width, unsigned }
                }
                Some(ColumnType::Double { .. }) => {
                    let precision = match values.get(idx) {
                        Some(ColumnValue::Integer(p)) => *p as usize,
                        _ => 10,
                    };
                    idx += 1;
                    let scale = match values.get(idx) {
                        Some(ColumnValue::Integer(s)) => *s as usize,
                        _ => 0,
                    };
                    idx += 1;
                    let unsigned = match values.get(idx) {
                        Some(ColumnValue::Integer(u)) => *u == 1,
                        _ => false,
                    };
                    idx += 1;
                    ColumnType::Double {
                        precision,
                        scale,
                        unsigned,
                    }
                }
                Some(ColumnType::Date) => ColumnType::Date,
                Some(other) => other,
                None => return Err(io::Error::new(io::ErrorKind::Other, "bad type")),
            };
            let nn = match values.get(idx) {
                Some(ColumnValue::Integer(i)) => *i == 1,
                _ => false,
            };
            idx += 1;
            let has_default = match values.get(idx) {
                Some(ColumnValue::Integer(i)) => *i == 1,
                _ => false,
            };
            idx += 1;
            let default = if has_default {
                if let Some(ColumnValue::Text(s)) = values.get(idx) {
                    idx += 1;
                    Some(decode_default_expr(s)?)
                } else {
                    None
                }
            } else {
                None
            };
            let ai = match values.get(idx) {
                Some(ColumnValue::Integer(i)) => *i == 1,
                _ => false,
            };
            idx += 1;
            columns.push((name, ty));
            not_null.push(nn);
            defaults.push(default);
            auto_inc.push(ai);
        }
        let num_fks = match values.get(idx) {
            Some(ColumnValue::Integer(i)) => *i as usize,
            _ => 0,
        };
        idx += 1;
        let mut fks = Vec::new();
        for _ in 0..num_fks {
            let num_cols = match values.get(idx) {
                Some(ColumnValue::Integer(i)) => *i as usize,
                _ => return Err(io::Error::new(io::ErrorKind::Other, "fk cols")),
            };
            idx += 1;
            let mut cols = Vec::new();
            for _ in 0..num_cols {
                if let Some(ColumnValue::Text(c)) = values.get(idx) {
                    cols.push(Ident::quoted(c.clone()));
                    idx += 1;
                } else {
                    return Err(io::Error::new(io::ErrorKind::Other, "fk col name"));
                }
            }
            let parent_table = match values.get(idx) {
                Some(ColumnValue::Text(s)) => s.clone(),
                _ => return Err(io::Error::new(io::ErrorKind::Other, "fk parent table")),
            };
            idx += 1;
            let num_pcols = match values.get(idx) {
                Some(ColumnValue::Integer(i)) => *i as usize,
                _ => return Err(io::Error::new(io::ErrorKind::Other, "fk pcols")),
            };
            idx += 1;
            let mut parent_cols = Vec::new();
            for _ in 0..num_pcols {
                if let Some(ColumnValue::Text(pc)) = values.get(idx) {
                    parent_cols.push(Ident::quoted(pc.clone()));
                    idx += 1;
                } else {
                    return Err(io::Error::new(io::ErrorKind::Other, "fk pcol name"));
                }
            }
            let action_from = |v: i32| match v {
                1 => Some(crate::sql::ast::Action::Cascade),
                2 => Some(crate::sql::ast::Action::Restrict),
                _ => Some(crate::sql::ast::Action::NoAction),
            };
            let on_delete = match values.get(idx) {
                Some(ColumnValue::Integer(i)) => action_from(*i),
                _ => None,
            };
            idx += 1;
            let on_update = match values.get(idx) {
                Some(ColumnValue::Integer(i)) => action_from(*i),
                _ => None,
            };
            idx += 1;
            fks.push(crate::sql::ast::ForeignKey {
                columns: cols,
                parent_table: Ident::quoted(parent_table),
                parent_columns: parent_cols,
                on_delete,
                on_update,
            });
        }
        let pk_len = match values.get(idx) {
            Some(ColumnValue::Integer(i)) => *i as usize,
            _ => 0,
        };
        idx += 1;
        let mut pk_cols = Vec::new();
        for _ in 0..pk_len {
            if let Some(ColumnValue::Text(c)) = values.get(idx) {
                pk_cols.push(c.clone());
                idx += 1;
            }
        }
        let bad = |what: &str| io::Error::new(io::ErrorKind::InvalidData, format!("catalog row: bad {}", what));
        let text_at = |i: usize, what: &str| match values.get(i) {
            Some(ColumnValue::Text(s)) => Ok(s.clone()),
            _ => Err(bad(what)),
        };
        let count_at = |i: usize, what: &str| match values.get(i) {
            Some(ColumnValue::Integer(n)) if *n >= 0 => Ok(*n as usize),
            _ => Err(bad(what)),
        };
        // UNIQUE and CHECK sections (absent in rows written before 0.6.0).
        let mut uniques = Vec::new();
        if idx < values.len() {
            let n = count_at(idx, "unique count")?;
            idx += 1;
            for _ in 0..n {
                let uname = text_at(idx, "unique name")?;
                let ncols = count_at(idx + 1, "unique column count")?;
                idx += 2;
                let mut cols = Vec::with_capacity(ncols);
                for _ in 0..ncols {
                    cols.push(text_at(idx, "unique column")?);
                    idx += 1;
                }
                uniques.push(UniqueConstraint { name: uname, columns: cols });
            }
        }
        let mut checks = Vec::new();
        if idx < values.len() {
            let n = count_at(idx, "check count")?;
            idx += 1;
            for _ in 0..n {
                let cname = text_at(idx, "check name")?;
                let sql = text_at(idx + 1, "check expression")?;
                idx += 2;
                let expr = crate::sql::parser::parse_expression_sql(&sql).map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("bad stored CHECK '{}': {}", sql, e))
                })?;
                checks.push(CheckConstraint { name: cname, expr });
            }
        }
        Ok(TableInfo {
            name,
            root_page,
            columns,
            not_null,
            default_values: defaults,
            auto_increment: auto_inc,
            fks,
            primary_key: if pk_cols.is_empty() { None } else { Some(pk_cols) },
            uniques,
            checks,
        })
    }

    fn serialize_sequence_row(name: &str, current: i64, start: i64, increment: i64) -> RowData {
        RowData(vec![
            ColumnValue::Text(name.to_string()),
            ColumnValue::Integer(current as i32),
            ColumnValue::Integer(start as i32),
            ColumnValue::Integer(increment as i32),
        ])
    }

    fn deserialize_sequence_row(row: &Row) -> io::Result<(String, i64, i64, i64)> {
        let vals = &row.data.0;
        if vals.len() < 4 {
            return Err(io::Error::new(io::ErrorKind::Other, "sequence row"));
        }
        let name = match &vals[0] {
            ColumnValue::Text(s) => s.clone(),
            _ => return Err(io::Error::new(io::ErrorKind::Other, "seq name")),
        };
        let cur = match vals[1] {
            ColumnValue::Integer(i) => i as i64,
            _ => 0,
        };
        let start = match vals[2] {
            ColumnValue::Integer(i) => i as i64,
            _ => 1,
        };
        let inc = match vals[3] {
            ColumnValue::Integer(i) => i as i64,
            _ => 1,
        };
        Ok((name, cur, start, inc))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::value::Value;

    #[test]
    fn decodes_marked_and_legacy_defaults() {
        let marked = format!("{}{}", DEFAULT_SQL_MARKER, "(1 + 2)");
        assert_eq!(decode_default_expr(&marked).unwrap().to_sql(), "(1 + 2)");
        for legacy in ["CURRENT_TIMESTAMP", "current_timestamp()", "GETDATE()", "GETUTCDATE()"] {
            assert!(matches!(decode_default_expr(legacy).unwrap(), Expr::Function { .. }), "{legacy}");
        }
        // 0.5.1 stored literal text unquoted; it stays a text literal coerced on insert.
        assert_eq!(decode_default_expr("18").unwrap(), Expr::Literal(Value::Text("18".into())));
        assert!(decode_default_expr(&format!("{}{}", DEFAULT_SQL_MARKER, "1 +")).is_err());
    }

    fn catalog_with_table() -> (tempfile::TempDir, Catalog) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rowid.db");
        let pager = Pager::new(path.to_str().unwrap()).unwrap();
        let mut catalog = Catalog::open(pager).unwrap();
        let stmt = crate::sql::parser::parse_sql("CREATE TABLE n (s TEXT)").unwrap();
        crate::execution::runtime::handle_statement(&mut catalog, stmt).unwrap();
        (dir, catalog)
    }

    #[test]
    fn allocate_rowid_exhaustion_is_numeric_overflow() {
        let (_dir, mut catalog) = catalog_with_table();
        catalog.rowid_counters.insert("n".into(), i32::MAX as i64);
        assert_eq!(catalog.allocate_rowid("n").unwrap(), i32::MAX);
        assert!(matches!(catalog.allocate_rowid("n"), Err(crate::error::DbError::NumericOverflow(_))));
        assert!(matches!(catalog.allocate_rowid("n"), Err(crate::error::DbError::NumericOverflow(_))));
    }

    #[test]
    fn allocate_rowid_skips_keys_that_have_versions() {
        let (_dir, mut catalog) = catalog_with_table();
        assert_eq!(catalog.allocate_rowid("n").unwrap(), 1);
        // Versions written behind the counter's back (e.g. raw tree writes).
        let root = catalog.get_table("n").unwrap().root_page;
        let mut tree = BTree::open_root(&catalog.pager, root).unwrap();
        tree.insert(2, RowData(vec![ColumnValue::Text("a".into())])).unwrap();
        tree.insert(3, RowData(vec![ColumnValue::Text("b".into())])).unwrap();
        tree.delete(3).unwrap();
        drop(tree);
        assert_eq!(catalog.allocate_rowid("n").unwrap(), 4);
    }

    #[test]
    fn note_rowid_used_advances_counter() {
        let (_dir, mut catalog) = catalog_with_table();
        assert_eq!(catalog.allocate_rowid("n").unwrap(), 1);
        catalog.note_rowid_used("n", 10);
        catalog.note_rowid_used("n", 5);
        assert_eq!(catalog.allocate_rowid("n").unwrap(), 11);
    }

    fn constrained_table() -> TableInfo {
        use crate::sql::ast::{Action, ForeignKey};
        TableInfo {
            name: "t".into(),
            root_page: 7,
            columns: vec![("a".into(), ColumnType::Integer), ("b".into(), ColumnType::Text)],
            not_null: vec![true, false],
            default_values: vec![None, None],
            auto_increment: vec![false, false],
            fks: vec![ForeignKey {
                columns: vec![Ident::quoted("a")],
                parent_table: Ident::quoted("p"),
                parent_columns: vec![Ident::quoted("id")],
                on_delete: Some(Action::Restrict),
                on_update: Some(Action::Cascade),
            }],
            primary_key: Some(vec!["a".into()]),
            uniques: vec![UniqueConstraint { name: "t_a_b_key".into(), columns: vec!["a".into(), "b".into()] }],
            checks: vec![CheckConstraint {
                name: "t_a_check".into(),
                expr: crate::sql::parser::parse_expression_sql("\"a\" > 0 AND \"b\" <> 'x'").unwrap(),
            }],
        }
    }

    #[test]
    fn catalog_row_round_trips_constraints() {
        use crate::sql::ast::Action;
        let info = constrained_table();
        let row = Row::new(1, Catalog::serialize_catalog_row(&info));
        let back = Catalog::deserialize_catalog_row(&row).unwrap();
        assert_eq!(back.uniques, info.uniques);
        assert_eq!(back.checks, info.checks);
        assert_eq!(back.fks, info.fks);
        assert_eq!(back.fks[0].on_delete, Some(Action::Restrict));
        assert_eq!(back.fks[0].on_update, Some(Action::Cascade));
        assert_eq!(back.primary_key, info.primary_key);
        assert_eq!(back.columns, info.columns);
        assert_eq!(back.not_null, info.not_null);
        assert_eq!(back.root_page, 7);
    }

    #[test]
    fn pre_0_6_catalog_row_decodes_without_unique_or_check() {
        // A 0.5.1 row ends after the PRIMARY KEY section.
        let mut info = constrained_table();
        info.uniques.clear();
        info.checks.clear();
        let mut data = Catalog::serialize_catalog_row(&info);
        assert_eq!(data.0.split_off(data.0.len() - 2), vec![ColumnValue::Integer(0), ColumnValue::Integer(0)]);
        let back = Catalog::deserialize_catalog_row(&Row::new(1, data)).unwrap();
        assert!(back.uniques.is_empty() && back.checks.is_empty());
        assert_eq!(back.fks, info.fks);
        assert_eq!(back.primary_key, info.primary_key);
    }

    #[test]
    fn view_rows_round_trip_and_are_not_tables() {
        let sql = "SELECT \"t\".\"id\" FROM t WHERE (id > 1)";
        let query = match crate::sql::parser::parse_sql(sql).unwrap() {
            crate::sql::ast::Statement::Query(q) => *q,
            _ => unreachable!(),
        };
        let view = ViewInfo { name: "v".into(), columns: vec!["Id".into()], sql: query.to_sql(), query };
        let row = Row::new(3, Catalog::serialize_view_row(&view));
        assert_eq!(row.data.0[0], ColumnValue::Text(VIEW_ROW_TAG.into()));
        match Catalog::decode_catalog_row(&row).unwrap() {
            CatalogEntry::View(back) => {
                assert_eq!(back.name, "v");
                assert_eq!(back.columns, vec!["Id".to_string()]);
                assert_eq!(back.sql, view.sql);
                assert_eq!(back.query, view.query);
            }
            CatalogEntry::Table(_) => panic!("a view row decoded as a table"),
        }
        // A table row still decodes as a table.
        let table = Row::new(1, Catalog::serialize_catalog_row(&constrained_table()));
        assert!(matches!(Catalog::decode_catalog_row(&table).unwrap(), CatalogEntry::Table(_)));
    }

    #[test]
    fn legacy_null_default_decodes_to_sql_null() {
        // 0.5.1 persisted `DEFAULT NULL` as the bare text NULL (any case).
        for legacy in ["NULL", "null", "Null"] {
            assert_eq!(decode_default_expr(legacy).unwrap(), Expr::Literal(Value::Null), "{legacy}");
        }
        // Quoted text 'NULL' in a marked default stays a string.
        let marked = format!("{}{}", DEFAULT_SQL_MARKER, "'NULL'");
        assert_eq!(decode_default_expr(&marked).unwrap(), Expr::Literal(Value::Text("NULL".into())));
    }
}
