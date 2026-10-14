//! `information_schema`: read-only virtual views over the catalog (an SQL
//! F021 subset).
//!
//! A FROM item `information_schema.<view>` (schema and view name follow the
//! identifier rule: unquoted case-insensitive, quoted exact) is materialized
//! from the in-memory catalog when the query runs — so it reflects the
//! current transaction's own DDL — and then flows through the ordinary
//! SELECT executor (WHERE, joins, GROUP BY, ORDER BY, subqueries and views
//! over it all work). No other schema exists: any other qualifier is
//! `TableNotFound` (42P01). Writing to or creating/dropping/altering anything
//! in `information_schema` is `FeatureNotSupported` (0A000,
//! "information_schema is read-only").
//!
//! Every table and view lives in catalog `aerodb`, schema `public`. Rows come
//! in a deterministic order: by table name, then by position (constraints:
//! PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK, each in declaration order).
//! Values are TEXT except `ordinal_position`, `character_maximum_length`,
//! `numeric_precision` and `numeric_scale` (INTEGER, NULL when not
//! applicable).
//!
//! | view | columns |
//! |---|---|
//! | `tables` | table_catalog, table_schema, table_name, table_type ('BASE TABLE' / 'VIEW') |
//! | `columns` | table_catalog, table_schema, table_name, column_name, ordinal_position, column_default, is_nullable, data_type, character_maximum_length, numeric_precision, numeric_scale |
//! | `table_constraints` | constraint_catalog, constraint_schema, constraint_name, table_schema, table_name, constraint_type ('PRIMARY KEY' / 'UNIQUE' / 'FOREIGN KEY' / 'CHECK') |
//! | `key_column_usage` | constraint_name, table_schema, table_name, column_name, ordinal_position (within the constraint) |
//! | `referential_constraints` | constraint_name, unique_constraint_name, update_rule, delete_rule ('NO ACTION' / 'RESTRICT' / 'CASCADE') |
//! | `check_constraints` | constraint_name, check_clause |
//! | `views` | table_name, view_definition |
//!
//! Notes on the subset:
//! - `data_type` uses the standard spellings (INTEGER, SMALLINT, DECIMAL,
//!   DOUBLE PRECISION, CHARACTER, CHARACTER VARYING, BOOLEAN, DATE, TIME,
//!   TIMESTAMP) plus the engine's non-standard types MEDIUMINT, BIGINT (view
//!   columns only), TEXT, DATETIME and YEAR.
//! - `numeric_precision` / `numeric_scale` follow PostgreSQL: DECIMAL(p,s)
//!   gives p and s (decimal digits); the integer types give their binary
//!   precision (SMALLINT 16, MEDIUMINT 24, INTEGER 32, BIGINT 64) with scale
//!   0, DOUBLE PRECISION gives 53 and a NULL scale.
//! - `is_nullable` is 'NO' for NOT NULL and PRIMARY KEY columns. NOT NULL is
//!   a column property here, not a listed constraint (no CHECK row for it).
//! - `column_default` is the default's SQL text (a default written by 0.5.1
//!   is shown as the character literal it is coerced from), or NULL.
//! - View columns are listed with the view's result types as the binder
//!   derives them; a column with no derivable type (an untyped NULL) is
//!   reported as TEXT, as the executor's result header does.
//! - Constraint names are the persisted ones; tables from 0.5.1 files (no
//!   stored names) get the deterministic defaults `{table}_pkey` and
//!   `{table}_{cols}_fkey`. `unique_constraint_name` is the referenced
//!   table's PRIMARY KEY or UNIQUE constraint over the referenced columns.

use crate::catalog::{Catalog, Relation, TableInfo};
use crate::constraints::keys::{default_pk_name, fk_name};
use crate::error::{DbError, DbResult};
use crate::sql::ast::{Action, Expr, ForeignKey, Ident, quote_ident};
use crate::sql::value::{SqlType, Value};
use crate::storage::row::ColumnType;

/// One information_schema view: its name and (lowercase) columns.
#[derive(Debug)]
pub struct InfoView {
    pub name: &'static str,
    pub columns: &'static [(&'static str, SqlType)],
}

const T: SqlType = SqlType::Text;
const I: SqlType = SqlType::Integer;

const VIEWS: &[InfoView] = &[
    InfoView {
        name: "tables",
        columns: &[("table_catalog", T), ("table_schema", T), ("table_name", T), ("table_type", T)],
    },
    InfoView {
        name: "columns",
        columns: &[
            ("table_catalog", T),
            ("table_schema", T),
            ("table_name", T),
            ("column_name", T),
            ("ordinal_position", I),
            ("column_default", T),
            ("is_nullable", T),
            ("data_type", T),
            ("character_maximum_length", I),
            ("numeric_precision", I),
            ("numeric_scale", I),
        ],
    },
    InfoView {
        name: "table_constraints",
        columns: &[
            ("constraint_catalog", T),
            ("constraint_schema", T),
            ("constraint_name", T),
            ("table_schema", T),
            ("table_name", T),
            ("constraint_type", T),
        ],
    },
    InfoView {
        name: "key_column_usage",
        columns: &[
            ("constraint_name", T),
            ("table_schema", T),
            ("table_name", T),
            ("column_name", T),
            ("ordinal_position", I),
        ],
    },
    InfoView {
        name: "referential_constraints",
        columns: &[("constraint_name", T), ("unique_constraint_name", T), ("update_rule", T), ("delete_rule", T)],
    },
    InfoView { name: "check_constraints", columns: &[("constraint_name", T), ("check_clause", T)] },
    InfoView { name: "views", columns: &[("table_name", T), ("view_definition", T)] },
];

const CATALOG_NAME: &str = "aerodb";
const SCHEMA_NAME: &str = "public";

/// Whether `ident` names the `information_schema` schema.
pub fn is_schema(ident: &Ident) -> bool {
    ident.matches("information_schema")
}

/// The error for any write / DDL aimed at information_schema.
pub fn read_only() -> DbError {
    DbError::FeatureNotSupported("information_schema is read-only".into())
}

/// The view `schema.name` of a FROM item; any schema other than
/// information_schema, or an unknown view, is `TableNotFound`.
pub fn resolve(schema: &Ident, name: &Ident) -> DbResult<&'static InfoView> {
    let not_found = || DbError::TableNotFound(format!("{}.{}", schema.value, name.value));
    if !is_schema(schema) {
        return Err(not_found());
    }
    VIEWS.iter().find(|v| name.matches(v.name)).ok_or_else(not_found)
}

fn text(s: impl Into<String>) -> Value {
    Value::Text(s.into())
}

fn int(n: impl Into<i64>) -> Value {
    Value::Int(n.into())
}

fn opt_int(n: Option<i64>) -> Value {
    n.map_or(Value::Null, Value::Int)
}

/// (data_type, character_maximum_length, numeric_precision, numeric_scale).
type TypeInfo = (&'static str, Option<i64>, Option<i64>, Option<i64>);

fn sql_type_info(ty: &SqlType) -> TypeInfo {
    match ty {
        SqlType::Integer => ("INTEGER", None, Some(32), Some(0)),
        SqlType::SmallInt => ("SMALLINT", None, Some(16), Some(0)),
        SqlType::BigInt => ("BIGINT", None, Some(64), Some(0)),
        SqlType::Decimal { precision, scale } => ("DECIMAL", None, Some(*precision as i64), Some(*scale as i64)),
        SqlType::Double => ("DOUBLE PRECISION", None, Some(53), None),
        SqlType::Char(n) => ("CHARACTER", Some(*n as i64), None, None),
        SqlType::Varchar(n) => ("CHARACTER VARYING", Some(*n as i64), None, None),
        SqlType::Text | SqlType::Null => ("TEXT", None, None, None),
        SqlType::Boolean => ("BOOLEAN", None, None, None),
        SqlType::Date => ("DATE", None, None, None),
        SqlType::Time => ("TIME", None, None, None),
        SqlType::Timestamp => ("TIMESTAMP", None, None, None),
        SqlType::DateTime => ("DATETIME", None, None, None),
        SqlType::Year => ("YEAR", None, None, None),
    }
}

fn column_type_info(ty: &ColumnType) -> TypeInfo {
    match ty {
        ColumnType::MediumInt { .. } => ("MEDIUMINT", None, Some(24), Some(0)),
        other => sql_type_info(&SqlType::from_column_type(other)),
    }
}

fn sorted_tables(catalog: &Catalog) -> Vec<TableInfo> {
    let mut tables = catalog.all_tables();
    tables.sort_by(|a, b| a.name.cmp(&b.name));
    tables
}

/// The declared name of `col` among `columns` (identifier rule), else as given.
fn declared(columns: &[(String, ColumnType)], col: &Ident) -> String {
    columns.iter().find(|(n, _)| col.matches(n)).map_or_else(|| col.value.clone(), |(n, _)| n.clone())
}

fn pk_name(t: &TableInfo) -> String {
    t.primary_key_name.clone().unwrap_or_else(|| default_pk_name(&t.name))
}

fn fk_columns(t: &TableInfo, fk: &ForeignKey) -> Vec<String> {
    fk.columns.iter().map(|c| declared(&t.columns, c)).collect()
}

fn rule(a: &Option<Action>) -> &'static str {
    match a {
        None | Some(Action::NoAction) => "NO ACTION",
        Some(Action::Restrict) => "RESTRICT",
        Some(Action::Cascade) => "CASCADE",
    }
}

/// The PRIMARY KEY or UNIQUE constraint of the table `fk` references that
/// covers exactly the referenced columns.
fn unique_constraint_name(catalog: &Catalog, fk: &ForeignKey) -> Option<String> {
    let Ok(Relation::Table(parent)) = catalog.resolve_relation(&fk.parent_table) else {
        return None;
    };
    let mut referenced: Vec<String> = if fk.parent_columns.is_empty() {
        parent.primary_key.clone()?
    } else {
        fk.parent_columns.iter().map(|c| declared(&parent.columns, c)).collect()
    };
    referenced.sort();
    let same = |cols: &[String]| {
        let mut c = cols.to_vec();
        c.sort();
        c == referenced
    };
    if parent.primary_key.as_deref().is_some_and(same) {
        return Some(pk_name(parent));
    }
    parent.uniques.iter().find(|u| same(&u.columns)).map(|u| u.name.clone())
}

/// CHECK text as written by a user: the stored expression with the
/// canonical (quoted) column references shown unquoted where possible.
fn check_clause(expr: &Expr) -> String {
    expr.transform(&mut |e| match e {
        Expr::Column { table, name } if name.quoted && quote_ident(&name.value) == name.value => {
            Expr::Column { table, name: Ident::new(name.value) }
        }
        other => other,
    })
    .to_sql()
}

/// Materialize the rows of `view` from the catalog.
pub fn rows(catalog: &Catalog, view: &InfoView) -> DbResult<Vec<Vec<Value>>> {
    let mut out = Vec::new();
    match view.name {
        "tables" => {
            let mut rels: Vec<(String, &str)> =
                catalog.all_tables().into_iter().map(|t| (t.name, "BASE TABLE")).collect();
            rels.extend(catalog.all_views().into_iter().map(|v| (v.name, "VIEW")));
            rels.sort();
            for (name, kind) in rels {
                out.push(vec![text(CATALOG_NAME), text(SCHEMA_NAME), text(name), text(kind)]);
            }
        }
        "columns" => {
            // (relation name, rows) so tables and views interleave by name.
            let mut rels: Vec<(String, Vec<Vec<Value>>)> = Vec::new();
            for t in catalog.all_tables() {
                let pk = t.primary_key.clone().unwrap_or_default();
                let rows = t
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(i, (name, ty))| {
                        let nullable = !(t.not_null.get(i).copied().unwrap_or(false) || pk.contains(name));
                        let default = t.default_values.get(i).cloned().flatten().map(|d| d.to_sql());
                        column_row(&t.name, name, i, default, nullable, column_type_info(ty))
                    })
                    .collect();
                rels.push((t.name.clone(), rows));
            }
            for v in catalog.all_views() {
                let types = crate::execution::bind::view_column_types(catalog, &v.query)?;
                let rows = v
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(i, name)| {
                        let ty = types.get(i).copied().unwrap_or(SqlType::Null);
                        column_row(&v.name, name, i, None, true, sql_type_info(&ty))
                    })
                    .collect();
                rels.push((v.name.clone(), rows));
            }
            rels.sort_by(|a, b| a.0.cmp(&b.0));
            out.extend(rels.into_iter().flat_map(|(_, r)| r));
        }
        "table_constraints" => {
            for t in sorted_tables(catalog) {
                let mut push = |name: String, kind: &str| {
                    out.push(vec![
                        text(CATALOG_NAME),
                        text(SCHEMA_NAME),
                        text(name),
                        text(SCHEMA_NAME),
                        text(t.name.clone()),
                        text(kind),
                    ])
                };
                if t.primary_key.as_ref().is_some_and(|pk| !pk.is_empty()) {
                    push(pk_name(&t), "PRIMARY KEY");
                }
                for u in &t.uniques {
                    push(u.name.clone(), "UNIQUE");
                }
                for fk in &t.fks {
                    push(fk_name(&t, fk), "FOREIGN KEY");
                }
                for c in &t.checks {
                    push(c.name.clone(), "CHECK");
                }
            }
        }
        "key_column_usage" => {
            for t in sorted_tables(catalog) {
                let mut push = |constraint: String, cols: &[String]| {
                    for (i, c) in cols.iter().enumerate() {
                        out.push(vec![
                            text(constraint.clone()),
                            text(SCHEMA_NAME),
                            text(t.name.clone()),
                            text(c.clone()),
                            int(i as i64 + 1),
                        ]);
                    }
                };
                if let Some(pk) = t.primary_key.as_ref().filter(|pk| !pk.is_empty()) {
                    push(pk_name(&t), pk);
                }
                for u in &t.uniques {
                    push(u.name.clone(), &u.columns);
                }
                for fk in &t.fks {
                    push(fk_name(&t, fk), &fk_columns(&t, fk));
                }
            }
        }
        "referential_constraints" => {
            for t in sorted_tables(catalog) {
                for fk in &t.fks {
                    out.push(vec![
                        text(fk_name(&t, fk)),
                        unique_constraint_name(catalog, fk).map_or(Value::Null, text),
                        text(rule(&fk.on_update)),
                        text(rule(&fk.on_delete)),
                    ]);
                }
            }
        }
        "check_constraints" => {
            for t in sorted_tables(catalog) {
                for c in &t.checks {
                    out.push(vec![text(c.name.clone()), text(check_clause(&c.expr))]);
                }
            }
        }
        "views" => {
            let mut views = catalog.all_views();
            views.sort_by(|a, b| a.name.cmp(&b.name));
            for v in views {
                out.push(vec![text(v.name), text(v.sql)]);
            }
        }
        other => return Err(DbError::TableNotFound(format!("information_schema.{other}"))),
    }
    Ok(out)
}

fn column_row(
    relation: &str,
    column: &str,
    index: usize,
    default: Option<String>,
    nullable: bool,
    (data_type, max_len, precision, scale): TypeInfo,
) -> Vec<Value> {
    vec![
        text(CATALOG_NAME),
        text(SCHEMA_NAME),
        text(relation),
        text(column),
        int(index as i64 + 1),
        default.map_or(Value::Null, text),
        text(if nullable { "YES" } else { "NO" }),
        text(data_type),
        opt_int(max_len),
        opt_int(precision),
        opt_int(scale),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_view_resolves_by_identifier_rule() {
        let schema = Ident::new("INFORMATION_SCHEMA");
        for v in VIEWS {
            assert_eq!(resolve(&schema, &Ident::new(v.name.to_uppercase())).unwrap().name, v.name);
            assert_eq!(resolve(&Ident::quoted("information_schema"), &Ident::quoted(v.name)).unwrap().name, v.name);
            assert!(v.columns.iter().all(|(c, _)| c.chars().all(|ch| ch.is_ascii_lowercase() || ch == '_')));
        }
        assert!(resolve(&Ident::quoted("INFORMATION_SCHEMA"), &Ident::new("tables")).is_err());
        assert!(resolve(&Ident::new("public"), &Ident::new("tables")).is_err());
        assert!(resolve(&schema, &Ident::quoted("TABLES")).is_err());
    }
}
