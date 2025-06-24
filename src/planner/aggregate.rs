use std::collections::HashSet;

use crate::error::{DbError, DbResult};
use crate::sql::ast::{Expr, SelectExpr, SelectItem};
use crate::storage::row::ColumnType;

fn normalize(name: &str) -> String {
    name.split('.').last().unwrap_or(name).to_ascii_uppercase()
}

fn is_agg_token(token: &str) -> bool {
    let up = token.to_ascii_uppercase();
    up.starts_with("SUM(") || up.starts_with("COUNT(") || up.starts_with("AVG(") || up.starts_with("MIN(") || up.starts_with("MAX(")
}

fn collect_expr_columns(expr: &Expr, cols: &HashSet<String>, out: &mut HashSet<String>, aggs: &mut bool) {
    match expr {
        Expr::Equals { left, right }
        | Expr::NotEquals { left, right }
        | Expr::Add { left, right }
        | Expr::Subtract { left, right }
        | Expr::Multiply { left, right }
        | Expr::Divide { left, right }
        | Expr::Modulo { left, right }
        | Expr::BitwiseAnd { left, right }
        | Expr::BitwiseOr { left, right }
        | Expr::BitwiseXor { left, right }
        | Expr::GreaterThan { left, right }
        | Expr::GreaterOrEquals { left, right }
        | Expr::LessThan { left, right }
        | Expr::LessOrEquals { left, right } => {
            for tok in [left, right] {
                let n = normalize(tok);
                if is_agg_token(tok) {
                    *aggs = true;
                } else if cols.contains(&n) {
                    out.insert(n);
                }
            }
        }
        Expr::Between { expr, low, high } => {
            collect_expr_columns(&Expr::GreaterOrEquals { left: expr.clone(), right: low.clone() }, cols, out, aggs);
            collect_expr_columns(&Expr::LessOrEquals { left: expr.clone(), right: high.clone() }, cols, out, aggs);
        }
        Expr::And(a, b) | Expr::Or(a, b) => {
            collect_expr_columns(a, cols, out, aggs);
            collect_expr_columns(b, cols, out, aggs);
        }
        Expr::FunctionCall { name, args } => {
            let tok = format!("{}(", name);
            if is_agg_token(&tok) {
                *aggs = true;
            }
            for a in args {
                collect_expr_columns(a, cols, out, aggs);
            }
        }
        Expr::InSubquery { .. } | Expr::ExistsSubquery { .. } | Expr::Subquery(_) => {}
        Expr::Literal(_) | Expr::DefaultValue => {}
    }
}

pub fn validate_group_by(
    projections: &[SelectExpr],
    group_by: Option<&[String]>,
    having: Option<&Expr>,
    table_columns: &[(String, ColumnType)],
) -> DbResult<()> {
    let col_set: HashSet<String> = table_columns.iter().map(|(c, _)| c.to_ascii_uppercase()).collect();
    let mut group_cols: HashSet<String> = HashSet::new();
    if let Some(gb) = group_by {
        for c in gb {
            group_cols.insert(normalize(c));
        }
    }
    let mut agg_present = false;
    let mut select_cols: HashSet<String> = HashSet::new();
    let mut agg_cols: HashSet<String> = HashSet::new();
    for expr in projections {
        match &expr.expr {
            SelectItem::Column(c) => {
                let n = normalize(c);
                select_cols.insert(n.clone());
            }
            SelectItem::Aggregate { column: Some(c), .. } => {
                agg_present = true;
                agg_cols.insert(normalize(c));
            }
            SelectItem::Aggregate { .. } => {
                agg_present = true;
            }
            SelectItem::Expr(e) => {
                let mut cols = HashSet::new();
                collect_expr_columns(e, &col_set, &mut cols, &mut agg_present);
                select_cols.extend(cols);
            }
            SelectItem::All => {
                for c in &col_set {
                    select_cols.insert(c.clone());
                }
            }
            SelectItem::Subquery(_) | SelectItem::Literal(_) => {}
        }
    }

    if let Some(have) = having {
        let mut cols = HashSet::new();
        collect_expr_columns(have, &col_set, &mut cols, &mut agg_present);
        for c in cols {
            if !group_cols.contains(&c) && !agg_cols.contains(&c) {
                return Err(DbError::GroupByMismatch(c));
            }
        }
    }

    for c in &select_cols {
        if !group_cols.contains(c) {
            return Err(DbError::GroupByMismatch(c.clone()));
        }
    }

    if !agg_present {
        for c in &group_cols {
            if !select_cols.contains(c) {
                return Err(DbError::GroupByMismatch(c.clone()));
            }
        }
    }

    Ok(())
}

