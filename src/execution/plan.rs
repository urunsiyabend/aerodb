use crate::sql::ast::{Expr, Statement, OrderBy, JoinClause, SelectExpr, Predicate};
use crate::storage::row::ColumnType;

#[derive(Debug)]
pub enum PlanNode {
    CreateTable {
        table_name: String,
        columns: Vec<(String, ColumnType)>,
        if_not_exists: bool,
    },
    CreateIndex {
        index_name: String,
        table_name: String,
        column_name: String,
    },
    DropTable {
        table_name: String,
        if_exists: bool,
    },
    Insert {
        table_name: String,
        values: Vec<String>,
    },
    Select {
        table_name: String,
        selection: Option<Expr>,
        limit: Option<usize>,
        offset: Option<usize>,
        order_by: Option<OrderBy>,
    },
    Delete {
        table_name: String,
        selection: Option<Expr>,
    },
    Update {
        table_name: String,
        assignments: Vec<(String, String)>,
        selection: Option<Expr>,
    },
    MultiJoin(MultiJoinPlan),
    Exit,
}

#[derive(Debug, Clone)]
pub struct MultiJoinPlan {
    pub base_table: String,
    pub joins: Vec<JoinClause>,
    pub projections: Vec<SelectExpr>,
    pub where_predicate: Option<Predicate>,
}

pub fn plan_statement(stmt: Statement) -> PlanNode {
    match stmt {
        Statement::CreateTable { table_name, columns, if_not_exists, .. } => {
            PlanNode::CreateTable { table_name, columns, if_not_exists }
        }
        Statement::CreateIndex { index_name, table_name, column_name } => {
            PlanNode::CreateIndex { index_name, table_name, column_name }
        }
        Statement::Insert { table_name, values } => {
            PlanNode::Insert { table_name, values }
        }
        Statement::Select { columns, from, joins, where_predicate, group_by: _ } => {
            let table_name = match from.first().unwrap() {
                crate::sql::ast::TableRef::Named(t) => t.clone(),
                _ => return PlanNode::Select { table_name: String::new(), selection: None, limit: None, offset: None, order_by: None },
            };
            if joins.is_empty() {
                PlanNode::Select {
                    table_name,
                    selection: where_predicate,
                    limit: None,
                    offset: None,
                    order_by: None,
                }
            } else {
                PlanNode::MultiJoin(MultiJoinPlan {
                    base_table: table_name,
                    joins,
                    projections: columns,
                    where_predicate,
                })
            }
        }
        Statement::DropTable { table_name, if_exists } => PlanNode::DropTable { table_name, if_exists },
        Statement::Delete { table_name, selection } => PlanNode::Delete { table_name, selection },
        Statement::Update { table_name, assignments, selection } => PlanNode::Update { table_name, assignments, selection },
        Statement::BeginTransaction { .. } | Statement::Commit | Statement::Rollback => PlanNode::Exit,
        Statement::Exit => PlanNode::Exit,
    }
}