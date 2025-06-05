use crate::sql::ast::{Expr, Statement, OrderBy};
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
    Exit,
}

pub fn plan_statement(stmt: Statement) -> PlanNode {
    match stmt {
        Statement::CreateTable { table_name, columns, if_not_exists } => {
            PlanNode::CreateTable { table_name, columns, if_not_exists }
        }
        Statement::CreateIndex { index_name, table_name, column_name } => {
            PlanNode::CreateIndex { index_name, table_name, column_name }
        }
        Statement::Insert { table_name, values } => {
            PlanNode::Insert { table_name, values }
        }
        Statement::Select { table_name, selection, limit, offset, order_by } => {
            PlanNode::Select {
                table_name,
                selection,
                limit,
                offset,
                order_by,
            }
        }
        Statement::DropTable { table_name, if_exists } => PlanNode::DropTable { table_name, if_exists },
        Statement::Delete { table_name, selection } => PlanNode::Delete { table_name, selection },
        Statement::Exit => PlanNode::Exit,
    }
}