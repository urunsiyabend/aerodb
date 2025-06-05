use crate::sql::ast::{Expr, Statement};

#[derive(Debug)]
pub enum PlanNode {
    CreateTable {
        table_name: String,
        columns: Vec<String>,
    },
    Insert {
        table_name: String,
        values: Vec<String>,
    },
    Select {
        table_name: String,
        selection: Option<Expr>,
    },
    Exit,
}

pub fn plan_statement(stmt: Statement) -> PlanNode {
    match stmt {
        Statement::CreateTable { table_name, columns } => {
            PlanNode::CreateTable { table_name, columns }
        }
        Statement::Insert { table_name, values } => {
            PlanNode::Insert { table_name, values }
        }
        Statement::Select { table_name, selection } => PlanNode::Select { table_name, selection },
        Statement::Exit => PlanNode::Exit,
    }
}