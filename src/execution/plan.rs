use crate::sql::ast::Statement;

#[derive(Debug)]
pub enum PlanNode {
    Insert { key: i32, payload: String },
    Select { key: i32 },
    Exit,
}

pub fn plan_statement(stmt: Statement) -> PlanNode {
    match stmt {
        Statement::Insert { key, payload } => PlanNode::Insert { key, payload },
        Statement::Select { key } => PlanNode::Select { key },
        Statement::Exit => PlanNode::Exit,
    }
}
