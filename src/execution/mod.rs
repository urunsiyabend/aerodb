pub mod executor;
pub mod plan;
pub mod runtime;

pub use executor::Executor;
pub use plan::PlanNode;
pub use runtime::{execute_delete, execute_select_with_indexes, execute_update, handle_statement};

/// Entry point for executing a plan (stub).
pub fn execute_plan(plan: PlanNode /*, btree: &mut storage::BTree */) {
    match plan {
        PlanNode::CreateTable { table_name, columns, .. } => {
            println!("Planning create table {} {:?}", table_name, columns);
        }
        PlanNode::DropTable { table_name, .. } => {
            println!("Planning drop table {}", table_name);
        }
        PlanNode::Insert { table_name, values } => {
            println!("Executing: Insert into {} {:?}", table_name, values);
            // In future: btree.insert(key, &payload).unwrap();
        }
        PlanNode::CreateIndex { index_name, table_name, column_name } => {
            println!(
                "Planning create index {} on {} ({})",
                index_name, table_name, column_name
            );
        }
        PlanNode::Select { table_name, selection, .. } => {
            println!("Executing: Select from {} where {:?}", table_name, selection);
            // In future: if let Some(row) = btree.find(key).unwrap() { ... }
        }
        PlanNode::Delete { table_name, selection } => {
            println!("Executing: Delete from {} where {:?}", table_name, selection);
            // In future: btree.delete(key).unwrap();
        }
        PlanNode::Update { table_name, assignments, selection } => {
            println!(
                "Executing: Update {} set {:?} where {:?}",
                table_name, assignments, selection
            );
            // Future: btree.update(...)
        }
        PlanNode::Exit => {
            // No action; main loop handles exit.
        }
    }
}
