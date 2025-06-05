pub mod executor;
pub mod plan;

pub use executor::Executor;
pub use plan::PlanNode;

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
        PlanNode::Select { table_name, selection } => {
            println!("Executing: Select from {} where {:?}", table_name, selection);
            // In future: if let Some(row) = btree.find(key).unwrap() { ... }
        }
        PlanNode::Delete { table_name, selection } => {
            println!("Executing: Delete from {} where {:?}", table_name, selection);
            // In future: btree.delete(key).unwrap();
        }
        PlanNode::Exit => {
            // No action; main loop handles exit.
        }
    }
}
