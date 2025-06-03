pub mod executor;
pub mod plan;

pub use executor::Executor;
pub use plan::PlanNode;

/// Entry point for executing a plan (stub).
pub fn execute_plan(plan: PlanNode /*, btree: &mut storage::BTree */) {
    match plan {
        PlanNode::Insert { key, payload } => {
            println!("Executing: Insert key={}, payload={}", key, payload);
            // In future: btree.insert(key, &payload).unwrap();
        }
        PlanNode::Select { key } => {
            println!("Executing: Select key={}", key);
            // In future: if let Some(row) = btree.find(key).unwrap() { ... }
        }
        PlanNode::Exit => {
            // No action; main loop handles exit.
        }
    }
}
