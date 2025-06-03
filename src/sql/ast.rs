#[derive(Debug)]
pub enum Statement {
    Insert { key: i32, payload: String },
    Select { key: i32 },
    Exit,
}
