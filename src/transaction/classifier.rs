use crate::sql::ast::Statement;

/// Returns true for statements that change persistent database state and should
/// therefore be wrapped in an implicit transaction when no explicit one exists.
pub fn statement_requires_transaction(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::Insert { .. }
            | Statement::Update { .. }
            | Statement::Delete { .. }
            | Statement::CreateTable { .. }
            | Statement::DropTable { .. }
            | Statement::CreateIndex { .. }
            | Statement::DropIndex { .. }
            | Statement::CreateSequence(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::parse_statement;

    #[test]
    fn identifies_mutating_statements() {
        let insert = parse_statement("INSERT INTO users VALUES (1)").unwrap();
        let select = parse_statement("SELECT * FROM users").unwrap();

        assert!(statement_requires_transaction(&insert));
        assert!(!statement_requires_transaction(&select));
    }
}
