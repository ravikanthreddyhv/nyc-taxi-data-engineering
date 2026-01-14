## Data Quality Strategy

All transformations are validated using SQL-based tests.

### Types of Tests
- Referential integrity checks
- Null validation
- Reconciliation tests
- Aggregate consistency tests

### Execution Order
1. Transform data
2. Run quality checks
3. Block downstream if failures occur
4. Log results for audit
