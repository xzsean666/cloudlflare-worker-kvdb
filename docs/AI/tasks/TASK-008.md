# TASK-008: Physical Schema Tables, Dynamic Multi-Keys, and Native B-Tree Indexes

## Objective
Implement schema-aware tables allowing developers to define custom primary keys, secondary indexed columns, and composite B-tree indexes directly in Cloudflare D1/DO SQLite, with zero-downtime dynamic key additions (`table.addKey()`).

## Scope
- Implement `src/core/schema.ts`:
  - `TableSchema` definition types (`primaryKey`, `keys`, `indexes`).
  - Runtime validation for schema definitions.
  - SQL DDL generator for physical table creation (`CREATE TABLE IF NOT EXISTS _kvdb_t_<name> (...)`).
  - B-Tree index generator (`CREATE INDEX IF NOT EXISTS idx_<name>_<col> ON _kvdb_t_<name> (...)`).
- Implement schema-aware Table methods:
  - `table.set({ keys, value })`: Writes physical key columns alongside `value` JSON text in an atomic statement.
  - `table.getBy(column, val)`: High-performance O(1) point lookup using secondary B-Tree index.
  - `table.addKey(columnName, options)`: Dynamic schema evolution executing `ALTER TABLE ... ADD COLUMN` and `CREATE INDEX` if indexed.
- Write unit and integration tests verifying schema creation, secondary lookups, and dynamic column addition.

## Allowed Files
- `src/core/schema.ts`
- `src/core/table.ts`
- `src/drivers/d1/sql-builder.ts`
- `test/core/schema.test.ts`
- `docs/AI/SESSION_STATE.md`
- `docs/AI/tasks/TASK-008.md`

## Dependencies
- TASK-007 (Query AST & Compiler)

## Inputs and Outputs
- **Inputs**: Schema definition object with primary key and secondary indexes.
- **Outputs**: Physical table with real SQL columns and B-Tree indexes in D1.

## Acceptance Criteria
- [x] Schema table automatically creates the physical SQL table and indexes upon initialization.
- [x] Point lookup `getBy("email", "test@domain.com")` queries the physical column with index scan.
- [x] Calling `table.addKey("status", { type: "string", index: true })` executes safe `ALTER TABLE` and creates the secondary index without data loss.
- [x] All tests pass against simulated D1.

## Verification Commands
```bash
pnpm test test/core/schema.test.ts
pnpm typecheck
```

## Risks and Assumptions
- SQLite `ALTER TABLE ADD COLUMN` has restrictions (e.g. cannot add columns with `NOT NULL` without default values). The DDL generator must provide sensible defaults.

## Status
DONE

