# TASK-007: Mongo-Style Query AST Parser and SQLite `json_extract` Visitor Compiler

## Objective
Implement the query parsing and AST compilation pipeline that translates MongoDB-style query filters (comparison, logical, element, and nested dot-path queries) directly into native SQLite JSON functions (`json_extract` / `->>`), enabling high-performance engine-level filtering in Cloudflare D1 and Durable Objects SQLite.

## Scope
- Implement `src/query/ast.ts`: Query AST node types (`CompareNode`, `LogicalNode`, `ExistsNode`, `FieldPath`).
- Implement `src/query/operators.ts`: Supported operator definitions (`$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$exists`, `$and`, `$or`, `$nor`, `$not`).
- Implement `src/query/parser.ts`: Recursive descent parser converting user query filter objects into canonical AST nodes.
- Implement `src/query/compiler.ts`: SQLite visitor compiler lowering AST nodes to parameterized SQL `WHERE` clauses and bound values.
- Implement tests covering nested fields, type coercion (numerical comparison on JSON extracted values), and edge cases (`$exists: false` vs `null`).

## Allowed Files
- `src/query/ast.ts`
- `src/query/operators.ts`
- `src/query/parser.ts`
- `src/query/compiler.ts`
- `test/query/parser.test.ts`
- `test/query/compiler.test.ts`
- `docs/AI/SESSION_STATE.md`
- `docs/AI/tasks/TASK-007.md`

## Dependencies
- TASK-006 (Core Table facade)

## Inputs and Outputs
- **Inputs**: Mongo-style query filter objects (e.g. `{ "profile.age": { $gt: 18 }, status: "active" }`).
- **Outputs**: Parameterized SQL fragment `CAST(json_extract(value, '$.profile.age') AS NUMERIC) > ? AND json_extract(value, '$.status') = ?` with bound parameters `[18, "active"]`.

## Acceptance Criteria
- [x] Parsing `$eq`, `$gt`, `$lt`, `$in`, `$and`, `$or`, `$not`, and `$exists` produces valid AST.
- [x] Nested dot-paths (`"profile.settings.theme"`) are correctly compiled to `json_extract(value, '$.profile.settings.theme')`.
- [x] Numeric values in comparisons are wrapped with `CAST(... AS NUMERIC)` to guarantee correct numerical sorting instead of lexicographical string comparison.
- [x] 100% test coverage across all query operators.

## Verification Commands
```bash
pnpm test test/query/
pnpm typecheck
```

## Risks and Assumptions
- SQLite JSON functions treat string-encoded numbers as strings unless explicitly cast. The compiler must inspect comparison operand types and apply `CAST(... AS NUMERIC)` appropriately.

## Status
DONE

