# TASK-006: Core Table Facade and CRUD Operations with Parameter-Safe Batching

## Objective
Implement the top-level `CloudflareKVDB` client container and `Table` facade providing high-level, type-safe CRUD operations, prefix scanning, and lifecycle management for application developers.

## Scope
- Implement `src/core/kvdb.ts`:
  - `CloudflareKVDB` initialization with Cloudflare bindings (`d1`, `kv`, `r2`, `tablePrefix`, `ctx`, `sessionBookmark`).
  - Table namespace caching and lifecycle release (`close`).
- Implement `src/core/table.ts`:
  - `Table<V>` facade wrapping driver operations with namespace prefixing.
  - CRUD: `get`, `set`, `delete`, `has`, `clear`.
  - Batch: `getMany`, `setMany`, `deleteMany`.
  - Prefix: `getByPrefix`, `deleteByPrefix`.
- Implement `src/index.ts`: Public package exports.
- Write integration tests for `CloudflareKVDB` and `Table`.

## Allowed Files
- `src/core/kvdb.ts`
- `src/core/table.ts`
- `src/index.ts`
- `test/core/table.test.ts`
- `docs/AI/SESSION_STATE.md`
- `docs/AI/tasks/TASK-006.md`

## Dependencies
- TASK-004 (D1 Driver)
- TASK-005 (KV Driver)

## Inputs and Outputs
- **Inputs**: `CloudflareKVDBOptions`, `D1Database`, `KVNamespace`.
- **Outputs**: Top-level API entrypoint ready for application use.

## Acceptance Criteria
- [x] Developers can instantiate `new CloudflareKVDB({ d1 })` and perform full typed CRUD operations.
- [x] Namespace isolation works transparently: `db.table("users")` and `db.table("tokens")` do not collide.
- [x] Integration tests pass without errors.

## Verification Commands
```bash
pnpm test test/core/table.test.ts
pnpm typecheck
```

## Risks and Assumptions
- Tables must seamlessly support both basic KV mode and future physical schema tables without breaking public signatures.

## Status
DONE

