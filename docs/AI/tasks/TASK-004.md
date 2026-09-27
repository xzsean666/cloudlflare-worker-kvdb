# TASK-004: Cloudflare D1 Driver with `db.batch()` Bundling and D1 Sessions API Bookmarks

## Objective
Implement the production-ready Cloudflare D1 driver implementing the `Driver` interface, featuring automatic `db.batch()` aggregation, parameter-safe prepared statements, and D1 Sessions API bookmark propagation for Read-Your-Own-Writes (RYW) consistency.

## Scope
- Implement `src/drivers/types.ts`: `DriverCapabilities`, `Driver`, and `KVStore` contracts.
- Implement `src/drivers/d1/sessions.ts`: D1 Sessions API wrapper (`withSession(bookmark)`) and bookmark manager.
- Implement `src/drivers/d1/driver.ts`:
  - Initialization & table bootstrap (`CREATE TABLE IF NOT EXISTS _kvdb_entries ...`).
  - Single-key CRUD: `get`, `set`, `delete`, `has`, `clear`.
  - Native batch operations via `db.batch()`: `getMany`, `setMany`, `deleteMany`.
  - Monotonic timestamp assignment on write.
  - Transparent integration with `chunker.ts` to enforce <= 100 bound parameters per query.
- Write unit and integration tests against simulated D1 database.

## Allowed Files
- `src/drivers/types.ts`
- `src/drivers/d1/driver.ts`
- `src/drivers/d1/sessions.ts`
- `src/drivers/d1/sql-builder.ts`
- `test/drivers/d1.test.ts`
- `docs/AI/SESSION_STATE.md`
- `docs/AI/tasks/TASK-004.md`

## Dependencies
- TASK-003 (Core foundations: clock, chunker, serializer)

## Inputs and Outputs
- **Inputs**: `@cloudflare/workers-types` `D1Database`, `D1PreparedStatement`, and session bookmark specifications.
- **Outputs**: High-performance D1 driver with zero N+1 queries and parameter safety.

## Acceptance Criteria
- [x] `setMany` with 200 items correctly executes via `db.batch()` in chunked statements without hitting the 100-parameter limit.
- [x] `getMany` correctly retrieves 200 items using chunked `IN (?, ...)` queries and merges results preserving order.
- [x] Session bookmarks are captured on write and made retrievable via `driver.getBookmark()`.
- [x] All D1 driver tests pass against simulated D1.

## Verification Commands
```bash
pnpm test test/drivers/d1.test.ts
pnpm typecheck
```

## Risks and Assumptions
- Mocking or simulating D1 in tests must accurately enforce the 100-parameter limit to catch regressions.

## Status
DONE

