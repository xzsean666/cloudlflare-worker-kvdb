# TASK-012: Cloudflare Durable Objects SQLite Driver (`ctx.storage.sql`)

## Objective
Implement a specialized driver for Cloudflare Durable Objects backed by `ctx.storage.sql`, enabling strongly consistent, low-latency in-actor relational and KV operations with ACID transactions.

## Scope
- Implement `src/drivers/do-sql/driver.ts`:
  - `DurableObjectSqlDriver` wrapping `ctx.storage.sql`.
  - Transactional CRUD: `get`, `set`, `delete`, `has`, `clear`.
  - In-memory execution using SQLite statements with `cursor()` / `toArray()`.
  - Schema creation and index support on DO SQLite.
- Write tests using Miniflare / Vitest Workers pool with DO SQLite simulation.

## Allowed Files
- `src/drivers/do-sql/driver.ts`
- `test/drivers/do-sql.test.ts`
- `docs/AI/SESSION_STATE.md`
- `docs/AI/tasks/TASK-012.md`

## Dependencies
- TASK-004 (D1 Driver base patterns)

## Inputs and Outputs
- **Inputs**: Cloudflare Durable Objects `SqlStorage` interface (`ctx.storage.sql`).
- **Outputs**: Ultra-low latency, strongly consistent SQLite driver for Durable Objects.

## Acceptance Criteria
- [x] Driver correctly initializes tables on `ctx.storage.sql`.
- [x] Fast transactional read/write operations execute in sub-millisecond time inside DO actor.
- [x] Tests pass in Vitest Workers pool.

## Verification Commands
```bash
pnpm test test/drivers/do-sql.test.ts
pnpm typecheck
pnpm build
```

## Risks and Assumptions
- Available only in SQLite-backed Durable Objects (legacy KV-backed Durable Objects do not have `ctx.storage.sql`).

## Status
DONE

