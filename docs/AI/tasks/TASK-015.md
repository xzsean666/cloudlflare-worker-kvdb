# TASK-015: Cloudflare Hyperdrive Postgres Driver Integration

## Objective
Implement the Cloudflare Hyperdrive driver leveraging `env.HYPERDRIVE` connection pooling to connect to PostgreSQL or MySQL databases with edge latency acceleration, providing full KVDB API compliance on top of external databases.

## Scope
- Implement `src/drivers/hyperdrive/driver.ts`:
  - Connects using Hyperdrive connection string (`env.HYPERDRIVE.connectionString`).
  - Table initialization and physical column mapping.
  - JSON querying using native Postgres `jsonb` operators (`@>`, `->>`).
- Write integration tests.

## Allowed Files
- `src/drivers/hyperdrive/driver.ts`
- `test/drivers/hyperdrive.test.ts`
- `docs/AI/SESSION_STATE.md`
- `docs/AI/tasks/TASK-015.md`

## Dependencies
- TASK-004 (Base Driver patterns)

## Inputs and Outputs
- **Inputs**: Cloudflare `Hyperdrive` binding (`env.HYPERDRIVE`).
- **Outputs**: Edge-accelerated external Postgres driver.

## Acceptance Criteria
- [x] Connects via Hyperdrive connection pooling.
- [x] Executes CRUD and JSON query operations matching the compliance suite.
- [x] Tests pass in Vitest.

## Verification Commands
```bash
pnpm test test/drivers/hyperdrive.test.ts
pnpm typecheck
pnpm build
```

## Risks and Assumptions
- Requires Postgres wire protocol driver compatible with Cloudflare Workers (e.g. `postgres.js` or `@neondatabase/serverless`).

## Status
DONE

