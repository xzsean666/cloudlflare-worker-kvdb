# TASK-005: Cloudflare Workers KV Driver with Native TTL and Prefix Scanning

## Objective
Implement the Cloudflare Workers KV driver implementing both the `KVStore` and `Driver` interfaces, featuring native edge TTL expiration, prefix scanning with cursor pagination, and batch operations.

## Scope
- Implement `src/drivers/kv/driver.ts`:
  - `get`, `set` (with `expirationTtl` / `expiration`), `delete`, `has`, `clear`.
  - Prefix scan using `kv.list({ prefix, cursor, limit })`.
  - Batch operations `getMany`, `setMany`, `deleteMany` (concurrent promise batching).
- Write unit tests against simulated `KVNamespace`.

## Allowed Files
- `src/drivers/kv/driver.ts`
- `test/drivers/kv.test.ts`
- `docs/AI/SESSION_STATE.md`
- `docs/AI/tasks/TASK-005.md`

## Dependencies
- TASK-003 (Core foundations: clock, key encoder, serializer)

## Inputs and Outputs
- **Inputs**: `@cloudflare/workers-types` `KVNamespace`.
- **Outputs**: Fully functional Cloudflare Workers KV driver.

## Acceptance Criteria
- [x] `set` correctly converts milliseconds TTL to Cloudflare KV integer seconds `expirationTtl`.
- [x] `getByPrefix` handles multi-page pagination using `list_complete` and `cursor`.
- [x] All KV driver tests pass.

## Verification Commands
```bash
pnpm test test/drivers/kv.test.ts
pnpm typecheck
```

## Risks and Assumptions
- Cloudflare Workers KV has a 1 write/sec per key soft limit; documentation must caution against high-frequency updates on a single key.

## Status
DONE

