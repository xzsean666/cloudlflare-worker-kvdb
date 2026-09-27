# TASK-009: Multi-Tier Caching System (L1 Memory + L2 KV + `waitUntil` SWR)

## Objective
Implement a multi-tier caching engine combining in-isolate L1 Memory LRU cache (<0.05ms) and global L2 Cloudflare Workers KV (5-15ms) backed by D1, featuring Stale-While-Revalidate (SWR) and non-blocking background revalidation via `ctx.waitUntil()`.

## Scope
- Implement `src/cache/types.ts`: `KVStore` cache contract and cache options.
- Implement `src/cache/stores/l1-memory.ts`: High-performance in-isolate LRU cache store with entry TTL and capacity limits.
- Implement `src/cache/stores/l2-kv.ts`: Cloudflare Workers KV cache store wrapping `KVNamespace`.
- Implement `src/cache/cache.ts`:
  - Tiered fallback lookup (L1 -> L2 -> Fetcher/D1).
  - Multi-tier write propagation (Write-Through / Write-Invalidate).
  - Stale-While-Revalidate (SWR) with `wrap(key, fn, { ttlMs, refreshThreshold })`.
  - Non-blocking execution using `ctx.waitUntil(revalidationPromise)`.
- Write unit tests verifying cache hits, SWR async refresh, and eviction.

## Allowed Files
- `src/cache/types.ts`
- `src/cache/cache.ts`
- `src/cache/stores/l1-memory.ts`
- `src/cache/stores/l2-kv.ts`
- `src/cache/swr.ts`
- `test/cache/cache.test.ts`
- `docs/AI/SESSION_STATE.md`
- `docs/AI/tasks/TASK-009.md`

## Dependencies
- TASK-006 (Core Table facade)

## Inputs and Outputs
- **Inputs**: Cache configuration with L1 capacity and optional L2 `KVNamespace`.
- **Outputs**: High-speed multi-tier cache module saving D1 row-read charges.

## Acceptance Criteria
- [x] L1 memory cache returns cached values in <0.05ms without hitting L2 or D1.
- [x] L1 miss falls back to L2 KV; L2 hit repopulates L1.
- [x] Stale-While-Revalidate returns cached value immediately and dispatches background refresh via `ctx.waitUntil()`.
- [x] Unit tests pass with complete coverage.

## Verification Commands
```bash
pnpm test test/cache/
pnpm typecheck
```

## Risks and Assumptions
- Cloudflare isolates can be recycled by the edge runtime at any time; L1 memory cannot be relied upon for durable persistence.

## Status
DONE

