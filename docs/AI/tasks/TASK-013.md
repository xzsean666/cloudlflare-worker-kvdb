# TASK-013: Serverless Reliable Job Queue on D1 / DO SQLite with Leases, Backoff, and DLQ

## Objective
Implement a production-ready, serverless job queue engine built on top of physical schema tables in D1 or Durable Objects SQLite, featuring atomic lease locking (`lockToken`), visibility timeouts, exponential backoff retries, dead-letter queues (DLQ), and managed worker runners.

## Scope
- Implement `src/queue/types.ts`: `Job`, `JobOptions`, `QueueConfig`, `WorkerOptions`.
- Implement `src/queue/queue.ts`:
  - Queue client interface: `push`, `pushMany`, `pop`, `ack`, `nack`, `stats`.
  - Queue bootstrap: creates physical queue table with composite indexes `[state, available_at, priority]`.
  - Deduplication via `dedupKey`.
- Implement `src/queue/runner.ts`:
  - Managed worker loop with concurrency control.
  - Automatic heartbeat lease extension while processing long jobs.
  - Exponential backoff calculation and transition to `delayed` or `failed` (DLQ).
- Implement `src/queue/reaper.ts`:
  - Visibility timeout recovery: recovers orphaned active jobs whose workers crashed or timed out.
- Write unit and concurrency stress tests.

## Allowed Files
- `src/queue/types.ts`
- `src/queue/queue.ts`
- `src/queue/runner.ts`
- `src/queue/reaper.ts`
- `test/queue/queue.test.ts`
- `docs/AI/SESSION_STATE.md`
- `docs/AI/tasks/TASK-013.md`

## Dependencies
- TASK-008 (Schema Tables & Physical Indexes)

## Inputs and Outputs
- **Inputs**: Queue name and payload type.
- **Outputs**: Fully functional, Redis-free reliable job queue running inside Cloudflare Workers / DO.

## Acceptance Criteria
- [x] Enqueued jobs with `delayMs` or `dedupKey` are properly prioritized and deduplicated.
- [x] Worker runner pops ready jobs with atomic lock token, preventing race conditions across concurrent workers.
- [x] Failed jobs retry with exponential backoff; jobs exceeding `maxAttempts` transition to DLQ.
- [x] Orphaned jobs with expired visibility leases are automatically recovered.
- [x] Concurrency tests pass under load.

## Verification Commands
```bash
pnpm test test/queue/
pnpm typecheck
pnpm build
```

## Risks and Assumptions
- High write concurrency on D1 queues could hit write locks; Durable Objects SQLite is recommended for extremely high-frequency queues (>50 jobs/sec).

## Status
DONE

