# TASK-014: Scheduled TTL Sweeper & Vacuum Engine (Cron Garbage Collection & Blob Cleanup)

## Objective
Implement an automated garbage collection and TTL sweeper engine designed to run inside Cloudflare Workers Cron Triggers (`scheduled(event, env, ctx)`), purging expired rows from D1, deleting orphaned R2 overflow blobs, and preventing database size bloat.

## Scope
- Implement `src/maintenance/sweeper.ts`:
  - `db.sweepExpired(options)`: Scans all or specific tables for records where `expires_at <= now()`.
  - Cascading blob deletion: inspects expired rows for R2 overflow pointers and deletes the underlying R2 objects in batches.
  - Chunked deletion: splits large deletes into safe batches (<=100 records) to avoid exceeding execution time limits or write locks.
- Implement helper `createScheduledHandler(db)`: Ready-to-use Cloudflare Worker `scheduled` handler for `wrangler.jsonc` cron triggers.
- Write unit and integration tests verifying cleanup of expired records and associated R2 files.

## Allowed Files
- `src/maintenance/sweeper.ts`
- `src/maintenance/handler.ts`
- `test/maintenance/sweeper.test.ts`
- `docs/AI/SESSION_STATE.md`
- `docs/AI/tasks/TASK-014.md`

## Dependencies
- TASK-006 (Core Table facade)
- TASK-011 (R2 Blob Overflow Engine)

## Inputs and Outputs
- **Inputs**: Cloudflare Worker ScheduledController / cron event.
- **Outputs**: Cleaned database with expired records and orphaned R2 objects purged.

## Acceptance Criteria
- [x] `sweepExpired()` correctly purges records where `expires_at < now`.
- [x] Associated R2 blob objects are deleted alongside their expired D1 records.
- [x] Chunking prevents statement parameter overflow or execution timeout when purging thousands of expired records.
- [x] Tests pass in Vitest.

## Verification Commands
```bash
pnpm test test/maintenance/
pnpm typecheck
pnpm build
```

## Risks and Assumptions
- Cloudflare Workers Cron Triggers run once per minute minimum (`* * * * *`). Large backlogs must be deleted in bounded batches to stay well within Worker CPU time limits.

## Status
DONE

