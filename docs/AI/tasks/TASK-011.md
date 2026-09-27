# TASK-011: Transparent Cloudflare R2 Blob Overflow Engine (Breaking 2MB D1 Row Limit)

## Objective
Implement the transparent R2 Blob Overflow Storage Engine to bypass Cloudflare D1's 2MB row limit and prevent B-Tree node page bloat, allowing large JSON documents and media payloads to be stored and retrieved seamlessly via the standard Table interface.

## Scope
- Implement `src/drivers/r2/overflow.ts`:
  - R2 overflow manager accepting an `R2Bucket` binding and configurable threshold (e.g. 64 KB or 1 MB).
  - Inspection during write: if payload bytes exceed threshold, calculate SHA-256 hash, upload raw bytes to R2 (`__blobs/<prefix>/<hash>`), and replace payload with metadata pointer descriptor (`__cf_blob_overflow: true`).
  - Inspection during read: if descriptor is detected, fetch object from R2, deserialize, and return reconstituted payload.
  - Delete hook: cleans up referenced R2 object when record is deleted.
- Integrate overflow manager into `Table.set`, `Table.get`, and `Table.delete`.
- Write unit and integration tests using Miniflare R2 mock.

## Allowed Files
- `src/drivers/r2/overflow.ts`
- `src/core/serializer.ts`
- `src/core/table.ts`
- `test/drivers/r2-overflow.test.ts`
- `docs/AI/SESSION_STATE.md`
- `docs/AI/tasks/TASK-011.md`

## Dependencies
- TASK-006 (Core Table facade)

## Inputs and Outputs
- **Inputs**: Serialized payload exceeding threshold and `R2Bucket` binding.
- **Outputs**: Small metadata record in D1 with full blob payload in R2, transparent to caller.

## Acceptance Criteria
- [x] Payloads smaller than threshold (e.g. 10 KB) remain stored directly in D1/KV.
- [x] Payloads larger than threshold (e.g. 200 KB or 5 MB) are written to R2, leaving a compact descriptor in D1.
- [x] `table.get(key)` on an overflow record returns the complete original object seamlessly.
- [x] `table.delete(key)` removes both the D1 record and the corresponding R2 object.
- [x] Tests pass in Miniflare R2 test environment.

## Verification Commands
```bash
pnpm test test/drivers/r2-overflow.test.ts
pnpm typecheck
pnpm build
```

## Risks and Assumptions
- Concurrent deletes or shared deduplicated hashes must handle reference counts or safe object deletion.

## Status
DONE

