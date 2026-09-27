# TASK-003: Core Foundations: Clock, Chunker, Key Encoder & Serializer

## Objective
Implement the fundamental core utilities that guarantee data consistency and runtime safety on Cloudflare: the monotonic clock (`getMonotonicNow`), the 100-parameter safety chunker, the key/prefix encoder, and the canonical JSON serializer.

## Scope
- Implement `src/core/clock.ts`: Monotonic clock preventing millisecond timestamp collisions within isolates.
- Implement `src/core/chunker.ts`: Parameter and statement safety chunker strictly limiting bound parameters to <= 100.
- Implement `src/core/key.ts`: Table prefix, namespace formatting, and key delimiter sanitization.
- Implement `src/core/serializer.ts`: Deterministic canonical JSON serialization and deserialization with blob descriptor detection.
- Implement `src/core/errors.ts`: Discriminated typed error hierarchy (`D1LimitError`, `SerializationError`, `KeyNotFoundError`).
- Write comprehensive unit tests for each utility.

## Allowed Files
- `src/core/clock.ts`
- `src/core/chunker.ts`
- `src/core/key.ts`
- `src/core/serializer.ts`
- `src/core/errors.ts`
- `test/core/clock.test.ts`
- `test/core/chunker.test.ts`
- `test/core/key.test.ts`
- `test/core/serializer.test.ts`
- `docs/AI/SESSION_STATE.md`
- `docs/AI/tasks/TASK-003.md`

## Dependencies
- TASK-002 (Scaffolding and Vitest test environment)

## Inputs and Outputs
- **Inputs**: Cloudflare D1's 100-parameter limit specifications and monotonic clock algorithm from `web3-chat-worker-legacy`.
- **Outputs**: Fully tested core foundation modules.

## Acceptance Criteria
- [x] Monotonic clock produces strictly increasing numbers even in a tight loop of 10,000 iterations.
- [x] Parameter chunker correctly splits an array of 500 items into chunks of at most 80 parameters.
- [x] Serializer consistently produces canonical JSON strings and handles roundtrips for all JSON primitive types.
- [x] All unit tests pass with 100% coverage on core utilities.

## Verification Commands
```bash
pnpm test test/core/
pnpm typecheck
```

## Risks and Assumptions
- Parameter chunking must handle variable numbers of columns per record (e.g. 5 columns -> max 20 rows per chunk).

## Status
DONE

