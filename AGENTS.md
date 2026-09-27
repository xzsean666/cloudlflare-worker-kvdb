# AGENTS.md — Cloudflare Worker KVDB Agent Guidelines

This file serves as the fundamental rules and context for AI engineering agents working in this repository.

## 1. Project Context

`cloudflare-worker-kvdb` is a high-performance, developer-friendly, production-grade **KV Database SDK** designed specifically for the **Cloudflare Serverless Ecosystem** (Cloudflare Workers, Pages Functions, and Durable Objects).

It provides:
- A unified KV and document storage interface across Cloudflare storage engines:
  - **Cloudflare D1** (Serverless SQLite with edge read replicas)
  - **Cloudflare Workers KV** (Ultra-low latency global KV)
  - **Cloudflare Durable Objects Storage / SQLite** (`ctx.storage.sql`, strongly consistent transactional actor storage)
  - **Cloudflare Hyperdrive** (Accelerated edge connection pooling to Postgres/MySQL)
  - **Cloudflare R2** (Transparent blob overflow storage for large payloads)
- Advanced D1 performance optimizations:
  - Atomic batch write aggregation via `db.batch()` to overcome single-leader write lock contention.
  - Parameter-safe statement chunking (strictly abiding by D1's 100 bound parameters per query limit).
  - Read-Your-Own-Writes (RYW) sequential consistency via D1 Sessions API and bookmarks.
  - Monotonic millisecond clocks to guarantee total ordering and prevent tie conflicts.
  - Cursor-based pagination `(created_at, id)` eliminating slow `OFFSET` scans.
  - Multi-tier caching (L1 Worker isolate LRU -> L2 Workers KV -> L3 D1).
  - Physical Schema tables, multi-keys, and native B-Tree indexes.
  - Reliable Job Queue with leases, visibility timeouts, exponential backoff, and DLQ.

## 2. Fact Sources & Project Rules

All development work must adhere to the single source of truth:
- **Developer Instructions**: [`docs/AI_AGENT_PROMPT.md`](file:///ssd0/git/cloudlflare-worker-kvdb/docs/AI_AGENT_PROMPT.md)
- **Project Goal**: [`docs/AI/GOAL.md`](file:///ssd0/git/cloudlflare-worker-kvdb/docs/AI/GOAL.md)
- **Task Index**: [`docs/AI/TASK_INDEX.md`](file:///ssd0/git/cloudlflare-worker-kvdb/docs/AI/TASK_INDEX.md)
- **Session State**: [`docs/AI/SESSION_STATE.md`](file:///ssd0/git/cloudlflare-worker-kvdb/docs/AI/SESSION_STATE.md)
- **Current Tasks**: [`docs/AI/tasks/TASK-xxx.md`](file:///ssd0/git/cloudlflare-worker-kvdb/docs/AI/tasks/)
- **Architecture**: [`docs/AI/ARCHITECTURE.md`](file:///ssd0/git/cloudlflare-worker-kvdb/docs/AI/ARCHITECTURE.md)
- **Decisions Log**: [`docs/AI/DECISIONS.md`](file:///ssd0/git/cloudlflare-worker-kvdb/docs/AI/DECISIONS.md)

## 3. Strict Operating Principles

1. **One Task per Session**: Focus strictly on the assigned active task. Do not implement features out of scope.
2. **Never Break Working Directory Boundaries**: Repository root is `/ssd0/git/cloudlflare-worker-kvdb`.
3. **GitHub Account Routing**:
   - For `/ssd0/git/*`, GitHub user is `xzsean666`.
   - Never use `0xcube-666` in this repository.
4. **Cloudflare Runtime Compatibility**:
   - Code must run in the Cloudflare Workers / `workerd` environment.
   - Do NOT import Node.js native binary addons (like `better-sqlite3`). Use `@cloudflare/workers-types` and web standard APIs (`fetch`, `crypto`, `Streams`, `TextEncoder`).
5. **No Blind Claims**:
   - Never claim a test has passed without actually executing the verification command.
   - Record exact commands and outputs in session handover notes.
