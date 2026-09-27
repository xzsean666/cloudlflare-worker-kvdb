# Project Goal: Cloudflare Worker KVDB (`cf-kvdb`)

## 1. Executive Summary

The goal of this project is to build a high-performance, developer-friendly, production-grade **KV Database SDK** designed specifically for the **Cloudflare Serverless Ecosystem** (Cloudflare Workers, Pages Functions, and Durable Objects).

Referencing the clean API abstraction of [`kvdb-sdk`](file:///ssd0/git/kvdb-nodejs) and the battle-tested D1 optimization patterns from [`web3-chat-worker-legacy`](file:///ssd0/git/web3-chat-worker-legacy), this SDK solves the major performance bottlenecks of Cloudflare databases (particularly D1 single-leader write lock serialization, 100-parameter query limits, row size limits, and edge consistency) while delivering an ultra-smooth developer experience.

---

## 2. In-Scope MVP & Production Features

### 2.1 Multi-Driver Cloudflare Storage Adapters
1. **Cloudflare D1 Driver (`d1`)**:
   - Primary relational / KV storage on edge SQLite.
   - Built on Cloudflare's `D1Database` binding.
   - Support for JSON query translation to SQLite `json_extract()` and `->>`.
   - Full support for schema tables, dynamic multi-keys, and native B-Tree indexes.
2. **Cloudflare Workers KV Driver (`kv`)**:
   - Ultra-low latency (<10-15ms) global edge read key-value store.
   - Built on `KVNamespace` binding.
   - Native TTL (`expirationTtl`, `expiration`), prefix listing, and pagination.
   - Operates as a standalone driver or as an L2 distributed cache layer.
3. **Cloudflare Durable Objects SQLite Driver (`durable-object-sql`)**:
   - Strong consistency, ACID transactions on `ctx.storage.sql`.
   - Zero network latency for stateful DO actors.
   - Ideal for coordination, counters, atomic leases, and high-frequency lock tables.
4. **Cloudflare Hyperdrive Driver (`hyperdrive`)**:
   - Edge connection pooling and query caching for external Postgres / MySQL databases via `env.HYPERDRIVE`.
5. **Cloudflare R2 Transparent Blob Overflow**:
   - Overcomes D1's 2MB row limit and Workers KV's 25MB limit.
   - Large values automatically offload payload bytes to R2 (`env.R2`), storing a compact reference pointer in D1/KV.

---

### 2.2 Deep Cloudflare D1 Performance Optimizations
1. **Atomic Batch Write Aggregation (`db.batch`)**:
   - Cloudflare D1 writes are handled by a single leader node; unbatched sequential writes cause lock queuing (`SQLITE_BUSY`) and severe latency penalties.
   - The SDK automatically packs multi-item operations (`setMany`, `deleteMany`, queue state transitions) into atomic `db.batch()` arrays, reducing roundtrips by 85%-90%.
2. **100-Bound-Parameter Guard (Strict Parameter Safety)**:
   - D1 enforces a hard limit of **100 bound parameters per SQL statement**.
   - Queries with `IN (?, ...)` or batch inserts are automatically split into deterministic chunks (e.g. 50-80 items per statement), preventing runtime crashes.
3. **Read-Your-Own-Writes (RYW) Sequential Consistency**:
   - Support for D1 Sessions API: `env.DB.withSession(bookmark)`.
   - Allows propagation of bookmarks in headers/context to guarantee clients read their own latest writes on edge read replicas without serving stale reads.
4. **Monotonic Clocks & Zero-Conflict Ordering**:
   - Edge isolates executing in parallel can produce identical timestamps.
   - Embedded monotonic clock (`getMonotonicNow()`) ensures strict chronological incrementing for tie-breaking.
5. **Cursor-Based Leak-Free Pagination**:
   - D1 bills by rows read; `OFFSET` queries scan and discard leading rows.
   - The SDK enforces deterministic composite cursor queries: `(created_at > ? OR (created_at = ? AND id > ?))` with composite indexes for O(1) page access.
6. **Multi-Tier Caching Architecture**:
   - **L1 Cache**: In-isolate memory LRU cache (zero roundtrip for repetitive reads within the isolate lifetime).
   - **L2 Cache**: Distributed Workers KV (global edge cache with configurable TTL and stale-while-revalidate).
   - **L3 Primary**: Persistent D1 / DO SQL database.
7. **Cloudflare Worker Lifecycle Integration (`ctx.waitUntil`)**:
   - Non-blocking execution: write-behind cache invalidation, metric collection, and cleanup routines can be deferred via `ctx.waitUntil()`, preserving sub-10ms HTTP response times.

---

### 2.3 Ergonomic Data & Query APIs
1. **Unified Table Interface**:
   - Simple CRUD: `get`, `set`, `delete`, `has`, `clear`.
   - Batch APIs: `getMany`, `setMany`, `deleteMany`.
   - Prefix scans: `getByPrefix`, `deleteByPrefix`.
2. **MongoDB-Style JSON Query Engine**:
   - Comparison: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`.
   - Logical: `$and`, `$or`, `$nor`, `$not`.
   - Element / Path: `$exists`, nested dot-paths (`"profile.age"`).
   - Lowered directly into SQLite `json_extract()` and indexable expressions.
3. **Physical Schema & Secondary Indexes**:
   - Custom primary keys, indexed secondary columns, composite B-Tree indexes.
   - Zero-downtime dynamic key additions (`table.addKey()`).
4. **Reliable Serverless Job Queue**:
   - Production-ready job queue engine built on D1 / DO SQLite.
   - Leases with visibility timeout, auto-heartbeat, exponential backoff, dead-letter queue (DLQ).
   - Optional native bridge to Cloudflare Queues (`env.QUEUE`).

---

## 3. Non-Goals

1. **Not a Heavyweight Generic ORM**:
   - No complex relational schema migrations, GraphQL generation, or polymorphic relations.
2. **No Node.js Native Binary Dependencies**:
   - Zero dependency on `better-sqlite3`, `pg-native`, `bcrypt` or other C++ binaries incompatible with Cloudflare's `workerd` engine.
3. **No External Broker Dependencies**:
   - Operates completely self-contained within Cloudflare bindings without requiring external Redis or RabbitMQ servers.

---

## 4. Success Criteria

1. **Clean Developer Experience**:
   - A single cohesive import `import { CloudflareKVDB } from "cloudflare-worker-kvdb"`.
   - Full TypeScript IntelliSense and type inference.
2. **Robustness & Production Readiness**:
   - Handles all edge cases: D1 parameter limits, write lock contention, stale read replicas, and payload size overflows.
   - 100% compliant with Cloudflare Workers runtime.
3. **Benchmark Verification**:
   - Automated unit and integration test suite runnable in Vitest with `@cloudflare/vitest-pool-workers` or Miniflare.
