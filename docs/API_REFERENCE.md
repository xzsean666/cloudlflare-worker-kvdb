# Cloudflare Worker KVDB (`cloudflare-worker-kvdb`) — API Reference

`cloudflare-worker-kvdb` is a production-grade, zero-binary-dependency KV and document database SDK engineered specifically for Cloudflare Workers, Pages Functions, and Durable Objects.

---

## Table of Contents

1. [Entry Points & Client Initialization](#1-client-initialization)
2. [Table Operations & CRUD (`Table<T>`)](#2-table-operations--crud)
3. [Query Engine & AST Compiler (`find`)](#3-query-engine--ast-compiler)
4. [Physical Schemas & Secondary Indexes](#4-physical-schemas--secondary-indexes)
5. [Storage Drivers](#5-storage-drivers)
   - [D1 Driver (`D1Driver`)](#d1-driver)
   - [Workers KV Driver (`KVDriver`)](#workers-kv-driver)
   - [Durable Objects SQLite Driver (`DurableObjectSqlDriver`)](#durable-objects-sqlite-driver)
   - [Hyperdrive Postgres Driver (`HyperdriveDriver`)](#hyperdrive-postgres-driver)
6. [Transparent R2 Blob Overflow Engine](#6-transparent-r2-blob-overflow-engine)
7. [Multi-Tier Caching System (`TieredCache`)](#7-multi-tier-caching-system)
8. [Method Caching Decorators](#8-method-caching-decorators)
9. [Serverless Reliable Job Queue](#9-serverless-reliable-job-queue)
10. [Scheduled TTL Sweeper & Vacuum Engine](#10-scheduled-ttl-sweeper--vacuum-engine)
11. [Core Foundations & Utilities](#11-core-foundations--utilities)
12. [Error Hierarchy](#12-error-hierarchy)

---

## 1. Client Initialization

### `CloudflareKVDB`

The primary orchestrator and factory for accessing database tables.

```ts
import { CloudflareKVDB } from "cloudflare-worker-kvdb";

const kvdb = new CloudflareKVDB(options);
```

#### `KVDBConfig` Options

| Option | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `d1` | `D1Database` | Optional | Cloudflare D1 database binding. |
| `kv` | `KVNamespace` | Optional | Cloudflare Workers KV namespace binding. |
| `doSql` | `SqlStorage` | Optional | Cloudflare Durable Objects SQLite `ctx.storage.sql` binding. |
| `hyperdrive` | `HyperdriveClient` | Optional | Hyperdrive client or connection adapter. |
| `r2` | `R2Bucket` | Optional | Cloudflare R2 bucket binding for transparent large payload overflow. |
| `driver` | `StorageDriver` | Optional | Explicit custom driver instance. |
| `ctx` | `ExecutionContext` | Optional | Cloudflare execution context for non-blocking asynchronous operations (`ctx.waitUntil`). |
| `sessionBookmark`| `string` | Optional | D1 Sessions API bookmark string for Read-Your-Own-Writes (RYW) consistency. |
| `overflowThresholdBytes` | `number` | Optional | Global byte threshold before offloading value payloads to R2. Default: `65536` (64 KB). |

#### Instance Methods

##### `kvdb.table<T>(name: string, options?: TableOptions): Table<T>`
Returns a strongly-typed `Table<T>` facade configured for the chosen table.

##### `kvdb.getSessionBookmark(): string | undefined`
Returns the latest D1 Sessions API bookmark token acquired after write operations.

##### `kvdb.setSessionBookmark(bookmark?: string): void`
Manually updates the active D1 session bookmark.

##### `kvdb.getDriver(): StorageDriver`
Returns the active underlying storage driver.

##### `kvdb.getR2OverflowManager(): R2BlobOverflowManager | undefined`
Returns the active R2 overflow manager if an R2 bucket was provided.

---

## 2. Table Operations & CRUD (`Table<T>`)

Each `Table<T>` represents a logical or physical collection of documents.

### Constructor & Schema Options

```ts
const table = kvdb.table<User>("users", {
  schema: {
    tableName: "t_users", // Optional custom physical table name
    primaryKey: { name: "id", type: "string" },
    columns: {
      email: { type: "string", index: true },
      role: { type: "string", index: true },
      age: { type: "number" },
    },
  },
  overflowThresholdBytes: 64 * 1024,
});
```

### Table Methods

#### `init(): Promise<void>`
Initializes the table schema and secondary indexes in the database (executes `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS`).

```ts
await table.init();
```

#### `get(id: string, options?: ReadOptions): Promise<T | null>`
Retrieves a record by its primary key. Transparently reconstitutes R2 overflow blobs if applicable.

```ts
const user = await table.get("user_001");
```

#### `getMany(ids: string[], options?: ReadOptions): Promise<Array<T | null>>`
Retrieves multiple records concurrently. Employs 80-parameter chunking on SQLite backends.

```ts
const users = await table.getMany(["user_001", "user_002", "user_003"]);
```

#### `set(id: string, value: T, options?: WriteOptions): Promise<void>`
Inserts or updates a record.
- Automatically handles monotonic timestamp recording.
- Offloads payloads to R2 if size exceeds threshold.
- Populates physical schema columns from `value` fields if configured.

```ts
await table.set("user_001", {
  email: "alice@example.com",
  role: "admin",
  age: 30,
}, { ttlMs: 86400_000 }); // Optional TTL in milliseconds
```

#### `setMany(records: Array<{ id: string; value: T; options?: WriteOptions }>): Promise<void>`
Performs an atomic, parameter-safe batch write. On D1, bundles statements using `db.batch()` to prevent SQLite lock contention.

```ts
await table.setMany([
  { id: "u1", value: { email: "a@test.com", role: "user" } },
  { id: "u2", value: { email: "b@test.com", role: "vip" } },
]);
```

#### `delete(id: string): Promise<boolean>`
Deletes a record by primary key. If the record contained an R2 overflow blob, the associated R2 object is automatically deleted.

```ts
const wasDeleted = await table.delete("user_001");
```

#### `deleteMany(ids: string[]): Promise<number>`
Deletes multiple records and their associated R2 overflow blobs in parameter-safe batches. Returns total rows deleted.

```ts
const count = await table.deleteMany(["u1", "u2"]);
```

#### `has(id: string): Promise<boolean>`
Returns `true` if a non-expired record exists with the specified ID.

#### `count(filter?: QueryFilter): Promise<number>`
Returns the total count of non-expired records matching an optional filter.

```ts
const adminCount = await table.count({ role: "admin" });
```

---

## 3. Query Engine & AST Compiler (`find`)

`Table.find()` offers expressive, MongoDB-style document querying compiled directly into native, index-aware SQL with SQLite `json_extract()`.

```ts
const results = await table.find(
  {
    role: "admin",
    age: { $gte: 21, $lt: 65 },
    "settings.notifications": true,
    tags: { $in: ["security", "infra"] },
  },
  {
    limit: 25,
    sort: [{ field: "createdAt", order: "desc" }],
    after: lastCursor,
  }
);
```

### Supported Operators

| Operator | Description | SQLite Compilation Target |
| :--- | :--- | :--- |
| `{ field: value }` | Implicit equality | `col = ?` or `json_extract(value, '$.field') = ?` |
| `$eq` | Explicit equality | `= ?` |
| `$ne` | Not equal | `!= ?` |
| `$gt`, `$gte` | Greater than / or equal | `> ?` / `>= ?` |
| `$lt`, `$lte` | Less than / or equal | `< ?` / `<= ?` |
| `$in` | Value in array | `IN (?, ?, ...)` |
| `$nin` | Value not in array | `NOT IN (?, ?, ...)` |
| `$exists` | Key existence | `IS NOT NULL` / `IS NULL` |
| `$and` | Logical AND | `(...) AND (...)` |
| `$or` | Logical OR | `(...) OR (...)` |
| `$not` | Logical NOT | `NOT (...)` |

### Keyset Cursor Pagination (`after`)

Avoids slow `OFFSET` scans that burn Cloudflare D1 rows-read quotas:
- Cursors are encoded opaque strings containing `(sortValue, id)`.
- Keyset seeks jump directly via index `WHERE (col, id) > (?, ?)`.

---

## 4. Physical Schemas & Secondary Indexes

Physical schemas elevate JSON properties to real SQLite table columns with native B-Tree indexes for maximum read throughput.

```ts
const users = kvdb.table<UserProfile>("users", {
  schema: {
    tableName: "t_users",
    primaryKey: { name: "id" },
    columns: {
      username: { type: "string", index: true, unique: true },
      tenantId: { type: "string", index: true },
      score: { type: "number", index: true },
    },
  },
});

// Dynamic Schema Migration: Add secondary index on existing table
await users.addKey("status", { type: "string", index: true });

// Point lookup using secondary index
const user = await users.getBy("username", "alice");
```

---

## 5. Storage Drivers

### D1 Driver (`D1Driver`)
Optimized for Cloudflare D1 serverless SQLite.
- Bundles mutations into atomic `db.batch()` calls.
- Enforces 80-parameter chunking boundary (safely under D1's 100 limit).
- Reads and updates D1 session bookmarks for sequential consistency.

### Workers KV Driver (`KVDriver`)
High-performance global key-value store.
- Sub-15ms edge read latency.
- Native TTL expiration via `expirationTtl`.
- Native prefix listing via `kv.list({ prefix, cursor, limit })`.

### Durable Objects SQLite Driver (`DurableObjectSqlDriver`)
Strongly consistent actor storage via `ctx.storage.sql`.
- In-memory SQLite speed within Durable Object instances.
- Zero network serialization overhead.
- True ACID transaction safety.

### Hyperdrive Postgres Driver (`HyperdriveDriver`)
Accelerated connection pooling to remote PostgreSQL instances.
- Parameter placeholder rewriting (`?` to `$1, $2, ...`).
- Prepared query caching.

---

## 6. Transparent R2 Blob Overflow Engine

When documents exceed SQLite row limits (or a user-configured threshold), the `R2BlobOverflowManager` automatically offloads the payload to Cloudflare R2:

1. **Content-Addressed Hashing**: Calculates SHA-256 hash of payload: `blobs/{sha256}.json`.
2. **Deduplication**: Identical payloads share the same R2 object.
3. **Descriptor Storage**: Replaces row value in database with a lightweight `BlobDescriptor`:
   ```json
   {
     "__kvdb_blob": true,
     "r2Key": "blobs/3a8f...",
     "sizeBytes": 131072,
     "sha256": "3a8f...",
     "contentType": "application/json"
   }
   ```
4. **Transparent Reconstitution**: `table.get()` automatically fetches the R2 object and returns the original deserialized document.
5. **Cascading Deletion**: Deleting database records automatically deletes the associated R2 object.

---

## 7. Multi-Tier Caching System (`TieredCache`)

A 3-tier read-through cache topology:
- **L1**: In-isolate LRU memory (`MemoryCacheStore`, <0.05ms latency).
- **L2**: Cloudflare Workers KV (`KVCacheStore`, 5-15ms global latency).
- **L3**: Persistent database (D1 / DO SQL).

```ts
import { TieredCache } from "cloudflare-worker-kvdb";

const cache = new TieredCache({
  l1: { max: 1000, defaultTTLMs: 60_000 },
  l2: { namespace: env.CACHE_KV, defaultTTLSeconds: 3600 },
  ctx, // ExecutionContext enables non-blocking background SWR revalidation
});

// Read-Through with Stale-While-Revalidate (SWR)
const data = await cache.wrap(
  "cache:key",
  async () => fetchExpensiveData(),
  { ttlMs: 300_000, swrMs: 60_000 }
);
```

---

## 8. Method Caching Decorators

Stage 3 TC39 standard TypeScript method decorators for declarative caching.

```ts
import { Cacheable, CacheClear } from "cloudflare-worker-kvdb";

class ProductService {
  constructor(private cache: TieredCache) {}

  @Cacheable({
    prefix: "product",
    ttlMs: 300_000,
    swrMs: 60_000,
  })
  async getProduct(id: string) {
    return await db.table("products").get(id);
  }

  @CacheClear({
    prefix: "product",
    tags: ["products"],
  })
  async updateProduct(id: string, updates: Partial<Product>) {
    await db.table("products").set(id, updates);
  }
}
```

---

## 9. Serverless Reliable Job Queue

A high-performance background job queue designed for Cloudflare D1 or Durable Objects SQLite (`SqlStorage`).

### Features
- **Atomic Subquery Leases**: Concurrent workers never pop the same job.
- **Configurable Lease Duration**: Prevents jobs from being stuck if a worker isolate restarts.
- **Automatic Heartbeats**: `QueueWorker` extends active leases while processing long-running jobs.
- **Dead-Letter Queue (DLQ)**: Moves jobs to `failed` state after `maxAttempts` with exponential backoff.
- **Orphan Lease Reaper**: `QueueReaper` restores abandoned jobs to `ready`.

```ts
import { JobQueue, QueueWorker, QueueReaper } from "cloudflare-worker-kvdb";

// 1. Initialize Queue
const queue = new JobQueue<{ email: string; template: string }>({
  db: env.DB, // or ctx.storage.sql
  queueName: "emails",
  leaseSeconds: 30,
  defaultMaxAttempts: 3,
});
await queue.init();

// 2. Enqueue Job
await queue.push(
  { email: "user@example.com", template: "welcome" },
  { priority: 10, delayMs: 5000, dedupKey: "welcome:user@example.com" }
);

// 3. Process with Managed Worker
const worker = new QueueWorker(queue, async (job) => {
  await sendEmail(job.payload);
}, { concurrency: 5 });

await worker.processBatch();

// 4. Reap Expired Leases
const reaper = new QueueReaper({
  adapter: queue.getAdapter(),
  tableName: queue.tableName,
});
await reaper.reap();
```

---

## 10. Scheduled TTL Sweeper & Vacuum Engine

Provides automated background garbage collection and SQLite defragmentation for Cloudflare Worker cron triggers.

```ts
import { TTLSweeper, createScheduledHandler } from "cloudflare-worker-kvdb";

export default {
  // Cloudflare Cron Handler (e.g. triggers every 5 minutes)
  scheduled: createScheduledHandler({
    db: env.DB,
    r2Bucket: env.BLOBS,
    autoDiscoverTables: true,
    batchSize: 200,
    onSuccess: (result) => {
      console.log(`Cleaned ${result.expiredRowsDeleted} rows and ${result.blobsDeleted} blobs`);
    },
  }),
};
```

---

## 11. Core Foundations & Utilities

### Monotonic Clock (`getMonotonicNow`, `formatTimestampId`)
Guarantees strictly increasing millisecond timestamps even within high-frequency loops in the same isolate.

### Parameter-Safe Chunker (`chunkItems`, `chunkStatements`)
Safely splits batch arrays and statement lists into chunks strictly conforming to driver parameter limits (`MAX_D1_PARAMS = 80`).

### Canonical Serializer (`serialize`, `deserialize`)
Fast deterministic JSON serialization preserving ISO dates, BigInt, and blob references.

### Key Validator (`validateKey`)
Enforces Cloudflare KV and SQLite key naming restrictions (no null bytes, invalid path traversals, or oversized keys).

---

## 12. Error Hierarchy

All errors thrown by the SDK inherit from `KVDBError`:

```
KVDBError
 ├── ValidationError        (Invalid keys, schemas, or query syntax)
 ├── DriverError            (Underlying D1, KV, or DO storage failure)
 ├── SerializationError     (Malformed JSON or serialization failure)
 ├── OverflowError         (R2 offload or download error)
 └── QueueError             (Queue lock collision, invalid state transition)
```
