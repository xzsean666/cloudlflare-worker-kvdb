---
name: cloudflare-worker-kvdb
description: Complete developer and AI agent guide for integrating and using the cloudflare-worker-kvdb SDK (KVDB) in Cloudflare Workers, Pages Functions, and Durable Objects. Activates when building serverless databases, multi-key tables, high-throughput caching, background job queues, or D1 performance optimizations.
---

# Cloudflare Worker KVDB — AI Agent & Developer Integration Skill

This document is the authoritative, machine-readable skill guide for AI coding agents and human engineers integrating `cloudflare-worker-kvdb` (or `KVDB`) into Cloudflare serverless environments.

---

## 1. Architectural Mental Model

`cloudflare-worker-kvdb` is an edge-native, zero-native-binary KV and document database SDK designed specifically for the Cloudflare Serverless Ecosystem.

```
┌────────────────────────────────────────────────────────────────────────┐
│                   Unified KVDB Interface (Table<V, Keys>)               │
└────────┬──────────────────┬──────────────────┬─────────────────┬───────┘
         │                  │                  │                 │
         ▼                  ▼                  ▼                 ▼
 ┌───────────────┐  ┌───────────────┐  ┌───────────────┐ ┌───────────────┐
 │ Cloudflare D1 │  │ Workers KV    │  │ Durable Object│ │  Hyperdrive   │
 │ (Serverless   │  │ (Global Ultra-│  │ SQLite        │ │  (Postgres /  │
 │  SQLite BTree)│  │  Low Latency) │  │ ctx.storage   │ │   Edge Pool)  │
 └───────┬───────┘  └───────────────┘  └───────────────┘ └───────────────┘
         │
         ▼ (Transparent Overflow > 1MB)
 ┌───────────────┐
 │ Cloudflare R2 │
 └───────────────┘
```

### Critical Environmental Constraints for AI Agents
1. **Workerd / Cloudflare Edge Runtime**: Never import Node.js native C++ binary addons (such as `better-sqlite3`, `pg-native`). Use standard Web APIs (`fetch`, `crypto.subtle`, `Streams`, `TextEncoder`) and `@cloudflare/workers-types`.
2. **D1 Single-Leader Write Limits**: D1 serializes writes through a single leader. Always leverage `autoBatch` (micro-batching) or `table.setMany()` to group writes into atomic `db.batch()` calls instead of issuing hundreds of discrete `table.set()` calls.
3. **D1 100-Parameter Limit**: D1 queries cannot exceed 100 bound SQL parameters. The SDK automatically chunks statements, but AI agents should prefer keyset cursor pagination (`findPage`) over large `$in: [...]` sets.
4. **Primary Keys**: Supported types are `string` or `number` (validated finite integer).

---

## 2. Fast Initialization & Setup

### Package Installation
```bash
# 跟踪 main 最新分支
pnpm add github:xzsean666/cloudlflare-worker-kvdb#main

# 锁定特定 Commit Hash (生产环境推荐)
pnpm add github:xzsean666/cloudlflare-worker-kvdb#<commit-hash>
# 例如: pnpm add github:xzsean666/cloudlflare-worker-kvdb#aeb845a

# npm 替代:
# npm install github:xzsean666/cloudlflare-worker-kvdb#main
```

在 `package.json` 中配置：
```json
{
  "dependencies": {
    "cloudflare-worker-kvdb": "github:xzsean666/cloudlflare-worker-kvdb#main"
  }
}
```

### Initializing KVDB in Cloudflare Workers
```typescript
import { KVDB } from "cloudflare-worker-kvdb";

export interface Env {
  DB: D1Database;
  CACHE_KV: KVNamespace;
  BLOB_BUCKET: R2Bucket;
}

export default {
  async fetch(req: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const db = new KVDB({
      d1: env.DB,                     // Primary D1 Database
      kv: env.CACHE_KV,               // Optional L2 KV Cache
      r2: env.BLOB_BUCKET,            // Optional Transparent Blob Overflow
      executionCtx: ctx,              // Required for background async flushes
    });

    const users = db.table("users");
    // ...
  }
};
```

### Supported Storage Drivers
| Driver | Option | Typical Use Case |
| :--- | :--- | :--- |
| **Cloudflare D1** | `new KVDB({ d1: env.DB })` | Relational SQLite, B-Tree indexes, multi-key tables, queries |
| **Workers KV** | `new KVDB({ kv: env.KV })` | Low-latency global read-heavy caching, key-value point lookups |
| **Durable Objects SQL** | `new KVDB({ doSql: ctx.storage.sql })` | Strongly consistent transactional actor state with SQLite |
| **Hyperdrive** | `new KVDB({ hyperdrive: env.HYPERDRIVE })` | Accelerated edge connection pooling to Postgres |

---

## 3. Core Data Modeling: Simple vs. Multi-Key Tables

### A. Simple KV Table
Best for simple schema-less key-value or document storage.
```typescript
interface UserProfile {
  name: string;
  email: string;
  theme: "light" | "dark";
}

const table = db.table<UserProfile>("profiles");

// Write
await table.set("user_1001", { name: "Alice", email: "alice@example.com", theme: "dark" });

// Read
const user = await table.get("user_1001");

// Delete
await table.delete("user_1001");
```

---

### B. Dynamic Multi-Key Table (`MultiKeySchema`)
Best for high-performance indexing, secondary key point lookups, and SQL B-Tree acceleration.

```typescript
import { KVDB, type MultiKeySchema, type PhysicalRecord } from "cloudflare-worker-kvdb";

interface BlockData {
  miner: string;
  txCount: number;
}

type BlockKeys = {
  chainId: string;
  hash: string;
  gasUsed: number;
  status?: string;
};

// 1. Declare Schema: Custom Primary Key + Secondary Index Keys
const blockSchema: MultiKeySchema<BlockKeys, "integer"> = {
  primaryKey: { name: "blockNumber", type: "integer" },
  keys: {
    chainId: { type: "string", index: true },                   // Single B-Tree index
    hash: { type: "string", index: { unique: true } },          // Unique B-Tree index
    gasUsed: { type: "number" },
  },
  indexes: [
    // Optional composite index at creation time
    { name: "chain_gas_idx", keys: ["chainId", "gasUsed"] }
  ]
};

const blocks = db.table<BlockData, BlockKeys>("blocks", { schema: blockSchema });

// 2. Set with secondary keys
await blocks.set(1001, { miner: "0xpoolA", txCount: 42 }, {
  keys: {
    chainId: "ethereum",
    hash: "0xabc1",
    gasUsed: 21000,
  }
});

// 3. Fast O(1) Secondary Key Lookup
const byHash = await blocks.getBy("hash", "0xabc1");
console.log(byHash?.key);            // 1001 (primary key)
console.log(byHash?.columns.chainId);// "ethereum"
console.log(byHash?.value);          // { miner: "0xpoolA", txCount: 42 }

// 4. Retrieve Full Physical Record
const rec = await blocks.getRecord(1001);
// rec => { key: 1001, columns: { chainId: ..., hash: ... }, value: { ... } }
```

---

## 4. Dynamic Schema Evolution & Indexing

Multi-Key tables allow adding keys and indexes on the fly without database recreation:

```typescript
// 1. Dynamically add a column with default value and index
await blocks.addKey("status", { type: "string", default: "finalized", index: true });

// 2. Dynamically add a Composite B-Tree Index
await blocks.addIndex({
  name: "chain_status_idx",
  keys: ["chainId", "status"],
});

// 3. Dynamically add a Composite Unique Index
await blocks.addIndex({
  name: "uniq_chain_block",
  keys: ["chainId", "blockNumber"],
  unique: true,
});
```

> **AI Safety Note**: The SDK automatically flushes pending buffered writes before executing `addKey` or `addIndex` DDL statements to prevent lock contention.

---

## 5. Querying, Filtering & Pagination

### Mongo-Style Filter Queries (`find`, `findRecords`)
```typescript
// Filter against declared physical keys and/or JSON document fields
const results = await blocks.find({
  where: {
    chainId: "ethereum",
    gasUsed: { $gte: 20000 },
  },
  sort: [{ path: "gasUsed", direction: "desc" }],
  limit: 10,
});

// Or find full physical records (including metadata columns)
const records = await blocks.findRecords({
  where: { status: "pending" }
});
```

### Supported Query Operators
- **Comparison**: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$like`
- **Logical**: `$and`, `$or`, `$nor`, `$not`
- **Element**: `$exists`

### Production Keyset Cursor Pagination (`findPage`)
**CRITICAL**: Never use large `offset` queries in production serverless SQL. Always use cursor seek pagination:

```typescript
// Page 1
const page1 = await blocks.findPage(
  { chainId: "ethereum" },
  { limit: 20, sort: [{ field: "created_at", direction: "desc" }] }
);

console.log(page1.items);       // Array of items
console.log(page1.cursor);      // Opaque string token for next page
console.log(page1.complete);    // boolean

// Page 2
if (!page1.complete && page1.cursor) {
  const page2 = await blocks.findPage(
    { chainId: "ethereum" },
    { limit: 20, cursor: page1.cursor }
  );
}
```

---

## 6. High-Throughput Write Optimization (Pillars for D1)

### A. Automatic Micro-Batching (`autoBatch`)
When multiple worker requests concurrently perform `table.set()` or `table.delete()`, enabling `autoBatch` coalesces discrete writes into an atomic D1 `db.batch()`:

```typescript
const table = db.table<EventLog>("events", {
  autoBatch: {
    maxBatchSize: 50,  // Flush when buffer reaches 50 items
    maxWaitMs: 50,     // Or flush after 50ms window
  }
});

// Non-blocking writes into the micro-batch queue:
await table.set("evt_1", { type: "login" });
await table.set("evt_2", { type: "click" });

// Strict Read-Your-Own-Writes (RYW):
// If table.get("evt_1") is called, the buffer automatically flushes first!
```

### B. Explicit Batch Writes (`setMany`)
```typescript
// Safely chunked into 50-statement D1 batches under the hood:
await table.setMany([
  { key: "k1", value: { count: 1 } },
  { key: "k2", value: { count: 2 } },
]);
```

### C. Sequential Consistency (D1 Sessions API)
```typescript
// Retrieve the D1 session bookmark after mutations:
const bookmark = db.getSessionBookmark();

// In subsequent read requests, feed the bookmark back to guarantee read-after-write consistency:
const sessionDb = new KVDB({ d1: env.DB, sessionBookmark: bookmark });
```

---

## 7. Job Queue & Background Workflows

Built-in serverless job queue with subquery leases, exponential backoff, and Dead Letter Queue (DLQ):

```typescript
import { JobQueue, QueueWorker } from "cloudflare-worker-kvdb";

const queue = new JobQueue<{ email: string; template: string }>(env.DB, "email_queue", {
  visibilityTimeoutSeconds: 60,
  maxAttempts: 5,
});

// Producer: enqueue jobs
await queue.push({ email: "user@example.com", template: "welcome" });

// Batch Producer:
await queue.pushMany([
  { payload: { email: "a@test.com", template: "news" } },
  { payload: { email: "b@test.com", template: "news" } },
]);

// Consumer (inside Worker or Cron Trigger):
const worker = new QueueWorker(queue, async (job) => {
  await sendEmail(job.payload.email, job.payload.template);
});

// Process up to 20 jobs concurrently:
const result = await worker.drain(20);
console.log(`Processed: ${result.processed}, Failed: ${result.failed}`);
```

---

## 8. Anti-Patterns & Rules for AI Agents

| ❌ What NOT To Do | ✅ What To Do Instead | Rationale |
| :--- | :--- | :--- |
| `import Database from "better-sqlite3"` | Use `new KVDB({ d1: env.DB })` | Better-sqlite3 has C++ binaries; incompatible with Cloudflare Workers. |
| Issuing 100 individual `await table.set()` in a loop | Use `table.setMany([...])` or enable `autoBatch` | Prevents D1 write lock serialization bottlenecks. |
| Using `{ offset: 5000 }` for paging through rows | Use `table.findPage(where, { cursor, limit })` | `OFFSET` causes full table B-Tree scans; cursors perform O(1) seek. |
| Storing > 1MB JSON directly in standard D1 | Configure `new KVDB({ d1, r2: env.BUCKET })` | D1 has strict statement/row size limits; R2 overflow is transparent. |
| Manually writing raw SQL string concatenations | Use `table.find()`, `table.getBy()`, or query AST | Eliminates SQL injection vulnerabilities and ensures column whitelisting. |
| Manually deleting R2 blobs before database records | Use `table.delete(key)` | The SDK guarantees deferred deletion: blobs delete only after DB commits. |

---

## 9. Complete Production Worker Template

```typescript
import { KVDB, type MultiKeySchema } from "cloudflare-worker-kvdb";

export interface Env {
  DB: D1Database;
  CACHE: KVNamespace;
  BLOBS: R2Bucket;
}

interface Order {
  userId: string;
  total: number;
  status: "pending" | "paid" | "shipped";
}

type OrderKeys = {
  userId: string;
  status: string;
};

const orderSchema: MultiKeySchema<OrderKeys, "string"> = {
  primaryKey: { name: "orderId", type: "string" },
  keys: {
    userId: { type: "string", index: true },
    status: { type: "string", index: true },
  },
  indexes: [
    { name: "user_status_idx", keys: ["userId", "status"] },
  ],
};

export default {
  async fetch(req: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(req.url);
    const db = new KVDB({
      d1: env.DB,
      kv: env.CACHE,
      r2: env.BLOBS,
      executionCtx: ctx,
    });

    const orders = db.table<Order, OrderKeys>("orders", {
      schema: orderSchema,
      autoBatch: { maxBatchSize: 20, maxWaitMs: 50 },
    });

    // POST /orders
    if (req.method === "POST" && url.pathname === "/orders") {
      const body = await req.json<{ orderId: string; userId: string; total: number }>();
      await orders.set(body.orderId, { userId: body.userId, total: body.total, status: "pending" }, {
        keys: { userId: body.userId, status: "pending" },
      });
      return Response.json({ success: true, orderId: body.orderId });
    }

    // GET /orders?userId=...
    if (req.method === "GET" && url.pathname === "/orders") {
      const userId = url.searchParams.get("userId");
      if (!userId) return new Response("Missing userId", { status: 400 });

      const userOrders = await orders.find({
        where: { userId },
        sort: [{ path: "created_at", direction: "desc" }],
        limit: 50,
      });
      return Response.json({ orders: userOrders });
    }

    return new Response("Not Found", { status: 404 });
  }
};
```
