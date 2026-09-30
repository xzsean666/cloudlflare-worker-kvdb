# AI Integration Skill & Agent Guidelines

> **Target Audience**: AI Coding Assistants, LLMs, and System Prompt Integrations  
> **Canonical Skill Path**: [`docs/skills/cloudflare-worker-kvdb/SKILL.md`](skills/cloudflare-worker-kvdb/SKILL.md)

This document provides a concise, high-density reference enabling AI agents to correctly and safely generate code using the `cloudflare-worker-kvdb` SDK.

---

## 0. Package Installation (GitHub Main & Commit Hash)

```bash
# 跟踪 main 最新分支
pnpm add github:xzsean666/cloudlflare-worker-kvdb#main

# 锁定特定 Commit Hash (生产环境推荐，保证不可变稳定构建)
pnpm add github:xzsean666/cloudlflare-worker-kvdb#<commit-hash>
# 例如: pnpm add github:xzsean666/cloudlflare-worker-kvdb#aeb845a
```

或者在 `package.json` 的 `dependencies` 中直接声明：
```json
{
  "dependencies": {
    "cloudflare-worker-kvdb": "github:xzsean666/cloudlflare-worker-kvdb#main"
  }
}
```

---

## 1. Quick Import & Setup Cheat-Sheet

```typescript
import { KVDB, type MultiKeySchema, type PhysicalRecord } from "cloudflare-worker-kvdb";

// Cloudflare Workers entrypoint
const db = new KVDB({
  d1: env.DB,                     // Cloudflare D1 (Primary SQLite Engine)
  kv: env.KV_CACHE,               // Optional Workers KV (L2 Edge Cache)
  r2: env.R2_BLOBS,               // Optional R2 Bucket (Transparent Overflow for >1MB values)
  executionCtx: ctx,              // Required for background write queue flushes
});
```

---

## 2. Table Creation Patterns

### Simple Key-Value / Document Table
```typescript
interface User {
  name: string;
  email: string;
}
const users = db.table<User>("users");

await users.set("u1", { name: "Alice", email: "alice@example.com" });
const user = await users.get("u1");
await users.delete("u1");
```

### Dynamic Multi-Key Table (Physical Columns + Native B-Tree Indexes)
```typescript
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

const blockSchema: MultiKeySchema<BlockKeys, "integer"> = {
  primaryKey: { name: "blockNumber", type: "integer" }, // Supports "integer" or "string"
  keys: {
    chainId: { type: "string", index: true },
    hash: { type: "string", index: { unique: true } },
    gasUsed: { type: "number" },
  },
  indexes: [
    { name: "chain_gas_idx", keys: ["chainId", "gasUsed"] }
  ]
};

const blocks = db.table<BlockData, BlockKeys>("blocks", {
  schema: blockSchema,
  autoBatch: { maxBatchSize: 50, maxWaitMs: 50 }, // Micro-batching write buffer
});

// Set record with secondary keys
await blocks.set(1001, { miner: "0xpoolA", txCount: 42 }, {
  keys: { chainId: "ethereum", hash: "0xabc1", gasUsed: 21000 }
});

// O(1) Point lookup by indexed secondary key
const byHash = await blocks.getBy("hash", "0xabc1");

// Retrieve full physical record with metadata columns
const fullRecord = await blocks.getRecord(1001);
// fullRecord => { key: 1001, columns: { chainId, hash, gasUsed }, value: { miner, txCount } }
```

---

## 3. Dynamic Schema Evolution

```typescript
// Add new physical column dynamically with default value and index
await blocks.addKey("status", { type: "string", default: "finalized", index: true });

// Add composite B-Tree index
await blocks.addIndex({ name: "chain_status_idx", keys: ["chainId", "status"] });

// Add composite unique index
await blocks.addIndex({ name: "uniq_chain_block", keys: ["chainId", "blockNumber"], unique: true });
```

---

## 4. Querying & Keyset Cursor Pagination

```typescript
// Mongo-style filtering & sorting on physical keys and/or JSON fields
const results = await blocks.find({
  where: { chainId: "ethereum", gasUsed: { $gte: 20000 } },
  sort: [{ path: "gasUsed", direction: "desc" }],
  limit: 20,
});

// Find full physical records
const records = await blocks.findRecords({
  where: { status: "pending" }
});

// High-Performance Keyset Cursor Pagination (No slow OFFSET scans)
const page1 = await blocks.findPage(
  { chainId: "ethereum" },
  { limit: 20, sort: [{ field: "created_at", direction: "desc" }] }
);

if (!page1.complete && page1.cursor) {
  const page2 = await blocks.findPage(
    { chainId: "ethereum" },
    { limit: 20, cursor: page1.cursor }
  );
}
```

---

## 5. D1 Performance Rules for AI Agents

1. **Mitigate Single-Leader Write Contention**:
   - Prefer `table.setMany([...])` or configure `autoBatch: { maxBatchSize: 50, maxWaitMs: 50 }`.
   - Never issue hundreds of individual `await table.set()` in a `for` loop without batching.
2. **Strict Statement Chunking**:
   - D1 allows max 100 parameters per query. `setMany()` automatically chunks into 50 statements.
3. **Read-Your-Own-Writes Sequential Consistency**:
   - Retrieve `const bookmark = db.getSessionBookmark()` after mutations.
   - Pass `sessionBookmark` to subsequent reads if strict sequential ordering is required across worker instances.
4. **Reliable Serverless Job Queue**:
   - Use `JobQueue` and `QueueWorker` for async background tasks with subquery atomic leases, exponential backoff, and DLQ.

---

For the full detailed documentation, refer to:
- [`docs/skills/cloudflare-worker-kvdb/SKILL.md`](skills/cloudflare-worker-kvdb/SKILL.md) — Canonical AI skill file with YAML frontmatter.
- [`docs/API_REFERENCE.md`](API_REFERENCE.md) — Comprehensive API reference.
- [`docs/AI/ARCHITECTURE.md`](AI/ARCHITECTURE.md) — Deep architectural specification.
