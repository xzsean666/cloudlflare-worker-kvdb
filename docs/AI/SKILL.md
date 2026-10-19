---
name: cloudflare-worker-kvdb
description: "专为 Cloudflare Serverless 生态 (Workers, Pages, Durable Objects) 打造的高性能、生产级边缘 KV/文档数据库 SDK。支持 D1 (SQLite), Workers KV, DO SqlStorage, Hyperdrive 与 R2 溢出存储，提供 Multi-Key 动态多键与 B-Tree 二级索引、Auto-Batch 微批写聚合、Keyset 游标分页、分层缓存与持久化 JobQueue。"
---

# cloudflare-worker-kvdb AI Skill & Integration Guide

> 本文档专供 AI 编程助手与开发者查阅，系统化涵盖 `cloudflare-worker-kvdb` 的架构定位、核心 API 签名、**Multi-Key 动态多键与物理索引**、**D1 单主写性能优化 (Auto-Batch 微批聚合)**、**Keyset 游标分页**、**分层缓存与装饰器**、**持久化 JobQueue**、实战代码用例以及边缘环境避坑指南。

---

## 1. 模块定位与核心能力

- **模块定位**：专为 Cloudflare Serverless 生态（Cloudflare Workers、Pages Functions、Durable Objects）量身定制的高性能、零 Node C++ 原生依赖的边缘 KV 与文档数据库 SDK。
- **源码根路径**：`/ssd0/git/cloudlflare-worker-kvdb`
- **托管账号**：`xzsean666`
- **Git 远程仓库**：`https://github.com/xzsean666/cloudlflare-worker-kvdb.git`
- **依赖声明规范 (Git + Commit Hash)**：
  业务项目接入时按规范使用 Git 配合目标 Commit Hash 锁定依赖。不写死单一旧 Hash，接入或更新时由 AI / 开发者通过 `git -C /ssd0/git/cloudlflare-worker-kvdb rev-parse HEAD` 获取当前最新 Commit Hash，或在开发阶段临时使用 `#main` 分支：
  ```json
  {
    "dependencies": {
      "cloudflare-worker-kvdb": "git+https://github.com/xzsean666/cloudlflare-worker-kvdb.git#fc67f85b86eccd0e48f3bf69b574037f02c28405"
    }
  }
  ```
  命令行安装与更新：
  ```bash
  pnpm add git+https://github.com/xzsean666/cloudlflare-worker-kvdb.git#fc67f85b86eccd0e48f3bf69b574037f02c28405
  ```

- **核心能力矩阵**：
  - **边缘原生多存储引擎抽象**：
    - **Cloudflare D1**：无服务器 SQLite，支持 B-Tree 物理二级索引与多列复合索引。
    - **Workers KV**：全球极低延迟、读多写少的轻量键值缓存。
    - **Durable Objects SQL** (`ctx.storage.sql`)：强一致性分布式 Actor 事务存储。
    - **Cloudflare Hyperdrive**：边缘加速连接池，直连远端 PostgreSQL / MySQL。
    - **Cloudflare R2**：大 Payload (>1MB) 自动透明溢出存储，彻底突破 D1 行记录大小限制。
  - **D1 单主写入性能优化 (Auto-Batch)**：
    - 将高频并发的离散 `set`/`delete` 自动汇聚为参数化 `db.batch()` 批量事务执行。
    - 内存级净增量写聚合（In-Memory Write Coalescing），同键写覆盖与键属性浅合并。
    - 严格单请求 100 SQL 参数上限切片（Parameter-Safe Chunker），规避 D1 参数超限报错。
  - **动态多键 (Multi-Key) 物理二级索引**：
    - 突破传统 KV 仅支持主键点查的瓶颈，将检索列映射为底层物理列并挂载 B-Tree 索引。
    - 原生 O(1) 极速二级点查（`table.getBy()`），亚毫秒级响应。
    - 运行时零停机动态加列（`addKey`）与动态加索引（`addIndex`）。
  - **Keyset 游标高效分页 (`findPage`)**：
    - 基于 `(created_at, id)` 游标向前 seek，彻底杜绝 SQLite 在大偏移量 `OFFSET` 下的全表性能滑坡。
  - **因果一致性 (Read-Your-Own-Writes / RYW)**：
    - 读操作自动穿透检查内存中尚未落盘的批次数据；
    - 集成 D1 Sessions API 与 Bookmark 机制，确保跨 Worker 实例读写一致。
  - **边缘分层缓存与装饰器**：
    - L1 内存 LRU (<0.05ms) + L2 Workers KV (5-15ms) 分层缓存；
    - 防击穿（SingleFlight 合并）、SWR（Stale-While-Revalidate）、`@Cacheable` / `@CacheClear` 装饰器。
  - **可靠任务队列 (`JobQueue` / `QueueWorker`)**：
    - 基于 D1 或 DO SQLite 的持久化队列，支持原子子查询 Lease 抢占、指数退避重试、心跳续约与死信队列 (DLQ)。
  - **自动垃圾回收与维护 (`TTLSweeper` / `createScheduledHandler`)**：
    - 自动发现过期数据并分批清扫，级联清理孤儿 R2 Blob。开箱即用的 Workers `scheduled` 定时任务处理器。

---

## 2. 导出清单与核心 API (API Reference)

### 2.1 顶级入口与核心类
```typescript
import {
  KVDB, CloudflareKVDB,    // 核心数据库门面类
  Table,                   // 数据表操作对象
  WriteBatcher,            // 微批写聚合引擎
  TieredCache,             // L1/L2 分层缓存系统
  MemoryCacheStore,        // L1 内存 LRU 缓存 Store
  KVCacheStore,            // L2 Workers KV 缓存 Store
  Cacheable, CacheClear,   // 缓存装饰器
  JobQueue, QueueWorker,   // 可靠持久化任务队列与执行器
  QueueReaper,             // 队列超时回收器
  TTLSweeper,              // TTL 过期与孤儿 Blob 清理器
  createScheduledHandler,  // Cloudflare Worker cron 定时任务处理器
  KVDBError, StorageError, // 核心错误类
} from "cloudflare-worker-kvdb";
```

### 2.2 `KVDB` 配置项 (`CloudflareKVDBOptions`)
```typescript
const db = new KVDB({
  d1?: D1Database,                        // Cloudflare D1 主数据库绑定
  kv?: KVNamespace,                       // Cloudflare Workers KV 绑定
  r2?: R2Bucket,                          // Cloudflare R2 Bucket 绑定 (用于 >1MB 大文件透明溢出)
  doSql?: SqlStorage,                     // Durable Objects SQL 存储 (ctx.storage.sql)
  hyperdrive?: Hyperdrive,                // Cloudflare Hyperdrive 绑定
  hyperdriveClient?: HyperdriveClient,    // Hyperdrive 客户端连接实例
  driver?: Driver,                        // 自定义 Driver 实例
  sessionBookmark?: string | null,        // 初始传入的 D1 Session Bookmark
  tableName?: string,                     // 默认底层主物理表名 (默认 "_cf_kvdb")
  ctx?: ExecutionContext,                 // Worker ExecutionContext (用于 ctx.waitUntil 异步刷盘)
  autoBatch?: boolean | AutoBatchConfig, // 全局微批写入配置 (默认 false，生产推荐开启)
});
```

- **`autoBatch` 配置项 (`AutoBatchConfig`)**：
  ```typescript
  interface AutoBatchConfig {
    maxBatchSize?: number;      // 批次最大条数，达到即刻触发 flush (默认 50)
    maxWaitMs?: number;         // 批次最长防抖等待毫秒数 (默认 50ms)
    batchTimeoutMs?: number;    // 单个批次的最长等待超时 (默认 200ms)
  }
  ```

### 2.3 `Table` 核心操作方法
```typescript
interface Table<V = unknown, Keys extends Record<string, unknown> = Record<string, unknown>> {
  // --- 写入与更新 ---
  set(key: string | number, value: V, options?: { keys?: Partial<Keys>; ttlSeconds?: number }): Promise<void>;
  set(entry: { key: string | number; value: V; keys?: Partial<Keys>; ttlSeconds?: number }): Promise<void>;
  setMany(entries: readonly TableSetEntry<V, Keys>[]): Promise<void>;
  update(key: string | number, updater: (current: V | null) => V, options?: UpdateOptions<Keys>): Promise<V>;

  // --- 读取与点查 ---
  get(key: string | number): Promise<V | null>;
  getRecord(key: string | number): Promise<PhysicalRecord<Keys, V> | null>;
  getBy(column: keyof Keys | string, value: unknown): Promise<(PhysicalRecord<Keys, V> & V) | null>; // 【核心】O(1) 物理二级键点查
  getMany(keys: readonly (string | number)[]): Promise<(V | null)[]>;
  getByPrefix(prefix?: string): Promise<Map<string, V>>;
  has(key: string | number): Promise<boolean>;

  // --- 删除与清理 ---
  delete(key: string | number): Promise<boolean>;
  deleteMany(keys: readonly (string | number)[]): Promise<number>;
  deleteByPrefix(prefix: string): Promise<number>;
  clear(): Promise<void>;

  // --- 查询与游标分页 ---
  find(query?: FindQuery | Record<string, unknown>): Promise<V[]>;
  findRecords(query?: FindQuery | Record<string, unknown>): Promise<PhysicalRecord<Keys, V>[]>;
  findPage(query?: FindQuery | Record<string, unknown>, options?: FindQuery): Promise<PageResult<V>>; // 【核心】Keyset 游标高效分页

  // --- 运行时 Schema 动态演进 ---
  addKey(columnName: string, definition: KeyDefinition): Promise<void>;
  addIndex(definition: TableIndexDefinition | MultiKeyIndexDefinition): Promise<void>;

  // --- 微批控制 ---
  flush(): Promise<void>; // 手动强制刷新本表内存微批缓冲区
}
```

---

## 3. 动态多键（Multi-Key）物理索引与 Schema 演进

> [!IMPORTANT]
> **边缘 SQLite / D1 下为什么必须使用 Multi-Key？**  
> 传统 KV 模式将整条记录序列化为单个 JSON 文本保存在单一列中。若业务频繁需要按非主键字段（如 `status`、`userId`、`hash`）进行查询，必须解析 JSON 或全表扫描，在无服务器单主写和计算资源受限的边缘环境下会导致严重的 CPU 飙升与吞吐雪崩。  
> **Multi-Key 机制将高频检索字段直接提升为真实的物理列（Physical Columns），并挂载原生物理 B-Tree 索引**，实现亚毫秒级 O(1) 二级点查与复合条件极速过滤！

### 3.1 `MultiKeySchema` 契约声明规范
```typescript
import { KVDB, type MultiKeySchema } from "cloudflare-worker-kvdb";

// 1. 定义数据 Value 与二级键 Keys 类型
interface TokenData {
  name: string;
  decimals: number;
  metadata?: Record<string, unknown>;
}

type TokenKeys = {
  symbol: string;
  chainId: string;
  status?: string;
  holderCount?: number;
};

// 2. 声明 Multi-Key 契约
const schema: MultiKeySchema<TokenKeys, "string"> = {
  // 可选：指定主键名与类型 (支持 "string" 或 "integer")，默认 { name: "key", type: "string" }
  primaryKey: { name: "address", type: "string" },

  // 可选：是否自动创建并维护 created_at 与 updated_at 毫秒时间戳列 (默认 true)
  timestamps: true,

  // 二级键物理列定义
  keys: {
    symbol: {
      type: "string",
      index: true, // 自动创建底层 B-Tree 物理索引
    },
    chainId: {
      type: "string",
      index: true,
    },
    status: {
      type: "string",
      nullable: true,
      default: "active",
      index: true,
    },
    holderCount: {
      type: "integer",
      default: 0,
    },
  },

  // 表级复合物理索引 (联合索引)
  indexes: [
    {
      name: "idx_chain_symbol",
      keys: ["chainId", "symbol"],
      unique: true, // 唯一约束
    },
    {
      name: "idx_chain_status_holders",
      keys: ["chainId", "status", "holderCount"],
    },
  ],
};

const tokens = db.table<TokenData, TokenKeys>("tokens", { schema });
```

### 3.2 支持的数据类型与属性
| 类型 (`type`) | 底层 SQLite (D1) 映射 | 说明 |
|---|---|---|
| `"string"` | `TEXT` | 字符串、哈希、状态、枚举 |
| `"integer"` | `INTEGER` | 整数、时间戳、自增计数器 |
| `"number"` | `REAL` | 浮点数、金额、比率 |
| `"boolean"` | `INTEGER` (0/1) | 布尔开关 |
| `"json"` | `TEXT` | 结构化子对象字段 |

- **列属性 (`KeyDefinition`)**：
  - `nullable?: boolean`：是否允许为 NULL（默认 `true`）。
  - `default?: unknown`：列默认值。
  - `index?: boolean | { name?: string; unique?: boolean }`：单列物理索引定义。

### 3.3 写入与 O(1) 二级点查
```typescript
// 写入携带物理二级键
await tokens.set("0x1111", { name: "Ethereum", decimals: 18 }, {
  keys: {
    symbol: "ETH",
    chainId: "ethereum",
    status: "active",
    holderCount: 5000000,
  },
});

// 【核心性能点查】通过物理二级索引执行 O(1) 查询：
// 底层生成原生 SQL: SELECT * FROM "tokens" WHERE "symbol" = ? LIMIT 1;
const eth = await tokens.getBy("symbol", "ETH");
console.log(eth?.key);            // "0x1111" (主键)
console.log(eth?.columns.chainId);// "ethereum" (物理二级列)
console.log(eth?.value);          // { name: "Ethereum", decimals: 18 } (主数据)
```

### 3.4 运行时零停机动态加键与索引演进
```typescript
// 1. 动态增加物理列 (带默认值和索引)
// ⚠️ 重要：对已有数据的表新增非空列必须指定 default
await tokens.addKey("isL2", {
  type: "boolean",
  default: false,
  index: true,
});

// 2. 动态创建复合索引
await tokens.addIndex({
  name: "idx_chain_l2",
  keys: ["chainId", "isL2"],
  unique: false,
});
```

---

## 4. D1 单主写入性能优化与 Auto-Batch 微批处理

### 4.1 核心痛点与 Auto-Batch 架构
Cloudflare D1 底层基于 SQLite 单主（Single-Leader）架构，所有写入请求必须排队进入 Leader 节点执行写入锁。当多个 Worker 请求并发执行单个 `table.set()` 时，高频离散的网络往返与锁竞争会导致极高的写入延迟甚至请求超时。

`cloudflare-worker-kvdb` 内置 `WriteBatcher` 微批处理引擎：
1. **聚合执行**：在可配置的短窗口（如 50ms / 50 条）内，将离散的写操作汇聚为一个原生的 D1 `db.batch()` 事务执行。
2. **内存净增量聚合 (Coalescing)**：若在同一个批次窗口内对同一主键进行多次写入，SDK 自动在内存中合并操作（保留最新 Value，浅合并二级键），仅向数据库下发一条最终结果。
3. **参数安全分块 (Parameter-Safe Chunker)**：Cloudflare D1 严格限制**单条查询绑定的 SQL 参数不超过 100 个**。SDK 会自动按照每条记录绑定的参数数量动态计算安全上限（默认每批 50 条以内），彻底杜绝超限崩溃。

```typescript
const table = db.table<EventLog>("events", {
  autoBatch: {
    maxBatchSize: 50,  // 达到 50 条立即刷新
    maxWaitMs: 50,     // 窗口等待 50ms
  },
  ctx: executionCtx,   // 必须传递 Worker 的 ExecutionContext
});

// 极速非阻塞写入，自动进入内存微批队列
await table.set("evt_1", { type: "click" });
await table.set("evt_2", { type: "purchase" });

// 严格保证读己之写 (Read-Your-Own-Writes):
// 若紧接着调用 table.get("evt_1")，SDK 自动先检测并合并内存缓冲区，确保数据绝对最新！
const event = await table.get("evt_1");
```

### 4.2 D1 Sessions API 跨请求因果一致性 (RYW Bookmark)
在多区域边缘部署下，为避免因 D1 只读副本同步延迟导致的用户“写完读不到”问题：
```typescript
// 1. 写入完成后提取 D1 会话书签 (Bookmark)
const bookmark = db.getSessionBookmark();

// 2. 将书签返回给客户端（如通过 Cookie 或 Header）并在后续请求中注入：
const sessionDb = new KVDB({
  d1: env.DB,
  sessionBookmark: request.headers.get("x-d1-bookmark"),
});
```

### 4.3 Cloudflare R2 大文件透明溢出存储
- **机制**：当单条数据的 JSON 序列化大小超过 1MB（或自定义 `overflowThresholdBytes`）时，SDK 自动将数据保存至关联的 Cloudflare R2 Bucket，并在 D1 物理表中仅保留包含 SHA-256 和路径的 Blob 指针。
- **读取透明**：`table.get()` 或 `table.find()` 会自动透明地从 R2 拉取并反序列化，上层业务代码无需关心底层存储细节。
- **防脏数据安全性**：
  - 写入流程：**先写 R2 -> 确认成功后再提交 D1 记录**（即使中断也不会产生指向无效数据的悬垂指针）。
  - 删除流程：**先提交 D1 删除 -> 确认成功后再异步清理 R2 对应对象**。

---

## 5. Keyset 游标分页与高级查询

> [!CAUTION]
> **切勿在边缘数据库中使用大 `OFFSET`！**  
> 在 SQLite / D1 中执行 `LIMIT 20 OFFSET 5000` 时，B-Tree 引擎必须逐条扫描并丢弃前 5000 条索引记录，耗时将呈线性恶化。生产环境必须采用 Keyset 游标分页！

### 5.1 Keyset 游标分页 (`findPage`)
`findPage` 基于复合游标 `(created_at, primary_key)` 执行 O(1) Seek：
```typescript
// 第一页查询
const page1 = await tokens.findPage(
  { chainId: "ethereum", status: "active" },
  { limit: 20, sort: [{ field: "created_at", direction: "desc" }] }
);

console.log("Page 1 items:", page1.items);
console.log("Next cursor:", page1.cursor);
console.log("Is complete:", page1.complete);

// 第二页查询：传入上一页返回的 cursor
if (!page1.complete && page1.cursor) {
  const page2 = await tokens.findPage(
    { chainId: "ethereum", status: "active" },
    { limit: 20, cursor: page1.cursor }
  );
  console.log("Page 2 items:", page2.items);
}
```

### 5.2 Mongo 风格查询操作符
`find` / `findRecords` 支持丰富的过滤语法：
```typescript
const activeEthTokens = await tokens.find({
  where: {
    chainId: "ethereum",
    holderCount: { $gte: 1000 },
    status: { $in: ["active", "verified"] },
  },
  sort: [{ path: "holderCount", direction: "desc" }],
  limit: 50,
});
```
- **支持的操作符**：
  - 比较：`$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$like`
  - 逻辑：`$and`, `$or`, `$nor`, `$not`
  - 元素：`$exists`

---

## 6. 边缘分层缓存与装饰器 (TieredCache & Decorators)

### 6.1 `TieredCache` 双层缓存架构
- **L1 In-Isolate Memory (<0.05ms)**：利用 Worker Isolate 内存的高速 LRU 缓存。
- **L2 Workers KV (5-15ms)**：跨边缘节点共享的低延迟分布式持久化键值缓存。
- **SWR (Stale-While-Revalidate)**：过期瞬间优先返回过期旧数据，并通过 `ctx.waitUntil()` 在后台静默异步刷新，用户端零阻塞。
- **SingleFlight 防击穿**：同一时刻对相同 Key 的高并发请求被聚合成单一底层请求，防止数据库被击穿。

```typescript
import { TieredCache } from "cloudflare-worker-kvdb";

const cache = new TieredCache({
  l1: { max: 1000, ttlMs: 60_000 }, // L1 内存 LRU
  l2: env.KV_CACHE,                  // L2 Workers KV
  ctx: executionCtx,
});

// 使用 wrap 防击穿读取
const user = await cache.wrap("user:1001", async () => {
  return await fetchUserFromDb("1001");
}, { ttlMs: 300_000, swrMs: 60_000 });
```

### 6.2 装饰器缓存 (`@Cacheable` / `@CacheClear`)
同时支持 TC39 标准装饰器与 TypeScript legacy experimentalDecorators：
```typescript
import { Cacheable, CacheClear, setDefaultCache } from "cloudflare-worker-kvdb";

setDefaultCache(cache);

class UserService {
  @Cacheable({
    ttlMs: 60_000,
    swrMs: 30_000,
    cacheKey: (id: string) => `user:${id}`,
  })
  async getUser(id: string) {
    return await db.table("users").get(id);
  }

  @CacheClear({
    cacheKey: (id: string) => `user:${id}`,
  })
  async updateUser(id: string, data: any) {
    return await db.table("users").set(id, data);
  }
}
```

---

## 7. 可靠任务队列与后台清理 (JobQueue & Maintenance)

### 7.1 `JobQueue` 与 `QueueWorker`
基于 D1 / Durable Objects SQLite 构建的高可靠 At-Least-Once 队列，适用于异步任务编排、邮件发送、数据上链等场景：
```typescript
import { JobQueue, QueueWorker } from "cloudflare-worker-kvdb";

// 1. 初始化队列实例
const emailQueue = new JobQueue<{ to: string; subject: string }>({
  db: env.DB,                     // D1 或 Durable Objects SqlStorage
  queueName: "emails",
  leaseSeconds: 30,               // 单次抢占锁租期 30s
  defaultMaxAttempts: 5,          // 最多重试 5 次后自动进入 DLQ
  baseBackoffMs: 1000,            // 基础退避时间 1s (指数退避)
});

// 首次启动时确保建表与索引
await emailQueue.init();

// 2. 生产者投递任务 (支持延迟投递与单任务指定重试次数)
await emailQueue.push({ to: "alice@example.com", subject: "Welcome!" }, {
  delaySeconds: 10,
  priority: 1,
});

// 3. 消费者处理 (可在 Worker fetch 或 Cron scheduled 中执行)
const worker = new QueueWorker(emailQueue, async (job) => {
  console.log(`Processing Job ${job.id}, attempt: ${job.attempts}`);
  await sendEmail(job.payload.to, job.payload.subject);
}, {
  concurrency: 5,
  autoHeartbeat: true, // 长任务自动在后台执行续租
});

// 消费并排空最多 20 条可用任务
const stats = await worker.drain(20);
console.log(`Processed: ${stats.processed}, Failed: ${stats.failed}`);
```

### 7.2 定时垃圾回收与清理 (`createScheduledHandler`)
在 `wrangler.toml` 中配置了 `[triggers] crons` 后，直接导出维护处理器：
```typescript
import { createScheduledHandler } from "cloudflare-worker-kvdb";

export default {
  fetch(req: Request, env: Env, ctx: ExecutionContext) {
    // 业务 HTTP 请求处理...
  },

  // 自动清理过期记录与孤儿 R2 Blobs
  scheduled: createScheduledHandler({
    db: env.DB,
    r2Bucket: env.R2_BLOBS,
    autoDiscoverTables: true, // 自动发现所有含 expires_at 物理列的表
    batchSize: 100,
    onSuccess: (res) => console.log(`Cleaned ${res.deletedRows} rows`),
  }),
};
```

---

## 8. 常见陷阱与最佳实践 (Pitfalls & Best Practices)

### 1. 【环境陷阱】严禁在 Worker 中引入 Node.js 原生 C++ 二进制扩展
- **错误姿势**：在代码中 `import Database from "better-sqlite3"` 或 `import { Client } from "pg-native"`。
- **正确姿势**：Cloudflare Workers 是基于 V8 的轻量级 `workerd` 隔离区。必须使用 Cloudflare 原生绑定 `env.DB`（D1）、`env.KV`、`ctx.storage.sql` 或通过 Hyperdrive 接入标准 Web/Postgres 协议。

### 2. 【并发陷阱】切忌在循环中执行无 Batch 的离散写
- **错误姿势**：
  ```typescript
  for (const item of items) {
    await table.set(item.id, item); // 100 次独立的 HTTP/Leader 写入往返，导致 D1 锁排队超时崩溃！
  }
  ```
- **正确姿势**：使用 `await table.setMany(items)` 或配置 `autoBatch: { maxBatchSize: 50, maxWaitMs: 50 }`。

### 3. 【生命周期陷阱】开启 `autoBatch` 时必须传递 `ctx` (`ExecutionContext`)
- **原因**：Worker 处理 HTTP 请求后，若未通过 `ctx.waitUntil()` 声明后台任务，边缘运行时会在 Response 返回瞬间冻结 Isolate，导致未落盘的内存微批被挂起或丢失。
- **正确姿势**：在创建 `KVDB` 或 `table` 时传入 `ctx: executionCtx`。

### 4. 【参数限制】规避 D1 100 Bound SQL Parameters 限制
- **说明**：Cloudflare D1 限制单次 SQL 执行中绑定的变量占位符（`?`）最多 100 个。
- **应对**：SDK 的 `setMany` 与批量操作已内置智能切片。开发者在使用 `$in` 过滤时，建议保持集合数量在合理范围，或分批查询。

### 5. 【Schema 演进】已有数据的表动态增加非空列未指定 `default`
- **错误姿势**：`await table.addKey("isVerified", { type: "boolean", nullable: false })`。底层执行 `ALTER TABLE ADD COLUMN NOT NULL` 会因为旧行缺少数据直接抛错。
- **正确姿势**：动态添加非空列时强制提供默认值：`await table.addKey("isVerified", { type: "boolean", nullable: false, default: false })`。

### 6. 【数据一致性】切勿跳过 SDK 手工删除 R2 Blob
- **原因**：大文件溢出由 SDK 的 D1 指针与 R2 键联动维护。手动删除 R2 对象会导致 D1 表中的对应记录在被读取时抛出 `BLOB_NOT_FOUND` 异常。删除操作请统一通过 `table.delete(key)` 或 `TTLSweeper` 进行联动清理。

---

## 9. 快速上手完整 Worker 用例 (Quickstart)

以下是一个涵盖 **初始化、Multi-Key 物理表声明、Auto-Batch 写入、O(1) 点查、游标分页以及 Cron 清理** 的生产级完整 Worker 示例：

```typescript
import { KVDB, type MultiKeySchema, createScheduledHandler } from "cloudflare-worker-kvdb";

export interface Env {
  DB: D1Database;
  CACHE_KV: KVNamespace;
  BLOB_BUCKET: R2Bucket;
}

// 1. 定义数据类型与二级键契约
interface OrderData {
  items: Array<{ sku: string; price: number; quantity: number }>;
  shippingAddress: string;
}

type OrderKeys = {
  userId: string;
  status: string;
  totalAmount: number;
};

const orderSchema: MultiKeySchema<OrderKeys, "string"> = {
  primaryKey: { name: "orderId", type: "string" },
  timestamps: true,
  keys: {
    userId: { type: "string", index: true },
    status: { type: "string", index: true },
    totalAmount: { type: "number", index: true },
  },
  indexes: [
    { name: "idx_user_status", keys: ["userId", "status"] },
  ],
};

export default {
  async fetch(req: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(req.url);

    // 2. 初始化 KVDB 实例
    const db = new KVDB({
      d1: env.DB,
      kv: env.CACHE_KV,
      r2: env.BLOB_BUCKET,
      ctx: ctx,
      autoBatch: { maxBatchSize: 20, maxWaitMs: 50 },
    });

    const orders = db.table<OrderData, OrderKeys>("orders", {
      schema: orderSchema,
    });

    // POST /api/orders: 创建订单 (利用 Auto-Batch 极速写入)
    if (req.method === "POST" && url.pathname === "/api/orders") {
      const body = await req.json<{ orderId: string; userId: string; amount: number; items: any[] }>();

      await orders.set(body.orderId, {
        items: body.items,
        shippingAddress: "Singapore",
      }, {
        keys: {
          userId: body.userId,
          status: "pending",
          totalAmount: body.amount,
        },
      });

      return Response.json({ success: true, orderId: body.orderId, bookmark: db.getSessionBookmark() });
    }

    // GET /api/orders/user: Keyset 游标分页拉取用户订单
    if (req.method === "GET" && url.pathname === "/api/orders/user") {
      const userId = url.searchParams.get("userId");
      const cursor = url.searchParams.get("cursor") ?? undefined;

      if (!userId) return new Response("Missing userId", { status: 400 });

      const page = await orders.findPage(
        { userId, status: "pending" },
        { limit: 10, cursor, sort: [{ field: "created_at", direction: "desc" }] }
      );

      return Response.json({
        orders: page.items,
        cursor: page.cursor,
        hasMore: !page.complete,
      });
    }

    // GET /api/orders/detail: O(1) 物理点查
    if (req.method === "GET" && url.pathname === "/api/orders/detail") {
      const orderId = url.searchParams.get("orderId");
      if (!orderId) return new Response("Missing orderId", { status: 400 });

      const record = await orders.getRecord(orderId);
      if (!record) return new Response("Order not found", { status: 404 });

      return Response.json({ order: record });
    }

    return new Response("Not Found", { status: 404 });
  },

  // 定时清理过期记录与孤儿 Blobs
  scheduled: createScheduledHandler({
    db: (env: Env) => env.DB,
    r2Bucket: (env: Env) => env.BLOB_BUCKET,
  }),
};
```

---

## 10. 本地维护与热修复指引 (Hotfix SOP)

如在业务项目（如 `/ssd0/git/web3-chat-worker`）中接入 `cloudflare-worker-kvdb` 并发现需要修复 Bug 或扩展新能力：

1. **定位并进入 SDK 源码目录**：
   ```bash
   cd /ssd0/git/cloudlflare-worker-kvdb
   ```
2. **确认切换至指定 GitHub 账号**：
   ```bash
   gh auth switch --user xzsean666
   ```
3. **修复代码并运行单元测试与构建**：
   ```bash
   pnpm test && pnpm build
   ```
4. **提交代码并推送到远端主分支**：
   ```bash
   BRANCH=$(git branch --show-current)
   git add .
   git commit -m "fix(cf-kvdb): 修复缺陷或功能说明"
   git push origin "$BRANCH"
   ```
5. **获取最新的 Commit Hash**：
   ```bash
   NEW_HASH=$(git rev-parse HEAD)
   echo "最新 Hash 为: $NEW_HASH"
   ```
6. **返回业务项目更新依赖**：
   ```bash
   cd /path/to/your-business-project
   pnpm add git+https://github.com/xzsean666/cloudlflare-worker-kvdb.git#$NEW_HASH
   ```
