import { describe, it, expect, beforeEach, vi } from "vitest";
import { createMockD1Database } from "./helpers/mock-d1.js";
import { createMockSqlStorage } from "./helpers/mock-do-sql.js";
import { createMockR2Bucket } from "./helpers/mock-r2.js";
import { Table } from "../src/core/table.js";
import { D1Driver } from "../src/drivers/d1/driver.js";
import { DurableObjectSqlDriver } from "../src/drivers/do-sql/driver.js";
import { R2BlobOverflowManager } from "../src/drivers/r2/overflow.js";
import { parseWhere } from "../src/query/parser.js";
import { compileWhere } from "../src/query/compiler.js";
import { TieredCache } from "../src/cache/cache.js";
import { Cacheable, setDefaultCache } from "../src/decorators/cacheable.js";
import { CacheClear } from "../src/decorators/cache-clear.js";
import { JobQueue } from "../src/queue/queue.js";
import { QueueReaper } from "../src/queue/reaper.js";
import { KVDBError, StorageError } from "../src/core/errors.js";

describe("Audit Optimizations & Security Verification", () => {
  describe("Security: SQL Injection Protection in Table.getBy", () => {
    it("rejects unindexed or unknown columns", async () => {
      const db = createMockD1Database();
      const driver = new D1Driver(db);
      await driver.init();

      const table = new Table<{ id: string; name: string }>("users", driver, {
        schema: {
          primaryKey: { name: "id", type: "string" },
          columns: {
            name: { type: "string", index: true },
          },
        },
      });

      // Valid indexed column succeeds
      await table.set("u1", { id: "u1", name: "Alice" });
      const found = await table.getBy("name", "Alice");
      expect(found).toEqual({ id: "u1", name: "Alice" });

      // Malicious or arbitrary column rejected
      await expect(table.getBy("1=1 OR 1=1; --", "val")).rejects.toThrow(KVDBError);
      await expect(table.getBy("unknown_col", "val")).rejects.toThrow("Unknown or unindexed column");
    });
  });

  describe("Security: R2 Overflow Scope & Arbitrary Object Protection", () => {
    it("rejects read or delete of blob descriptors outside table prefix", async () => {
      const bucket = createMockR2Bucket();
      await bucket.put("other_tenant/secret.json", "sensitive data");

      const manager = new R2BlobOverflowManager({
        bucket,
        thresholdBytes: 100,
        prefix: "__blobs/my_table",
      });

      const maliciousDescriptor = {
        __isBlob: true as const,
        r2Key: "other_tenant/secret.json",
        size: 100,
        createdAt: Date.now(),
      };

      // Attempt to read arbitrary bucket key outside prefix
      await expect(manager.readBlob(maliciousDescriptor)).rejects.toThrow(StorageError);
      await expect(manager.readBlob(maliciousDescriptor)).rejects.toThrow("Unauthorized blob key access");

      // Attempt to delete arbitrary bucket key outside prefix
      await expect(manager.deleteBlob(maliciousDescriptor)).rejects.toThrow(StorageError);
      await expect(manager.deleteBlob("other_tenant/secret.json")).rejects.toThrow("Unauthorized blob deletion");

      // File in other tenant must remain untouched
      const file = await bucket.get("other_tenant/secret.json");
      expect(file).not.toBeNull();
      expect(await file!.text()).toBe("sensitive data");
    });

    it("uses fast-path length checking in shouldOverflow", () => {
      const bucket = createMockR2Bucket();
      const manager = new R2BlobOverflowManager({
        bucket,
        thresholdBytes: 100,
        prefix: "__blobs/fast",
      });

      // Short ASCII string (length * 3 <= 100)
      expect(manager.shouldOverflow("small")).toBe(false);
      // Clearly long string (length > 100)
      expect(manager.shouldOverflow("a".repeat(150))).toBe(true);
    });
  });

  describe("Performance & Functionality: Schema Table Batching and List Scan", () => {
    it("batches setMany and deleteMany on Schema tables with D1", async () => {
      const rawDb = createMockD1Database();
      const driver = new D1Driver(rawDb);
      await driver.init();

      const table = new Table<{ id: string; email: string }>("accounts", driver, {
        schema: {
          primaryKey: { name: "id", type: "string" },
          columns: {
            email: { type: "string", index: true },
          },
        },
      });

      const items = Array.from({ length: 20 }, (_, i) => ({
        key: `acc_${i.toString().padStart(2, "0")}`,
        value: { id: `acc_${i.toString().padStart(2, "0")}`, email: `user${i}@example.com` },
      }));

      // 1. Batch insert
      await table.setMany(items);

      // 2. Batch getMany
      const fetched = await table.getMany(["acc_00", "acc_05", "acc_missing"]);
      expect(fetched).toHaveLength(3);
      expect(fetched[0]).toEqual(items[0]!.value);
      expect(fetched[1]).toEqual(items[5]!.value);
      expect(fetched[2]).toBeNull();

      // 3. Schema table list() queries physical table correctly (not generic KV)
      const listRes = await table.list({ limit: 10 });
      expect(listRes.keys).toHaveLength(10);
      expect(listRes.keys[0]).toBe("acc_00");

      // 4. getByPrefix on schema table
      const prefixMap = await table.getByPrefix("acc_0");
      expect(prefixMap.size).toBe(10);
      expect(prefixMap.get("acc_00")).toEqual(items[0]!.value);

      // 5. Batch deleteMany
      const deletedCount = await table.deleteMany(["acc_00", "acc_01"]);
      expect(deletedCount).toBe(2);
      expect(await table.get("acc_00")).toBeNull();
      expect(await table.get("acc_01")).toBeNull();
    });

    it("batches setMany and deleteMany on Schema tables with DO-SQL", async () => {
      const rawSql = createMockSqlStorage();
      const driver = new DurableObjectSqlDriver(rawSql);
      await driver.init();

      const table = new Table<{ id: string; score: number }>("players", driver, {
        schema: {
          primaryKey: { name: "id", type: "string" },
          columns: {
            score: { type: "number", index: true },
          },
        },
      });

      const items = [
        { key: "p1", value: { id: "p1", score: 100 } },
        { key: "p2", value: { id: "p2", score: 200 } },
      ];

      await table.setMany(items);
      const fetched = await table.getMany(["p1", "p2"]);
      expect(fetched).toEqual([items[0]!.value, items[1]!.value]);

      const listRes = await table.list();
      expect(listRes.keys).toEqual(["p1", "p2"]);

      const deleted = await table.deleteMany(["p1"]);
      expect(deleted).toBe(1);
      expect(await table.get("p1")).toBeNull();
      expect(await table.get("p2")).not.toBeNull();
    });
  });

  describe("Pillar 2: Query Compiler 100-Parameter Safety Chunking", () => {
    it("chunks large $in arrays (>80 items) into safe subclauses", () => {
      const items = Array.from({ length: 150 }, (_, i) => `id_${i}`);
      const ast = parseWhere({ id: { $in: items } }, new Set(["id"]));
      const { sql, params } = compileWhere(ast);

      // Total parameters equal 150
      expect(params).toHaveLength(150);
      // SQL is split into two OR subclauses each having <= 80 items
      expect(sql).toContain("OR");
      expect(sql).toContain("id IN");
    });
  });

  describe("Pillar 5: Cursor-Based Pagination in Table.find", () => {
    it("uses cursor seek predicates to paginate efficiently without offset", async () => {
      const rawDb = createMockD1Database();
      const driver = new D1Driver(rawDb);
      await driver.init();

      const table = new Table<{ id: string; points: number }>("leaderboard", driver, {
        schema: {
          primaryKey: { name: "id", type: "string" },
          columns: {
            points: { type: "number", index: true },
          },
        },
      });

      await table.setMany([
        { key: "p1", value: { id: "p1", points: 10 } },
        { key: "p2", value: { id: "p2", points: 20 } },
        { key: "p3", value: { id: "p3", points: 30 } },
      ]);

      // Seek from cursor where id > 'p1'
      const cursor = btoa(JSON.stringify({ pk: "p1" }));
      const results = await table.find({}, { cursor, limit: 10 });
      expect(results.map((r) => r.id)).toEqual(["p2", "p3"]);
    });
  });

  describe("Performance: SingleFlight in TieredCache.wrap", () => {
    it("coalesces concurrent cache misses to a single fetcher execution", async () => {
      const cache = new TieredCache();
      let fetchCount = 0;

      const fetcher = async () => {
        fetchCount++;
        // Simulate small delay
        await new Promise((resolve) => setTimeout(resolve, 30));
        return { data: "single_flight_result" };
      };

      // Fire 5 simultaneous requests for the exact same key
      const results = await Promise.all([
        cache.wrap("shared_key", fetcher),
        cache.wrap("shared_key", fetcher),
        cache.wrap("shared_key", fetcher),
        cache.wrap("shared_key", fetcher),
        cache.wrap("shared_key", fetcher),
      ]);

      // All 5 received the same result
      expect(results).toHaveLength(5);
      results.forEach((r) => expect(r).toEqual({ data: "single_flight_result" }));

      // Fetcher was called exactly ONCE thanks to SingleFlight!
      expect(fetchCount).toBe(1);
    });
  });

  describe("Reliability: QueueReaper DLQ Poison Pill Protection", () => {
    it("transitions expired active jobs with attempts >= max_attempts to failed state", async () => {
      const rawDb = createMockD1Database();
      const queue = new JobQueue<{ task: string }>({
        db: rawDb,
        queueName: "tasks",
        leaseSeconds: 1,
        defaultMaxAttempts: 2,
      });
      await queue.init();

      // Enqueue job with maxAttempts = 2
      const job = await queue.push({ task: "crash_task" }, { maxAttempts: 2 });
      expect(job.state).toBe("ready");

      // Attempt 1: Worker leases job (attempts becomes 1)
      const pop1 = await queue.pop(1);
      expect(pop1).not.toBeNull();
      expect(pop1!.attempts).toBe(1);

      // Simulate lease expiration without ack/nack (e.g. worker OOM crash)
      const reaper = new QueueReaper({
        adapter: queue.getAdapter(),
        tableName: queue.tableName,
      });

      // Advance time beyond lease
      await new Promise((resolve) => setTimeout(resolve, 1100));
      const reapedCount1 = await reaper.reap("tasks");
      expect(reapedCount1).toBe(1);

      // Attempt 1 expired: attempts was 1 < max_attempts (2), so reaper reset to 'ready'
      const reapedJob1 = await queue.getJob(job.id);
      expect(reapedJob1!.state).toBe("ready");

      // Attempt 2: Worker leases job again (attempts becomes 2)
      const pop2 = await queue.pop(1);
      expect(pop2).not.toBeNull();
      expect(pop2!.attempts).toBe(2);

      // Worker crashes again without ack/nack
      await new Promise((resolve) => setTimeout(resolve, 1100));
      const reapedCount2 = await reaper.reap("tasks");
      expect(reapedCount2).toBe(1);

      // Attempt 2 expired: attempts was 2 >= max_attempts (2).
      // Reaper MUST move to 'failed' (DLQ), NOT reset to 'ready'!
      const reapedJob2 = await queue.getJob(job.id);
      expect(reapedJob2!.state).toBe("failed");
      expect(reapedJob2!.lastError).toContain("max attempts exceeded");
    });
  });

  describe("Query Engine: $like Pattern Matching", () => {
    it("compiles and filters records matching $like patterns", async () => {
      const rawDb = createMockD1Database();
      const driver = new D1Driver(rawDb);
      const table = new Table<{ id: string; email: string; role: string }>("users_like", driver, {
        schema: {
          primaryKey: { name: "id", type: "string" },
          columns: {
            email: { type: "string", index: true },
            role: { type: "string" },
          },
        },
      });

      await table.setMany([
        { key: "u1", value: { id: "u1", email: "alice@example.com", role: "admin" } },
        { key: "u2", value: { id: "u2", email: "bob@other.org", role: "user" } },
        { key: "u3", value: { id: "u3", email: "carol@example.com", role: "user" } },
      ]);

      const exampleUsers = await table.find({ email: { $like: "%@example.com" } });
      expect(exampleUsers.map((u) => u.id).sort()).toEqual(["u1", "u3"]);

      const bobUser = await table.find({ email: { $like: "bob%" } });
      expect(bobUser.map((u) => u.id)).toEqual(["u2"]);
    });
  });

  describe("R2 Blob Batch Operations & Table Clear Lifecycle", () => {
    it("deletes multiple blobs in batch and cleans R2 bucket on table.clear()", async () => {
      const bucket = createMockR2Bucket();
      const db = createMockD1Database();
      const driver = new D1Driver(db);
      const table = new Table<string>("docs", driver, {
        r2Bucket: bucket,
        overflowThresholdBytes: 50,
      });

      const bigDoc1 = "A".repeat(200);
      const bigDoc2 = "B".repeat(200);
      const bigDoc3 = "C".repeat(200);

      await table.set("doc1", bigDoc1);
      await table.set("doc2", bigDoc2);
      await table.set("doc3", bigDoc3);

      // Verify blobs exist in bucket
      const listBefore = await bucket.list({ prefix: "__blobs/docs/" });
      expect(listBefore.objects.length).toBe(3);

      // deleteMany with batch blob cleanup
      await table.deleteMany(["doc1", "doc2"]);
      const listAfterDeleteMany = await bucket.list({ prefix: "__blobs/docs/" });
      expect(listAfterDeleteMany.objects.length).toBe(1);

      // table.clear() purges remaining blobs
      await table.clear();
      const listAfterClear = await bucket.list({ prefix: "__blobs/docs/" });
      expect(listAfterClear.objects.length).toBe(0);
    });
  });

  describe("JobQueue: Batch pushMany Performance", () => {
    it("enqueues multiple jobs in a single batched statement", async () => {
      const rawDb = createMockD1Database();
      const queue = new JobQueue<{ message: string }>({
        db: rawDb,
        queueName: "batch_queue",
      });
      await queue.init();

      const jobs = await queue.pushMany([
        { payload: { message: "msg1" } },
        { payload: { message: "msg2" } },
        { payload: { message: "msg3" } },
      ]);

      expect(jobs).toHaveLength(3);
      expect(jobs[0]!.queue).toBe("batch_queue");

      const stats = await queue.stats();
      expect(stats.ready).toBe(3);
      expect(stats.total).toBe(3);
    });
  });

  describe("Decorators: Legacy TypeScript experimentalDecorators Compatibility", () => {
    it("decorates methods using legacy (target, key, descriptor) signatures", async () => {
      const cache = new TieredCache();
      let execCount = 0;

      class LegacyService {
        async compute(x: number): Promise<number> {
          execCount++;
          return x * 2;
        }
      }

      // Simulate legacy decorator invocation:
      // @Cacheable({ cache }) compute(x)
      const descriptor: PropertyDescriptor = {
        value: LegacyService.prototype.compute,
        writable: true,
        enumerable: false,
        configurable: true,
      };

      const decoratedDescriptor = Cacheable({ cache })(
        LegacyService.prototype,
        "compute",
        descriptor
      );

      LegacyService.prototype.compute = decoratedDescriptor.value;
      const instance = new LegacyService();

      const r1 = await instance.compute(5);
      expect(r1).toBe(10);
      expect(execCount).toBe(1);

      const r2 = await instance.compute(5);
      expect(r2).toBe(10);
      expect(execCount).toBe(1); // Cached!
    });
  });
});
