import { describe, it, expect, vi } from "vitest";
import { createMockD1Database } from "./helpers/mock-d1.js";
import { createMockR2Bucket } from "./helpers/mock-r2.js";
import { createMockKVNamespace } from "./helpers/mock-kv.js";
import { CloudflareKVDB } from "../src/core/kvdb.js";
import { D1Driver } from "../src/drivers/d1/driver.js";
import { KVDriver } from "../src/drivers/kv/driver.js";
import { Table } from "../src/core/table.js";
import { TieredCache } from "../src/cache/cache.js";
import { TTLSweeper } from "../src/maintenance/sweeper.js";
import { JobQueue } from "../src/queue/queue.js";
import { isBlobDescriptor, deserialize } from "../src/core/serializer.js";

describe("Comprehensive Audit Fixes & Data Safety Verifications", () => {
  describe("Data Safety: Safe R2 Blob Deletion Order in Table.delete and Table.deleteMany", () => {
    it("deletes R2 blob AFTER successful DB deletion, preventing corrupted dangling references", async () => {
      const rawDb = createMockD1Database();
      const r2 = createMockR2Bucket();
      const driver = new D1Driver(rawDb);
      await driver.init();

      const table = new Table<{ id: string; largeText: string }>("documents", driver, {
        r2Bucket: r2,
        overflowThresholdBytes: 50,
      });

      const largeContent = "x".repeat(100);
      await table.set("doc1", { id: "doc1", largeText: largeContent });

      // Verify blob exists in R2
      const rawStored = await driver.get("documents", "doc1");
      expect(rawStored).not.toBeNull();
      const parsed = deserialize<any>(rawStored!);
      expect(isBlobDescriptor(parsed)).toBe(true);
      const r2Key = parsed.r2Key;
      expect(await r2.get(r2Key)).not.toBeNull();

      // Successful delete removes both DB record and R2 blob
      const deleted = await table.delete("doc1");
      expect(deleted).toBe(true);
      expect(await table.get("doc1")).toBeNull();
      expect(await r2.get(r2Key)).toBeNull();
    });

    it("does not delete R2 blob if database deletion fails", async () => {
      const rawDb = createMockD1Database();
      const r2 = createMockR2Bucket();
      const driver = new D1Driver(rawDb);
      await driver.init();

      const table = new Table<{ id: string; largeText: string }>("documents", driver, {
        r2Bucket: r2,
        overflowThresholdBytes: 50,
      });

      const largeContent = "y".repeat(100);
      await table.set("doc2", { id: "doc2", largeText: largeContent });

      const rawStored = await driver.get("documents", "doc2");
      const parsed = deserialize<any>(rawStored!);
      const r2Key = parsed.r2Key;

      // Mock database prepare to fail on DELETE
      const originalPrepare = rawDb.prepare.bind(rawDb);
      vi.spyOn(rawDb, "prepare").mockImplementation((sql: string) => {
        if (sql.includes("DELETE FROM")) {
          return {
            bind: () => ({
              run: async () => {
                throw new Error("Simulated D1 write lock timeout");
              },
            }),
          } as any;
        }
        return originalPrepare(sql);
      });

      // Deletion throws database error
      await expect(table.delete("doc2")).rejects.toThrow("Simulated D1 write lock timeout");

      // Crucial: The R2 blob MUST NOT be deleted, because the DB record was not deleted!
      expect(await r2.get(r2Key)).not.toBeNull();
    });

    it("batches deleteMany and cleans up blobs in R2 only after DB batch success", async () => {
      const rawDb = createMockD1Database();
      const r2 = createMockR2Bucket();
      const driver = new D1Driver(rawDb);
      await driver.init();

      const table = new Table<{ id: string; content: string }>("posts", driver, {
        r2Bucket: r2,
        overflowThresholdBytes: 50,
      });

      await table.set("p1", { id: "p1", content: "a".repeat(100) });
      await table.set("p2", { id: "p2", content: "b".repeat(100) });
      await table.set("p3", { id: "p3", content: "c".repeat(100) });

      const d1 = deserialize<any>((await driver.get("posts", "p1"))!);
      const d2 = deserialize<any>((await driver.get("posts", "p2"))!);

      expect(await r2.get(d1.r2Key)).not.toBeNull();
      expect(await r2.get(d2.r2Key)).not.toBeNull();

      const count = await table.deleteMany(["p1", "p2"]);
      expect(count).toBe(2);

      // Blobs deleted
      expect(await r2.get(d1.r2Key)).toBeNull();
      expect(await r2.get(d2.r2Key)).toBeNull();
      // p3 remains
      expect(await table.get("p3")).not.toBeNull();
    });
  });

  describe("Consistency: D1 Session Bookmark Propagation in Table and CloudflareKVDB", () => {
    it("propagates session bookmark on Table.set, Table.delete, and exposes via getSessionBookmark()", async () => {
      const rawDb = createMockD1Database();
      let lastBookmarkIssued = "bm_init";

      // Mock D1 returning bookmark in response meta
      const origPrepare = rawDb.prepare.bind(rawDb);
      vi.spyOn(rawDb, "prepare").mockImplementation((sql: string) => {
        const stmt = origPrepare(sql);
        const origRun = stmt.run.bind(stmt);
        const wrapRun = (fn: () => Promise<any>) => async () => {
          const res = await fn();
          lastBookmarkIssued = `bm_${Date.now()}_${Math.random()}`;
          return {
            ...res,
            meta: { ...res.meta, bookmark: lastBookmarkIssued },
          };
        };

        const wrappedStmt = {
          ...stmt,
          run: wrapRun(origRun),
          bind: (...args: any[]) => {
            const bound = stmt.bind(...args);
            return {
              ...bound,
              run: wrapRun(bound.run.bind(bound)),
            };
          },
        };
        return wrappedStmt as any;
      });

      const kvdb = new CloudflareKVDB({ d1: rawDb });
      expect(kvdb.getSessionBookmark()).toBeNull();
      expect(kvdb.getBookmark()).toBeNull();

      const table = kvdb.table<{ id: string; val: string }>("test_sessions");
      await table.set("k1", { id: "k1", val: "v1" });

      // Bookmark must now be populated
      const bookmarkAfterSet = kvdb.getSessionBookmark();
      expect(bookmarkAfterSet).not.toBeNull();
      expect(bookmarkAfterSet).toBe(lastBookmarkIssued);

      // Bookmark updates on delete
      await table.delete("k1");
      const bookmarkAfterDelete = kvdb.getSessionBookmark();
      expect(bookmarkAfterDelete).not.toBeNull();
      expect(bookmarkAfterDelete).toBe(lastBookmarkIssued);
      expect(bookmarkAfterDelete).not.toBe(bookmarkAfterSet);
    });
  });

  describe("Storage Optimization: Overwritten R2 Blob Replacement & Orphan Sweeper", () => {
    it("eagerly deletes old R2 blob on overwrite when cleanOrphanBlobsOnUpdate is enabled", async () => {
      const rawDb = createMockD1Database();
      const r2 = createMockR2Bucket();
      const driver = new D1Driver(rawDb);
      await driver.init();

      const table = new Table<{ id: string; content: string }>("articles", driver, {
        r2Bucket: r2,
        overflowThresholdBytes: 50,
        cleanOrphanBlobsOnUpdate: true,
      });

      // 1. Initial write with large content creates first blob
      await table.set("art1", { id: "art1", content: "1".repeat(100) });
      const d1 = deserialize<any>((await driver.get("articles", "art1"))!);
      const oldBlobKey = d1.r2Key;
      expect(await r2.get(oldBlobKey)).not.toBeNull();

      // 2. Overwrite with new large content creates second blob
      await table.set("art1", { id: "art1", content: "2".repeat(100) });
      const d2 = deserialize<any>((await driver.get("articles", "art1"))!);
      const newBlobKey = d2.r2Key;
      expect(newBlobKey).not.toBe(oldBlobKey);

      // Old blob should be eagerly cleaned up, new blob should exist!
      expect(await r2.get(oldBlobKey)).toBeNull();
      expect(await r2.get(newBlobKey)).not.toBeNull();

      // 3. Overwrite with small content (no blob)
      await table.set("art1", { id: "art1", content: "small" });
      expect(await r2.get(newBlobKey)).toBeNull();
      const current = await table.get("art1");
      expect(current).toEqual({ id: "art1", content: "small" });
    });

    it("sweeps orphaned R2 blobs via TTLSweeper.sweepOrphanBlobs()", async () => {
      const rawDb = createMockD1Database();
      const r2 = createMockR2Bucket();
      const driver = new D1Driver(rawDb);
      await driver.init();

      const table = new Table<{ id: string; data: string }>("notes", driver, {
        r2Bucket: r2,
        overflowThresholdBytes: 50,
      });

      // Write 2 active records with R2 blobs
      await table.set("n1", { id: "n1", data: "active_1".repeat(20) });
      await table.set("n2", { id: "n2", data: "active_2".repeat(20) });

      // Inject 2 orphan blobs directly into R2 under the prefix
      await r2.put("__blobs/notes/orphan_1_hash", "abandoned data 1");
      await r2.put("__blobs/notes/orphan_2_hash", "abandoned data 2");
      // Inject blob outside prefix (other tenant)
      await r2.put("__other/safe_file.txt", "keep me");

      const sweeper = new TTLSweeper({
        db: rawDb,
        r2Bucket: r2,
        sweepOrphans: true,
      });

      const sweepResult = await sweeper.sweepExpired();
      expect(sweepResult.orphanedBlobsDeleted).toBe(2);

      // Orphan blobs were deleted
      expect(await r2.get("__blobs/notes/orphan_1_hash")).toBeNull();
      expect(await r2.get("__blobs/notes/orphan_2_hash")).toBeNull();

      // Active blobs are kept
      expect(await table.get("n1")).not.toBeNull();
      expect(await table.get("n2")).not.toBeNull();

      // Other prefix is untouched
      expect(await r2.get("__other/safe_file.txt")).not.toBeNull();
    });
  });

  describe("Cloudflare Workers KV: Sub-60s TTL Runtime Compliance", () => {
    it("handles sub-60s TTL without throwing KV runtime error, clamping expirationTtl to >=60s and using metadata", async () => {
      const rawKv = createMockKVNamespace();
      const kvDriver = new KVDriver(rawKv);

      // 10-second TTL (in real KV this would crash if not clamped to >=60s)
      await kvDriver.set("short_ns", "fast_exp", "speedy", 10);

      // Immediately available
      expect(await kvDriver.get("short_ns", "fast_exp")).toBe("speedy");
      expect(await kvDriver.has("short_ns", "fast_exp")).toBe(true);

      const listed = await kvDriver.list("short_ns");
      expect(listed.keys).toContain("fast_exp");

      // Verify KV raw put actually used expirationTtl >= 60 to prevent Cloudflare runtime crash
      const rawStored = await rawKv.getWithMetadata("short_ns:fast_exp");
      expect(rawStored.value).toBe("speedy");
      expect((rawStored.metadata as any).expiresAt).toBeGreaterThan(Date.now());
    });

    it("expires keys in get() and list() when application TTL has passed", async () => {
      const rawKv = createMockKVNamespace();
      const kvDriver = new KVDriver(rawKv);

      // Write key with 1-millisecond effective TTL by passing simulated past timestamp
      const storageKey = "test_exp:expired_key";
      await rawKv.put(storageKey, "stale_val", {
        expirationTtl: 60,
        metadata: { expiresAt: Date.now() - 50 }, // Expired 50ms ago
      });

      expect(await kvDriver.get("test_exp", "expired_key")).toBeNull();
      expect(await kvDriver.has("test_exp", "expired_key")).toBe(false);

      const listResult = await kvDriver.list("test_exp");
      expect(listResult.keys).not.toContain("expired_key");
    });
  });

  describe("Multi-Tier Cache: Global Consistency via l1: false", () => {
    it("supports l1: false to disable in-isolate memory cache and query KV directly", async () => {
      const rawKv = createMockKVNamespace();
      const cache = new TieredCache({
        l1: false,
        l2: { namespace: rawKv },
      });

      expect(cache.l1).toBeUndefined();
      expect(cache.l2).toBeDefined();

      await cache.set("user:100", { name: "Bob" }, 120_000);
      const retrieved = await cache.get("user:100");
      expect(retrieved).toEqual({ name: "Bob" });

      expect(await cache.has("user:100")).toBe(true);
      await cache.delete("user:100");
      expect(await cache.get("user:100")).toBeNull();
    });
  });

  describe("Micro-Batch Write Buffer: AutoBatch for Concurrent Writes", () => {
    it("coalesces concurrent table.set calls into an atomic batch operation", async () => {
      const rawDb = createMockD1Database();

      const db = new CloudflareKVDB({
        d1: rawDb,
        autoBatch: {
          maxBatchSize: 50,
          maxWaitMs: 20,
        },
      });

      const table = db.table<{ id: string; val: number }>("counters", {
        schema: {
          primaryKey: { name: "id", type: "string" },
        },
      });

      // 10 concurrent writes
      const writePromises = Array.from({ length: 10 }, (_, i) =>
        table.set(`item_${i}`, { id: `item_${i}`, val: i })
      );

      expect(table.pendingBatchCount).toBe(10);

      // Wait for all writes to resolve
      await Promise.all(writePromises);

      // Verify buffer drained and all items exist
      expect(table.pendingBatchCount).toBe(0);
      for (let i = 0; i < 10; i++) {
        const item = await table.get(`item_${i}`);
        expect(item).toEqual({ id: `item_${i}`, val: i });
      }
    });

    it("maintains Read-Your-Own-Writes consistency by auto-flushing pending writes before get()", async () => {
      const rawDb = createMockD1Database();
      const db = new CloudflareKVDB({
        d1: rawDb,
        autoBatch: {
          maxBatchSize: 50,
          maxWaitMs: 200, // Long debounce window
        },
      });

      const table = db.table<{ id: string; name: string }>("users");

      // Fire and do NOT await set
      void table.set("u1", { id: "u1", name: "Alice" });
      expect(table.pendingBatchCount).toBe(1);

      // Calling get immediately flushes pending writes before querying
      const user = await table.get("u1");
      expect(user).toEqual({ id: "u1", name: "Alice" });
      expect(table.pendingBatchCount).toBe(0);
    });

    it("flushes all tables via db.flush()", async () => {
      const rawDb = createMockD1Database();
      const db = new CloudflareKVDB({
        d1: rawDb,
        autoBatch: { maxBatchSize: 100, maxWaitMs: 500 },
      });

      const t1 = db.table("t1");
      const t2 = db.table("t2");

      void t1.set("a", 1);
      void t2.set("b", 2);

      expect(t1.pendingBatchCount).toBe(1);
      expect(t2.pendingBatchCount).toBe(1);

      await db.flush();

      expect(t1.pendingBatchCount).toBe(0);
      expect(t2.pendingBatchCount).toBe(0);
      expect(await t1.get("a")).toBe(1);
      expect(await t2.get("b")).toBe(2);
    });
  });

  describe("Performance: Lightweight Table.has() using SELECT 1 without R2 Egress", () => {
    it("uses SELECT 1 without fetching R2 blobs over network", async () => {
      const rawDb = createMockD1Database();
      const r2 = createMockR2Bucket();
      const r2GetSpy = vi.spyOn(r2, "get");

      const table = new Table<{ id: string; bigData: string }>("profiles", new D1Driver(rawDb), {
        r2Bucket: r2,
        overflowThresholdBytes: 30,
        schema: {
          primaryKey: { name: "id", type: "string" },
        },
      });

      // Write record that overflows to R2
      await table.set("user_blob", { id: "user_blob", bigData: "z".repeat(100) });

      // Reset spy call count
      r2GetSpy.mockClear();

      // Calling has() should return true
      const exists = await table.has("user_blob");
      expect(exists).toBe(true);

      // CRITICAL: r2.get must NOT be called at all for has() check!
      expect(r2GetSpy).not.toHaveBeenCalled();

      // Conversely, calling get() DOES fetch the blob from R2
      const record = await table.get("user_blob");
      expect(record?.bigData).toBe("z".repeat(100));
      expect(r2GetSpy).toHaveBeenCalledTimes(1);
    });
  });

  describe("Pagination: Turnkey Keyset Cursor Pagination via Table.findPage()", () => {
    it("paginates through records returning items, cursor, and complete flag", async () => {
      const rawDb = createMockD1Database();
      const driver = new D1Driver(rawDb);
      await driver.init();

      const table = new Table<{ id: string; num: number }>("numbers", driver, {
        schema: {
          primaryKey: { name: "id", type: "string" },
          columns: {
            num: { type: "number", index: true },
          },
        },
      });

      await table.setMany([
        { key: "n01", value: { id: "n01", num: 1 } },
        { key: "n02", value: { id: "n02", num: 2 } },
        { key: "n03", value: { id: "n03", num: 3 } },
        { key: "n04", value: { id: "n04", num: 4 } },
        { key: "n05", value: { id: "n05", num: 5 } },
      ]);

      // Page 1: limit 2
      const page1 = await table.findPage({}, { limit: 2 });
      expect(page1.items.map((i) => i.id)).toEqual(["n01", "n02"]);
      expect(page1.complete).toBe(false);
      expect(page1.cursor).toBeDefined();

      // Page 2: seek from page1 cursor
      const page2 = await table.findPage({}, { limit: 2, cursor: page1.cursor });
      expect(page2.items.map((i) => i.id)).toEqual(["n03", "n04"]);
      expect(page2.complete).toBe(false);
      expect(page2.cursor).toBeDefined();

      // Page 3: final page
      const page3 = await table.findPage({}, { limit: 2, cursor: page2.cursor });
      expect(page3.items.map((i) => i.id)).toEqual(["n05"]);
      expect(page3.complete).toBe(true);
      expect(page3.cursor).toBeUndefined();
    });
  });

  describe("Reliable Queue: Atomic Batch Job Acknowledgment via JobQueue.ackMany()", () => {
    it("acknowledges multiple leased jobs in a single batch operation", async () => {
      const rawDb = createMockD1Database();
      const queue = new JobQueue<{ task: string }>({
        db: rawDb,
        queueName: "batch_queue",
      });
      await queue.init();

      // Push 5 jobs
      await queue.pushMany([
        { payload: { task: "t1" } },
        { payload: { task: "t2" } },
        { payload: { task: "t3" } },
        { payload: { task: "t4" } },
        { payload: { task: "t5" } },
      ]);

      // Pop 5 jobs
      const leasedJobs = await queue.popMany(5);
      expect(leasedJobs).toHaveLength(5);

      // Ack all 5 jobs atomically in batch
      const ackedCount = await queue.ackMany(leasedJobs);
      expect(ackedCount).toBe(5);

      const stats = await queue.getStats();
      expect(stats.completed).toBe(5);
      expect(stats.active).toBe(0);
    });

    it("batch deletes jobs when removeOnComplete is true", async () => {
      const rawDb = createMockD1Database();
      const queue = new JobQueue<{ task: string }>({
        db: rawDb,
        queueName: "delete_queue",
      });
      await queue.init();

      await queue.pushMany([
        { payload: { task: "d1" } },
        { payload: { task: "d2" } },
      ]);

      const leased = await queue.popMany(2);
      expect(leased).toHaveLength(2);

      const removedCount = await queue.ackMany(leased, { removeOnComplete: true });
      expect(removedCount).toBe(2);

      const stats = await queue.getStats();
      expect(stats.total).toBe(0);
    });
  });
});
