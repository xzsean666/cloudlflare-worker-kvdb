import { describe, it, expect, beforeEach } from "vitest";
import { createMockD1Database } from "../helpers/mock-d1.js";
import { createMockR2Bucket } from "../helpers/mock-r2.js";
import { CloudflareKVDB } from "../../src/core/kvdb.js";
import { WriteBatcher } from "../../src/core/batcher.js";
import { D1SessionManager } from "../../src/drivers/d1/sessions.js";
import { R2BlobOverflowManager } from "../../src/drivers/r2/overflow.js";
import { TTLSweeper } from "../../src/maintenance/sweeper.js";
import { parseWhere } from "../../src/query/parser.js";
import { compileWhere } from "../../src/query/compiler.js";
import { createBlobDescriptor } from "../../src/core/serializer.js";
import {
  validateNamespace,
  joinKeyPath,
} from "../../src/core/key.js";
import {
  KVDBError,
  KeyNotFoundError,
  StorageError,
} from "../../src/core/errors.js";

describe("Comprehensive Edge Cases & Stress Scenarios", () => {
  let rawDb: D1Database;
  let rawR2: R2Bucket;
  let kvdb: CloudflareKVDB;

  beforeEach(() => {
    rawDb = createMockD1Database();
    rawR2 = createMockR2Bucket();
    kvdb = new CloudflareKVDB({
      d1: rawDb,
      r2: rawR2,
    });
  });

  describe("Table.list() Cursor Pagination & WriteBatcher Flush Parity", () => {
    it("paginates schema table items using keyset cursor", async () => {
      const table = kvdb.table<{ name: string; age: number }>("users_pagination", {
        schema: {
          tableName: "t_users_pagination",
          primaryKey: { name: "id" },
          columns: {
            name: { type: "string" },
            age: { type: "number" },
          },
        },
      });
      await table.init();

      // Seed 15 records: user_01 to user_15
      for (let i = 1; i <= 15; i++) {
        const id = `user_${i.toString().padStart(2, "0")}`;
        await table.set(id, { name: `Name ${i}`, age: 20 + i });
      }

      // Page 1: limit 5
      const page1 = await table.list({ prefix: "user_", limit: 5 });
      expect(page1.keys.length).toBe(5);
      expect(page1.keys[0]).toBe("user_01");
      expect(page1.keys[4]).toBe("user_05");
      expect(page1.complete).toBe(false);
      expect(page1.cursor).toBe("user_05");

      // Page 2: with cursor from page 1
      const page2 = await table.list({ prefix: "user_", limit: 5, cursor: page1.cursor });
      expect(page2.keys.length).toBe(5);
      expect(page2.keys[0]).toBe("user_06");
      expect(page2.keys[4]).toBe("user_10");
      expect(page2.complete).toBe(false);
      expect(page2.cursor).toBe("user_10");

      // Page 3: with cursor from page 2
      const page3 = await table.list({ prefix: "user_", limit: 5, cursor: page2.cursor });
      expect(page3.keys.length).toBe(5);
      expect(page3.keys[0]).toBe("user_11");
      expect(page3.keys[4]).toBe("user_15");
      expect(page3.complete).toBe(true);
    });

    it("automatically flushes pending WriteBatcher buffer before executing table.list()", async () => {
      const table = kvdb.table<{ title: string }>("batch_list_test", {
        schema: {
          tableName: "t_batch_list_test",
          primaryKey: { name: "id" },
        },
        autoBatch: {
          maxBatchSize: 10,
          maxWaitMs: 5000,
        },
      });
      await table.init();

      // Write items without waiting for batch timeout
      void table.set("item_1", { title: "One" });
      void table.set("item_2", { title: "Two" });

      // list() should automatically flush pending writes
      const listRes = await table.list();
      expect(listRes.keys).toContain("item_1");
      expect(listRes.keys).toContain("item_2");
    });
  });

  describe("WriteBatcher Lifecycle, Properties & Cancellation", () => {
    it("reports pendingCount and cancels buffered writes on batcher.clear()", async () => {
      const batchCommit = async () => {};
      const batcher = new WriteBatcher<{ val: string }>(batchCommit, {
        maxBatchSize: 10,
        maxWaitMs: 5000,
      });

      const p1 = batcher.enqueue({ key: "k1", value: { val: "v1" } });
      const p2 = batcher.enqueue({ key: "k2", value: { val: "v2" } });

      expect(batcher.pendingCount).toBe(2);
      expect(batcher.isFlushingNow).toBe(false);

      const cancelError = new Error("Write queue aborted");
      batcher.clear(cancelError);

      expect(batcher.pendingCount).toBe(0);
      await expect(p1).rejects.toThrow("Write queue aborted");
      await expect(p2).rejects.toThrow("Write queue aborted");
    });
  });

  describe("TTLSweeper Maintenance & Vacuum Edge Cases", () => {
    it("executes vacuum() without error", async () => {
      const sweeper = new TTLSweeper({ db: rawDb });
      await expect(sweeper.vacuum()).resolves.toBeUndefined();
    });

    it("sweeps tables without rowid (custom primary keys)", async () => {
      // Create a WITHOUT ROWID table with custom PK
      await rawDb.exec(`
        CREATE TABLE custom_pk_table (
          item_id TEXT PRIMARY KEY,
          expires_at INTEGER,
          data TEXT
        ) WITHOUT ROWID;
      `);

      const past = Date.now() - 5000;
      await rawDb.prepare(
        "INSERT INTO custom_pk_table (item_id, expires_at, data) VALUES (?, ?, ?);"
      ).bind("exp-1", past, "val-1").run();

      const sweeper = new TTLSweeper({
        db: rawDb,
        tableNames: ["custom_pk_table"],
      });

      const res = await sweeper.sweepExpired();
      expect(res.expiredRowsDeleted).toBe(1);

      // Verify row is deleted
      const check = await rawDb.prepare("SELECT * FROM custom_pk_table;").all();
      expect(check.results.length).toBe(0);
    });
  });

  describe("R2BlobOverflowManager Security & Boundary Tests", () => {
    let overflow: R2BlobOverflowManager;

    beforeEach(() => {
      overflow = new R2BlobOverflowManager({
        bucket: rawR2,
        prefix: "secure_table",
        thresholdBytes: 100,
      });
    });

    it("rejects unauthorized readBlobStream outside prefix", async () => {
      await expect(
        overflow.readBlobStream(createBlobDescriptor("other_table/blobs/abc", 200))
      ).rejects.toThrow(StorageError);
    });

    it("throws R2_BLOB_NOT_FOUND when blob is missing on readBlobStream", async () => {
      await expect(
        overflow.readBlobStream(createBlobDescriptor("secure_table/blobs/missing_hash", 200))
      ).rejects.toThrow("Blob object 'secure_table/blobs/missing_hash' not found in R2 bucket");
    });

    it("rejects unauthorized deleteBlob outside prefix", async () => {
      await expect(overflow.deleteBlob("unauthorized_prefix/blobs/item")).rejects.toThrow(
        StorageError
      );
    });

    it("deletes multiple blobs safely and enforces prefix check on each", async () => {
      const descriptor1 = await overflow.writeBlob("blob data 1");
      const descriptor2 = await overflow.writeBlob("blob data 2");

      // Valid batch delete
      await expect(
        overflow.deleteBlobs([descriptor1.r2Key, descriptor2.r2Key])
      ).resolves.toBeUndefined();

      // Empty deleteBlobs
      await expect(overflow.deleteBlobs([])).resolves.toBeUndefined();

      // Batch delete with one invalid prefix throws
      await expect(
        overflow.deleteBlobs([descriptor1.r2Key, "hacked/blob"])
      ).rejects.toThrow("Unauthorized blob deletion");
    });
  });

  describe("Query Parser & AST Compiler Edge Cases", () => {
    it("handles empty object filter as $eq {}", () => {
      const ast = parseWhere({ meta: {} });
      expect(ast).toEqual({
        kind: "cmp",
        op: "$eq",
        path: { source: "meta", sourceKind: "value", segments: [{ key: "meta" }] },
        value: {},
      });
    });

    it("throws on unknown query operator", () => {
      expect(() => parseWhere({ score: { $unknownOp: 50 } })).toThrow(KVDBError);
      expect(() => parseWhere({ score: { $unknownOp: 50 } })).toThrow(
        'Unknown operator "$unknownOp" for field "score"'
      );
    });

    it("throws when $like operator is given non-string value", () => {
      const ast = parseWhere({ title: { $like: 12345 } });
      expect(() => compileWhere(ast)).toThrow(
        'Operator $like requires a string pattern at "title"'
      );
    });

    it("compiles $eq with null to IS NULL", () => {
      const ast = parseWhere({ deletedAt: null });
      const compiled = compileWhere(ast);
      expect(compiled.sql).toContain("IS NULL");
      expect(compiled.params.length).toBe(0);
    });
  });

  describe("D1SessionManager clearBookmark & Key Formatting", () => {
    it("clears active session bookmark", () => {
      const manager = new D1SessionManager("initial-bookmark-123");
      expect(manager.getBookmark()).toBe("initial-bookmark-123");

      manager.clearBookmark();
      expect(manager.getBookmark()).toBeNull();
    });

    it("validates namespace and throws when delimiter is present", () => {
      expect(() => validateNamespace("bad:namespace", ":")).toThrow(KVDBError);
      expect(() => validateNamespace("bad:namespace", ":")).toThrow(
        'Namespace "bad:namespace" must not contain delimiter ":"'
      );
    });

    it("validates joinKeyPath and throws on empty parts or empty string components", () => {
      expect(() => joinKeyPath([])).toThrow("Key parts cannot be empty");
      expect(() => joinKeyPath(["valid", ""])).toThrow("All key parts must be non-empty strings");
    });

    it("formats KeyNotFoundError message correctly with and without namespace", () => {
      const errWithNs = new KeyNotFoundError("my-key", "my-namespace");
      expect(errWithNs.message).toBe("Key 'my-key' not found in namespace 'my-namespace'");

      const errWithoutNs = new KeyNotFoundError("my-key");
      expect(errWithoutNs.message).toBe("Key 'my-key' not found");
    });
  });

  describe("High Concurrency Stress Test", () => {
    it("handles 50 concurrent writes and reads with autoBatching enabled", async () => {
      const table = kvdb.table<{ orderId: string; amount: number; status: string }>(
        "stress_orders",
        {
          schema: {
            tableName: "t_stress_orders",
            primaryKey: { name: "orderId" },
            columns: {
              status: { type: "string", index: true },
              amount: { type: "number" },
            },
          },
          autoBatch: {
            maxBatchSize: 10,
            maxWaitMs: 50,
          },
        }
      );
      await table.init();

      // Launch 50 concurrent writes
      const writePromises = Array.from({ length: 50 }, (_, i) => {
        const id = `order_${i + 1}`;
        const status = i % 2 === 0 ? "completed" : "pending";
        return table.set(id, { orderId: id, amount: (i + 1) * 10, status });
      });

      await Promise.all(writePromises);

      // Verify all 50 items are written
      const readPromises = Array.from({ length: 50 }, async (_, i) => {
        const id = `order_${i + 1}`;
        const record = await table.get(id);
        expect(record).not.toBeNull();
        expect(record!.orderId).toBe(id);
        expect(record!.amount).toBe((i + 1) * 10);
      });

      await Promise.all(readPromises);

      // Query by secondary index
      const completedOrders = await table.find({ status: "completed" }, { limit: 100 });
      expect(completedOrders.length).toBe(25);
    });
  });

  describe("Unicode Keyset Cursor & Sorting Enhancements", () => {
    it("safely encodes and decodes Unicode cursors without btoa Latin1 crashes", async () => {
      const table = kvdb.table<{ id: string; title: string; category: string }>("unicode_articles", {
        schema: {
          tableName: "t_unicode_articles",
          primaryKey: { name: "id" },
          columns: {
            title: { type: "string", index: true },
            category: { type: "string" },
          },
        },
      });
      await table.init();

      // Insert records with Chinese characters and emojis
      await table.set("art_1", { id: "art_1", title: "🇨🇳 深度解析 Cloudflare D1 架构", category: "技术" });
      await table.set("art_2", { id: "art_2", title: "⚡ 边缘计算与 Serverless 实践", category: "技术" });
      await table.set("art_3", { id: "art_3", title: "🚀 打造极致性能全球 KV 数据库", category: "技术" });

      // Page 1 with limit 2, sorted by title
      const page1 = await table.findPage({ category: "技术" }, {
        limit: 2,
        sort: [{ field: "title", direction: "asc" }],
      });

      expect(page1.items.length).toBe(2);
      expect(page1.complete).toBe(false);
      expect(page1.cursor).toBeDefined();

      // Page 2 using cursor from page 1
      const page2 = await table.findPage({ category: "技术" }, {
        limit: 2,
        cursor: page1.cursor,
        sort: [{ field: "title", direction: "asc" }],
      });

      expect(page2.items.length).toBe(1);
      expect(page2.complete).toBe(true);
    });

    it("supports order property in SortSpec as alias for direction", async () => {
      const table = kvdb.table<{ id: string; score: number }>("players", {
        schema: {
          tableName: "t_players",
          primaryKey: { name: "id" },
          columns: { score: { type: "number", index: true } },
        },
      });
      await table.init();

      await table.set("p1", { id: "p1", score: 10 });
      await table.set("p2", { id: "p2", score: 90 });
      await table.set("p3", { id: "p3", score: 50 });

      const sortedDesc = await table.find({}, {
        sort: [{ field: "score", order: "desc" }],
      });

      expect(sortedDesc.map((p) => p.score)).toEqual([90, 50, 10]);
    });
  });

  describe("Polymorphic setMany & Integer Primary Key getMany", () => {
    it("handles numeric primary keys in getMany()", async () => {
      const table = kvdb.table<{ id: number; name: string }>("numeric_items", {
        schema: {
          tableName: "t_numeric_items",
          primaryKey: { name: "id", type: "integer" },
        },
      });
      await table.init();

      await table.set(100, { id: 100, name: "First" });
      await table.set(200, { id: 200, name: "Second" });
      await table.set(300, { id: 300, name: "Third" });

      const results = await table.getMany([300, 100, 999]);
      expect(results.length).toBe(3);
      expect(results[0]?.name).toBe("Third");
      expect(results[1]?.name).toBe("First");
      expect(results[2]).toBeNull();
    });

    it("supports setMany() with schema primary key property and direct record objects", async () => {
      const table = kvdb.table<{ id: string; team: string; score: number }>("members", {
        schema: {
          tableName: "t_members",
          primaryKey: { name: "id" },
          columns: {
            team: { type: "string", index: true },
            score: { type: "number" },
          },
        },
      });
      await table.init();

      // Passing array of objects directly with id and properties
      await table.setMany([
        { id: "m1", team: "core", score: 95 },
        { id: "m2", value: { id: "m2", team: "infra", score: 88 } },
        { key: "m3", value: { id: "m3", team: "core", score: 72 } },
      ]);

      const m1 = await table.get("m1");
      expect(m1?.team).toBe("core");
      expect(m1?.score).toBe(95);

      const m2 = await table.get("m2");
      expect(m2?.team).toBe("infra");
      expect(m2?.score).toBe(88);

      const m3 = await table.get("m3");
      expect(m3?.team).toBe("core");
      expect(m3?.score).toBe(72);
    });
  });
});

