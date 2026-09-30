import { describe, it, expect, vi } from "vitest";
import { createMockD1Database } from "./helpers/mock-d1.js";
import { createMockSqlStorage } from "./helpers/mock-do-sql.js";
import { D1Driver } from "../src/drivers/d1/driver.js";
import { DurableObjectSqlDriver } from "../src/drivers/do-sql/driver.js";
import { HyperdriveDriver } from "../src/drivers/hyperdrive/driver.js";
import { D1SqlBuilder } from "../src/drivers/d1/sql-builder.js";
import { JobQueue } from "../src/queue/queue.js";
import { Table } from "../src/core/table.js";
import { TieredCache } from "../src/cache/cache.js";
import { decodeCursor, encodeCursor } from "../src/core/serializer.js";
import { KVDBError, SerializationError } from "../src/core/errors.js";

describe("Security & Performance Audit Regression Tests", () => {
  describe("Security: SQL Injection Protection in findInternal Keyset Seek", () => {
    it("rejects malicious SQL injection in sort field during keyset pagination", async () => {
      const db = createMockD1Database();
      const driver = new D1Driver(db);
      await driver.init();

      const table = new Table<{ id: string; score: number }>("users", driver, {
        schema: {
          primaryKey: { name: "id", type: "string" },
          columns: { score: { type: "integer", index: true } },
        },
      });

      await table.set("u1", { id: "u1", score: 10 });
      const cursor = encodeCursor({ pk: "u1", val: 10 });

      // Malicious SQL injection attempt in sort field
      const maliciousSort = "score') OR (1=1) --";
      await expect(
        table.findPage({}, {
          sort: [{ path: maliciousSort, direction: "desc" }],
          cursor,
        })
      ).rejects.toThrow(KVDBError);
    });
  });

  describe("Security: Driver Table Identifier Validation", () => {
    it("rejects invalid or malicious table names across all SQL drivers", () => {
      // D1SqlBuilder
      expect(() => new D1SqlBuilder("users; DROP TABLE accounts; --")).toThrow(KVDBError);
      expect(() => new D1SqlBuilder("valid_users_123")).not.toThrow();

      // DurableObjectSqlDriver
      const sqlStorage = createMockSqlStorage();
      expect(() => new DurableObjectSqlDriver(sqlStorage, { tableName: "do_table; --" })).toThrow(KVDBError);
      expect(() => new DurableObjectSqlDriver(sqlStorage, { tableName: "valid_do_table" })).not.toThrow();

      // HyperdriveDriver
      expect(() => new HyperdriveDriver({ tableName: "hyper_table' OR 1=1" })).toThrow(KVDBError);
      expect(() => new HyperdriveDriver({ tableName: "valid_hyper_table" })).not.toThrow();

      // JobQueue
      const db = createMockD1Database();
      expect(() => new JobQueue({ db, tableName: "queue; DROP TABLE _cf_queue;" })).toThrow(KVDBError);
      expect(() => new JobQueue({ db, tableName: "valid_queue_table" })).not.toThrow();
    });

    it("rejects invalid table names or path traversal in Table constructor", () => {
      const db = createMockD1Database();
      const driver = new D1Driver(db);
      expect(() => new Table("users/../secrets", driver)).toThrow(KVDBError);
      expect(() => new Table("users:admin", driver)).toThrow(KVDBError);
      expect(() => new Table("", driver)).toThrow(KVDBError);
      expect(() => new Table("valid_table_name", driver)).not.toThrow();
    });
  });

  describe("Security: decodeCursor Robustness & Prototype Pollution Prevention", () => {
    it("throws SerializationError for malformed base64 or non-JSON cursors", () => {
      expect(() => decodeCursor("@@@not-base-64@@@")).toThrow(SerializationError);
      expect(() => decodeCursor(btoa("this is not json"))).toThrow(SerializationError);
    });

    it("strips __proto__ and constructor from decoded cursor payloads", () => {
      const maliciousJson = JSON.stringify({
        __proto__: { polluted: true },
        constructor: { evil: true },
        pk: "123",
        val: 99,
      });
      const encoded = btoa(maliciousJson);
      const decoded = decodeCursor<any>(encoded);

      expect(decoded.pk).toBe("123");
      expect(decoded.val).toBe(99);
      expect((decoded as any).polluted).toBeUndefined();
      expect(({} as any).polluted).toBeUndefined();
    });
  });

  describe("Performance: SWR Background Revalidation SingleFlight Coalescing", () => {
    it("coalesces 20 concurrent stale reads so background fetcher executes only ONCE", async () => {
      const cache = new TieredCache({ l1: { max: 100 } });
      const fetcher = vi.fn().mockImplementation(async () => {
        await new Promise((r) => setTimeout(r, 20));
        return { data: "fresh" };
      });

      // 1. Initial population: fetcher called once
      const val1 = await cache.wrap("hot-key", fetcher, { ttlMs: 10, swrMs: 500 });
      expect(val1).toEqual({ data: "fresh" });
      expect(fetcher).toHaveBeenCalledTimes(1);

      // 2. Wait until data becomes stale (>10ms, but <510ms)
      await new Promise((r) => setTimeout(r, 15));

      // 3. 20 concurrent requests hit the stale cache entry
      const results = await Promise.all(
        new Array(20).fill(null).map(() =>
          cache.wrap("hot-key", fetcher, { ttlMs: 10, swrMs: 500 })
        )
      );

      // All 20 requests immediately got the cached stale value
      for (const res of results) {
        expect(res).toEqual({ data: "fresh" });
      }

      // SingleFlight guarantee: exactly ONE background revalidation was triggered, not 20!
      expect(fetcher).toHaveBeenCalledTimes(2);

      // Wait for background revalidation to finish
      await new Promise((r) => setTimeout(r, 30));
    });
  });

  describe("Performance & Keyset Pagination: Nested Field Pagination", () => {
    it("correctly encodes and navigates cursor for nested JSON sort fields", async () => {
      const db = createMockD1Database();
      const driver = new D1Driver(db);
      await driver.init();

      const table = new Table<{ id: string; meta: { priority: number } }>("tasks", driver, {
        schema: {
          primaryKey: { name: "id", type: "string" },
        },
      });

      await table.set("t1", { id: "t1", meta: { priority: 100 } });
      await table.set("t2", { id: "t2", meta: { priority: 200 } });
      await table.set("t3", { id: "t3", meta: { priority: 300 } });

      // Page 1 with limit 2 sorted by nested field meta.priority
      const page1 = await table.findPage({}, {
        sort: [{ path: "meta.priority", direction: "asc" }],
        limit: 2,
      });

      expect(page1.items.length).toBe(2);
      expect(page1.complete).toBe(false);
      expect(page1.cursor).toBeDefined();

      // Check cursor content
      const decoded = decodeCursor<any>(page1.cursor!);
      expect(decoded.pk).toBe("t2");
      expect(decoded.val).toBe(200);

      // Page 2 using cursor
      const page2 = await table.findPage({}, {
        sort: [{ path: "meta.priority", direction: "asc" }],
        cursor: page1.cursor,
        limit: 2,
      });

      expect(page2.items.length).toBe(1);
      expect(page2.items[0]!.id).toBe("t3");
      expect(page2.complete).toBe(true);
    });
  });

  describe("Performance: getByPrefix Single Query on Schema Tables", () => {
    it("retrieves all matching keys and values in a schema table", async () => {
      const db = createMockD1Database();
      const driver = new D1Driver(db);
      await driver.init();

      const table = new Table<{ id: string; name: string }>("users", driver, {
        schema: {
          primaryKey: { name: "id", type: "string" },
          columns: { name: { type: "string" } },
        },
      });

      await table.set("org:eng:alice", { id: "org:eng:alice", name: "Alice" });
      await table.set("org:eng:bob", { id: "org:eng:bob", name: "Bob" });
      await table.set("org:sales:carol", { id: "org:sales:carol", name: "Carol" });

      const engMembers = await table.getByPrefix("org:eng:");
      expect(engMembers.size).toBe(2);
      expect(engMembers.get("org:eng:alice")?.name).toBe("Alice");
      expect(engMembers.get("org:eng:bob")?.name).toBe("Bob");
      expect(engMembers.has("org:sales:carol")).toBe(false);
    });
  });
});
