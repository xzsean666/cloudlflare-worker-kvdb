import { describe, it, expect, beforeEach } from "vitest";
import { createMockD1Database } from "../helpers/mock-d1.js";
import { createMockR2Bucket } from "../helpers/mock-r2.js";
import { createMockSqlStorage } from "../helpers/mock-do-sql.js";
import { D1Driver } from "../../src/drivers/d1/driver.js";
import { Table } from "../../src/core/table.js";
import { TTLSweeper } from "../../src/maintenance/sweeper.js";
import { createScheduledHandler } from "../../src/maintenance/handler.js";

describe("TTLSweeper on Cloudflare D1", () => {
  let rawDb: D1Database;
  let r2Bucket: R2Bucket;
  let driver: D1Driver;

  beforeEach(async () => {
    rawDb = createMockD1Database();
    r2Bucket = createMockR2Bucket();
    driver = new D1Driver(rawDb);
    await driver.init();
  });

  it("sweeps expired records while preserving active and permanent records", async () => {
    const table = new Table<string>("kv", driver);

    // 1. Permanent (no TTL)
    await table.set("perm", "permanent-value");

    // 2. Active (TTL in future)
    await table.set("active", "active-value", 3600);

    // 3. Expired (TTL in past)
    await table.set("expired1", "expired-value-1", -10);
    await table.set("expired2", "expired-value-2", -20);

    const sweeper = new TTLSweeper({
      db: rawDb,
      r2Bucket,
    });

    const result = await sweeper.sweepExpired();
    expect(result.expiredRowsDeleted).toBe(2);
    expect(result.blobsDeleted).toBe(0);

    // Verify database contents
    expect(await table.get("perm")).toBe("permanent-value");
    expect(await table.get("active")).toBe("active-value");
    expect(await table.get("expired1")).toBeNull();
    expect(await table.get("expired2")).toBeNull();
  });

  it("cascades deletion to R2 overflow blobs when records expire", async () => {
    const table = new Table<{ id: string; content: string }>("docs", driver, {
      r2Bucket,
      overflowThresholdBytes: 50,
    });

    // Active large blob
    const activeDoc = { id: "d_active", content: "active-large-content-chunk-".repeat(10) };
    await table.set("d_active", activeDoc, 3600);

    // Expired large blob
    const expiredDoc = { id: "d_expired", content: "expired-large-content-chunk-".repeat(10) };
    await table.set("d_expired", expiredDoc, -10);

    // Both blobs should currently exist in R2
    let r2List = await r2Bucket.list();
    expect(r2List.objects.length).toBe(2);

    const sweeper = new TTLSweeper({
      db: rawDb,
      r2Bucket,
    });

    const result = await sweeper.sweepExpired();
    expect(result.expiredRowsDeleted).toBe(1);
    expect(result.blobsDeleted).toBe(1);

    // Only 1 blob remains in R2 (the active one)
    r2List = await r2Bucket.list();
    expect(r2List.objects.length).toBe(1);

    // Active document is still completely intact
    const fetchedActive = await table.get("d_active");
    expect(fetchedActive).toEqual(activeDoc);
  });

  it("safely chunks large volumes of expired rows across batches", async () => {
    const table = new Table<string>("batch_cleanup", driver);

    // Insert 120 expired items
    for (let i = 1; i <= 120; i++) {
      await table.set(`exp_${i}`, `data_${i}`, -1);
    }

    const sweeper = new TTLSweeper({
      db: rawDb,
      batchSize: 30, // Small batch size: 4 batches of 30
    });

    const result = await sweeper.sweepExpired();
    expect(result.expiredRowsDeleted).toBe(120);

    // All should be gone
    for (let i = 1; i <= 120; i++) {
      expect(await table.get(`exp_${i}`)).toBeNull();
    }
  });

  it("sweeps physical schema tables", async () => {
    const table = new Table<{ id: string; tag: string; value: string }>("sessions", driver, {
      schema: {
        tableName: "t_sessions",
        primaryKey: { name: "id" },
        columns: {
          tag: { type: "string" },
        },
      },
    });

    await table.init();

    await table.set("s1", { id: "s1", tag: "auth", value: "tok1" }, -5); // expired
    await table.set("s2", { id: "s2", tag: "auth", value: "tok2" }, 3600); // active

    const sweeper = new TTLSweeper({
      db: rawDb,
      tableNames: ["t_sessions"],
    });

    const result = await sweeper.sweepExpired();
    expect(result.expiredRowsDeleted).toBe(1);

    expect(await table.get("s1")).toBeNull();
    expect(await table.get("s2")).not.toBeNull();
  });
});

describe("TTLSweeper on Durable Objects SqlStorage", () => {
  it("cleans expired records inside Durable Object SQLite", async () => {
    const mockSql = createMockSqlStorage();
    const sweeper = new TTLSweeper({ db: mockSql });

    // Bootstrap table in mockSql
    mockSql.exec(`
      CREATE TABLE IF NOT EXISTS _cf_kvdb (
        namespace TEXT,
        key TEXT,
        value TEXT,
        expires_at INTEGER,
        created_at INTEGER,
        updated_at INTEGER,
        PRIMARY KEY (namespace, key)
      );
    `);

    // Insert expired and valid rows
    mockSql.exec(
      `INSERT INTO _cf_kvdb VALUES ('ns', 'k_exp', 'val', 100, 100, 100);`
    );
    mockSql.exec(
      `INSERT INTO _cf_kvdb VALUES ('ns', 'k_act', 'val', 9999999999999, 100, 100);`
    );

    const result = await sweeper.sweepExpired();
    expect(result.expiredRowsDeleted).toBe(1);

    const remaining = mockSql.exec(`SELECT key FROM _cf_kvdb;`).toArray();
    expect(remaining).toHaveLength(1);
    expect(remaining[0]!.key).toBe("k_act");
  });
});

describe("createScheduledHandler", () => {
  it("executes sweep in response to Cloudflare Worker cron trigger", async () => {
    const rawDb = createMockD1Database();
    const driver = new D1Driver(rawDb);
    await driver.init();

    const table = new Table<string>("cron_test", driver);
    await table.set("c1", "exp", -1);

    let callbackFired = false;
    let sweptCount = 0;

    const handler = createScheduledHandler({
      db: rawDb,
      onSuccess: (res) => {
        callbackFired = true;
        sweptCount = res.expiredRowsDeleted;
      },
    });

    let waitedPromise: Promise<any> | null = null;
    const mockCtx: ExecutionContext = {
      waitUntil: (p: Promise<any>) => {
        waitedPromise = p;
      },
      passThroughOnException: () => {},
    } as any;

    const mockEvent = {
      cron: "* * * * *",
      scheduledTime: Date.now(),
      type: "scheduled",
    } as any;

    await handler(mockEvent, {}, mockCtx);
    if (waitedPromise) {
      await waitedPromise;
    }

    expect(callbackFired).toBe(true);
    expect(sweptCount).toBe(1);
  });
});
