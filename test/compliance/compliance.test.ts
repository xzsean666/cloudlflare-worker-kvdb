import { describe, it, expect, beforeEach } from "vitest";
import { DatabaseSync } from "node:sqlite";
import { createMockD1Database } from "../helpers/mock-d1.js";
import { createMockSqlStorage } from "../helpers/mock-do-sql.js";
import { D1Driver } from "../../src/drivers/d1/driver.js";
import { DurableObjectSqlDriver } from "../../src/drivers/do-sql/driver.js";
import { HyperdriveDriver, type HyperdriveClient } from "../../src/drivers/hyperdrive/driver.js";
import type { Driver } from "../../src/drivers/types.js";

class MockHyperdriveClient implements HyperdriveClient {
  private db: DatabaseSync;

  constructor() {
    this.db = new DatabaseSync(":memory:");
  }

  async query<T = Record<string, unknown>>(sql: string, params: unknown[] = []): Promise<T[]> {
    const normalized = sql
      .replace(/\$\d+/g, "?")
      .replace(/BIGINT/gi, "INTEGER")
      .replace(/VARCHAR\(\d+\)/gi, "TEXT");

    const stmt = this.db.prepare(normalized);
    return stmt.all(...(params as any[])) as T[];
  }

  async exec(sql: string, params: unknown[] = []): Promise<{ rowCount: number }> {
    const normalized = sql
      .replace(/\$\d+/g, "?")
      .replace(/BIGINT/gi, "INTEGER")
      .replace(/VARCHAR\(\d+\)/gi, "TEXT");

    const stmt = this.db.prepare(normalized);
    const res = stmt.run(...(params as any[]));
    return { rowCount: Number(res.changes) };
  }
}

interface DriverFactory {
  name: string;
  create: () => Promise<Driver>;
}

const driverFactories: DriverFactory[] = [
  {
    name: "Cloudflare D1 Driver",
    create: async () => {
      const driver = new D1Driver(createMockD1Database());
      await driver.init();
      return driver;
    },
  },
  {
    name: "Durable Objects SQLite Driver (ctx.storage.sql)",
    create: async () => {
      const driver = new DurableObjectSqlDriver(createMockSqlStorage());
      await driver.init();
      return driver;
    },
  },
  {
    name: "Cloudflare Hyperdrive Postgres Driver",
    create: async () => {
      const client = new MockHyperdriveClient();
      const driver = new HyperdriveDriver({ client });
      await driver.init();
      return driver;
    },
  },
];

describe.each(driverFactories)("Cross-Driver Compliance Suite: $name", ({ create }) => {
  let driver: Driver;

  beforeEach(async () => {
    driver = await create();
  });

  it("conforms to single-key CRUD semantics", async () => {
    // 1. Initial empty state
    expect(await driver.has("comp_ns", "key1")).toBe(false);
    expect(await driver.get("comp_ns", "key1")).toBeNull();

    // 2. Insert
    await driver.set("comp_ns", "key1", "val1");
    expect(await driver.has("comp_ns", "key1")).toBe(true);
    expect(await driver.get("comp_ns", "key1")).toBe("val1");

    // 3. Overwrite
    await driver.set("comp_ns", "key1", "val2");
    expect(await driver.get("comp_ns", "key1")).toBe("val2");

    // 4. Delete existing
    const deleted = await driver.delete("comp_ns", "key1");
    expect(deleted).toBe(true);
    expect(await driver.has("comp_ns", "key1")).toBe(false);
    expect(await driver.get("comp_ns", "key1")).toBeNull();

    // 5. Delete non-existent
    expect(await driver.delete("comp_ns", "key1")).toBe(false);
  });

  it("conforms to TTL expiration rules", async () => {
    // Expired TTL (negative or past)
    await driver.set("comp_ns", "k_expired", "expired_val", -10);
    expect(await driver.get("comp_ns", "k_expired")).toBeNull();
    expect(await driver.has("comp_ns", "k_expired")).toBe(false);

    // Active TTL
    await driver.set("comp_ns", "k_active", "active_val", 3600);
    expect(await driver.get("comp_ns", "k_active")).toBe("active_val");
    expect(await driver.has("comp_ns", "k_active")).toBe(true);
  });

  it("conforms to batch operations and preserves order", async () => {
    const entries = [
      { key: "b1", value: "val1" },
      { key: "b2", value: "val2" },
      { key: "b3", value: "val3" },
    ];

    await driver.setMany("comp_ns", entries);

    const values = await driver.getMany("comp_ns", ["b3", "b1", "b_missing", "b2"]);
    expect(values).toEqual(["val3", "val1", null, "val2"]);

    const deletedCount = await driver.deleteMany("comp_ns", ["b1", "b3", "b_missing"]);
    expect(deletedCount).toBe(2);

    expect(await driver.get("comp_ns", "b1")).toBeNull();
    expect(await driver.get("comp_ns", "b2")).toBe("val2");
    expect(await driver.get("comp_ns", "b3")).toBeNull();
  });

  it("conforms to prefix listing and cursor pagination", async () => {
    for (let i = 1; i <= 6; i++) {
      await driver.set("comp_page", `item:${i}`, `data_${i}`);
    }
    await driver.set("comp_page", "other:1", "other_data");

    // Prefix listing
    const prefixRes = await driver.list("comp_page", { prefix: "item:" });
    expect(prefixRes.keys).toHaveLength(6);
    expect(prefixRes.complete).toBe(true);

    // Limit pagination
    const p1 = await driver.list("comp_page", { prefix: "item:", limit: 2 });
    expect(p1.keys).toHaveLength(2);
    expect(p1.complete).toBe(false);
    expect(p1.cursor).toBeDefined();

    const p2 = await driver.list("comp_page", { prefix: "item:", limit: 2, cursor: p1.cursor });
    expect(p2.keys).toHaveLength(2);
    expect(p2.complete).toBe(false);
    expect(p2.cursor).toBeDefined();

    const p3 = await driver.list("comp_page", { prefix: "item:", limit: 2, cursor: p2.cursor });
    expect(p3.keys).toHaveLength(2);
    expect(p3.complete).toBe(true);

    // Total unique collected keys
    const allKeys = [...p1.keys, ...p2.keys, ...p3.keys];
    expect(allKeys).toHaveLength(6);
    expect(new Set(allKeys).size).toBe(6);
  });

  it("strictly isolates namespaces", async () => {
    await driver.set("ns_alpha", "common_key", "alpha_data");
    await driver.set("ns_beta", "common_key", "beta_data");

    expect(await driver.get("ns_alpha", "common_key")).toBe("alpha_data");
    expect(await driver.get("ns_beta", "common_key")).toBe("beta_data");

    await driver.clear("ns_alpha");

    expect(await driver.get("ns_alpha", "common_key")).toBeNull();
    expect(await driver.get("ns_beta", "common_key")).toBe("beta_data");
  });
});
