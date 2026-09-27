import { describe, it, expect, beforeEach } from "vitest";
import { DatabaseSync } from "node:sqlite";
import {
  HyperdriveDriver,
  toPostgresSql,
  type HyperdriveClient,
} from "../../src/drivers/hyperdrive/driver.js";
import { CloudflareKVDB } from "../../src/core/kvdb.js";

/**
 * Mock PostgreSQL client backed by in-memory SQLite for testing Hyperdrive driver logic.
 */
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

describe("HyperdriveDriver", () => {
  let client: MockHyperdriveClient;
  let driver: HyperdriveDriver;

  beforeEach(async () => {
    client = new MockHyperdriveClient();
    driver = new HyperdriveDriver({ client });
    await driver.init();
  });

  it("converts ? parameters to Postgres positional markers ($1, $2, ...)", () => {
    const input = "SELECT * FROM users WHERE id = ? AND status = ? AND age > ?;";
    const output = toPostgresSql(input);
    expect(output).toBe("SELECT * FROM users WHERE id = $1 AND status = $2 AND age > $3;");
  });

  it("reads connectionString from Hyperdrive binding", () => {
    const mockHyperdrive = {
      connectionString: "postgres://user:pass@127.0.0.1:5432/mydb",
      host: "127.0.0.1",
      port: 5432,
      user: "user",
      password: "pass",
      database: "mydb",
      connect: () => ({}) as any,
    } as any;

    const hdDriver = new HyperdriveDriver({ hyperdrive: mockHyperdrive });
    expect(hdDriver.connectionString).toBe("postgres://user:pass@127.0.0.1:5432/mydb");
    expect(hdDriver.hyperdrive).toBe(mockHyperdrive);
  });

  it("performs single key CRUD operations", async () => {
    expect(await driver.has("ns1", "key1")).toBe(false);
    expect(await driver.get("ns1", "key1")).toBeNull();

    await driver.set("ns1", "key1", "val1");
    expect(await driver.has("ns1", "key1")).toBe(true);
    expect(await driver.get("ns1", "key1")).toBe("val1");

    // Overwrite
    await driver.set("ns1", "key1", "val2");
    expect(await driver.get("ns1", "key1")).toBe("val2");

    // Delete
    const deleted = await driver.delete("ns1", "key1");
    expect(deleted).toBe(true);
    expect(await driver.has("ns1", "key1")).toBe(false);
    expect(await driver.get("ns1", "key1")).toBeNull();
  });

  it("handles TTL expiration properly", async () => {
    // Expired in past
    await driver.set("ns1", "expired", "val", -10);
    expect(await driver.get("ns1", "expired")).toBeNull();
    expect(await driver.has("ns1", "expired")).toBe(false);

    // Active in future
    await driver.set("ns1", "active", "val", 3600);
    expect(await driver.get("ns1", "active")).toBe("val");
    expect(await driver.has("ns1", "active")).toBe(true);
  });

  it("handles batch getMany, setMany, and deleteMany operations", async () => {
    const entries = [
      { key: "k1", value: "v1" },
      { key: "k2", value: "v2" },
      { key: "k3", value: "v3" },
    ];

    await driver.setMany("ns1", entries);

    const retrieved = await driver.getMany("ns1", ["k1", "k2", "nonexistent", "k3"]);
    expect(retrieved).toEqual(["v1", "v2", null, "v3"]);

    const deleted = await driver.deleteMany("ns1", ["k1", "k3"]);
    expect(deleted).toBe(2);

    expect(await driver.getMany("ns1", ["k1", "k2", "k3"])).toEqual([null, "v2", null]);
  });

  it("performs prefix scanning and cursor pagination", async () => {
    for (let i = 1; i <= 5; i++) {
      await driver.set("ns_page", `user:${i}`, `payload_${i}`);
    }
    await driver.set("ns_page", "other:1", "other_val");

    // Prefix listing
    const prefixList = await driver.list("ns_page", { prefix: "user:" });
    expect(prefixList.keys).toHaveLength(5);
    expect(prefixList.complete).toBe(true);

    // Limit pagination
    const page1 = await driver.list("ns_page", { prefix: "user:", limit: 2 });
    expect(page1.keys).toHaveLength(2);
    expect(page1.complete).toBe(false);
    expect(page1.cursor).toBeDefined();

    const page2 = await driver.list("ns_page", { prefix: "user:", limit: 2, cursor: page1.cursor });
    expect(page2.keys).toHaveLength(2);
    expect(page2.complete).toBe(false);

    const page3 = await driver.list("ns_page", { prefix: "user:", limit: 2, cursor: page2.cursor });
    expect(page3.keys).toHaveLength(1);
    expect(page3.complete).toBe(true);
  });

  it("clears all records in a namespace", async () => {
    await driver.set("ns_clear", "a", "1");
    await driver.set("ns_clear", "b", "2");
    await driver.set("ns_keep", "c", "3");

    await driver.clear("ns_clear");

    expect(await driver.get("ns_clear", "a")).toBeNull();
    expect(await driver.get("ns_clear", "b")).toBeNull();
    expect(await driver.get("ns_keep", "c")).toBe("3");
  });
});

describe("CloudflareKVDB with Hyperdrive", () => {
  it("initializes via hyperdriveClient option", async () => {
    const client = new MockHyperdriveClient();
    const kvdb = new CloudflareKVDB({ hyperdriveClient: client });

    expect(kvdb.getDriver().name).toBe("hyperdrive");

    const table = kvdb.table<{ id: string; name: string }>("users");
    await table.set("u1", { id: "u1", name: "Alice" });

    const user = await table.get("u1");
    expect(user).toEqual({ id: "u1", name: "Alice" });
  });
});
