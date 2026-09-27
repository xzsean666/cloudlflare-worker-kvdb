import { describe, it, expect, beforeEach } from "vitest";
import { createMockSqlStorage } from "../helpers/mock-do-sql.js";
import { DurableObjectSqlDriver } from "../../src/drivers/do-sql/driver.js";
import { Table } from "../../src/core/table.js";
import { CloudflareKVDB } from "../../src/core/kvdb.js";

describe("DurableObjectSqlDriver", () => {
  let mockSql: SqlStorage;
  let driver: DurableObjectSqlDriver;

  beforeEach(async () => {
    mockSql = createMockSqlStorage();
    driver = new DurableObjectSqlDriver(mockSql);
    await driver.init();
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

    // Delete non-existent
    expect(await driver.delete("ns1", "key1")).toBe(false);
  });

  it("handles TTL expiration properly", async () => {
    // Negative TTL (expired immediately)
    await driver.set("ns1", "expired", "data", -5);
    expect(await driver.get("ns1", "expired")).toBeNull();
    expect(await driver.has("ns1", "expired")).toBe(false);

    // Active TTL
    await driver.set("ns1", "active", "valid_data", 3600);
    expect(await driver.get("ns1", "active")).toBe("valid_data");
    expect(await driver.has("ns1", "active")).toBe(true);
  });

  it("handles batch setMany and getMany operations", async () => {
    const entries = [
      { key: "k1", value: "v1" },
      { key: "k2", value: "v2" },
      { key: "k3", value: "v3" },
    ];

    await driver.setMany("ns1", entries);

    const values = await driver.getMany("ns1", ["k1", "k2", "missing", "k3"]);
    expect(values).toEqual(["v1", "v2", null, "v3"]);

    const deleted = await driver.deleteMany("ns1", ["k1", "k3", "nonexistent"]);
    expect(deleted).toBe(2);

    expect(await driver.getMany("ns1", ["k1", "k2", "k3"])).toEqual([null, "v2", null]);
  });

  it("performs prefix scanning and cursor pagination", async () => {
    for (let i = 1; i <= 5; i++) {
      await driver.set("ns1", `user:${i}`, `data_${i}`);
    }
    await driver.set("ns1", "other:1", "other_data");

    // Prefix listing
    const prefixList = await driver.list("ns1", { prefix: "user:" });
    expect(prefixList.keys).toHaveLength(5);
    expect(prefixList.complete).toBe(true);

    // Pagination with limit
    const page1 = await driver.list("ns1", { prefix: "user:", limit: 2 });
    expect(page1.keys).toHaveLength(2);
    expect(page1.complete).toBe(false);
    expect(page1.cursor).toBeDefined();

    const page2 = await driver.list("ns1", { prefix: "user:", limit: 2, cursor: page1.cursor });
    expect(page2.keys).toHaveLength(2);
    expect(page2.complete).toBe(false);

    const page3 = await driver.list("ns1", { prefix: "user:", limit: 2, cursor: page2.cursor });
    expect(page3.keys).toHaveLength(1);
    expect(page3.complete).toBe(true);
  });

  it("executes operations inside ACID transactions with commit and rollback", async () => {
    // Successful transaction
    await driver.transaction(async () => {
      await driver.set("ns_tx", "k1", "val1");
      await driver.set("ns_tx", "k2", "val2");
    });

    expect(await driver.get("ns_tx", "k1")).toBe("val1");
    expect(await driver.get("ns_tx", "k2")).toBe("val2");

    // Failing transaction with rollback
    await expect(
      driver.transaction(async () => {
        await driver.set("ns_tx", "k3", "val3");
        throw new Error("Simulated transaction failure");
      })
    ).rejects.toThrow("Simulated transaction failure");

    // k3 must NOT exist
    expect(await driver.get("ns_tx", "k3")).toBeNull();
  });

  it("clears all records in a namespace", async () => {
    await driver.set("ns_clear", "a", "1");
    await driver.set("ns_clear", "b", "2");
    await driver.set("other_ns", "c", "3");

    await driver.clear("ns_clear");

    expect(await driver.get("ns_clear", "a")).toBeNull();
    expect(await driver.get("ns_clear", "b")).toBeNull();
    expect(await driver.get("other_ns", "c")).toBe("3");
  });
});

describe("Table with DurableObjectSqlDriver", () => {
  let mockSql: SqlStorage;
  let driver: DurableObjectSqlDriver;

  beforeEach(async () => {
    mockSql = createMockSqlStorage();
    driver = new DurableObjectSqlDriver(mockSql);
    await driver.init();
  });

  it("supports physical schema tables and secondary index lookups", async () => {
    interface Account {
      id: string;
      email: string;
      tier: string;
      balance: number;
    }

    const table = new Table<Account>("accounts", driver, {
      schema: {
        tableName: "t_accounts",
        primaryKey: { name: "id" },
        columns: {
          email: { type: "string", index: { unique: true } },
          tier: { type: "string", index: true },
          balance: { type: "number" },
        },
      },
    });

    await table.init();

    const acc1: Account = { id: "a1", email: "alice@example.com", tier: "gold", balance: 500 };
    const acc2: Account = { id: "a2", email: "bob@example.com", tier: "silver", balance: 250 };
    const acc3: Account = { id: "a3", email: "carol@example.com", tier: "gold", balance: 1000 };

    await table.set("a1", acc1);
    await table.set("a2", acc2);
    await table.set("a3", acc3);

    // Primary key lookup
    expect(await table.get("a1")).toEqual(acc1);

    // Secondary indexed column point lookup
    expect(await table.getBy("email", "bob@example.com")).toEqual(acc2);

    // Mongo-style query find
    const goldAccounts = await table.find(
      { tier: "gold" },
      { sort: [{ field: "balance", direction: "desc" }] }
    );
    expect(goldAccounts).toHaveLength(2);
    expect(goldAccounts[0]).toEqual(acc3);
    expect(goldAccounts[1]).toEqual(acc1);

    // Dynamic column addition
    await table.addKey("status", { type: "string", index: true });
    await table.set("a1", { ...acc1, status: "verified" } as any);

    const verified = await table.getBy("status", "verified");
    expect(verified).toBeDefined();

    // Delete
    expect(await table.delete("a1")).toBe(true);
    expect(await table.get("a1")).toBeNull();
  });
});

describe("CloudflareKVDB with DurableObjectSqlDriver", () => {
  it("initializes via doSql binding option", async () => {
    const mockSql = createMockSqlStorage();
    const kvdb = new CloudflareKVDB({ doSql: mockSql });

    expect(kvdb.getDriver().name).toBe("do-sql");

    const table = kvdb.table<{ id: string; name: string }>("users");
    await table.set("u1", { id: "u1", name: "Alice" });

    const retrieved = await table.get("u1");
    expect(retrieved).toEqual({ id: "u1", name: "Alice" });
  });
});
