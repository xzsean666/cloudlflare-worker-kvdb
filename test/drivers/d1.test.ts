import { describe, it, expect, beforeEach } from "vitest";
import { createMockD1Database } from "../helpers/mock-d1.js";
import { D1Driver } from "../../src/drivers/d1/driver.js";

describe("D1Driver", () => {
  let rawDb: D1Database;
  let driver: D1Driver;

  beforeEach(async () => {
    rawDb = createMockD1Database();
    driver = new D1Driver(rawDb);
    await driver.init();
  });

  it("performs single key CRUD operations", async () => {
    expect(await driver.has("ns1", "key1")).toBe(false);
    expect(await driver.get("ns1", "key1")).toBeNull();

    await driver.set("ns1", "key1", "value1");
    expect(await driver.has("ns1", "key1")).toBe(true);
    expect(await driver.get("ns1", "key1")).toBe("value1");

    // Overwrite
    await driver.set("ns1", "key1", "value2");
    expect(await driver.get("ns1", "key1")).toBe("value2");

    // Delete
    const deleted = await driver.delete("ns1", "key1");
    expect(deleted).toBe(true);
    expect(await driver.has("ns1", "key1")).toBe(false);
    expect(await driver.get("ns1", "key1")).toBeNull();
  });

  it("handles TTL expiration properly", async () => {
    // Expire in negative seconds (already expired)
    await driver.set("ns1", "expired_key", "val", -10);
    expect(await driver.get("ns1", "expired_key")).toBeNull();
    expect(await driver.has("ns1", "expired_key")).toBe(false);

    // Valid TTL
    await driver.set("ns1", "valid_key", "val", 3600);
    expect(await driver.get("ns1", "valid_key")).toBe("val");
    expect(await driver.has("ns1", "valid_key")).toBe(true);
  });

  it("correctly executes setMany with 200 items in chunked statements without hitting 100-param limit", async () => {
    // 200 items * 6 params = 1200 params. Without chunking this would trigger D1LimitError (> 100 params)
    const items = Array.from({ length: 200 }, (_, i) => ({
      key: `bulk_${i.toString().padStart(3, "0")}`,
      value: `data_${i}`,
    }));

    await driver.setMany("bulk_ns", items);

    // Verify first and last
    expect(await driver.get("bulk_ns", "bulk_000")).toBe("data_0");
    expect(await driver.get("bulk_ns", "bulk_199")).toBe("data_199");
  });

  it("correctly executes getMany with 200 items in chunked IN statements preserving order", async () => {
    const keys = Array.from({ length: 200 }, (_, i) => `get_k_${i.toString().padStart(3, "0")}`);
    const entries = keys.map((k, i) => ({ key: k, value: `val_${i}` }));

    await driver.setMany("get_ns", entries);

    // Include some non-existent keys in the lookup
    const lookupKeys = [...keys.slice(0, 10), "non_existent_1", ...keys.slice(10, 20), "non_existent_2"];
    const results = await driver.getMany("get_ns", lookupKeys);

    expect(results.length).toBe(lookupKeys.length);
    expect(results[0]).toBe("val_0");
    expect(results[9]).toBe("val_9");
    expect(results[10]).toBeNull();
    expect(results[11]).toBe("val_10");
    expect(results[21]).toBeNull();
  });

  it("executes deleteMany with 200 items in chunked statements", async () => {
    const items = Array.from({ length: 200 }, (_, i) => ({
      key: `del_${i.toString().padStart(3, "0")}`,
      value: `v_${i}`,
    }));

    await driver.setMany("del_ns", items);

    const keysToDelete = items.map((it) => it.key);
    const deletedCount = await driver.deleteMany("del_ns", keysToDelete);
    expect(deletedCount).toBe(200);

    const results = await driver.getMany("del_ns", keysToDelete);
    expect(results.every((r) => r === null)).toBe(true);
  });

  it("lists keys with prefix and pagination cursor", async () => {
    const items = [
      { key: "users:alice", value: "1" },
      { key: "users:bob", value: "2" },
      { key: "users:charlie", value: "3" },
      { key: "orders:1001", value: "4" },
    ];
    await driver.setMany("test_ns", items);

    // List users with limit 2
    const page1 = await driver.list("test_ns", { prefix: "users:", limit: 2 });
    expect(page1.keys).toEqual(["users:alice", "users:bob"]);
    expect(page1.complete).toBe(false);
    expect(page1.cursor).toBe("users:bob");

    // Page 2
    const page2 = await driver.list("test_ns", { prefix: "users:", limit: 2, cursor: page1.cursor });
    expect(page2.keys).toEqual(["users:charlie"]);
    expect(page2.complete).toBe(true);
  });

  it("clears entire namespace without affecting other namespaces", async () => {
    await driver.set("ns_a", "k1", "v1");
    await driver.set("ns_b", "k2", "v2");

    await driver.clear("ns_a");
    expect(await driver.get("ns_a", "k1")).toBeNull();
    expect(await driver.get("ns_b", "k2")).toBe("v2");
  });

  it("handles D1 Sessions API bookmarks correctly", () => {
    expect(driver.getBookmark()).toBeNull();
    driver.setBookmark("bookmark-xyz-001");
    expect(driver.getBookmark()).toBe("bookmark-xyz-001");

    const sessionDriver = driver.withSession("bookmark-xyz-002");
    expect(sessionDriver.getBookmark()).toBe("bookmark-xyz-002");
  });
});
