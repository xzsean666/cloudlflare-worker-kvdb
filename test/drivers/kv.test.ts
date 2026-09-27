import { describe, it, expect, beforeEach } from "vitest";
import { createMockKVNamespace } from "../helpers/mock-kv.js";
import { KVDriver } from "../../src/drivers/kv/driver.js";

describe("KVDriver", () => {
  let rawKv: KVNamespace;
  let driver: KVDriver;

  beforeEach(() => {
    rawKv = createMockKVNamespace();
    driver = new KVDriver(rawKv);
  });

  it("performs single key CRUD operations", async () => {
    expect(await driver.has("users", "alice")).toBe(false);
    expect(await driver.get("users", "alice")).toBeNull();

    await driver.set("users", "alice", JSON.stringify({ name: "Alice", role: "admin" }));
    expect(await driver.has("users", "alice")).toBe(true);

    const val = await driver.get("users", "alice");
    expect(JSON.parse(val!)).toEqual({ name: "Alice", role: "admin" });

    const deleted = await driver.delete("users", "alice");
    expect(deleted).toBe(true);
    expect(await driver.has("users", "alice")).toBe(false);
  });

  it("handles TTL expiration with both short (<60s) and long (>=60s) TTLs", async () => {
    // Short TTL (uses expiration epoch seconds)
    await driver.set("cache", "short_lived", "data", 1);
    expect(await driver.get("cache", "short_lived")).toBe("data");

    // Long TTL (uses expirationTtl)
    await driver.set("cache", "long_lived", "data2", 120);
    expect(await driver.get("cache", "long_lived")).toBe("data2");
  });

  it("handles batch operations with setMany, getMany, and deleteMany", async () => {
    const items = [
      { key: "item1", value: "val1" },
      { key: "item2", value: "val2" },
      { key: "item3", value: "val3" },
    ];

    await driver.setMany("inventory", items);

    const retrieved = await driver.getMany("inventory", ["item1", "missing", "item3"]);
    expect(retrieved).toEqual(["val1", null, "val3"]);

    const deletedCount = await driver.deleteMany("inventory", ["item1", "item2", "item3"]);
    expect(deletedCount).toBe(3);

    const afterDelete = await driver.getMany("inventory", ["item1", "item2", "item3"]);
    expect(afterDelete).toEqual([null, null, null]);
  });

  it("lists keys with prefix and supports multi-page cursor pagination", async () => {
    const items = [
      { key: "log:2026:01", value: "log1" },
      { key: "log:2026:02", value: "log2" },
      { key: "log:2026:03", value: "log3" },
      { key: "meta:config", value: "conf" },
    ];
    await driver.setMany("app", items);

    const page1 = await driver.list("app", { prefix: "log:", limit: 2 });
    expect(page1.keys).toEqual(["log:2026:01", "log:2026:02"]);
    expect(page1.complete).toBe(false);
    expect(page1.cursor).toBeDefined();

    const page2 = await driver.list("app", { prefix: "log:", limit: 2, cursor: page1.cursor });
    expect(page2.keys).toEqual(["log:2026:03"]);
    expect(page2.complete).toBe(true);
  });

  it("scans all keys via getByPrefix across multi-page boundaries", async () => {
    const items = Array.from({ length: 15 }, (_, i) => ({
      key: `msg:${i.toString().padStart(2, "0")}`,
      value: `content_${i}`,
    }));
    await driver.setMany("messages", items);

    const allByPrefix = await driver.getByPrefix("messages", "msg:");
    expect(allByPrefix.size).toBe(15);
    expect(allByPrefix.get("msg:00")).toBe("content_0");
    expect(allByPrefix.get("msg:14")).toBe("content_14");
  });

  it("clears entire namespace without touching other namespaces", async () => {
    await driver.set("ns1", "k1", "v1");
    await driver.set("ns1", "k2", "v2");
    await driver.set("ns2", "k1", "v3");

    await driver.clear("ns1");

    expect(await driver.get("ns1", "k1")).toBeNull();
    expect(await driver.get("ns1", "k2")).toBeNull();
    expect(await driver.get("ns2", "k1")).toBe("v3");
  });
});
