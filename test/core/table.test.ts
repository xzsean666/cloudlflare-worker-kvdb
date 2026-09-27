import { describe, it, expect } from "vitest";
import { CloudflareKVDB } from "../../src/core/kvdb.js";
import { KVDBError } from "../../src/core/errors.js";
import { createMockD1Database } from "../helpers/mock-d1.js";
import { createMockKVNamespace } from "../helpers/mock-kv.js";

interface UserProfile {
  id: string;
  name: string;
  score: number;
  roles: string[];
}

describe("Table Facade and CloudflareKVDB Client", () => {
  it("throws error when no storage engine is provided", () => {
    expect(() => new CloudflareKVDB({})).toThrow(KVDBError);
  });

  it("performs typed CRUD operations with D1 driver", async () => {
    const d1 = createMockD1Database();
    const db = new CloudflareKVDB({ d1 });
    const users = db.table<UserProfile>("users");

    expect(await users.has("user:1")).toBe(false);
    expect(await users.get("user:1")).toBeNull();

    const alice: UserProfile = { id: "1", name: "Alice", score: 99, roles: ["admin", "editor"] };
    await users.set("user:1", alice);

    expect(await users.has("user:1")).toBe(true);
    const fetched = await users.get("user:1");
    expect(fetched).toEqual(alice);

    // Overwrite
    const updatedAlice = { ...alice, score: 100 };
    await users.set("user:1", updatedAlice);
    expect(await users.get("user:1")).toEqual(updatedAlice);

    // Delete
    const deleted = await users.delete("user:1");
    expect(deleted).toBe(true);
    expect(await users.get("user:1")).toBeNull();
  });

  it("performs typed CRUD operations with Workers KV driver", async () => {
    const kv = createMockKVNamespace();
    const db = new CloudflareKVDB({ kv });
    const configs = db.table<{ enabled: boolean; maxRetries: number }>("configs");

    await configs.set("system", { enabled: true, maxRetries: 3 });
    const config = await configs.get("system");
    expect(config).toEqual({ enabled: true, maxRetries: 3 });
  });

  it("handles typed batch operations: setMany, getMany, and deleteMany", async () => {
    const d1 = createMockD1Database();
    const db = new CloudflareKVDB({ d1 });
    const items = db.table<string>("items");

    await items.setMany([
      { key: "a", value: "Alpha" },
      { key: "b", value: "Bravo" },
      { key: "c", value: "Charlie" },
    ]);

    const retrieved = await items.getMany(["a", "missing", "c"]);
    expect(retrieved).toEqual(["Alpha", null, "Charlie"]);

    const deleted = await items.deleteMany(["a", "b"]);
    expect(deleted).toBe(2);

    expect(await items.get("a")).toBeNull();
    expect(await items.get("c")).toBe("Charlie");
  });

  it("enforces complete namespace isolation", async () => {
    const d1 = createMockD1Database();
    const db = new CloudflareKVDB({ d1 });

    const users = db.table<{ role: string }>("users");
    const sessions = db.table<{ token: string }>("sessions");

    await users.set("entity_1", { role: "admin" });
    await sessions.set("entity_1", { token: "secret_xyz" });

    expect(await users.get("entity_1")).toEqual({ role: "admin" });
    expect(await sessions.get("entity_1")).toEqual({ token: "secret_xyz" });

    // Clear sessions should not affect users
    await sessions.clear();
    expect(await sessions.get("entity_1")).toBeNull();
    expect(await users.get("entity_1")).toEqual({ role: "admin" });
  });

  it("scans and deletes by prefix", async () => {
    const d1 = createMockD1Database();
    const db = new CloudflareKVDB({ d1 });
    const table = db.table<number>("stats");

    await table.setMany([
      { key: "2026:jan:clicks", value: 100 },
      { key: "2026:jan:views", value: 500 },
      { key: "2026:feb:clicks", value: 120 },
      { key: "other:metric", value: 42 },
    ]);

    const janStats = await table.getByPrefix("2026:jan:");
    expect(janStats.size).toBe(2);
    expect(janStats.get("2026:jan:clicks")).toBe(100);
    expect(janStats.get("2026:jan:views")).toBe(500);

    const deletedCount = await table.deleteByPrefix("2026:jan:");
    expect(deletedCount).toBe(2);

    expect(await table.get("2026:jan:clicks")).toBeNull();
    expect(await table.get("2026:feb:clicks")).toBe(120);
    expect(await table.get("other:metric")).toBe(42);
  });

  it("caches Table instances per namespace", () => {
    const d1 = createMockD1Database();
    const db = new CloudflareKVDB({ d1 });

    const tableA = db.table("accounts");
    const tableB = db.table("accounts");
    expect(tableA).toBe(tableB);
  });

  it("supports D1 Session bookmark cloning", () => {
    const d1 = createMockD1Database();
    const db = new CloudflareKVDB({ d1, sessionBookmark: "bm-initial" });
    expect(db.getBookmark()).toBe("bm-initial");

    const sessionDb = db.withSession("bm-next");
    expect(sessionDb.getBookmark()).toBe("bm-next");
  });
});
