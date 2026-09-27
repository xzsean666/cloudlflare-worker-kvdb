import { describe, it, expect } from "vitest";
import { CloudflareKVDB, VERSION } from "../src/index.js";
import { createMockD1Database } from "./helpers/mock-d1.js";
import { createMockKVNamespace } from "./helpers/mock-kv.js";
import { createMockR2Bucket } from "./helpers/mock-r2.js";

describe("Smoke Test Environment", () => {
  it("initializes CloudflareKVDB instance", () => {
    const d1 = createMockD1Database();
    const db = new CloudflareKVDB({ d1 });
    expect(db.getDriver().name).toBe("d1");
  });

  it("operates mock D1 database correctly", async () => {
    const d1 = createMockD1Database();
    await d1.exec("CREATE TABLE test (id TEXT PRIMARY KEY, val TEXT);");
    await d1.prepare("INSERT INTO test (id, val) VALUES (?, ?);").bind("k1", "v1").run();
    const row = await d1.prepare("SELECT * FROM test WHERE id = ?;").bind("k1").first<{ id: string; val: string }>();
    expect(row).toEqual({ id: "k1", val: "v1" });
  });

  it("operates mock KV namespace correctly", async () => {
    const kv = createMockKVNamespace();
    await kv.put("hello", "world");
    const val = await kv.get("hello");
    expect(val).toBe("world");
  });

  it("operates mock R2 bucket correctly", async () => {
    const r2 = createMockR2Bucket();
    await r2.put("blob.txt", "some content");
    const obj = await r2.get("blob.txt");
    expect(obj).not.toBeNull();
    const text = await obj!.text();
    expect(text).toBe("some content");
  });
});
