import { describe, it, expect, vi } from "vitest";
import { CloudflareKVDB } from "../../src/core/kvdb.js";
import { WriteBatcher } from "../../src/core/batcher.js";
import { createMockD1Database } from "../helpers/mock-d1.js";

describe("WriteBatcher & In-Memory Coalescing Engine (200ms Interval)", () => {
  it("defaults maxWaitMs to 200ms", () => {
    const batcher = new WriteBatcher(async () => {});
    // Verify default wait time is 200ms
    expect((batcher as any).maxWaitMs).toBe(200);
    expect((batcher as any).maxBatchSize).toBe(50);
  });

  it("coalesces multiple set() calls to the same key within the 200ms interval in memory", async () => {
    const rawDb = createMockD1Database();

    const db = new CloudflareKVDB({
      d1: rawDb,
      autoBatch: {
        maxWaitMs: 200,
      },
    });

    const table = db.table<{ id: string; count: number; name: string }>("users", {
      schema: { primaryKey: { name: "id", type: "string" } },
    });
    await table.init();

    const batchSpy = vi.spyOn(rawDb, "batch");

    // 3 writes to the same key in the same interval
    const p1 = table.set("u1", { id: "u1", count: 1, name: "Alice" });
    const p2 = table.set("u1", { id: "u1", count: 2, name: "Alice v2" });
    const p3 = table.set("u1", { id: "u1", count: 3, name: "Alice final" });

    // In memory, it coalesces into 1 pending operation
    expect(table.pendingBatchCount).toBe(1);

    // Wait for all 3 promises to resolve
    await Promise.all([p1, p2, p3]);

    // Buffer is drained
    expect(table.pendingBatchCount).toBe(0);

    // D1 received only 1 insert statement instead of 3
    const batchCalls = batchSpy.mock.calls;
    const statementsExecuted = batchCalls.flatMap((call) => call[0]);
    expect(statementsExecuted.length).toBe(1);

    // Final state in DB is consistent
    const user = await table.get("u1");
    expect(user).toEqual({ id: "u1", count: 3, name: "Alice final" });
  });

  it("aggregates multiple table.update() calls in memory before writing to DB", async () => {
    const rawDb = createMockD1Database();

    const db = new CloudflareKVDB({
      d1: rawDb,
      autoBatch: {
        maxWaitMs: 200,
      },
    });

    const table = db.table<{ id: string; count: number; tags: string[] }>("metrics", {
      schema: { primaryKey: { name: "id", type: "string" } },
    });
    await table.init();

    const batchSpy = vi.spyOn(rawDb, "batch");

    // Initial set
    void table.set("counter", { id: "counter", count: 1, tags: ["init"] });

    // Multiple updates in memory in the same window
    const u1 = table.update("counter", (prev) => ({
      id: "counter",
      count: (prev?.count ?? 0) + 1,
      tags: [...(prev?.tags ?? []), "step1"],
    }));

    const u2 = table.update("counter", (prev) => ({
      id: "counter",
      count: (prev?.count ?? 0) + 5,
      tags: [...(prev?.tags ?? []), "step2"],
    }));

    // Partial patch update
    const u3 = table.update("counter", {
      count: 100,
    });

    // In memory, only 1 operation is pending
    expect(table.pendingBatchCount).toBe(1);

    await Promise.all([u1, u2, u3]);
    await table.flush();

    // Verify final state in database
    const finalVal = await table.get("counter");
    expect(finalVal).toEqual({
      id: "counter",
      count: 100,
      tags: ["init", "step1", "step2"],
    });

    // Exactly 1 SQL statement was committed to D1
    const statementsExecuted = batchSpy.mock.calls.flatMap((call) => call[0]);
    expect(statementsExecuted.length).toBe(1);
  });

  it("cancels set() in memory when followed by delete() in the same interval", async () => {
    const rawDb = createMockD1Database();

    const db = new CloudflareKVDB({
      d1: rawDb,
      autoBatch: {
        maxWaitMs: 200,
      },
    });

    const table = db.table<{ id: string; name: string }>("temp_records", {
      schema: { primaryKey: { name: "id", type: "string" } },
    });
    await table.init();

    const batchSpy = vi.spyOn(rawDb, "batch");

    // Write then immediately delete in the same window
    const setPromise = table.set("temp1", { id: "temp1", name: "To be deleted" });
    const deletePromise = table.delete("temp1");

    expect(table.pendingBatchCount).toBe(1);

    const [, deletedResult] = await Promise.all([setPromise, deletePromise]);
    expect(deletedResult).toBe(true);

    // Flush and verify
    await table.flush();
    expect(table.pendingBatchCount).toBe(0);

    // The set() was cancelled: no INSERT was ever sent! Only DELETE statement was sent
    const statementsExecuted = batchSpy.mock.calls.flatMap((call) => call[0]);
    const insertStmts = statementsExecuted.filter((stmt: any) =>
      stmt.sql.includes("INSERT INTO")
    );
    expect(insertStmts.length).toBe(0);

    // Record does not exist in DB
    const record = await table.get("temp1");
    expect(record).toBeNull();
  });

  it("handles delete() followed by set() in the same interval by writing final set state", async () => {
    const rawDb = createMockD1Database();

    const db = new CloudflareKVDB({
      d1: rawDb,
      autoBatch: {
        maxWaitMs: 200,
      },
    });

    const table = db.table<{ id: string; status: string }>("status_tbl", {
      schema: { primaryKey: { name: "id", type: "string" } },
    });

    // Delete then re-set in the same window
    const delPromise = table.delete("status_1");
    const setPromise = table.set("status_1", { id: "status_1", status: "active" });

    await Promise.all([delPromise, setPromise]);
    await table.flush();

    const result = await table.get("status_1");
    expect(result).toEqual({ id: "status_1", status: "active" });
  });

  it("updates key directly from DB when not in memory and then coalesces subsequent updates", async () => {
    const rawDb = createMockD1Database();

    const db = new CloudflareKVDB({
      d1: rawDb,
      autoBatch: {
        maxWaitMs: 200,
      },
    });

    const table = db.table<{ id: string; visits: number }>("analytics", {
      schema: { primaryKey: { name: "id", type: "string" } },
    });

    // Initial write that is committed to DB
    await table.set("page_home", { id: "page_home", visits: 10 });
    await table.flush();

    // Now update without existing in batch memory
    const u1 = table.update("page_home", (prev) => ({
      id: "page_home",
      visits: (prev?.visits ?? 0) + 1,
    }));

    const u2 = table.update("page_home", (prev) => ({
      id: "page_home",
      visits: (prev?.visits ?? 0) + 1,
    }));

    await Promise.all([u1, u2]);
    await table.flush();

    const result = await table.get("page_home");
    expect(result).toEqual({ id: "page_home", visits: 12 });
  });

  it("propagates database errors to all coalesced promises when batch flush fails", async () => {
    const rawDb = createMockD1Database();
    // Simulate D1 failure during batch()
    vi.spyOn(rawDb, "batch").mockRejectedValue(new Error("Simulated D1 disk I/O failure"));

    const db = new CloudflareKVDB({
      d1: rawDb,
      autoBatch: { maxWaitMs: 200 },
    });

    const table = db.table<{ id: string }>("error_tbl", {
      schema: { primaryKey: { name: "id", type: "string" } },
    });

    const p1 = table.set("k1", { id: "k1" });
    const p2 = table.set("k1", { id: "k1" });
    const p3 = table.delete("k2");

    await expect(p1).rejects.toThrow("Simulated D1 disk I/O failure");
    await expect(p2).rejects.toThrow("Simulated D1 disk I/O failure");
    await expect(p3).rejects.toThrow("Simulated D1 disk I/O failure");

    // Ensure batcher is not stuck in flushing state and can recover
    expect(table.pendingBatchCount).toBe(0);
  });

  it("coalesces deleteMany() in memory with pending writes and executes atomic batch delete", async () => {
    const rawDb = createMockD1Database();

    const db = new CloudflareKVDB({
      d1: rawDb,
      autoBatch: { maxWaitMs: 200 },
    });

    const table = db.table<{ id: string; val: number }>("multi_del", {
      schema: { primaryKey: { name: "id", type: "string" } },
    });

    void table.set("item_1", { id: "item_1", val: 1 });
    void table.set("item_2", { id: "item_2", val: 2 });
    void table.set("item_3", { id: "item_3", val: 3 });

    // delete item_1 and item_2 in same interval
    const delPromise = table.deleteMany(["item_1", "item_2"]);

    // item_3 is still pending write, item_1 & item_2 are cancelled from write and queued for delete
    const deletedCount = await delPromise;
    expect(deletedCount).toBe(2);

    await table.flush();

    expect(await table.get("item_1")).toBeNull();
    expect(await table.get("item_2")).toBeNull();
    expect(await table.get("item_3")).toEqual({ id: "item_3", val: 3 });
  });

  it("coalesces operations on non-schema D1 tables with multi-row batch execution", async () => {
    const rawDb = createMockD1Database();

    const db = new CloudflareKVDB({
      d1: rawDb,
      autoBatch: { maxWaitMs: 200 },
    });

    // Non-schema table (default KV entries)
    const table = db.table<{ message: string }>("default_kv");

    void table.set("msg_1", { message: "initial" });
    void table.set("msg_1", { message: "updated" });
    void table.set("msg_2", { message: "hello" });
    void table.delete("msg_2");

    await table.flush();

    expect(await table.get("msg_1")).toEqual({ message: "updated" });
    expect(await table.get("msg_2")).toBeNull();
  });
});
