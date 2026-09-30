import { describe, it, expect } from "vitest";
import { createMockD1Database } from "../helpers/mock-d1.js";
import { createMockSqlStorage } from "../helpers/mock-do-sql.js";
import { createQueueSqlAdapter } from "../../src/queue/adapter.js";
import { StorageError } from "../../src/core/errors.js";

describe("createQueueSqlAdapter", () => {
  it("throws StorageError when null or undefined db is passed", () => {
    expect(() => createQueueSqlAdapter(null as any)).toThrow(StorageError);
    expect(() => createQueueSqlAdapter(null as any)).toThrow("Queue requires a valid D1Database or SqlStorage instance");
  });

  it("throws StorageError when unsupported db object is passed", () => {
    expect(() => createQueueSqlAdapter({} as any)).toThrow(StorageError);
    expect(() => createQueueSqlAdapter({} as any)).toThrow("Unsupported database object provided to Queue adapter");
  });

  it("handles D1 adapter exec, query, and empty batch", async () => {
    const rawDb = createMockD1Database();
    const adapter = createQueueSqlAdapter(rawDb);

    await adapter.exec("CREATE TABLE test_d1 (id INTEGER PRIMARY KEY, active INTEGER);");
    const writeRes = await adapter.exec("INSERT INTO test_d1 (id, active) VALUES (?, ?);", 1, true);
    expect(writeRes.changes).toBe(1);

    const rows = await adapter.query<{ id: number; active: number }>("SELECT * FROM test_d1 WHERE id = ?;", 1);
    expect(rows.length).toBe(1);
    expect(rows[0]!.active).toBe(1);

    // Empty batch returns empty array
    const batchRes = await adapter.batch([]);
    expect(batchRes).toEqual([]);
  });

  it("handles SqlStorage adapter exec, query, and batch with transaction commit", async () => {
    const mockSql = createMockSqlStorage();
    const adapter = createQueueSqlAdapter(mockSql);

    await adapter.exec("CREATE TABLE test_sql (id INTEGER PRIMARY KEY, name TEXT, active INTEGER);");
    
    // Batch inserts
    const batchRes = await adapter.batch([
      { sql: "INSERT INTO test_sql (id, name, active) VALUES (?, ?, ?);", params: [1, "alice", true] },
      { sql: "INSERT INTO test_sql (id, name, active) VALUES (?, ?, ?);", params: [2, "bob", false] },
    ]);

    expect(batchRes.length).toBe(2);
    expect(batchRes[0]!.changes).toBe(1);
    expect(batchRes[1]!.changes).toBe(1);

    const rows = await adapter.query<{ id: number; name: string; active: number }>("SELECT * FROM test_sql ORDER BY id ASC;");
    expect(rows.length).toBe(2);
    expect(rows[0]!.name).toBe("alice");
    expect(rows[0]!.active).toBe(1);
    expect(rows[1]!.name).toBe("bob");
    expect(rows[1]!.active).toBe(0);

    // Empty batch
    expect(await adapter.batch([])).toEqual([]);
  });

  it("rolls back SqlStorage transaction when batch statement fails", async () => {
    const mockSql = createMockSqlStorage();
    const adapter = createQueueSqlAdapter(mockSql);

    await adapter.exec("CREATE TABLE items (id INTEGER PRIMARY KEY, title TEXT);");
    await adapter.exec("INSERT INTO items (id, title) VALUES (1, 'initial');");

    // Attempt batch with a duplicate primary key error in the second statement
    await expect(
      adapter.batch([
        { sql: "INSERT INTO items (id, title) VALUES (2, 'second');" },
        { sql: "INSERT INTO items (id, title) VALUES (1, 'conflict');" },
      ])
    ).rejects.toThrow();

    // Verify rollback: item 2 should not exist in the table
    const rows = await adapter.query("SELECT * FROM items;");
    expect(rows.length).toBe(1);
  });
});
