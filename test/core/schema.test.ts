import { describe, it, expect, beforeEach } from "vitest";
import { createMockD1Database } from "../helpers/mock-d1.js";
import { CloudflareKVDB } from "../../src/core/kvdb.js";
import type { TableSchema } from "../../src/core/schema.js";

interface UserAccount {
  id: string;
  email: string;
  age: number;
  role: string;
  status?: string;
  metadata?: Record<string, unknown>;
}

describe("Physical Schema Tables & B-Tree Indexes", () => {
  let d1: D1Database;
  let db: CloudflareKVDB;

  const userSchema: TableSchema = {
    primaryKey: { name: "id", type: "string" },
    columns: {
      email: { type: "string", nullable: false, index: { unique: true } },
      age: { type: "integer", index: true },
      role: { type: "string", default: "user" },
    },
    indexes: [
      { name: "idx_user_role_age", columns: ["role", "age"] },
    ],
  };

  beforeEach(() => {
    d1 = createMockD1Database();
    db = new CloudflareKVDB({ d1 });
  });

  it("automatically creates the physical SQL table and B-Tree indexes on init", async () => {
    const table = db.table<UserAccount>("users", { schema: userSchema });
    await table.init();

    // Verify table exists by inserting a record
    const user: UserAccount = {
      id: "u101",
      email: "alice@example.com",
      age: 28,
      role: "admin",
      metadata: { theme: "dark" },
    };

    await table.set(user.id, user);

    const retrieved = await table.get("u101");
    expect(retrieved).toEqual(user);
  });

  it("performs O(1) point lookups via secondary indexed columns using getBy", async () => {
    const table = db.table<UserAccount>("users", { schema: userSchema });

    await table.set("u1", { id: "u1", email: "alice@company.com", age: 25, role: "member" });
    await table.set("u2", { id: "u2", email: "bob@company.com", age: 35, role: "admin" });

    const alice = await table.getBy("email", "alice@company.com");
    expect(alice).not.toBeNull();
    expect(alice!.id).toBe("u1");
    expect(alice!.age).toBe(25);

    const bob = await table.getBy("email", "bob@company.com");
    expect(bob).not.toBeNull();
    expect(bob!.id).toBe("u2");

    const nonExistent = await table.getBy("email", "nobody@company.com");
    expect(nonExistent).toBeNull();
  });

  it("dynamically evolves schema using addKey without data loss", async () => {
    const table = db.table<UserAccount>("users", { schema: userSchema });

    await table.set("u1", { id: "u1", email: "alice@company.com", age: 25, role: "member" });

    // Dynamically add "status" column with index
    await table.addKey("status", { type: "string", index: true, default: "active" });

    // Existing data is preserved
    const aliceBefore = await table.get("u1");
    expect(aliceBefore).not.toBeNull();
    expect(aliceBefore!.email).toBe("alice@company.com");

    // Can store new column and query by it
    await table.set("u2", {
      id: "u2",
      email: "bob@company.com",
      age: 30,
      role: "admin",
      status: "pending",
    }, { keys: { status: "pending" } });

    const bob = await table.getBy("status", "pending");
    expect(bob).not.toBeNull();
    expect(bob!.id).toBe("u2");
  });

  it("queries physical columns and JSON fields via find()", async () => {
    const table = db.table<UserAccount>("users", { schema: userSchema });

    await table.set("u1", { id: "u1", email: "a@test.com", age: 20, role: "user" });
    await table.set("u2", { id: "u2", email: "b@test.com", age: 30, role: "admin" });
    await table.set("u3", { id: "u3", email: "c@test.com", age: 40, role: "admin" });

    // Query on physical column `role` and `age`
    const admins = await table.find({ role: "admin", age: { $gte: 30 } }, {
      sort: [{ path: { segments: [{ key: "age" }], source: "age", sourceKind: "column" }, direction: "desc" }],
    });

    expect(admins.length).toBe(2);
    expect(admins[0]!.id).toBe("u3");
    expect(admins[1]!.id).toBe("u2");
  });

  it("deletes records and clears physical schema table cleanly", async () => {
    const table = db.table<UserAccount>("users", { schema: userSchema });
    await table.set("u1", { id: "u1", email: "a@test.com", age: 20, role: "user" });

    expect(await table.has("u1")).toBe(true);
    await table.delete("u1");
    expect(await table.has("u1")).toBe(false);
    expect(await table.get("u1")).toBeNull();

    await table.set("u2", { id: "u2", email: "b@test.com", age: 25, role: "user" });
    await table.clear();
    expect(await table.has("u2")).toBe(false);
  });
});
