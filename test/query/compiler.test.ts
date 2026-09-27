import { describe, it, expect, beforeEach } from "vitest";
import { DatabaseSync } from "node:sqlite";
import { parseWhere } from "../../src/query/parser.js";
import { compileWhere, compileOrderBy } from "../../src/query/compiler.js";

describe("Query Compiler", () => {
  let db: DatabaseSync;

  beforeEach(() => {
    db = new DatabaseSync(":memory:");
    db.exec(`
      CREATE TABLE documents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        key TEXT NOT NULL,
        value TEXT NOT NULL
      );
    `);

    const seed = [
      { key: "u1", val: { name: "Alice", age: 25, active: true, tags: ["admin", "staff"], score: 95.5 } },
      { key: "u2", val: { name: "Bob", age: 17, active: false, tags: ["guest"], score: 45.0 } },
      { key: "u3", val: { name: "Charlie", age: 30, active: true, tags: ["staff"], score: 88.0, extra: null } },
      { key: "u4", val: { name: "David", age: 40, active: true, tags: ["admin"], score: 100 } },
    ];

    for (const item of seed) {
      db.prepare("INSERT INTO documents (key, value) VALUES (?, ?);").run(item.key, JSON.stringify(item.val));
    }
  });

  it("compiles simple equality and executes on SQLite", () => {
    const ast = parseWhere({ active: true });
    const { sql, params } = compileWhere(ast);

    expect(sql).toBe("json_extract(value, '$.active') = ?");
    expect(params).toEqual([1]);

    const rows = db.prepare(`SELECT key FROM documents WHERE ${sql} ORDER BY key;`).all(...(params as any[])) as { key: string }[];
    expect(rows.map((r) => r.key)).toEqual(["u1", "u3", "u4"]);
  });

  it("compiles numeric comparisons with CAST AS NUMERIC", () => {
    const ast = parseWhere({ age: { $gte: 25 } });
    const { sql, params } = compileWhere(ast);

    expect(sql).toContain("CAST(json_extract(value, '$.age') AS NUMERIC) >=");
    expect(params).toEqual([25]);

    const rows = db.prepare(`SELECT key FROM documents WHERE ${sql} ORDER BY key;`).all(...(params as any[])) as { key: string }[];
    expect(rows.map((r) => r.key)).toEqual(["u1", "u3", "u4"]);
  });

  it("compiles $in and $nin operators", () => {
    const inAst = parseWhere({ name: { $in: ["Alice", "Bob"] } });
    const inQuery = compileWhere(inAst);
    expect(inQuery.sql).toContain("json_extract(value, '$.name') IN (?, ?)");

    const inRows = db.prepare(`SELECT key FROM documents WHERE ${inQuery.sql} ORDER BY key;`).all(...(inQuery.params as any[])) as { key: string }[];
    expect(inRows.map((r) => r.key)).toEqual(["u1", "u2"]);

    const ninAst = parseWhere({ name: { $nin: ["Alice", "Bob"] } });
    const ninQuery = compileWhere(ninAst);
    const ninRows = db.prepare(`SELECT key FROM documents WHERE ${ninQuery.sql} ORDER BY key;`).all(...(ninQuery.params as any[])) as { key: string }[];
    expect(ninRows.map((r) => r.key)).toEqual(["u3", "u4"]);
  });

  it("compiles $exists correctly", () => {
    const existsAst = parseWhere({ extra: { $exists: true } });
    const { sql, params } = compileWhere(existsAst);

    // json_extract(value, '$.extra') IS NOT NULL
    expect(sql).toBe("json_extract(value, '$.extra') IS NOT NULL");
    expect(params).toEqual([]);
  });

  it("compiles nested dot-paths: 'profile.settings.theme'", () => {
    const ast = parseWhere({ "profile.settings.theme": "dark" });
    const { sql, params } = compileWhere(ast);

    expect(sql).toBe("json_extract(value, '$.profile.settings.theme') = ?");
    expect(params).toEqual(["dark"]);
  });

  it("compiles complex logical trees: ($and, $or, $not)", () => {
    const ast = parseWhere({
      $or: [
        { age: { $lt: 18 } },
        { score: { $gte: 90 } },
      ],
      active: true,
    });

    const { sql, params } = compileWhere(ast);
    const rows = db.prepare(`SELECT key FROM documents WHERE ${sql} ORDER BY key;`).all(...(params as any[])) as { key: string }[];
    // u1 (active, score 95.5 >= 90) and u4 (active, score 100 >= 90); u2 is < 18 but active is false
    expect(rows.map((r) => r.key)).toEqual(["u1", "u4"]);
  });

  it("compiles ORDER BY clauses", () => {
    const orderBy = compileOrderBy([
      { path: { segments: [{ key: "score" }], source: "score" }, direction: "desc" },
      { path: { segments: [{ key: "name" }], source: "name" }, direction: "asc" },
    ]);

    expect(orderBy).toBe("json_extract(value, '$.score') DESC, json_extract(value, '$.name') ASC");
  });
});
