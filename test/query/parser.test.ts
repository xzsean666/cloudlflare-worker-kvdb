import { describe, it, expect } from "vitest";
import { parsePath, parseWhere } from "../../src/query/parser.js";
import { KVDBError } from "../../src/core/errors.js";

describe("Query Parser", () => {
  it("parses valid dot-delimited paths and array indices", () => {
    const p1 = parsePath("user.name");
    expect(p1.segments).toEqual([{ key: "user" }, { key: "name" }]);
    expect(p1.source).toBe("user.name");
    expect(p1.sourceKind).toBe("value");

    const p2 = parsePath("items.0.sku");
    expect(p2.segments).toEqual([{ key: "items" }, { index: 0 }, { key: "sku" }]);

    const p3 = parsePath("key", new Set(["key", "value"]));
    expect(p3.sourceKind).toBe("column");
  });

  it("rejects empty or invalid paths", () => {
    expect(() => parsePath("")).toThrow(KVDBError);
    expect(() => parsePath("a..b")).toThrow(KVDBError);
    expect(() => parsePath("a.b space")).toThrow(KVDBError);
  });

  it("parses empty where as true node", () => {
    expect(parseWhere()).toEqual({ kind: "true" });
    expect(parseWhere({})).toEqual({ kind: "true" });
  });

  it("parses implicit $eq on primitive values", () => {
    const node = parseWhere({ status: "active", count: 5 });
    expect(node.kind).toBe("and");
    if (node.kind === "and") {
      expect(node.children.length).toBe(2);
      expect(node.children[0]).toMatchObject({
        kind: "cmp",
        op: "$eq",
        path: { source: "status" },
        value: "active",
      });
      expect(node.children[1]).toMatchObject({
        kind: "cmp",
        op: "$eq",
        path: { source: "count" },
        value: 5,
      });
    }
  });

  it("parses explicit comparison operators and $exists", () => {
    const node = parseWhere({
      "profile.age": { $gte: 21, $lt: 65 },
      deleted: { $exists: false },
    });

    expect(node.kind).toBe("and");
    if (node.kind === "and") {
      expect(node.children.length).toBe(2);
      // profile.age has both $gte and $lt -> nested and
      const ageNode = node.children[0]!;
      expect(ageNode.kind).toBe("and");
      const existsNode = node.children[1]!;
      expect(existsNode).toMatchObject({
        kind: "exists",
        path: { source: "deleted" },
        value: false,
      });
    }
  });

  it("parses logical operators ($and, $or, $nor, $not)", () => {
    const node = parseWhere({
      $or: [{ role: "admin" }, { "permissions.write": true }],
      $not: { banned: true },
    });

    expect(node.kind).toBe("and");
    if (node.kind === "and") {
      expect(node.children[0]!.kind).toBe("or");
      expect(node.children[1]!.kind).toBe("not");
    }
  });

  it("prevents prototype pollution", () => {
    expect(() => parseWhere(JSON.parse('{"__proto__": {"admin": true}}'))).toThrow(KVDBError);
    expect(() => parseWhere({ constructor: { admin: true } })).toThrow(KVDBError);
    expect(() => parseWhere({ prototype: { admin: true } })).toThrow(KVDBError);
  });
});
