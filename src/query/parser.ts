import type { FieldPath, PathSegment, QueryNode, CompareOp } from "./ast.js";
import { isCompareOp, isLogicalOp, isElementOp } from "./operators.js";
import { KVDBError } from "../core/errors.js";

const VALID_PATH_SEGMENT = /^[A-Za-z0-9_$-]+$/;

/**
 * Parses a dotted path string (e.g. "profile.age" or "items.0.name") into a structured FieldPath.
 */
export function parsePath(source: string, knownColumns?: Set<string>): FieldPath {
  if (typeof source !== "string" || source.trim().length === 0) {
    throw new KVDBError("Empty or invalid field path", "INVALID_QUERY_PATH");
  }

  const isColumn = Boolean(knownColumns && knownColumns.has(source));
  const segments: PathSegment[] = source.split(".").map((part) => {
    if (part.length === 0) {
      throw new KVDBError(`Empty segment in field path "${source}"`, "INVALID_QUERY_PATH");
    }
    if (/^\d+$/.test(part)) {
      return { index: Number(part) };
    }
    if (!VALID_PATH_SEGMENT.test(part)) {
      throw new KVDBError(
        `Invalid character in field path segment "${part}" of "${source}"`,
        "INVALID_QUERY_PATH"
      );
    }
    return { key: part };
  });

  return {
    segments,
    source,
    sourceKind: isColumn ? "column" : "value",
  };
}

/**
 * Parses a Mongo-style filter document into a canonical QueryNode AST.
 */
export function parseWhere(
  where?: Record<string, unknown>,
  knownColumns?: Set<string>
): QueryNode {
  if (!where || Object.keys(where).length === 0) {
    return { kind: "true" };
  }

  for (const key of Object.getOwnPropertyNames(where)) {
    if (key === "__proto__" || key === "constructor" || key === "prototype") {
      throw new KVDBError(
        `Invalid query key "${key}": prototype pollution prevention`,
        "PROTOTYPE_POLLUTION"
      );
    }
  }

  const children: QueryNode[] = [];

  for (const [key, value] of Object.entries(where)) {

    if (isLogicalOp(key)) {
      children.push(parseLogicalEntry(key, value, knownColumns));
    } else {
      children.push(parseFieldEntry(key, value, knownColumns));
    }
  }

  if (children.length === 0) {
    return { kind: "true" };
  }
  if (children.length === 1) {
    return children[0]!;
  }
  return { kind: "and", children };
}

function parseLogicalEntry(
  op: string,
  value: unknown,
  knownColumns?: Set<string>
): QueryNode {
  if (op === "$not") {
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
      throw new KVDBError("$not requires a query object", "INVALID_QUERY_FILTER");
    }
    return { kind: "not", child: parseWhere(value as Record<string, unknown>, knownColumns) };
  }

  if (!Array.isArray(value)) {
    throw new KVDBError(`${op} requires an array of query filter objects`, "INVALID_QUERY_FILTER");
  }

  const children = value.map((item) => {
    if (typeof item !== "object" || item === null) {
      throw new KVDBError(`${op} elements must be objects`, "INVALID_QUERY_FILTER");
    }
    return parseWhere(item as Record<string, unknown>, knownColumns);
  });

  if (op === "$and") {
    return { kind: "and", children };
  }
  if (op === "$or") {
    return { kind: "or", children };
  }
  if (op === "$nor") {
    return { kind: "nor", children };
  }

  throw new KVDBError(`Unknown logical operator "${op}"`, "INVALID_QUERY_OPERATOR");
}

function parseFieldEntry(
  fieldPathStr: string,
  value: unknown,
  knownColumns?: Set<string>
): QueryNode {
  const path = parsePath(fieldPathStr, knownColumns);

  // If value is null or primitive or array, it's an implicit $eq
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return { kind: "cmp", op: "$eq", path, value };
  }

  // Value is an object: check for operators
  const subObj = value as Record<string, unknown>;
  const subKeys = Object.keys(subObj);

  if (subKeys.length === 0) {
    // Empty object treated as $eq {}
    return { kind: "cmp", op: "$eq", path, value };
  }

  const nodes: QueryNode[] = [];

  for (const [subKey, subVal] of Object.entries(subObj)) {
    if (isCompareOp(subKey)) {
      nodes.push({ kind: "cmp", op: subKey, path, value: subVal });
    } else if (isElementOp(subKey)) {
      if (subKey === "$exists") {
        nodes.push({ kind: "exists", path, value: Boolean(subVal) });
      }
    } else {
      throw new KVDBError(
        `Unknown operator "${subKey}" for field "${fieldPathStr}"`,
        "INVALID_QUERY_OPERATOR"
      );
    }
  }

  if (nodes.length === 1) {
    return nodes[0]!;
  }
  return { kind: "and", children: nodes };
}
