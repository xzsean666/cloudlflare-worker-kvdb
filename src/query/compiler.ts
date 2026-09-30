import type { CompareOp, FieldPath, QueryNode, SortSpec } from "./ast.js";
import { parsePath } from "./parser.js";
import { KVDBError } from "../core/errors.js";
import { chunkArray } from "../core/chunker.js";

export interface CompiledQuery {
  sql: string;
  params: unknown[];
}

/**
 * Builds a SQLite JSON path string from a FieldPath (e.g. "profile.age" -> "$.profile.age").
 */
export function formatJsonPath(path: FieldPath): string {
  let result = "$";
  for (const segment of path.segments) {
    if ("index" in segment) {
      result += `[${segment.index}]`;
    } else {
      result += `.${segment.key}`;
    }
  }
  return result;
}

/**
 * Generates the SQL expression for accessing a field.
 * If target is a table column, returns the column name directly.
 * Otherwise, wraps in `json_extract(value, '$.path')`.
 */
export function renderFieldExpression(path: FieldPath, valueHint?: unknown): string {
  if (path.sourceKind === "column") {
    return path.source;
  }

  const jsonPath = formatJsonPath(path);
  const extractExpr = `json_extract(value, '${jsonPath}')`;

  // Apply NUMERIC cast when comparing against numbers to ensure mathematical ordering
  if (typeof valueHint === "number" || typeof valueHint === "bigint") {
    return `CAST(${extractExpr} AS NUMERIC)`;
  }

  return extractExpr;
}

/**
 * Compiles a QueryNode AST into a parameterized SQLite WHERE clause.
 */
export function compileWhere(node: QueryNode): CompiledQuery {
  const params: unknown[] = [];
  const sql = walkNode(node, params);
  return { sql, params };
}

/**
 * Compiles sort specifications into an ORDER BY clause.
 */
export function compileOrderBy(sort: readonly SortSpec[], knownColumns?: Set<string>): string {
  if (sort.length === 0) return "";
  return sort
    .map((spec) => {
      const rawPath = spec.path ?? spec.field;
      const path =
        typeof rawPath === "string"
          ? parsePath(rawPath, knownColumns)
          : (rawPath as FieldPath | undefined);
      if (!path) {
        throw new KVDBError("Sort specification must provide either 'path' or 'field'", "INVALID_QUERY_SORT");
      }
      const expr = renderFieldExpression(path);
      const dir = (spec.direction ?? spec.order) === "desc" ? "DESC" : "ASC";
      return `${expr} ${dir}`;
    })
    .join(", ");
}

function walkNode(node: QueryNode, params: unknown[]): string {
  switch (node.kind) {
    case "true":
      return "1=1";
    case "and":
      return combineNodes(node.children, "AND", params);
    case "or":
      return combineNodes(node.children, "OR", params);
    case "nor":
      return `NOT (${combineNodes(node.children, "OR", params)})`;
    case "not":
      return `NOT (${walkNode(node.child, params)})`;
    case "exists":
      return compileExists(node.path, node.value);
    case "cmp":
      return compileCompare(node.op, node.path, node.value, params);
    default: {
      const exhaustive: never = node;
      throw new KVDBError(`Unhandled query AST node: ${JSON.stringify(exhaustive)}`, "COMPILATION_ERROR");
    }
  }
}

function combineNodes(
  children: readonly QueryNode[],
  operator: "AND" | "OR",
  params: unknown[]
): string {
  if (children.length === 0) {
    return operator === "AND" ? "1=1" : "1=0";
  }
  const parts = children.map((c) => `(${walkNode(c, params)})`);
  return parts.join(` ${operator} `);
}

function compileExists(path: FieldPath, value: boolean): string {
  const expr = renderFieldExpression(path);
  return value ? `${expr} IS NOT NULL` : `${expr} IS NULL`;
}

export function coerceSqlParam(value: unknown): unknown {
  if (value === undefined) {
    return null;
  }
  if (typeof value === "boolean") {
    return value ? 1 : 0;
  }
  return value;
}

function bindParam(params: unknown[], value: unknown): string {
  params.push(coerceSqlParam(value));
  return "?";
}

function compileCompare(
  op: CompareOp,
  path: FieldPath,
  value: unknown,
  params: unknown[]
): string {
  if (op === "$in" || op === "$nin") {
    if (!Array.isArray(value)) {
      throw new KVDBError(`Operator ${op} requires an array value at "${path.source}"`, "INVALID_QUERY_FILTER");
    }
    if (value.length === 0) {
      return op === "$in" ? "1=0" : "1=1";
    }

    const firstItem = value[0];
    const left = renderFieldExpression(path, firstItem);

    if (value.length <= 80) {
      const placeholders = value.map((item) => bindParam(params, item));
      const inClause = `(${placeholders.join(", ")})`;
      return op === "$in" ? `${left} IN ${inClause}` : `${left} NOT IN ${inClause}`;
    }

    // Chunk into batches of at most 80 parameters to strictly abide by D1 100-bound-param limit
    const chunks = chunkArray(value, 80);
    const chunkClauses = chunks.map((chunk) => {
      const placeholders = chunk.map((item) => bindParam(params, item));
      const inClause = `(${placeholders.join(", ")})`;
      return op === "$in" ? `${left} IN ${inClause}` : `${left} NOT IN ${inClause}`;
    });

    return op === "$in"
      ? `(${chunkClauses.join(" OR ")})`
      : `(${chunkClauses.join(" AND ")})`;
  }

  const left = renderFieldExpression(path, value);

  if (op === "$ne") {
    if (value === null) {
      return `${left} IS NOT NULL`;
    }
    const ph = bindParam(params, value);
    return `(${left} IS NULL OR ${left} <> ${ph})`;
  }

  if (op === "$eq") {
    if (value === null) {
      return `${left} IS NULL`;
    }
    const ph = bindParam(params, value);
    return `${left} = ${ph}`;
  }

  if (op === "$like") {
    if (typeof value !== "string") {
      throw new KVDBError(`Operator $like requires a string pattern at "${path.source}"`, "INVALID_QUERY_FILTER");
    }
    const ph = bindParam(params, value);
    return `${left} LIKE ${ph} ESCAPE '\\'`;
  }

  const sqlOpMap: Record<"$gt" | "$gte" | "$lt" | "$lte", string> = {
    $gt: ">",
    $gte: ">=",
    $lt: "<",
    $lte: "<=",
  };

  const sqlOp = sqlOpMap[op];
  if (!sqlOp) {
    throw new KVDBError(`Unsupported comparison operator "${op}"`, "INVALID_QUERY_OPERATOR");
  }

  const ph = bindParam(params, value);
  return `${left} ${sqlOp} ${ph}`;
}
