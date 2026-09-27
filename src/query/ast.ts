/** Comparison operators supported in Mongo-style queries */
export type CompareOp =
  | "$eq"
  | "$ne"
  | "$gt"
  | "$gte"
  | "$lt"
  | "$lte"
  | "$in"
  | "$nin"
  | "$like";

/** Logical operators */
export type LogicalOp = "$and" | "$or" | "$nor" | "$not";

/** Element operators */
export type ElementOp = "$exists";

/**
 * A segment of a dotted path into a document.
 */
export type PathSegment = { key: string } | { index: number };

export interface FieldPath {
  segments: PathSegment[];
  /** Original dotted field path, e.g. "profile.age" */
  source: string;
  /** Whether the field targets a physical SQLite table column or a JSON extracted attribute */
  sourceKind?: "value" | "column";
}

export type QueryNode =
  | { kind: "and"; children: QueryNode[] }
  | { kind: "or"; children: QueryNode[] }
  | { kind: "nor"; children: QueryNode[] }
  | { kind: "not"; child: QueryNode }
  | { kind: "cmp"; op: CompareOp; path: FieldPath; value: unknown }
  | { kind: "exists"; path: FieldPath; value: boolean }
  | { kind: "true" };

/** Sort direction for a single field */
export interface SortSpec {
  field?: string;
  path?: FieldPath;
  direction: "asc" | "desc";
}

/** Options accompanying a query */
export interface QueryOptions {
  limit?: number;
  offset?: number;
  sort?: SortSpec[];
  cursor?: string;
}
