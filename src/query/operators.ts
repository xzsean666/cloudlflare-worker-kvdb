import type { CompareOp, LogicalOp, ElementOp } from "./ast.js";

export const COMPARE_OPERATORS: ReadonlySet<string> = new Set<CompareOp>([
  "$eq",
  "$ne",
  "$gt",
  "$gte",
  "$lt",
  "$lte",
  "$in",
  "$nin",
  "$like",
]);

export const LOGICAL_OPERATORS: ReadonlySet<string> = new Set<LogicalOp>([
  "$and",
  "$or",
  "$nor",
  "$not",
]);

export const ELEMENT_OPERATORS: ReadonlySet<string> = new Set<ElementOp>([
  "$exists",
]);

export function isCompareOp(op: string): op is CompareOp {
  return COMPARE_OPERATORS.has(op);
}

export function isLogicalOp(op: string): op is LogicalOp {
  return LOGICAL_OPERATORS.has(op);
}

export function isElementOp(op: string): op is ElementOp {
  return ELEMENT_OPERATORS.has(op);
}
