import { D1LimitError } from "./errors.js";

/** Maximum parameters allowed per statement by Cloudflare D1 */
export const D1_MAX_PARAMS_LIMIT = 100;

/** Default safe parameter threshold per chunk to stay well below D1 limits */
export const DEFAULT_CHUNK_MAX_PARAMS = 80;

/**
 * Splits an array into chunks of fixed item size.
 */
export function chunkArray<T>(items: readonly T[], chunkSize: number): T[][] {
  if (chunkSize <= 0) {
    throw new Error(`chunkSize must be greater than 0, received ${chunkSize}`);
  }
  if (items.length === 0) {
    return [];
  }
  const result: T[][] = [];
  for (let i = 0; i < items.length; i += chunkSize) {
    result.push(items.slice(i, i + chunkSize));
  }
  return result;
}

/**
 * Splits items into chunks based on parameters per item, ensuring total parameters
 * per chunk does not exceed maxParams (default 80, D1 limit is 100).
 */
export function chunkByParamCount<T>(
  items: readonly T[],
  paramsPerItem: number,
  maxParams: number = DEFAULT_CHUNK_MAX_PARAMS
): T[][] {
  if (paramsPerItem <= 0) {
    throw new Error(`paramsPerItem must be greater than 0, received ${paramsPerItem}`);
  }
  if (maxParams > D1_MAX_PARAMS_LIMIT) {
    throw new D1LimitError(
      `maxParams (${maxParams}) exceeds Cloudflare D1 strict limit of ${D1_MAX_PARAMS_LIMIT}`
    );
  }
  if (paramsPerItem > maxParams) {
    throw new D1LimitError(
      `paramsPerItem (${paramsPerItem}) exceeds chunk maxParams (${maxParams})`
    );
  }

  const itemsPerChunk = Math.max(1, Math.floor(maxParams / paramsPerItem));
  return chunkArray(items, itemsPerChunk);
}

/**
 * Generates SQL placeholders for an IN clause: (?, ?, ?)
 */
export function buildInClausePlaceholders(count: number): string {
  if (count <= 0) return "()";
  return `(${new Array(count).fill("?").join(", ")})`;
}

/**
 * Generates SQL placeholders for multi-row insert: (?, ?), (?, ?)
 */
export function buildMultiRowPlaceholders(rowCount: number, colCount: number): string {
  if (rowCount <= 0 || colCount <= 0) return "";
  const singleRow = `(${new Array(colCount).fill("?").join(", ")})`;
  return new Array(rowCount).fill(singleRow).join(", ");
}

/**
 * Asserts that the total number of bound parameters does not exceed D1 limit.
 */
export function assertD1ParamLimit(paramsCount: number): void {
  if (paramsCount > D1_MAX_PARAMS_LIMIT) {
    throw new D1LimitError(
      `Query contains ${paramsCount} bound parameters, which exceeds Cloudflare D1 hard limit of ${D1_MAX_PARAMS_LIMIT}`
    );
  }
}
