import { KVDBError } from "./errors.js";

export const DEFAULT_KEY_DELIMITER = ":";

/**
 * Validates a key string.
 */
export function validateKey(key: string): void {
  if (typeof key !== "string" || key.trim().length === 0) {
    throw new KVDBError("Key must be a non-empty string", "INVALID_KEY");
  }
}

/**
 * Validates a namespace string.
 */
export function validateNamespace(
  namespace: string,
  delimiter: string = DEFAULT_KEY_DELIMITER
): void {
  if (typeof namespace !== "string" || namespace.trim().length === 0) {
    throw new KVDBError("Namespace must be a non-empty string", "INVALID_NAMESPACE");
  }
  if (delimiter && namespace.includes(delimiter)) {
    throw new KVDBError(
      `Namespace "${namespace}" must not contain delimiter "${delimiter}"`,
      "INVALID_NAMESPACE"
    );
  }
}

/**
 * Encodes a table/namespace and key into a compound storage key: `namespace:key`.
 */
export function encodeKey(
  namespace: string,
  key: string,
  delimiter: string = DEFAULT_KEY_DELIMITER
): string {
  validateNamespace(namespace, delimiter);
  validateKey(key);
  return `${namespace}${delimiter}${key}`;
}

/**
 * Decodes a compound storage key into `{ namespace, key }`.
 */
export function decodeKey(
  fullKey: string,
  delimiter: string = DEFAULT_KEY_DELIMITER
): { namespace: string; key: string } {
  if (typeof fullKey !== "string" || !fullKey.includes(delimiter)) {
    throw new KVDBError(
      `Invalid storage key '${fullKey}'. Expected format '<namespace>${delimiter}<key>'`,
      "INVALID_KEY_FORMAT"
    );
  }
  const idx = fullKey.indexOf(delimiter);
  const namespace = fullKey.substring(0, idx);
  const key = fullKey.substring(idx + delimiter.length);
  return { namespace, key };
}

/**
 * Builds a search prefix for listing keys in a namespace.
 */
export function buildPrefix(
  namespace: string,
  delimiter: string = DEFAULT_KEY_DELIMITER
): string {
  validateNamespace(namespace, delimiter);
  return `${namespace}${delimiter}`;
}

/**
 * Joins multiple path components into a single key string.
 */
export function joinKeyPath(
  parts: readonly string[],
  delimiter: string = DEFAULT_KEY_DELIMITER
): string {
  if (parts.length === 0) {
    throw new KVDBError("Key parts cannot be empty", "INVALID_KEY");
  }
  for (const part of parts) {
    if (typeof part !== "string" || part.length === 0) {
      throw new KVDBError("All key parts must be non-empty strings", "INVALID_KEY");
    }
  }
  return parts.join(delimiter);
}
