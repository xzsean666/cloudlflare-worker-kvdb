import { SerializationError } from "./errors.js";

export interface BlobDescriptor {
  __isBlob: true;
  __cf_blob_overflow?: boolean;
  r2Key: string;
  size: number;
  mimeType?: string;
  etag?: string;
  createdAt: number;
}

const BLOB_KEY_FORMAT_REGEX = /^[A-Za-z0-9_.-]+(?:\/[A-Za-z0-9_.-]+)*$/;

/**
 * Checks if a given object is a transparent R2 blob descriptor.
 */
export function isBlobDescriptor(value: unknown): value is BlobDescriptor {
  if (typeof value !== "object" || value === null) return false;
  const obj = value as Record<string, unknown>;
  return (
    (obj.__isBlob === true || obj.__cf_blob_overflow === true) &&
    typeof obj.r2Key === "string" &&
    obj.r2Key.length > 0 &&
    BLOB_KEY_FORMAT_REGEX.test(obj.r2Key) &&
    typeof obj.size === "number" &&
    obj.size >= 0
  );
}

/**
 * Creates a blob descriptor for transparent offloading to Cloudflare R2.
 */
export function createBlobDescriptor(
  r2Key: string,
  size: number,
  options?: { mimeType?: string; etag?: string; createdAt?: number }
): BlobDescriptor {
  return {
    __isBlob: true,
    __cf_blob_overflow: true,
    r2Key,
    size,
    mimeType: options?.mimeType,
    etag: options?.etag,
    createdAt: options?.createdAt ?? Date.now(),
  };
}

/**
 * Recursively normalizes an object for canonical sorting and special type preservation:
 * - Sorts object keys alphabetically
 * - Converts BigInt to `{ __type: "BigInt", value: "..." }`
 * - Converts Date to `{ __type: "Date", value: isoString }`
 * - Converts Uint8Array to `{ __type: "Uint8Array", value: base64 }`
 */
function normalizeForCanonical(value: unknown): unknown {
  if (value === null || typeof value !== "object") {
    if (typeof value === "bigint") {
      return { __type: "BigInt", value: value.toString() };
    }
    return value;
  }

  if (value instanceof Date) {
    return { __type: "Date", value: value.toISOString() };
  }

  if (value instanceof Uint8Array) {
    let binary = "";
    const len = value.byteLength;
    const CHUNK_SIZE = 8192;
    for (let i = 0; i < len; i += CHUNK_SIZE) {
      const chunk = value.subarray(i, Math.min(i + CHUNK_SIZE, len));
      binary += String.fromCharCode.apply(null, chunk as unknown as number[]);
    }
    return { __type: "Uint8Array", value: btoa(binary) };
  }

  if (Array.isArray(value)) {
    return value.map(normalizeForCanonical);
  }

  const obj = value as Record<string, unknown>;
  const sortedKeys = Object.keys(obj).sort();
  const sortedObj: Record<string, unknown> = {};
  for (const k of sortedKeys) {
    sortedObj[k] = normalizeForCanonical(obj[k]);
  }
  return sortedObj;
}

/**
 * Custom reviver restoring Date, BigInt, and Uint8Array.
 */
function canonicalReviver(_key: string, value: unknown): unknown {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    const obj = value as Record<string, unknown>;
    if (obj.__type === "BigInt" && typeof obj.value === "string") {
      return BigInt(obj.value);
    }
    if (obj.__type === "Date" && typeof obj.value === "string") {
      return new Date(obj.value);
    }
    if (obj.__type === "Uint8Array" && typeof obj.value === "string") {
      const binary = atob(obj.value);
      const bytes = new Uint8Array(binary.length);
      for (let i = 0; i < binary.length; i++) {
        bytes[i] = binary.charCodeAt(i);
      }
      return bytes;
    }
  }
  return value;
}

/**
 * Deterministically serializes a value into a canonical JSON string with sorted keys.
 */
export function canonicalStringify(value: unknown): string {
  try {
    const normalized = normalizeForCanonical(value);
    return JSON.stringify(normalized);
  } catch (err: any) {
    throw new SerializationError(`Failed to canonically serialize value: ${err.message}`, err);
  }
}

/**
 * Serializes any JavaScript value into a JSON string.
 */
export function serialize(value: unknown): string {
  return canonicalStringify(value);
}

/**
 * Deserializes a JSON string into its original typed representation.
 */
export function deserialize<T = unknown>(text: string): T {
  try {
    return JSON.parse(text, canonicalReviver) as T;
  } catch (err: any) {
    throw new SerializationError(`Failed to deserialize JSON string: ${err.message}`, err);
  }
}

/**
 * Safe Unicode-compliant Base64 JSON cursor encoder for keyset pagination.
 */
export function encodeCursor(data: unknown): string {
  const json = JSON.stringify(data);
  const bytes = new TextEncoder().encode(json);
  let binary = "";
  for (let i = 0; i < bytes.length; i++) {
    binary += String.fromCharCode(bytes[i]!);
  }
  return btoa(binary);
}

/**
 * Safe Unicode-compliant Base64 JSON cursor decoder for keyset pagination.
 */
export function decodeCursor<T = unknown>(cursor: string): T {
  const binary = atob(cursor);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  const json = new TextDecoder().decode(bytes);
  return JSON.parse(json) as T;
}

