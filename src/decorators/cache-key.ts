import { canonicalStringify } from "../core/serializer.js";

/**
 * An argument can supply its own cache key token instead of undergoing full serialization.
 */
export interface CacheableKey {
  cacheKey: string;
}

function argToken(arg: unknown): string {
  if (
    arg !== null &&
    typeof arg === "object" &&
    typeof (arg as CacheableKey).cacheKey === "string"
  ) {
    return (arg as CacheableKey).cacheKey;
  }
  return canonicalStringify(arg);
}

/**
 * Builds the default cache key for a decorated class method: `<ClassName>.<methodName>(<canonicalArgs>)`.
 */
export function defaultKey(className: string, methodName: string, args: readonly unknown[]): string {
  return `${className}.${methodName}(${args.map(argToken).join(",")})`;
}

export type CacheKeyBuilder = (args: readonly unknown[], thisArg: unknown) => string;

/**
 * Resolves the effective cache key from an explicit override string, a key builder function, or the default.
 */
export function resolveKey(
  override: string | CacheKeyBuilder | undefined,
  thisArg: unknown,
  methodName: string,
  args: readonly unknown[]
): string {
  if (typeof override === "string") return override;
  if (typeof override === "function") return override(args, thisArg);
  const className =
    (thisArg as { constructor?: { name?: string } } | null)?.constructor?.name ?? "Class";
  return defaultKey(className, methodName, args);
}
