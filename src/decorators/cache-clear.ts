import type { TieredCache } from "../cache/cache.js";
import { resolveKey, type CacheKeyBuilder } from "./cache-key.js";
import { getDefaultCache } from "./cacheable.js";
import { KVDBError } from "../core/errors.js";

export interface CacheClearOptions {
  cache?: TieredCache;
  cacheKey: string | CacheKeyBuilder | (string | CacheKeyBuilder)[];
}

/**
 * Dual-mode method decorator supporting both TC39 Stage 3 standard decorators
 * and legacy TypeScript experimentalDecorators. Executes the method, then evicts specified cache keys.
 */
export function CacheClear(options: CacheClearOptions) {
  return function (
    target: any,
    contextOrKey: any,
    descriptor?: PropertyDescriptor
  ): any {
    const keysToClear = Array.isArray(options.cacheKey) ? options.cacheKey : [options.cacheKey];

    // 1. Legacy TypeScript decorator (@experimentalDecorators)
    if (descriptor && typeof descriptor.value === "function") {
      const originalMethod = descriptor.value;
      const methodName = String(contextOrKey);
      descriptor.value = async function (this: any, ...args: any[]) {
        const result = await originalMethod.apply(this, args);
        const cache = options.cache ?? getDefaultCache();
        if (!cache) {
          throw new KVDBError(
            "No cache instance available for @CacheClear decorator.",
            "MISSING_CACHE_INSTANCE"
          );
        }

        for (const k of keysToClear) {
          const resolved = resolveKey(k, this, methodName, args);
          await cache.delete(resolved);
        }

        return result;
      };
      return descriptor;
    }

    // 2. Standard TC39 Stage 3 method decorator
    const methodName = String(contextOrKey?.name ?? "method");
    const originalMethod = target;

    return async function (this: any, ...args: any[]): Promise<any> {
      const result = await originalMethod.apply(this, args);
      const cache = options.cache ?? getDefaultCache();
      if (!cache) {
        throw new KVDBError(
          "No cache instance available for @CacheClear decorator.",
          "MISSING_CACHE_INSTANCE"
        );
      }

      for (const k of keysToClear) {
        const resolved = resolveKey(k, this, methodName, args);
        await cache.delete(resolved);
      }

      return result;
    };
  };
}
