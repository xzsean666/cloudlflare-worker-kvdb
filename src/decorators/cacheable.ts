import type { TieredCache } from "../cache/cache.js";
import { resolveKey, type CacheKeyBuilder } from "./cache-key.js";
import { KVDBError } from "../core/errors.js";

let globalDefaultCache: TieredCache | undefined;

/**
 * Sets the default TieredCache instance used by decorators when not explicitly passed in options.
 */
export function setDefaultCache(cache: TieredCache): void {
  globalDefaultCache = cache;
}

/**
 * Clears the default TieredCache instance.
 */
export function clearDefaultCache(): void {
  globalDefaultCache = undefined;
}

/**
 * Gets the currently configured default cache instance.
 */
export function getDefaultCache(): TieredCache | undefined {
  return globalDefaultCache;
}

export interface CacheableOptions {
  cache?: TieredCache;
  ttlMs?: number;
  swrMs?: number;
  cacheKey?: string | CacheKeyBuilder;
}

/**
 * Dual-mode method decorator supporting both TC39 Stage 3 standard decorators
 * and legacy TypeScript experimentalDecorators. Caches (async) method invocations using TieredCache.
 */
export function Cacheable(options: CacheableOptions = {}) {
  return function (
    target: any,
    contextOrKey: any,
    descriptor?: PropertyDescriptor
  ): any {
    // 1. Legacy TypeScript decorator (@experimentalDecorators)
    if (descriptor && typeof descriptor.value === "function") {
      const originalMethod = descriptor.value;
      const methodName = String(contextOrKey);
      descriptor.value = async function (this: any, ...args: any[]) {
        const cache = options.cache ?? globalDefaultCache;
        if (!cache) {
          throw new KVDBError(
            "No cache instance available for @Cacheable decorator. Pass { cache } or call setDefaultCache(cache).",
            "MISSING_CACHE_INSTANCE"
          );
        }

        const key = resolveKey(options.cacheKey, this, methodName, args);
        return await cache.wrap(
          key,
          () => Promise.resolve(originalMethod.apply(this, args)),
          { ttlMs: options.ttlMs, swrMs: options.swrMs }
        );
      };
      return descriptor;
    }

    // 2. Standard TC39 Stage 3 method decorator
    const methodName = String(contextOrKey?.name ?? "method");
    const originalMethod = target;

    return async function (this: any, ...args: any[]): Promise<any> {
      const cache = options.cache ?? globalDefaultCache;
      if (!cache) {
        throw new KVDBError(
          "No cache instance available for @Cacheable decorator. Pass { cache } or call setDefaultCache(cache).",
          "MISSING_CACHE_INSTANCE"
        );
      }

      const key = resolveKey(options.cacheKey, this, methodName, args);
      return await cache.wrap(
        key,
        () => Promise.resolve(originalMethod.apply(this, args)),
        { ttlMs: options.ttlMs, swrMs: options.swrMs }
      );
    };
  };
}
