import type { TieredCacheOptions, WrapOptions, RawCacheEntry } from "./types.js";
import { MemoryCacheStore } from "./stores/l1-memory.js";
import { KVCacheStore } from "./stores/l2-kv.js";

/**
 * Multi-Tier Caching System with in-isolate L1 (<0.05ms) and Workers KV L2 (5-15ms),
 * featuring Stale-While-Revalidate (SWR) and background revalidation via `ctx.waitUntil()`.
 */
export class TieredCache {
  public readonly l1: MemoryCacheStore;
  public readonly l2?: KVCacheStore;
  private readonly ctx?: ExecutionContext;
  private readonly inFlight = new Map<string, Promise<any>>();

  constructor(options: TieredCacheOptions = {}) {
    this.l1 = new MemoryCacheStore(options.l1);
    if (options.l2) {
      this.l2 = new KVCacheStore(options.l2);
    }
    this.ctx = options.ctx;
  }

  /**
   * Retrieves a cached value, checking L1 first, falling back to L2, and repopulating L1 on L2 hit.
   */
  async get<T = unknown>(key: string): Promise<T | null> {
    const entry = await this.getEntry<T>(key);
    return entry ? entry.value : null;
  }

  /**
   * Retrieves raw cache entry including metadata (expiresAt, createdAt).
   */
  async getEntry<T = unknown>(key: string): Promise<RawCacheEntry<T> | null> {
    // 1. Check L1 Memory (<0.05ms)
    const l1Entry = await this.l1.get<T>(key);
    if (l1Entry) {
      return l1Entry;
    }

    // 2. Check L2 KV (5-15ms)
    if (this.l2) {
      const l2Entry = await this.l2.get<T>(key);
      if (l2Entry) {
        // Repopulate L1 memory cache with remaining TTL
        const remainingTTL = l2Entry.expiresAt ? Math.max(1, l2Entry.expiresAt - Date.now()) : undefined;
        await this.l1.set(key, l2Entry.value, remainingTTL);
        return l2Entry;
      }
    }

    return null;
  }

  /**
   * Stores a value in L1 and L2 cache.
   */
  async set<T = unknown>(key: string, value: T, ttlMs?: number): Promise<void> {
    await this.l1.set(key, value, ttlMs);
    if (this.l2) {
      await this.l2.set(key, value, ttlMs);
    }
  }

  /**
   * Deletes a key from all cache tiers.
   */
  async delete(key: string): Promise<boolean> {
    const l1Deleted = await this.l1.delete(key);
    const l2Deleted = this.l2 ? await this.l2.delete(key) : false;
    return l1Deleted || l2Deleted;
  }

  /**
   * Checks whether a key is present in either cache tier.
   */
  async has(key: string): Promise<boolean> {
    const inL1 = await this.l1.has(key);
    if (inL1) return true;
    if (this.l2) {
      return await this.l2.has(key);
    }
    return false;
  }

  /**
   * Clears all cache tiers.
   */
  async clear(): Promise<void> {
    await this.l1.clear();
    if (this.l2) {
      await this.l2.clear();
    }
  }

  /**
   * Transparent function wrapper implementing Stale-While-Revalidate (SWR).
   * If fresh data is cached, returns it immediately.
   * If stale data is cached, returns it immediately while revalidating in background via ctx.waitUntil().
   * If no data is cached, invokes the fetcher and populates cache.
   */
  async wrap<T>(
    key: string,
    fetcher: () => Promise<T>,
    options: WrapOptions = {}
  ): Promise<T> {
    const ttlMs = options.ttlMs ?? 60000; // 60s default freshness
    const swrMs = options.swrMs ?? ttlMs; // 60s default stale window
    const totalLifetime = ttlMs + swrMs;

    const entry = await this.getEntry<T>(key);
    const now = Date.now();

    if (entry) {
      const age = now - entry.createdAt;
      const isFresh = age <= ttlMs;

      if (isFresh) {
        return entry.value;
      }

      // Entry is stale: trigger background revalidation
      const revalidate = async () => {
        try {
          const freshVal = await fetcher();
          await this.set(key, freshVal, totalLifetime);
        } catch {
          // Swallow background revalidation errors so consumer still receives cached value
        }
      };

      if (this.ctx && typeof (this.ctx as any).waitUntil === "function") {
        (this.ctx as any).waitUntil(revalidate());
      } else {
        // Run in background without blocking
        void revalidate();
      }

      return entry.value;
    }

    // Cache miss: execute fetcher synchronously with SingleFlight coalescing
    const existing = this.inFlight.get(key);
    if (existing) {
      return (await existing) as T;
    }

    const fetchPromise = (async () => {
      try {
        const value = await fetcher();
        await this.set(key, value, totalLifetime);
        return value;
      } finally {
        this.inFlight.delete(key);
      }
    })();

    this.inFlight.set(key, fetchPromise);
    return await fetchPromise;
  }
}
