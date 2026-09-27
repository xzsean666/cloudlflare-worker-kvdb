import { LRUCache } from "lru-cache";
import type { CacheStore, RawCacheEntry, L1MemoryOptions } from "../types.js";

/**
 * High-performance in-isolate L1 LRU Memory cache store (<0.05ms read latency).
 */
export class MemoryCacheStore implements CacheStore {
  public readonly name = "l1-memory";
  private readonly cache: LRUCache<string, RawCacheEntry<any>>;

  constructor(options: L1MemoryOptions = {}) {
    this.cache = new LRUCache<string, RawCacheEntry<any>>({
      max: options.max ?? 1000,
      ttl: options.defaultTTLMs,
    });
  }

  async get<T = unknown>(key: string): Promise<RawCacheEntry<T> | null> {
    const entry = this.cache.get(key);
    if (!entry) return null;
    if (entry.expiresAt && Date.now() > entry.expiresAt) {
      this.cache.delete(key);
      return null;
    }
    return entry as RawCacheEntry<T>;
  }

  async set<T = unknown>(key: string, value: T, ttlMs?: number): Promise<void> {
    const now = Date.now();
    const expiresAt = ttlMs ? now + ttlMs : undefined;
    const entry: RawCacheEntry<T> = {
      value,
      expiresAt,
      createdAt: now,
    };
    this.cache.set(key, entry, { ttl: ttlMs });
  }

  async delete(key: string): Promise<boolean> {
    return this.cache.delete(key);
  }

  async has(key: string): Promise<boolean> {
    return this.cache.has(key);
  }

  async clear(): Promise<void> {
    this.cache.clear();
  }

  size(): number {
    return this.cache.size;
  }
}
