export interface RawCacheEntry<T = unknown> {
  value: T;
  expiresAt?: number;
  createdAt: number;
}

export interface CacheStore {
  readonly name: string;
  get<T = unknown>(key: string): Promise<RawCacheEntry<T> | null>;
  set<T = unknown>(key: string, value: T, ttlMs?: number): Promise<void>;
  delete(key: string): Promise<boolean>;
  has(key: string): Promise<boolean>;
  clear(): Promise<void>;
}

export interface L1MemoryOptions {
  max?: number;
  defaultTTLMs?: number;
}

export interface L2KVOptions {
  namespace: KVNamespace;
  prefix?: string;
  defaultTTLSeconds?: number;
}

export interface TieredCacheOptions {
  /**
   * Configuration for in-isolate L1 LRU memory cache.
   * Set to `false` to disable L1 memory cache and rely solely on globally distributed L2 Workers KV.
   */
  l1?: L1MemoryOptions | false;
  l2?: L2KVOptions;
  ctx?: ExecutionContext;
}

export interface WrapOptions {
  ttlMs?: number;
  swrMs?: number;
}
