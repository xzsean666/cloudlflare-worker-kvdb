import type { CacheStore, RawCacheEntry, L2KVOptions } from "../types.js";
import { serialize, deserialize } from "../../core/serializer.js";

/**
 * Cloudflare Workers KV L2 edge distributed cache store (5-15ms global read latency).
 */
export class KVCacheStore implements CacheStore {
  public readonly name = "l2-kv";
  private readonly kv: KVNamespace;
  private readonly prefix: string;
  private readonly defaultTTLSeconds?: number;

  constructor(options: L2KVOptions) {
    this.kv = options.namespace;
    this.prefix = options.prefix ?? "cf_cache:";
    this.defaultTTLSeconds = options.defaultTTLSeconds;
  }

  private storageKey(key: string): string {
    return `${this.prefix}${key}`;
  }

  async get<T = unknown>(key: string): Promise<RawCacheEntry<T> | null> {
    const raw = await this.kv.get(this.storageKey(key));
    if (!raw) return null;
    try {
      const entry = deserialize<RawCacheEntry<T>>(raw);
      if (entry.expiresAt && Date.now() > entry.expiresAt) {
        return null;
      }
      return entry;
    } catch {
      return null;
    }
  }

  async set<T = unknown>(key: string, value: T, ttlMs?: number): Promise<void> {
    const now = Date.now();
    const expiresAt = ttlMs ? now + ttlMs : undefined;
    const entry: RawCacheEntry<T> = {
      value,
      expiresAt,
      createdAt: now,
    };

    const putOptions: KVNamespacePutOptions = {};
    const effectiveTTLSec = ttlMs ? Math.ceil(ttlMs / 1000) : this.defaultTTLSeconds;
    if (effectiveTTLSec && effectiveTTLSec > 0) {
      if (effectiveTTLSec >= 60) {
        putOptions.expirationTtl = effectiveTTLSec;
      } else {
        putOptions.expiration = Math.floor(now / 1000) + effectiveTTLSec;
      }
    }

    await this.kv.put(this.storageKey(key), serialize(entry), putOptions);
  }

  async delete(key: string): Promise<boolean> {
    await this.kv.delete(this.storageKey(key));
    return true;
  }

  async has(key: string): Promise<boolean> {
    const entry = await this.get(key);
    return entry !== null;
  }

  async clear(): Promise<void> {
    let nextCursor: string | undefined = undefined;
    while (true) {
      const res: KVNamespaceListResult<unknown, string> = await this.kv.list({
        prefix: this.prefix,
        limit: 1000,
        ...(nextCursor ? { cursor: nextCursor } : {}),
      });
      if (res.keys.length > 0) {
        // Chunk deletes by 25 to respect subrequest limits
        for (let i = 0; i < res.keys.length; i += 25) {
          const chunk = res.keys.slice(i, i + 25);
          await Promise.all(chunk.map((k) => this.kv.delete(k.name)));
        }
      }
      if (res.list_complete || !res.cursor) {
        break;
      }
      nextCursor = res.cursor;
    }
  }
}
