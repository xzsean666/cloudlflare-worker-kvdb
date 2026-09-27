import type { Driver, DriverCapabilities, DriverListOptions, DriverListResult, DriverSetItem } from "../types.js";
import { encodeKey, decodeKey, buildPrefix, DEFAULT_KEY_DELIMITER } from "../../core/key.js";
import { chunkArray } from "../../core/chunker.js";
import { StorageError } from "../../core/errors.js";

export interface KVDriverOptions {
  delimiter?: string;
  batchConcurrency?: number;
}

export class KVDriver implements Driver {
  public readonly name = "kv";
  public readonly capabilities: DriverCapabilities = {
    supportsTTL: true,
    supportsBatch: true,
    supportsSessions: false,
    supportsJSONQuery: false,
    supportsIndexes: false,
  };

  private readonly delimiter: string;
  private readonly batchConcurrency: number;

  constructor(
    private readonly kv: KVNamespace,
    options: KVDriverOptions = {}
  ) {
    this.delimiter = options.delimiter ?? DEFAULT_KEY_DELIMITER;
    this.batchConcurrency = options.batchConcurrency ?? 25;
  }

  async init(): Promise<void> {
    // Workers KV namespaces do not require table schema initialization
  }

  private toStorageKey(namespace: string, key: string): string {
    return encodeKey(namespace, key, this.delimiter);
  }

  private fromStorageKey(namespace: string, fullKey: string): string {
    const nsPrefix = buildPrefix(namespace, this.delimiter);
    if (fullKey.startsWith(nsPrefix)) {
      return fullKey.slice(nsPrefix.length);
    }
    return fullKey;
  }

  async get(namespace: string, key: string): Promise<string | null> {
    try {
      const storageKey = this.toStorageKey(namespace, key);
      const res = await this.kv.getWithMetadata<{ expiresAt?: number }>(storageKey);
      if (res.value === null || res.value === undefined) return null;
      if (res.metadata?.expiresAt && Date.now() > res.metadata.expiresAt) {
        return null;
      }
      return res.value;
    } catch (err: any) {
      throw new StorageError(`Workers KV get failed for key '${key}': ${err.message}`, err);
    }
  }

  async getMany(namespace: string, keys: readonly string[]): Promise<(string | null)[]> {
    if (keys.length === 0) return [];
    try {
      const results: (string | null)[] = new Array(keys.length);
      const chunks = chunkArray(
        keys.map((k, idx) => ({ key: k, idx })),
        this.batchConcurrency
      );

      for (const chunk of chunks) {
        await Promise.all(
          chunk.map(async ({ key, idx }) => {
            results[idx] = await this.get(namespace, key);
          })
        );
      }
      return results;
    } catch (err: any) {
      throw new StorageError(`Workers KV getMany failed: ${err.message}`, err);
    }
  }

  async set(namespace: string, key: string, value: string, ttlSeconds?: number): Promise<void> {
    try {
      const storageKey = this.toStorageKey(namespace, key);
      const putOptions: KVNamespacePutOptions = {};

      if (ttlSeconds !== undefined && ttlSeconds > 0) {
        // Cloudflare Workers KV requires expirationTtl to be >= 60 seconds.
        // We set expirationTtl to at least 60 seconds to satisfy the KV API,
        // and record the precise application-level expiration in metadata.
        putOptions.expirationTtl = Math.max(60, Math.ceil(ttlSeconds));
        putOptions.metadata = {
          expiresAt: Date.now() + Math.ceil(ttlSeconds * 1000),
        };
      }

      await this.kv.put(storageKey, value, putOptions);
    } catch (err: any) {
      throw new StorageError(`Workers KV set failed for key '${key}': ${err.message}`, err);
    }
  }

  async setMany(namespace: string, entries: readonly DriverSetItem[]): Promise<void> {
    if (entries.length === 0) return;
    try {
      const chunks = chunkArray(entries, this.batchConcurrency);
      for (const chunk of chunks) {
        await Promise.all(
          chunk.map((item) => this.set(namespace, item.key, item.value, item.ttlSeconds))
        );
      }
    } catch (err: any) {
      throw new StorageError(`Workers KV setMany failed: ${err.message}`, err);
    }
  }

  async delete(namespace: string, key: string): Promise<boolean> {
    try {
      const storageKey = this.toStorageKey(namespace, key);
      await this.kv.delete(storageKey);
      return true;
    } catch (err: any) {
      throw new StorageError(`Workers KV delete failed for key '${key}': ${err.message}`, err);
    }
  }

  async deleteMany(namespace: string, keys: readonly string[]): Promise<number> {
    if (keys.length === 0) return 0;
    try {
      const chunks = chunkArray(keys, this.batchConcurrency);
      let count = 0;
      for (const chunk of chunks) {
        await Promise.all(
          chunk.map(async (k) => {
            await this.delete(namespace, k);
            count++;
          })
        );
      }
      return count;
    } catch (err: any) {
      throw new StorageError(`Workers KV deleteMany failed: ${err.message}`, err);
    }
  }

  async has(namespace: string, key: string): Promise<boolean> {
    const val = await this.get(namespace, key);
    return val !== null;
  }

  async clear(namespace: string): Promise<void> {
    try {
      let cursor: string | undefined = undefined;
      do {
        const page = await this.list(namespace, { limit: 1000, cursor });
        if (page.keys.length > 0) {
          await this.deleteMany(namespace, page.keys);
        }
        cursor = page.complete ? undefined : page.cursor;
      } while (cursor);
    } catch (err: any) {
      throw new StorageError(`Workers KV clear failed for namespace '${namespace}': ${err.message}`, err);
    }
  }

  async list(namespace: string, options?: DriverListOptions): Promise<DriverListResult> {
    try {
      const nsPrefix = buildPrefix(namespace, this.delimiter);
      const searchPrefix = options?.prefix ? `${nsPrefix}${options.prefix}` : nsPrefix;
      const limit = Math.max(1, Math.min(options?.limit ?? 1000, 1000));

      const res = await this.kv.list({
        prefix: searchPrefix,
        limit,
        cursor: options?.cursor,
      });

      const activeKeys = res.keys.filter((k) => {
        const meta = k.metadata as { expiresAt?: number } | undefined;
        if (meta?.expiresAt && Date.now() > meta.expiresAt) {
          return false;
        }
        return true;
      });
      const keys = activeKeys.map((k) => this.fromStorageKey(namespace, k.name));
      return {
        keys,
        cursor: res.list_complete ? undefined : res.cursor,
        complete: res.list_complete,
      };
    } catch (err: any) {
      throw new StorageError(`Workers KV list failed for namespace '${namespace}': ${err.message}`, err);
    }
  }

  /**
   * Scans and retrieves all keys and values matching prefix, automatically paginating until complete.
   */
  async getByPrefix(
    namespace: string,
    prefix = ""
  ): Promise<Map<string, string>> {
    const map = new Map<string, string>();
    let cursor: string | undefined = undefined;

    do {
      const page = await this.list(namespace, { prefix, limit: 1000, cursor });
      if (page.keys.length > 0) {
        const values = await this.getMany(namespace, page.keys);
        for (let i = 0; i < page.keys.length; i++) {
          const val = values[i];
          if (val !== null && val !== undefined) {
            map.set(page.keys[i]!, val);
          }
        }
      }
      cursor = page.complete ? undefined : page.cursor;
    } while (cursor);

    return map;
  }
}
