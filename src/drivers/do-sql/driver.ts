import type {
  Driver,
  DriverCapabilities,
  DriverListOptions,
  DriverListResult,
  DriverSetItem,
} from "../types.js";
import { getMonotonicNow } from "../../core/clock.js";
import { chunkArray } from "../../core/chunker.js";
import { StorageError, KVDBError } from "../../core/errors.js";

export interface DurableObjectSqlDriverOptions {
  tableName?: string;
}

export const DEFAULT_DO_SQL_TABLE = "_cf_kvdb";
const IDENTIFIER_REGEX = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * Cloudflare Durable Objects SQLite Driver backed by `ctx.storage.sql`.
 *
 * Provides strongly consistent, ultra-low latency, transactional in-actor storage
 * for Cloudflare SQLite-backed Durable Objects.
 */
export class DurableObjectSqlDriver implements Driver {
  public readonly name = "do-sql";
  public readonly capabilities: DriverCapabilities = {
    supportsTTL: true,
    supportsBatch: true,
    supportsSessions: false,
    supportsJSONQuery: true,
    supportsIndexes: true,
    supportsTransactions: true,
  };

  public readonly tableName: string;
  private isInitialized = false;

  constructor(
    private readonly sql: SqlStorage,
    options: DurableObjectSqlDriverOptions = {}
  ) {
    if (!sql) {
      throw new StorageError("DurableObjectSqlDriver requires a valid SqlStorage binding (ctx.storage.sql)", "INVALID_CONFIG");
    }
    const tName = options.tableName ?? DEFAULT_DO_SQL_TABLE;
    if (!IDENTIFIER_REGEX.test(tName)) {
      throw new KVDBError(`Invalid table name identifier "${tName}"`, "INVALID_SCHEMA");
    }
    this.tableName = tName;
  }

  /**
   * Returns the underlying Cloudflare SqlStorage interface.
   */
  public getSql(): SqlStorage {
    return this.sql;
  }

  /**
   * Initializes the SQLite table and indexes inside the Durable Object.
   */
  async init(): Promise<void> {
    if (this.isInitialized) return;
    try {
      this.sql.exec(`
        CREATE TABLE IF NOT EXISTS ${this.tableName} (
          namespace TEXT NOT NULL,
          key TEXT NOT NULL,
          value TEXT NOT NULL,
          expires_at INTEGER,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          PRIMARY KEY (namespace, key)
        );
      `);
      this.sql.exec(`CREATE INDEX IF NOT EXISTS idx_${this.tableName}_expires ON ${this.tableName} (expires_at);`);
      this.sql.exec(`CREATE INDEX IF NOT EXISTS idx_${this.tableName}_cursor ON ${this.tableName} (namespace, created_at, key);`);
      this.isInitialized = true;
    } catch (err: any) {
      throw new StorageError(`Failed to initialize Durable Object SQLite storage: ${err.message}`, err);
    }
  }

  /**
   * Executes a callback within an explicit ACID transaction (BEGIN / COMMIT / ROLLBACK).
   */
  async transaction<T>(fn: () => T | Promise<T>): Promise<T> {
    await this.init();
    this.sql.exec("BEGIN;");
    try {
      const result = await fn();
      this.sql.exec("COMMIT;");
      return result;
    } catch (err) {
      try {
        this.sql.exec("ROLLBACK;");
      } catch {
        // Suppress rollback errors if transaction already terminated
      }
      throw err;
    }
  }

  async get(namespace: string, key: string): Promise<string | null> {
    await this.init();
    const now = getMonotonicNow();
    const sql = `
      SELECT value FROM ${this.tableName}
      WHERE namespace = ? AND key = ? AND (expires_at IS NULL OR expires_at > ?)
      LIMIT 1;
    `;
    const cursor = this.sql.exec<{ value: string }>(sql, namespace, key, now);
    const rows = cursor.toArray();
    return rows.length > 0 ? rows[0]!.value : null;
  }

  async getMany(namespace: string, keys: readonly string[]): Promise<(string | null)[]> {
    if (keys.length === 0) return [];
    await this.init();
    const now = getMonotonicNow();
    const results = new Map<string, string>();

    for (const chunk of chunkArray(keys, 50)) {
      const placeholders = chunk.map(() => "?").join(", ");
      const sql = `
        SELECT key, value FROM ${this.tableName}
        WHERE namespace = ? AND key IN (${placeholders}) AND (expires_at IS NULL OR expires_at > ?);
      `;
      const cursor = this.sql.exec<{ key: string; value: string }>(sql, namespace, ...chunk, now);
      for (const row of cursor.toArray()) {
        results.set(row.key, row.value);
      }
    }

    return keys.map((k) => results.get(k) ?? null);
  }

  async set(namespace: string, key: string, value: string, ttlSeconds?: number): Promise<void> {
    await this.init();
    const now = getMonotonicNow();
    const expiresAt = ttlSeconds ? now + ttlSeconds * 1000 : null;

    const sql = `
      INSERT INTO ${this.tableName} (namespace, key, value, expires_at, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT (namespace, key) DO UPDATE SET
        value = excluded.value,
        expires_at = excluded.expires_at,
        updated_at = excluded.updated_at;
    `;

    this.sql.exec(sql, namespace, key, value, expiresAt, now, now);
  }

  async setMany(namespace: string, entries: readonly DriverSetItem[]): Promise<void> {
    if (entries.length === 0) return;
    await this.init();

    await this.transaction(() => {
      const now = getMonotonicNow();
      const sql = `
        INSERT INTO ${this.tableName} (namespace, key, value, expires_at, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (namespace, key) DO UPDATE SET
          value = excluded.value,
          expires_at = excluded.expires_at,
          updated_at = excluded.updated_at;
      `;

      for (const entry of entries) {
        const expiresAt = entry.ttlSeconds ? now + entry.ttlSeconds * 1000 : null;
        this.sql.exec(sql, namespace, entry.key, entry.value, expiresAt, now, now);
      }
    });
  }

  async delete(namespace: string, key: string): Promise<boolean> {
    await this.init();
    const sql = `DELETE FROM ${this.tableName} WHERE namespace = ? AND key = ?;`;
    const cursor = this.sql.exec(sql, namespace, key);
    return cursor.rowsWritten > 0;
  }

  async deleteMany(namespace: string, keys: readonly string[]): Promise<number> {
    if (keys.length === 0) return 0;
    await this.init();
    let totalDeleted = 0;

    for (const chunk of chunkArray(keys, 50)) {
      const placeholders = chunk.map(() => "?").join(", ");
      const sql = `DELETE FROM ${this.tableName} WHERE namespace = ? AND key IN (${placeholders});`;
      const cursor = this.sql.exec(sql, namespace, ...chunk);
      totalDeleted += cursor.rowsWritten;
    }

    return totalDeleted;
  }

  async has(namespace: string, key: string): Promise<boolean> {
    await this.init();
    const now = getMonotonicNow();
    const sql = `
      SELECT 1 FROM ${this.tableName}
      WHERE namespace = ? AND key = ? AND (expires_at IS NULL OR expires_at > ?)
      LIMIT 1;
    `;
    const cursor = this.sql.exec(sql, namespace, key, now);
    return cursor.toArray().length > 0;
  }

  async clear(namespace: string): Promise<void> {
    await this.init();
    const sql = `DELETE FROM ${this.tableName} WHERE namespace = ?;`;
    this.sql.exec(sql, namespace);
  }

  async list(namespace: string, options?: DriverListOptions): Promise<DriverListResult> {
    await this.init();
    const now = getMonotonicNow();
    const limit = options?.limit ?? 1000;
    const fetchLimit = limit + 1;

    let sql = `
      SELECT key, created_at FROM ${this.tableName}
      WHERE namespace = ? AND (expires_at IS NULL OR expires_at > ?)
    `;
    const params: unknown[] = [namespace, now];

    if (options?.prefix) {
      const escapedPrefix = options.prefix.replace(/[%_\\]/g, "\\$&");
      sql += ` AND key LIKE ? ESCAPE '\\'`;
      params.push(`${escapedPrefix}%`);
    }

    if (options?.cursor) {
      try {
        const decoded = JSON.parse(atob(options.cursor));
        sql += ` AND (created_at > ? OR (created_at = ? AND key > ?))`;
        params.push(decoded.createdAt, decoded.createdAt, decoded.key);
      } catch {
        // Ignore invalid cursor
      }
    }

    sql += ` ORDER BY created_at ASC, key ASC LIMIT ?;`;
    params.push(fetchLimit);

    const cursor = this.sql.exec<{ key: string; created_at: number }>(sql, ...params);
    const rows = cursor.toArray();

    const hasMore = rows.length > limit;
    const resultRows = hasMore ? rows.slice(0, limit) : rows;

    let nextCursor: string | undefined = undefined;
    if (hasMore && resultRows.length > 0) {
      const last = resultRows[resultRows.length - 1]!;
      nextCursor = btoa(JSON.stringify({ createdAt: last.created_at, key: last.key }));
    }

    return {
      keys: resultRows.map((r) => r.key),
      cursor: nextCursor,
      complete: !hasMore,
    };
  }
}
