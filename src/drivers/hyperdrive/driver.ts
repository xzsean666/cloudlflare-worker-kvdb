import type {
  Driver,
  DriverCapabilities,
  DriverListOptions,
  DriverListResult,
  DriverSetItem,
} from "../types.js";
import { getMonotonicNow } from "../../core/clock.js";
import { chunkArray } from "../../core/chunker.js";
import { StorageError } from "../../core/errors.js";

export interface HyperdriveClient {
  query<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<T[]>;
  exec(sql: string, params?: unknown[]): Promise<{ rowCount: number }>;
}

export interface HyperdriveDriverOptions {
  /**
   * Cloudflare Hyperdrive binding (env.HYPERDRIVE).
   */
  hyperdrive?: Hyperdrive;

  /**
   * Database connection string, automatically read from hyperdrive.connectionString if available.
   */
  connectionString?: string;

  /**
   * Active database client (e.g. postgres.js, neon serverless, or pg Pool).
   */
  client?: HyperdriveClient;

  /**
   * Storage table name. Default: "_cf_kvdb".
   */
  tableName?: string;
}

export const DEFAULT_HYPERDRIVE_TABLE = "_cf_kvdb";

/**
 * Converts SQLite-style '?' placeholders into PostgreSQL positional markers ($1, $2, ...).
 */
export function toPostgresSql(sql: string): string {
  let paramIndex = 1;
  return sql.replace(/\?/g, () => `$${paramIndex++}`);
}

/**
 * Cloudflare Hyperdrive Postgres Driver.
 *
 * Connects to external PostgreSQL or MySQL databases using Cloudflare's edge TCP socket
 * connection pooling (`env.HYPERDRIVE`), providing low-latency edge-accelerated queries.
 */
export class HyperdriveDriver implements Driver {
  public readonly name = "hyperdrive";
  public readonly capabilities: DriverCapabilities = {
    supportsTTL: true,
    supportsBatch: true,
    supportsSessions: false,
    supportsJSONQuery: true,
    supportsIndexes: true,
    supportsTransactions: true,
  };

  public readonly tableName: string;
  public readonly hyperdrive?: Hyperdrive;
  public readonly connectionString?: string;
  private client?: HyperdriveClient;
  private isInitialized = false;

  constructor(options: HyperdriveDriverOptions = {}) {
    this.tableName = options.tableName ?? DEFAULT_HYPERDRIVE_TABLE;
    this.hyperdrive = options.hyperdrive;
    this.connectionString = options.connectionString ?? options.hyperdrive?.connectionString;
    this.client = options.client;
  }

  /**
   * Attaches or updates the database client connection.
   */
  setClient(client: HyperdriveClient): void {
    this.client = client;
  }

  private getClient(): HyperdriveClient {
    if (!this.client) {
      throw new StorageError(
        "HyperdriveDriver requires an active HyperdriveClient to execute database queries",
        "MISSING_CLIENT"
      );
    }
    return this.client;
  }

  /**
   * Initializes PostgreSQL tables and B-Tree indexes.
   */
  async init(): Promise<void> {
    if (this.isInitialized) return;
    const client = this.getClient();
    try {
      await client.exec(`
        CREATE TABLE IF NOT EXISTS ${this.tableName} (
          namespace VARCHAR(255) NOT NULL,
          key VARCHAR(255) NOT NULL,
          value TEXT NOT NULL,
          expires_at BIGINT,
          created_at BIGINT NOT NULL,
          updated_at BIGINT NOT NULL,
          PRIMARY KEY (namespace, key)
        );
      `);
      await client.exec(`CREATE INDEX IF NOT EXISTS idx_${this.tableName}_expires ON ${this.tableName} (expires_at);`);
      await client.exec(`CREATE INDEX IF NOT EXISTS idx_${this.tableName}_cursor ON ${this.tableName} (namespace, created_at, key);`);
      this.isInitialized = true;
    } catch (err: any) {
      throw new StorageError(`Failed to initialize Hyperdrive database tables: ${err.message}`, err);
    }
  }

  async get(namespace: string, key: string): Promise<string | null> {
    await this.init();
    const client = this.getClient();
    const now = getMonotonicNow();

    const sql = `
      SELECT value FROM ${this.tableName}
      WHERE namespace = $1 AND key = $2 AND (expires_at IS NULL OR expires_at > $3)
      LIMIT 1;
    `;

    const rows = await client.query<{ value: string }>(sql, [namespace, key, now]);
    return rows.length > 0 ? rows[0]!.value : null;
  }

  async getMany(namespace: string, keys: readonly string[]): Promise<(string | null)[]> {
    if (keys.length === 0) return [];
    await this.init();
    const client = this.getClient();
    const now = getMonotonicNow();
    const results = new Map<string, string>();

    for (const chunk of chunkArray(keys, 100)) {
      const placeholders = chunk.map((_, i) => `$${i + 2}`).join(", ");
      const sql = `
        SELECT key, value FROM ${this.tableName}
        WHERE namespace = $1 AND key IN (${placeholders}) AND (expires_at IS NULL OR expires_at > $${chunk.length + 2});
      `;
      const rows = await client.query<{ key: string; value: string }>(sql, [
        namespace,
        ...chunk,
        now,
      ]);
      for (const row of rows) {
        results.set(row.key, row.value);
      }
    }

    return keys.map((k) => results.get(k) ?? null);
  }

  async set(namespace: string, key: string, value: string, ttlSeconds?: number): Promise<void> {
    await this.init();
    const client = this.getClient();
    const now = getMonotonicNow();
    const expiresAt = ttlSeconds ? now + ttlSeconds * 1000 : null;

    const sql = `
      INSERT INTO ${this.tableName} (namespace, key, value, expires_at, created_at, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (namespace, key) DO UPDATE SET
        value = EXCLUDED.value,
        expires_at = EXCLUDED.expires_at,
        updated_at = EXCLUDED.updated_at;
    `;

    await client.exec(sql, [namespace, key, value, expiresAt, now, now]);
  }

  async setMany(namespace: string, entries: readonly DriverSetItem[]): Promise<void> {
    if (entries.length === 0) return;
    await this.init();
    const client = this.getClient();
    const now = getMonotonicNow();

    // Chunk into 50 rows per batch (300 parameters per statement) for high performance
    const chunks = chunkArray(entries, 50);
    for (const chunk of chunks) {
      const valuePlaceholders: string[] = [];
      const params: unknown[] = [];
      let pIdx = 1;

      for (const item of chunk) {
        const expiresAt = item.ttlSeconds ? now + item.ttlSeconds * 1000 : null;
        valuePlaceholders.push(`($${pIdx}, $${pIdx + 1}, $${pIdx + 2}, $${pIdx + 3}, $${pIdx + 4}, $${pIdx + 5})`);
        params.push(namespace, item.key, item.value, expiresAt, now, now);
        pIdx += 6;
      }

      const sql = `
        INSERT INTO ${this.tableName} (namespace, key, value, expires_at, created_at, updated_at)
        VALUES ${valuePlaceholders.join(", ")}
        ON CONFLICT (namespace, key) DO UPDATE SET
          value = EXCLUDED.value,
          expires_at = EXCLUDED.expires_at,
          updated_at = EXCLUDED.updated_at;
      `;

      await client.exec(sql, params);
    }
  }

  async delete(namespace: string, key: string): Promise<boolean> {
    await this.init();
    const client = this.getClient();
    const sql = `DELETE FROM ${this.tableName} WHERE namespace = $1 AND key = $2;`;
    const res = await client.exec(sql, [namespace, key]);
    return res.rowCount > 0;
  }

  async deleteMany(namespace: string, keys: readonly string[]): Promise<number> {
    if (keys.length === 0) return 0;
    await this.init();
    const client = this.getClient();
    let totalDeleted = 0;

    for (const chunk of chunkArray(keys, 100)) {
      const placeholders = chunk.map((_, i) => `$${i + 2}`).join(", ");
      const sql = `DELETE FROM ${this.tableName} WHERE namespace = $1 AND key IN (${placeholders});`;
      const res = await client.exec(sql, [namespace, ...chunk]);
      totalDeleted += res.rowCount;
    }

    return totalDeleted;
  }

  async has(namespace: string, key: string): Promise<boolean> {
    await this.init();
    const client = this.getClient();
    const now = getMonotonicNow();

    const sql = `
      SELECT 1 FROM ${this.tableName}
      WHERE namespace = $1 AND key = $2 AND (expires_at IS NULL OR expires_at > $3)
      LIMIT 1;
    `;

    const rows = await client.query(sql, [namespace, key, now]);
    return rows.length > 0;
  }

  async clear(namespace: string): Promise<void> {
    await this.init();
    const client = this.getClient();
    const sql = `DELETE FROM ${this.tableName} WHERE namespace = $1;`;
    await client.exec(sql, [namespace]);
  }

  async list(namespace: string, options?: DriverListOptions): Promise<DriverListResult> {
    await this.init();
    const client = this.getClient();
    const now = getMonotonicNow();
    const limit = options?.limit ?? 1000;
    const fetchLimit = limit + 1;

    let sql = `
      SELECT key, created_at FROM ${this.tableName}
      WHERE namespace = $1 AND (expires_at IS NULL OR expires_at > $2)
    `;
    const params: unknown[] = [namespace, now];
    let paramIndex = 3;

    if (options?.prefix) {
      const escaped = options.prefix.replace(/[%_\\]/g, "\\$&");
      sql += ` AND key LIKE $${paramIndex++} ESCAPE '\\'`;
      params.push(`${escaped}%`);
    }

    if (options?.cursor) {
      try {
        const decoded = JSON.parse(atob(options.cursor));
        sql += ` AND (created_at > $${paramIndex++} OR (created_at = $${paramIndex++} AND key > $${paramIndex++}))`;
        params.push(decoded.createdAt, decoded.createdAt, decoded.key);
      } catch {
        // Ignore invalid cursor
      }
    }

    sql += ` ORDER BY created_at ASC, key ASC LIMIT $${paramIndex++};`;
    params.push(fetchLimit);

    const rows = await client.query<{ key: string; created_at: string | number }>(sql, params);

    const hasMore = rows.length > limit;
    const resultRows = hasMore ? rows.slice(0, limit) : rows;

    let nextCursor: string | undefined = undefined;
    if (hasMore && resultRows.length > 0) {
      const last = resultRows[resultRows.length - 1]!;
      nextCursor = btoa(JSON.stringify({ createdAt: Number(last.created_at), key: last.key }));
    }

    return {
      keys: resultRows.map((r) => r.key),
      cursor: nextCursor,
      complete: !hasMore,
    };
  }
}
