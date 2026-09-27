import type { Driver, DriverCapabilities, DriverListOptions, DriverListResult, DriverSetItem } from "../types.js";
import { D1SessionManager } from "./sessions.js";
import { D1SqlBuilder, DEFAULT_KVDB_TABLE } from "./sql-builder.js";
import { getMonotonicNow } from "../../core/clock.js";
import { chunkArray, chunkByParamCount } from "../../core/chunker.js";
import { StorageError } from "../../core/errors.js";

export interface D1DriverOptions {
  tableName?: string;
  bookmark?: string | null;
}

export class D1Driver implements Driver {
  public readonly name = "d1";
  public readonly capabilities: DriverCapabilities = {
    supportsTTL: true,
    supportsBatch: true,
    supportsSessions: true,
    supportsJSONQuery: true,
    supportsIndexes: true,
  };

  private readonly sqlBuilder: D1SqlBuilder;
  private readonly sessionManager: D1SessionManager;
  private isInitialized = false;

  constructor(
    private readonly rawDb: D1Database,
    private readonly options: D1DriverOptions = {}
  ) {
    this.sqlBuilder = new D1SqlBuilder(options.tableName ?? DEFAULT_KVDB_TABLE);
    this.sessionManager = new D1SessionManager(options.bookmark);
  }

  /**
   * Returns the D1Database instance, wrapped with the active session bookmark if applicable.
   */
  public getDb(): D1Database {
    return this.sessionManager.wrapDatabase(this.rawDb);
  }

  /**
   * Initializes the database schema and indexes.
   */
  async init(): Promise<void> {
    if (this.isInitialized) return;
    try {
      const sqlStatements = this.sqlBuilder.buildBootstrapSql();
      const stmts = sqlStatements.map((sql) => this.rawDb.prepare(sql));
      await this.rawDb.batch(stmts);
      this.isInitialized = true;
    } catch (err: any) {
      throw new StorageError(`Failed to initialize D1 database: ${err.message}`, err);
    }
  }

  /**
   * Retrieves the current D1 session bookmark.
   */
  getBookmark(): string | null {
    return this.sessionManager.getBookmark();
  }

  /**
   * Manually sets or advances the active D1 session bookmark.
   */
  setBookmark(bookmark: string | null): void {
    this.sessionManager.setBookmark(bookmark);
  }

  /**
   * Returns a new D1Driver instance configured with a specific session bookmark.
   */
  withSession(bookmark?: string | null): D1Driver {
    return new D1Driver(this.rawDb, {
      tableName: this.sqlBuilder.tableName,
      bookmark: bookmark ?? this.sessionManager.getBookmark(),
    });
  }

  async get(namespace: string, key: string): Promise<string | null> {
    await this.init();
    try {
      const now = getMonotonicNow();
      const stmt = this.getDb().prepare(this.sqlBuilder.buildGetSql()).bind(namespace, key, now);
      const row = await stmt.first<{ value: string }>();
      return row ? row.value : null;
    } catch (err: any) {
      throw new StorageError(`D1 get failed for key '${key}': ${err.message}`, err);
    }
  }

  async getMany(namespace: string, keys: readonly string[]): Promise<(string | null)[]> {
    if (keys.length === 0) return [];
    await this.init();

    try {
      const now = getMonotonicNow();
      // Chunk keys by 75 (1 namespace + 75 keys + 1 now = 77 parameters <= 100)
      const keyChunks = chunkArray(keys, 75);
      const stmts = keyChunks.map((chunk) => {
        const sql = this.sqlBuilder.buildGetManySql(chunk.length);
        return this.getDb().prepare(sql).bind(namespace, ...chunk, now);
      });

      const batchResults = await this.getDb().batch<{ key: string; value: string }>(stmts);
      const resultMap = new Map<string, string>();

      for (const res of batchResults) {
        if (res.results) {
          for (const row of res.results) {
            resultMap.set(row.key, row.value);
          }
        }
      }

      return keys.map((k) => resultMap.get(k) ?? null);
    } catch (err: any) {
      throw new StorageError(`D1 getMany failed: ${err.message}`, err);
    }
  }

  async set(namespace: string, key: string, value: string, ttlSeconds?: number): Promise<void> {
    await this.init();
    try {
      const now = getMonotonicNow();
      const expiresAt = ttlSeconds ? now + ttlSeconds * 1000 : null;
      const stmt = this.getDb()
        .prepare(this.sqlBuilder.buildSetSql())
        .bind(namespace, key, value, expiresAt, now, now);

      const res = await stmt.run();
      // If D1 returned a bookmark in meta or headers, record it
      if ((res as any).meta?.bookmark) {
        this.sessionManager.setBookmark((res as any).meta.bookmark);
      }
    } catch (err: any) {
      throw new StorageError(`D1 set failed for key '${key}': ${err.message}`, err);
    }
  }

  async setMany(namespace: string, entries: readonly DriverSetItem[]): Promise<void> {
    if (entries.length === 0) return;
    await this.init();

    try {
      const now = getMonotonicNow();
      // 6 parameters per row. Chunk by 12 items -> 72 parameters per statement <= 100 limit
      const itemChunks = chunkByParamCount(entries, 6, 80);
      const stmts: D1PreparedStatement[] = [];

      for (const chunk of itemChunks) {
        const sql = this.sqlBuilder.buildMultiRowSetSql(chunk.length);
        const params: unknown[] = [];
        for (const item of chunk) {
          const expiresAt = item.ttlSeconds ? now + item.ttlSeconds * 1000 : null;
          params.push(namespace, item.key, item.value, expiresAt, now, now);
        }
        stmts.push(this.getDb().prepare(sql).bind(...params));
      }

      // Execute all chunked statements atomically via D1 batch
      const results = await this.getDb().batch(stmts);
      const lastResult = results[results.length - 1];
      if ((lastResult as any)?.meta?.bookmark) {
        this.sessionManager.setBookmark((lastResult as any).meta.bookmark);
      }
    } catch (err: any) {
      throw new StorageError(`D1 setMany failed: ${err.message}`, err);
    }
  }

  async delete(namespace: string, key: string): Promise<boolean> {
    await this.init();
    try {
      const stmt = this.getDb().prepare(this.sqlBuilder.buildDeleteSql()).bind(namespace, key);
      const res = await stmt.run();
      if ((res as any).meta?.bookmark) {
        this.sessionManager.setBookmark((res as any).meta.bookmark);
      }
      return (res.meta.changes ?? 0) > 0;
    } catch (err: any) {
      throw new StorageError(`D1 delete failed for key '${key}': ${err.message}`, err);
    }
  }

  async deleteMany(namespace: string, keys: readonly string[]): Promise<number> {
    if (keys.length === 0) return 0;
    await this.init();

    try {
      // Chunk keys by 75 (1 namespace + 75 keys = 76 parameters <= 100)
      const keyChunks = chunkArray(keys, 75);
      const stmts = keyChunks.map((chunk) => {
        const sql = this.sqlBuilder.buildDeleteManySql(chunk.length);
        return this.getDb().prepare(sql).bind(namespace, ...chunk);
      });

      const batchResults = await this.getDb().batch(stmts);
      const lastResult = batchResults[batchResults.length - 1];
      if ((lastResult as any)?.meta?.bookmark) {
        this.sessionManager.setBookmark((lastResult as any).meta.bookmark);
      }
      let totalDeleted = 0;
      for (const res of batchResults) {
        totalDeleted += res.meta.changes ?? 0;
      }
      return totalDeleted;
    } catch (err: any) {
      throw new StorageError(`D1 deleteMany failed: ${err.message}`, err);
    }
  }

  async has(namespace: string, key: string): Promise<boolean> {
    await this.init();
    try {
      const now = getMonotonicNow();
      const stmt = this.getDb().prepare(this.sqlBuilder.buildHasSql()).bind(namespace, key, now);
      const row = await stmt.first();
      return row !== null;
    } catch (err: any) {
      throw new StorageError(`D1 has failed for key '${key}': ${err.message}`, err);
    }
  }

  async clear(namespace: string): Promise<void> {
    await this.init();
    try {
      const stmt = this.getDb().prepare(this.sqlBuilder.buildClearSql()).bind(namespace);
      await stmt.run();
    } catch (err: any) {
      throw new StorageError(`D1 clear failed for namespace '${namespace}': ${err.message}`, err);
    }
  }

  async list(namespace: string, options?: DriverListOptions): Promise<DriverListResult> {
    await this.init();
    try {
      const now = getMonotonicNow();
      const limit = Math.max(1, options?.limit ?? 100);
      // Fetch 1 extra to determine whether more keys exist
      const queryLimit = limit + 1;
      const hasPrefix = Boolean(options?.prefix);
      const hasCursor = Boolean(options?.cursor);

      const { sql } = this.sqlBuilder.buildListQuery(hasPrefix, hasCursor);
      const params: unknown[] = [namespace, now];
      if (hasPrefix) {
        const escapedPrefix = options!.prefix!.replace(/[%_\\]/g, "\\$&");
        params.push(`${escapedPrefix}%`);
      }
      if (hasCursor) {
        params.push(options!.cursor);
      }
      params.push(queryLimit);

      const stmt = this.getDb().prepare(sql).bind(...params);
      const rows = (await stmt.all<{ key: string }>()).results;

      const complete = rows.length <= limit;
      const keys = rows.slice(0, limit).map((r) => r.key);
      const nextCursor = complete ? undefined : keys[keys.length - 1];

      return {
        keys,
        cursor: nextCursor,
        complete,
      };
    } catch (err: any) {
      throw new StorageError(`D1 list failed for namespace '${namespace}': ${err.message}`, err);
    }
  }
}
