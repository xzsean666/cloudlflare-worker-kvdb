import type { Driver, DriverListOptions, DriverListResult } from "../drivers/types.js";
import { D1Driver } from "../drivers/d1/driver.js";
import { DurableObjectSqlDriver } from "../drivers/do-sql/driver.js";
import { serialize, deserialize, isBlobDescriptor } from "./serializer.js";
import { validateKey } from "./key.js";
import { getMonotonicNow } from "./clock.js";
import { chunkArray, buildInClausePlaceholders, assertD1ParamLimit } from "./chunker.js";
import {
  type TableSchema,
  type NormalizedSchema,
  type KeyDefinition,
  normalizeTableSchema,
  generateCreateTableSql,
  generateIndexSqls,
  generateAddColumnSql,
} from "./schema.js";
import { parseWhere } from "../query/parser.js";
import { compileWhere, compileOrderBy } from "../query/compiler.js";
import type { SortSpec, QueryOptions } from "../query/ast.js";
import { KVDBError, StorageError } from "./errors.js";
import { R2BlobOverflowManager } from "../drivers/r2/overflow.js";
import { WriteBatcher, type AutoBatchConfig } from "./batcher.js";

export interface SetOptions {
  keys?: Record<string, unknown>;
  ttlSeconds?: number;
}

export interface TableOptions {
  defaultTTL?: number;
  schema?: TableSchema;
  r2Bucket?: R2Bucket;
  overflowThresholdBytes?: number;
  blobOverflow?: R2BlobOverflowManager;
  cleanOrphanBlobsOnUpdate?: boolean;
  autoBatch?: boolean | AutoBatchConfig;
  ctx?: ExecutionContext;
}

export interface TableSetItem<V = unknown> {
  key: string;
  value: V;
  keys?: Record<string, unknown>;
  ttlSeconds?: number;
}

export interface PageResult<T> {
  items: T[];
  cursor?: string;
  complete: boolean;
}

/**
 * Type-safe Table facade providing CRUD, batch, prefix, schema, and query operations.
 */
export class Table<V = unknown> {
  private normalizedSchema?: NormalizedSchema;
  private physicalTableName?: string;
  private isSchemaInitialized = false;
  private blobOverflow?: R2BlobOverflowManager;
  private writeBatcher?: WriteBatcher<V>;

  constructor(
    public readonly name: string,
    public readonly driver: Driver,
    public readonly options: TableOptions = {}
  ) {
    if (options.schema) {
      this.normalizedSchema = normalizeTableSchema(options.schema);
      this.physicalTableName = this.normalizedSchema.tableName ?? `_kvdb_t_${this.name.replace(/[^A-Za-z0-9_]/g, "_")}`;
    }
    if (options.blobOverflow) {
      this.blobOverflow = options.blobOverflow;
    } else if (options.r2Bucket) {
      this.blobOverflow = new R2BlobOverflowManager({
        bucket: options.r2Bucket,
        thresholdBytes: options.overflowThresholdBytes,
        prefix: `__blobs/${this.name}`,
      });
    }
    if (options.autoBatch) {
      const autoBatchConfig: AutoBatchConfig = {
        ctx: options.ctx,
        ...(typeof options.autoBatch === "object" ? options.autoBatch : {}),
      };
      this.writeBatcher = new WriteBatcher<V>(
        async (items) => this.setMany(items),
        autoBatchConfig
      );
    }
  }

  /**
   * Returns the configured R2 blob overflow manager, if any.
   */
  getBlobOverflow(): R2BlobOverflowManager | undefined {
    return this.blobOverflow;
  }

  /**
   * Immediately flushes any pending writes in the auto-batch buffer.
   */
  async flush(): Promise<void> {
    if (this.writeBatcher) {
      await this.writeBatcher.flush();
    }
  }

  /**
   * Returns the count of pending writes waiting in the auto-batch buffer.
   */
  get pendingBatchCount(): number {
    return this.writeBatcher?.pendingCount ?? 0;
  }

  /**
   * Serializes a value for storage, offloading to R2 if larger than threshold.
   */
  private async serializeForStorage(val: V, key?: string): Promise<string> {
    let serialized = serialize(val);
    if (this.blobOverflow && this.blobOverflow.shouldOverflow(serialized)) {
      const descriptor = await this.blobOverflow.writeBlob(serialized, { key });
      serialized = serialize(descriptor);
    }
    return serialized;
  }

  /**
   * Resolves a deserialized stored value, reconstituting from R2 if it is a blob descriptor.
   */
  private async resolveStoredValue(parsed: unknown): Promise<V | null> {
    if (parsed === null || parsed === undefined) return null;
    if (isBlobDescriptor(parsed)) {
      if (!this.blobOverflow) {
        throw new StorageError(
          "Encountered transparent R2 blob descriptor but no R2 blob overflow manager is configured",
          "MISSING_BLOB_MANAGER"
        );
      }
      const text = await this.blobOverflow.readBlob(parsed);
      return deserialize<V>(text);
    }
    return parsed as V;
  }

  /**
   * Identifies R2 blob key if the given record stores a blob descriptor.
   */
  private async getBlobKeyForRecord(key: string): Promise<string | null> {
    if (!this.blobOverflow) return null;
    try {
      let raw: string | null = null;
      if (this.normalizedSchema && this.driver instanceof D1Driver) {
        await this.init();
        const pkName = this.normalizedSchema.primaryKey.name;
        const sql = `SELECT value FROM ${this.physicalTableName} WHERE ${pkName} = ? LIMIT 1;`;
        const row = await this.driver.getDb().prepare(sql).bind(key).first<{ value: string }>();
        raw = row ? row.value : null;
      } else if (this.normalizedSchema && this.driver instanceof DurableObjectSqlDriver) {
        await this.init();
        const pkName = this.normalizedSchema.primaryKey.name;
        const sql = `SELECT value FROM ${this.physicalTableName} WHERE ${pkName} = ? LIMIT 1;`;
        const rows = this.driver.getSql().exec<{ value: string }>(sql, key).toArray();
        raw = rows.length > 0 ? rows[0]!.value : null;
      } else {
        raw = await this.driver.get(this.name, key);
      }

      if (raw) {
        const parsed = deserialize<unknown>(raw);
        if (isBlobDescriptor(parsed)) {
          return parsed.r2Key;
        }
      }
    } catch {
      // Ignore lookup errors
    }
    return null;
  }

  /**
   * Identifies R2 blob keys for multiple records.
   */
  private async getBlobKeysForRecords(keys: readonly string[]): Promise<string[]> {
    if (!this.blobOverflow || keys.length === 0) return [];
    const blobKeys: string[] = [];
    try {
      if (this.normalizedSchema && this.driver instanceof D1Driver) {
        await this.init();
        const pkName = this.normalizedSchema.primaryKey.name;
        const keyChunks = chunkArray(keys, 75);
        const d1Db = this.driver.getDb();
        for (const chunk of keyChunks) {
          const inClause = buildInClausePlaceholders(chunk.length);
          const sql = `SELECT value FROM ${this.physicalTableName} WHERE ${pkName} IN ${inClause};`;
          const res = await d1Db.prepare(sql).bind(...chunk).all<{ value: string }>();
          for (const row of res.results) {
            try {
              const parsed = deserialize<unknown>(row.value);
              if (isBlobDescriptor(parsed)) {
                blobKeys.push(parsed.r2Key);
              }
            } catch {}
          }
        }
      } else if (this.normalizedSchema && this.driver instanceof DurableObjectSqlDriver) {
        await this.init();
        const pkName = this.normalizedSchema.primaryKey.name;
        const keyChunks = chunkArray(keys, 75);
        const doSql = this.driver.getSql();
        for (const chunk of keyChunks) {
          const inClause = buildInClausePlaceholders(chunk.length);
          const sql = `SELECT value FROM ${this.physicalTableName} WHERE ${pkName} IN ${inClause};`;
          const rows = doSql.exec<{ value: string }>(sql, ...chunk).toArray();
          for (const row of rows) {
            try {
              const parsed = deserialize<unknown>(row.value);
              if (isBlobDescriptor(parsed)) {
                blobKeys.push(parsed.r2Key);
              }
            } catch {}
          }
        }
      } else {
        const rawValues = await this.driver.getMany(this.name, keys);
        for (const raw of rawValues) {
          if (raw) {
            try {
              const parsed = deserialize<unknown>(raw);
              if (isBlobDescriptor(parsed)) {
                blobKeys.push(parsed.r2Key);
              }
            } catch {}
          }
        }
      }
    } catch {
      // Ignore lookup errors
    }
    return blobKeys;
  }

  /**
   * Initializes physical schema table and indexes if configured.
   */
  async init(): Promise<void> {
    if (!this.normalizedSchema || this.isSchemaInitialized) return;
    try {
      const createSql = generateCreateTableSql(this.physicalTableName!, this.normalizedSchema);
      const indexSqls = generateIndexSqls(this.physicalTableName!, this.normalizedSchema);

      if (this.driver instanceof D1Driver) {
        const db = this.driver.getDb();
        const stmts = [createSql, ...indexSqls].map((sql) => db.prepare(sql));
        await db.batch(stmts);
        this.isSchemaInitialized = true;
      } else if (this.driver instanceof DurableObjectSqlDriver) {
        const sql = this.driver.getSql();
        sql.exec(createSql);
        for (const idx of indexSqls) {
          sql.exec(idx);
        }
        this.isSchemaInitialized = true;
      }
    } catch (err: any) {
      throw new StorageError(`Failed to initialize physical schema for table '${this.name}': ${err.message}`, err);
    }
  }

  /**
   * Retrieves a typed value by key.
   */
  async get(key: string): Promise<V | null> {
    validateKey(key);
    if (this.writeBatcher && this.writeBatcher.pendingCount > 0) {
      await this.writeBatcher.flush();
    }
    if (this.normalizedSchema && this.driver instanceof D1Driver) {
      await this.init();
      const now = getMonotonicNow();
      const pkName = this.normalizedSchema.primaryKey.name;
      const sql = `SELECT value FROM ${this.physicalTableName} WHERE ${pkName} = ? AND (expires_at IS NULL OR expires_at > ?) LIMIT 1;`;
      const row = await this.driver.getDb().prepare(sql).bind(key, now).first<{ value: string }>();
      if (!row) return null;
      const parsed = deserialize<unknown>(row.value);
      return await this.resolveStoredValue(parsed);
    }

    if (this.normalizedSchema && this.driver instanceof DurableObjectSqlDriver) {
      await this.init();
      const now = getMonotonicNow();
      const pkName = this.normalizedSchema.primaryKey.name;
      const sql = `SELECT value FROM ${this.physicalTableName} WHERE ${pkName} = ? AND (expires_at IS NULL OR expires_at > ?) LIMIT 1;`;
      const rows = this.driver.getSql().exec<{ value: string }>(sql, key, now).toArray();
      if (rows.length === 0) return null;
      const parsed = deserialize<unknown>(rows[0]!.value);
      return await this.resolveStoredValue(parsed);
    }

    const raw = await this.driver.get(this.name, key);
    if (raw === null || raw === undefined) return null;
    const parsed = deserialize<unknown>(raw);
    return await this.resolveStoredValue(parsed);
  }

  /**
   * High-performance point lookup using a secondary column / B-Tree index.
   */
  async getBy(column: string, value: unknown): Promise<V | null> {
    if (this.writeBatcher && this.writeBatcher.pendingCount > 0) {
      await this.writeBatcher.flush();
    }
    if (!this.normalizedSchema || (!(this.driver instanceof D1Driver) && !(this.driver instanceof DurableObjectSqlDriver))) {
      throw new KVDBError("getBy() requires a Table with a defined physical schema on a D1 or DO-SQL driver", "UNSUPPORTED_OPERATION");
    }
    const isPk = this.normalizedSchema.primaryKey.name === column;
    const isCol = Boolean(this.normalizedSchema.columns[column]);
    if (!isPk && !isCol) {
      throw new KVDBError(`Unknown or unindexed column "${column}" for table "${this.name}"`, "INVALID_COLUMN");
    }
    await this.init();
    const now = getMonotonicNow();
    const sql = `SELECT value FROM ${this.physicalTableName} WHERE ${column} = ? AND (expires_at IS NULL OR expires_at > ?) LIMIT 1;`;

    if (this.driver instanceof D1Driver) {
      const row = await this.driver.getDb().prepare(sql).bind(value, now).first<{ value: string }>();
      if (!row) return null;
      const parsed = deserialize<unknown>(row.value);
      return await this.resolveStoredValue(parsed);
    } else {
      const rows = (this.driver as DurableObjectSqlDriver).getSql().exec<{ value: string }>(sql, value, now).toArray();
      if (rows.length === 0) return null;
      const parsed = deserialize<unknown>(rows[0]!.value);
      return await this.resolveStoredValue(parsed);
    }
  }

  /**
   * Retrieves multiple typed values by keys, preserving the input array order.
   */
  async getMany(keys: readonly string[]): Promise<(V | null)[]> {
    if (keys.length === 0) return [];
    if (this.writeBatcher && this.writeBatcher.pendingCount > 0) {
      await this.writeBatcher.flush();
    }
    for (const key of keys) {
      validateKey(key);
    }

    if (this.normalizedSchema && this.driver instanceof D1Driver) {
      await this.init();
      const now = getMonotonicNow();
      const pkName = this.normalizedSchema.primaryKey.name;
      const keyChunks = chunkArray(keys, 75);
      const d1Db = this.driver.getDb();
      const stmts = keyChunks.map((chunk) => {
        const inClause = buildInClausePlaceholders(chunk.length);
        const sql = `SELECT ${pkName} as key, value FROM ${this.physicalTableName} WHERE ${pkName} IN ${inClause} AND (expires_at IS NULL OR expires_at > ?);`;
        return d1Db.prepare(sql).bind(...chunk, now);
      });
      const batchResults = await d1Db.batch<{ key: string; value: string }>(stmts);
      const resultMap = new Map<string, string>();
      for (const res of batchResults) {
        if (res.results) {
          for (const row of res.results) {
            resultMap.set(row.key, row.value);
          }
        }
      }
      return await Promise.all(
        keys.map(async (k) => {
          const raw = resultMap.get(k);
          if (raw === undefined || raw === null) return null;
          const parsed = deserialize<unknown>(raw);
          return await this.resolveStoredValue(parsed);
        })
      );
    }

    if (this.normalizedSchema && this.driver instanceof DurableObjectSqlDriver) {
      await this.init();
      const now = getMonotonicNow();
      const pkName = this.normalizedSchema.primaryKey.name;
      const keyChunks = chunkArray(keys, 75);
      const resultMap = new Map<string, string>();
      for (const chunk of keyChunks) {
        const inClause = buildInClausePlaceholders(chunk.length);
        const sql = `SELECT ${pkName} as key, value FROM ${this.physicalTableName} WHERE ${pkName} IN ${inClause} AND (expires_at IS NULL OR expires_at > ?);`;
        const cursor = this.driver.getSql().exec<{ key: string; value: string }>(sql, ...chunk, now);
        for (const row of cursor.toArray()) {
          resultMap.set(row.key, row.value);
        }
      }
      return await Promise.all(
        keys.map(async (k) => {
          const raw = resultMap.get(k);
          if (raw === undefined || raw === null) return null;
          const parsed = deserialize<unknown>(raw);
          return await this.resolveStoredValue(parsed);
        })
      );
    }

    const rawValues = await this.driver.getMany(this.name, keys);
    return await Promise.all(
      rawValues.map(async (raw) => {
        if (raw === null || raw === undefined) return null;
        const parsed = deserialize<unknown>(raw);
        return await this.resolveStoredValue(parsed);
      })
    );
  }

  /**
   * Stores a typed value with optional TTL and secondary key columns.
   */
  async set(
    keyOrItem: string | TableSetItem<V>,
    value?: V,
    optionsOrTTL?: SetOptions | number
  ): Promise<void> {
    let key: string;
    let val: V;
    let secondaryKeys: Record<string, unknown> | undefined;
    let ttl: number | undefined;

    if (typeof keyOrItem === "object" && keyOrItem !== null) {
      key = keyOrItem.key;
      val = keyOrItem.value;
      secondaryKeys = keyOrItem.keys;
      ttl = keyOrItem.ttlSeconds ?? this.options.defaultTTL;
    } else {
      key = keyOrItem;
      val = value as V;
      if (typeof optionsOrTTL === "number") {
        ttl = optionsOrTTL;
      } else {
        ttl = optionsOrTTL?.ttlSeconds ?? this.options.defaultTTL;
        secondaryKeys = optionsOrTTL?.keys;
      }
    }

    validateKey(key);

    if (this.writeBatcher) {
      return this.writeBatcher.enqueue({
        key,
        value: val,
        keys: secondaryKeys,
        ttlSeconds: ttl,
      });
    }

    let previousBlobKey: string | null = null;
    if (this.blobOverflow && this.options.cleanOrphanBlobsOnUpdate) {
      previousBlobKey = await this.getBlobKeyForRecord(key);
    }

    const serializedValue = await this.serializeForStorage(val, key);

    if (this.normalizedSchema && (this.driver instanceof D1Driver || this.driver instanceof DurableObjectSqlDriver)) {
      await this.init();
      const now = getMonotonicNow();
      const expiresAt = ttl ? now + ttl * 1000 : null;

      const pkName = this.normalizedSchema.primaryKey.name;
      const colNames = Object.keys(this.normalizedSchema.columns);
      const cols = [pkName, ...colNames, "value", "expires_at", "created_at", "updated_at"];

      const params: unknown[] = [key];
      for (const colName of colNames) {
        let colVal = secondaryKeys?.[colName];
        if (colVal === undefined && typeof val === "object" && val !== null) {
          colVal = (val as Record<string, unknown>)[colName];
        }
        params.push(colVal ?? null);
      }
      params.push(serializedValue, expiresAt, now, now);

      const placeholders = new Array(cols.length).fill("?").join(", ");
      const updateSet = cols
        .filter((c) => c !== pkName && c !== "created_at")
        .map((c) => `${c} = excluded.${c}`)
        .join(", ");

      const sql = `INSERT INTO ${this.physicalTableName} (${cols.join(", ")})
        VALUES (${placeholders})
        ON CONFLICT(${pkName}) DO UPDATE SET ${updateSet};`;

      if (this.driver instanceof D1Driver) {
        const res = await this.driver.getDb().prepare(sql).bind(...params).run();
        if ((res as any)?.meta?.bookmark) {
          (this.driver as D1Driver).setBookmark((res as any).meta.bookmark);
        }
      } else {
        (this.driver as DurableObjectSqlDriver).getSql().exec(sql, ...params);
      }
    } else {
      await this.driver.set(this.name, key, serializedValue, ttl);
    }

    // Clean up replaced R2 blob if cleanOrphanBlobsOnUpdate is enabled
    if (previousBlobKey && this.blobOverflow) {
      let newBlobKey: string | null = null;
      try {
        const parsed = deserialize<unknown>(serializedValue);
        if (isBlobDescriptor(parsed)) {
          newBlobKey = parsed.r2Key;
        }
      } catch {}
      if (previousBlobKey !== newBlobKey) {
        try {
          await this.blobOverflow.deleteBlob(previousBlobKey);
        } catch {
          // Suppress cleanup error
        }
      }
    }
  }

  /**
   * Stores multiple typed values.
   */
  async setMany(entries: readonly TableSetItem<V>[]): Promise<void> {
    if (entries.length === 0) return;
    if (this.writeBatcher && this.writeBatcher.pendingCount > 0 && !this.writeBatcher.isFlushingNow) {
      await this.writeBatcher.flush();
    }
    if (this.normalizedSchema && this.driver instanceof D1Driver) {
      await this.init();
      const now = getMonotonicNow();
      const pkName = this.normalizedSchema.primaryKey.name;
      const colNames = Object.keys(this.normalizedSchema.columns);
      const cols = [pkName, ...colNames, "value", "expires_at", "created_at", "updated_at"];
      const placeholders = new Array(cols.length).fill("?").join(", ");
      const updateSet = cols
        .filter((c) => c !== pkName && c !== "created_at")
        .map((c) => `${c} = excluded.${c}`)
        .join(", ");
      const sql = `INSERT INTO ${this.physicalTableName} (${cols.join(", ")})
        VALUES (${placeholders})
        ON CONFLICT(${pkName}) DO UPDATE SET ${updateSet};`;

      const stmts: D1PreparedStatement[] = [];
      for (const entry of entries) {
        validateKey(entry.key);
        const ttl = entry.ttlSeconds ?? this.options.defaultTTL;
        const expiresAt = ttl ? now + ttl * 1000 : null;
        const serializedValue = await this.serializeForStorage(entry.value, entry.key);
        const params: unknown[] = [entry.key];
        for (const colName of colNames) {
          let colVal = entry.keys?.[colName];
          if (colVal === undefined && typeof entry.value === "object" && entry.value !== null) {
            colVal = (entry.value as Record<string, unknown>)[colName];
          }
          params.push(colVal ?? null);
        }
        params.push(serializedValue, expiresAt, now, now);
        stmts.push(this.driver.getDb().prepare(sql).bind(...params));
      }
      const batchResults = await this.driver.getDb().batch(stmts);
      const lastResult = batchResults[batchResults.length - 1];
      if ((lastResult as any)?.meta?.bookmark) {
        (this.driver as D1Driver).setBookmark((lastResult as any).meta.bookmark);
      }
      return;
    }

    if (this.normalizedSchema && this.driver instanceof DurableObjectSqlDriver) {
      await this.init();
      const now = getMonotonicNow();
      const pkName = this.normalizedSchema.primaryKey.name;
      const colNames = Object.keys(this.normalizedSchema.columns);
      const cols = [pkName, ...colNames, "value", "expires_at", "created_at", "updated_at"];
      const placeholders = new Array(cols.length).fill("?").join(", ");
      const updateSet = cols
        .filter((c) => c !== pkName && c !== "created_at")
        .map((c) => `${c} = excluded.${c}`)
        .join(", ");
      const sql = `INSERT INTO ${this.physicalTableName} (${cols.join(", ")})
        VALUES (${placeholders})
        ON CONFLICT(${pkName}) DO UPDATE SET ${updateSet};`;

      const sqlStorage = (this.driver as DurableObjectSqlDriver).getSql();
      await (this.driver as DurableObjectSqlDriver).transaction(async () => {
        for (const entry of entries) {
          validateKey(entry.key);
          const ttl = entry.ttlSeconds ?? this.options.defaultTTL;
          const expiresAt = ttl ? now + ttl * 1000 : null;
          const serializedValue = await this.serializeForStorage(entry.value, entry.key);
          const params: unknown[] = [entry.key];
          for (const colName of colNames) {
            let colVal = entry.keys?.[colName];
            if (colVal === undefined && typeof entry.value === "object" && entry.value !== null) {
              colVal = (entry.value as Record<string, unknown>)[colName];
            }
            params.push(colVal ?? null);
          }
          params.push(serializedValue, expiresAt, now, now);
          sqlStorage.exec(sql, ...params);
        }
      });
      return;
    }

    const rawEntries = await Promise.all(
      entries.map(async (entry) => {
        validateKey(entry.key);
        const ttl = entry.ttlSeconds ?? this.options.defaultTTL;
        const serialized = await this.serializeForStorage(entry.value, entry.key);
        return {
          key: entry.key,
          value: serialized,
          ttlSeconds: ttl,
        };
      })
    );
    await this.driver.setMany(this.name, rawEntries);
  }

  /**
   * Dynamically adds a new secondary key/column to the schema and physical table.
   */
  async addKey(columnName: string, definition: KeyDefinition): Promise<void> {
    if (!this.normalizedSchema || (!(this.driver instanceof D1Driver) && !(this.driver instanceof DurableObjectSqlDriver))) {
      throw new KVDBError("addKey() requires a Table with a defined physical schema on a D1 or DO-SQL driver", "UNSUPPORTED_OPERATION");
    }
    await this.init();

    const { alterSql, indexSql } = generateAddColumnSql(
      this.physicalTableName!,
      columnName,
      definition
    );

    if (this.driver instanceof D1Driver) {
      const db = this.driver.getDb();
      await db.exec(alterSql);
      if (indexSql) {
        await db.exec(indexSql);
      }
    } else {
      const sql = (this.driver as DurableObjectSqlDriver).getSql();
      sql.exec(alterSql);
      if (indexSql) {
        sql.exec(indexSql);
      }
    }

    // Update in-memory schema
    this.normalizedSchema.columns[columnName] = { ...definition };
    if (definition.index) {
      this.normalizedSchema.indexes.push({
        columns: [columnName],
        unique: typeof definition.index === "object" ? Boolean(definition.index.unique) : false,
      });
    }
  }

  /**
   * Internal find query compiler and executor.
   */
  private async findInternal(
    where?: Record<string, unknown>,
    queryOptions?: QueryOptions
  ): Promise<{ items: V[]; rawRows: { __pk: string; value: string }[] }> {
    if (!this.normalizedSchema || (!(this.driver instanceof D1Driver) && !(this.driver instanceof DurableObjectSqlDriver))) {
      throw new KVDBError("find() with native SQL compiler currently requires a schema table on D1 or DO-SQL", "UNSUPPORTED_OPERATION");
    }
    await this.init();

    const pkName = this.normalizedSchema.primaryKey.name;
    const knownCols = new Set([
      pkName,
      ...Object.keys(this.normalizedSchema.columns),
    ]);

    const ast = parseWhere(where, knownCols);
    const { sql: whereSql, params } = compileWhere(ast);
    const now = getMonotonicNow();

    let sql = `SELECT ${pkName} as __pk, value FROM ${this.physicalTableName} WHERE ${whereSql} AND (expires_at IS NULL OR expires_at > ?)`;
    params.push(now);

    if (queryOptions?.cursor) {
      try {
        const decoded = JSON.parse(atob(queryOptions.cursor));
        if (queryOptions.sort && queryOptions.sort.length > 0) {
          const firstSort = queryOptions.sort[0]!;
          const sortField = firstSort.field ?? firstSort.path?.source ?? "created_at";
          const sortCol = knownCols.has(sortField) ? sortField : `json_extract(value, '$.${sortField}')`;
          const cmpOp = firstSort.direction === "desc" ? "<" : ">";
          sql += ` AND (${sortCol} ${cmpOp} ? OR (${sortCol} = ? AND ${pkName} > ?))`;
          params.push(decoded.val, decoded.val, decoded.pk);
        } else {
          sql += ` AND ${pkName} > ?`;
          params.push(decoded.pk ?? decoded.id ?? queryOptions.cursor);
        }
      } catch {
        sql += ` AND ${pkName} > ?`;
        params.push(queryOptions.cursor);
      }
    }

    if (queryOptions?.sort && queryOptions.sort.length > 0) {
      const orderSql = compileOrderBy(queryOptions.sort, knownCols);
      if (orderSql) {
        sql += ` ORDER BY ${orderSql}`;
      }
    } else if (queryOptions?.cursor) {
      sql += ` ORDER BY ${pkName} ASC`;
    }

    if (queryOptions?.limit !== undefined) {
      sql += ` LIMIT ?`;
      params.push(queryOptions.limit);
    }

    if (!queryOptions?.cursor && queryOptions?.offset !== undefined) {
      sql += ` OFFSET ?`;
      params.push(queryOptions.offset);
    }

    if (this.driver instanceof D1Driver) {
      assertD1ParamLimit(params.length);
    }

    let rows: { __pk: string; value: string }[] = [];
    if (this.driver instanceof D1Driver) {
      const stmt = this.driver.getDb().prepare(sql).bind(...params);
      rows = (await stmt.all<{ __pk: string; value: string }>()).results;
    } else {
      rows = (this.driver as DurableObjectSqlDriver).getSql().exec<{ __pk: string; value: string }>(sql, ...params).toArray();
    }

    const items = await Promise.all(
      rows.map(async (r) => {
        const parsed = deserialize<unknown>(r.value);
        const resolved = await this.resolveStoredValue(parsed);
        return resolved as V;
      })
    );

    return { items, rawRows: rows };
  }

  /**
   * Executes a Mongo-style filter query against physical schema tables or JSON documents.
   */
  async find(
    where?: Record<string, unknown>,
    queryOptions?: QueryOptions
  ): Promise<V[]> {
    if (this.writeBatcher && this.writeBatcher.pendingCount > 0) {
      await this.writeBatcher.flush();
    }
    const { items } = await this.findInternal(where, queryOptions);
    return items;
  }

  /**
   * Executes a paginated find query returning items, an opaque cursor for the next page, and completion status.
   */
  async findPage(
    where?: Record<string, unknown>,
    queryOptions?: QueryOptions
  ): Promise<PageResult<V>> {
    if (this.writeBatcher && this.writeBatcher.pendingCount > 0) {
      await this.writeBatcher.flush();
    }
    const limit = Math.max(1, queryOptions?.limit ?? 50);
    const { items, rawRows } = await this.findInternal(where, {
      ...queryOptions,
      limit: limit + 1,
    });

    const complete = items.length <= limit;
    const pageItems = complete ? items : items.slice(0, limit);

    let nextCursor: string | undefined = undefined;
    if (!complete && pageItems.length > 0) {
      const lastRow = rawRows[limit - 1]!;
      const lastPk = lastRow.__pk;
      const lastItem = pageItems[pageItems.length - 1] as any;

      if (queryOptions?.sort && queryOptions.sort.length > 0) {
        const firstSort = queryOptions.sort[0]!;
        const sortField = firstSort.field ?? firstSort.path?.source ?? "created_at";
        const lastVal = typeof lastItem === "object" && lastItem !== null ? lastItem[sortField] : undefined;
        nextCursor = btoa(JSON.stringify({ pk: String(lastPk), val: lastVal }));
      } else {
        nextCursor = btoa(JSON.stringify({ pk: String(lastPk) }));
      }
    }

    return {
      items: pageItems,
      cursor: nextCursor,
      complete,
    };
  }

  /**
   * Deletes a key from the table.
   */
  async delete(key: string): Promise<boolean> {
    validateKey(key);
    if (this.writeBatcher && this.writeBatcher.pendingCount > 0) {
      await this.writeBatcher.flush();
    }
    // Identify blob key before deleting row, but do not delete blob yet to ensure atomicity
    const blobKeyToDelete = await this.getBlobKeyForRecord(key);

    let deleted = false;
    let bookmark: string | undefined;

    if (this.normalizedSchema && this.driver instanceof D1Driver) {
      await this.init();
      const pkName = this.normalizedSchema.primaryKey.name;
      const sql = `DELETE FROM ${this.physicalTableName} WHERE ${pkName} = ?;`;
      const res = await this.driver.getDb().prepare(sql).bind(key).run();
      deleted = (res.meta.changes ?? 0) > 0;
      bookmark = (res as any)?.meta?.bookmark;
    } else if (this.normalizedSchema && this.driver instanceof DurableObjectSqlDriver) {
      await this.init();
      const pkName = this.normalizedSchema.primaryKey.name;
      const sql = `DELETE FROM ${this.physicalTableName} WHERE ${pkName} = ?;`;
      const cursor = this.driver.getSql().exec(sql, key);
      deleted = cursor.rowsWritten > 0;
    } else {
      deleted = await this.driver.delete(this.name, key);
    }

    if (bookmark && this.driver instanceof D1Driver) {
      this.driver.setBookmark(bookmark);
    }

    // Safely delete R2 blob only AFTER database record has been deleted
    if (deleted && blobKeyToDelete && this.blobOverflow) {
      try {
        await this.blobOverflow.deleteBlob(blobKeyToDelete);
      } catch {
        // Suppress cleanup error so delete operation still succeeds
      }
    }

    return deleted;
  }

  /**
   * Deletes multiple keys from the table.
   */
  async deleteMany(keys: readonly string[]): Promise<number> {
    if (keys.length === 0) return 0;
    if (this.writeBatcher && this.writeBatcher.pendingCount > 0) {
      await this.writeBatcher.flush();
    }
    for (const key of keys) {
      validateKey(key);
    }

    // Identify blob keys before deleting rows, but do not delete blobs yet
    const blobKeysToDelete = await this.getBlobKeysForRecords(keys);

    let totalDeleted = 0;
    let bookmark: string | undefined;

    if (this.normalizedSchema && this.driver instanceof D1Driver) {
      await this.init();
      const pkName = this.normalizedSchema.primaryKey.name;
      const keyChunks = chunkArray(keys, 75);
      const d1Db = this.driver.getDb();
      const stmts = keyChunks.map((chunk) => {
        const inClause = buildInClausePlaceholders(chunk.length);
        const sql = `DELETE FROM ${this.physicalTableName} WHERE ${pkName} IN ${inClause};`;
        return d1Db.prepare(sql).bind(...chunk);
      });
      const batchResults = await d1Db.batch(stmts);
      totalDeleted = batchResults.reduce((acc, res) => acc + (res.meta.changes ?? 0), 0);
      const lastRes = batchResults[batchResults.length - 1];
      bookmark = (lastRes as any)?.meta?.bookmark;
    } else if (this.normalizedSchema && this.driver instanceof DurableObjectSqlDriver) {
      await this.init();
      const pkName = this.normalizedSchema.primaryKey.name;
      const keyChunks = chunkArray(keys, 75);
      const doDriver = this.driver;
      const doSql = doDriver.getSql();
      await doDriver.transaction(() => {
        for (const chunk of keyChunks) {
          const inClause = buildInClausePlaceholders(chunk.length);
          const sql = `DELETE FROM ${this.physicalTableName} WHERE ${pkName} IN ${inClause};`;
          const cursor = doSql.exec(sql, ...chunk);
          totalDeleted += cursor.rowsWritten;
        }
      });
    } else {
      totalDeleted = await this.driver.deleteMany(this.name, keys);
    }

    if (bookmark && this.driver instanceof D1Driver) {
      this.driver.setBookmark(bookmark);
    }

    // Safely delete R2 blobs only AFTER database rows have been deleted
    if (totalDeleted > 0 && blobKeysToDelete.length > 0 && this.blobOverflow) {
      try {
        await this.blobOverflow.deleteBlobs(blobKeysToDelete);
      } catch {
        // Suppress cleanup error
      }
    }

    return totalDeleted;
  }

  /**
   * Checks whether a key exists in the table.
   * Uses an index-only SELECT 1 query on SQL drivers to avoid reading payloads or fetching R2 blobs.
   */
  async has(key: string): Promise<boolean> {
    validateKey(key);
    if (this.writeBatcher && this.writeBatcher.pendingCount > 0) {
      await this.writeBatcher.flush();
    }
    if (this.normalizedSchema && this.driver instanceof D1Driver) {
      await this.init();
      const now = getMonotonicNow();
      const pkName = this.normalizedSchema.primaryKey.name;
      const sql = `SELECT 1 FROM ${this.physicalTableName} WHERE ${pkName} = ? AND (expires_at IS NULL OR expires_at > ?) LIMIT 1;`;
      const row = await this.driver.getDb().prepare(sql).bind(key, now).first();
      return row !== null;
    }
    if (this.normalizedSchema && this.driver instanceof DurableObjectSqlDriver) {
      await this.init();
      const now = getMonotonicNow();
      const pkName = this.normalizedSchema.primaryKey.name;
      const sql = `SELECT 1 FROM ${this.physicalTableName} WHERE ${pkName} = ? AND (expires_at IS NULL OR expires_at > ?) LIMIT 1;`;
      const rows = this.driver.getSql().exec(sql, key, now).toArray();
      return rows.length > 0;
    }
    return await this.driver.has(this.name, key);
  }

  /**
   * Deletes all records in this table and purges associated R2 overflow blobs.
   */
  async clear(): Promise<void> {
    if (this.writeBatcher) {
      this.writeBatcher.clear();
    }
    if (this.blobOverflow) {
      try {
        await this.blobOverflow.clearBlobs();
      } catch {
        // Ignore blob cleanup error
      }
    }
    if (this.normalizedSchema && this.driver instanceof D1Driver) {
      await this.init();
      await this.driver.getDb().exec(`DELETE FROM ${this.physicalTableName};`);
      return;
    }
    if (this.normalizedSchema && this.driver instanceof DurableObjectSqlDriver) {
      await this.init();
      this.driver.getSql().exec(`DELETE FROM ${this.physicalTableName};`);
      return;
    }
    await this.driver.clear(this.name);
  }

  /**
   * Lists keys with prefix, limit, and cursor pagination.
   */
  async list(options?: DriverListOptions): Promise<DriverListResult> {
    if (this.writeBatcher && this.writeBatcher.pendingCount > 0) {
      await this.writeBatcher.flush();
    }
    if (this.normalizedSchema && (this.driver instanceof D1Driver || this.driver instanceof DurableObjectSqlDriver)) {
      await this.init();
      const now = getMonotonicNow();
      const pkName = this.normalizedSchema.primaryKey.name;
      const limit = Math.max(1, options?.limit ?? 100);
      const queryLimit = limit + 1;
      const hasPrefix = Boolean(options?.prefix);
      const hasCursor = Boolean(options?.cursor);

      let sql = `SELECT ${pkName} as key FROM ${this.physicalTableName} WHERE (expires_at IS NULL OR expires_at > ?)`;
      const params: unknown[] = [now];
      if (hasPrefix) {
        const escaped = options!.prefix!.replace(/[%_\\]/g, "\\$&");
        sql += ` AND ${pkName} LIKE ? ESCAPE '\\'`;
        params.push(`${escaped}%`);
      }
      if (hasCursor) {
        sql += ` AND ${pkName} > ?`;
        params.push(options!.cursor);
      }
      sql += ` ORDER BY ${pkName} ASC LIMIT ?;`;
      params.push(queryLimit);

      let rows: { key: string }[] = [];
      if (this.driver instanceof D1Driver) {
        rows = (await this.driver.getDb().prepare(sql).bind(...params).all<{ key: string }>()).results;
      } else {
        rows = (this.driver as DurableObjectSqlDriver).getSql().exec<{ key: string }>(sql, ...params).toArray();
      }

      const complete = rows.length <= limit;
      const keys = rows.slice(0, limit).map((r) => r.key);
      const nextCursor = complete ? undefined : keys[keys.length - 1];
      return { keys, cursor: nextCursor, complete };
    }

    return await this.driver.list(this.name, options);
  }

  /**
   * Retrieves all key-value pairs matching a prefix across pages.
   */
  async getByPrefix(prefix = ""): Promise<Map<string, V>> {
    const resultMap = new Map<string, V>();
    let cursor: string | undefined = undefined;

    do {
      const page = await this.list({ prefix, limit: 1000, cursor });
      if (page.keys.length > 0) {
        const values = await this.getMany(page.keys);
        for (let i = 0; i < page.keys.length; i++) {
          const val = values[i];
          if (val !== null && val !== undefined) {
            resultMap.set(page.keys[i]!, val);
          }
        }
      }
      cursor = page.complete ? undefined : page.cursor;
    } while (cursor);

    return resultMap;
  }

  /**
   * Deletes all keys matching a prefix across all pages.
   */
  async deleteByPrefix(prefix: string): Promise<number> {
    let totalDeleted = 0;
    let cursor: string | undefined = undefined;

    do {
      const page = await this.list({ prefix, limit: 1000, cursor });
      if (page.keys.length > 0) {
        const deleted = await this.deleteMany(page.keys);
        totalDeleted += deleted;
      }
      cursor = page.complete ? undefined : page.cursor;
    } while (cursor);

    return totalDeleted;
  }
}
