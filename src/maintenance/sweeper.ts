import type { SweeperOptions, SweepResult } from "./types.js";
import { createQueueSqlAdapter, type QueueSqlAdapter } from "../queue/adapter.js";
import { getMonotonicNow } from "../core/clock.js";
import { deserialize, isBlobDescriptor } from "../core/serializer.js";
import { chunkArray } from "../core/chunker.js";

export class TTLSweeper {
  private readonly adapter: QueueSqlAdapter;
  private readonly r2Bucket?: R2Bucket;
  private readonly tableNames?: string[];
  private readonly autoDiscoverTables: boolean;
  private readonly batchSize: number;
  private readonly maxBatchesPerTable: number;
  private readonly sweepOrphans: boolean;

  constructor(options: SweeperOptions) {
    this.adapter = createQueueSqlAdapter(options.db);
    this.r2Bucket = options.r2Bucket;
    this.tableNames = options.tableNames;
    this.autoDiscoverTables = options.autoDiscoverTables ?? true;
    this.batchSize = options.batchSize ?? 100;
    this.maxBatchesPerTable = options.maxBatchesPerTable ?? 50;
    this.sweepOrphans = options.sweepOrphans ?? false;
  }

  /**
   * Discovers all database tables that should be swept for expired records.
   */
  private async getTablesToSweep(): Promise<string[]> {
    if (this.tableNames && this.tableNames.length > 0) {
      return this.tableNames;
    }

    if (!this.autoDiscoverTables) {
      return ["_cf_kvdb"];
    }

    try {
      const tableRows = await this.adapter.query<{ name: string }>(
        `SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%';`
      );

      const tablesWithTtl: string[] = [];
      for (const t of tableRows) {
        try {
          const safeTName = `"${t.name.replace(/"/g, '""')}"`;
          const cols = await this.adapter.query<{ name: string }>(`PRAGMA table_info(${safeTName});`);
          const hasExpiresAt = cols.some((c) => c.name === "expires_at");
          if (hasExpiresAt) {
            tablesWithTtl.push(t.name);
          }
        } catch {
          // Skip on error
        }
      }

      return tablesWithTtl.length > 0 ? tablesWithTtl : ["_cf_kvdb"];
    } catch {
      return ["_cf_kvdb"];
    }
  }

  /**
   * Sweeps orphaned R2 overflow blobs that are no longer referenced by any active database row.
   */
  async sweepOrphanBlobs(options?: { prefix?: string; batchSize?: number }): Promise<number> {
    if (!this.r2Bucket) return 0;
    const prefix = options?.prefix ?? "__blobs";
    const tables = await this.getTablesToSweep();

    // 1. Collect all active blob keys referenced in the database tables
    const activeBlobKeys = new Set<string>();
    for (const table of tables) {
      const safeTableName = `"${table.replace(/"/g, '""')}"`;
      try {
        const rows = await this.adapter.query<{ value: string }>(
          `SELECT value FROM ${safeTableName} WHERE value LIKE '%"__isBlob":true%';`
        );
        for (const row of rows) {
          try {
            const parsed = deserialize<unknown>(row.value);
            if (isBlobDescriptor(parsed)) {
              activeBlobKeys.add(parsed.r2Key);
            }
          } catch {}
        }
      } catch {
        // Table might not have value column or failed query
      }
    }

    // 2. Scan R2 bucket for blobs with matching prefix and delete unreferenced orphans
    let totalOrphansDeleted = 0;
    let cursor: string | undefined = undefined;

    do {
      const listRes = await this.r2Bucket.list({
        prefix,
        cursor,
        limit: options?.batchSize ?? 1000,
      });

      const orphanKeys: string[] = [];
      for (const obj of listRes.objects) {
        if (!activeBlobKeys.has(obj.key)) {
          orphanKeys.push(obj.key);
        }
      }

      if (orphanKeys.length > 0) {
        await this.r2Bucket.delete(orphanKeys);
        totalOrphansDeleted += orphanKeys.length;
      }

      cursor = listRes.truncated ? listRes.cursor : undefined;
    } while (cursor);

    return totalOrphansDeleted;
  }

  /**
   * Sweeps expired records across all configured tables, deleting associated R2 overflow blobs.
   */
  async sweepExpired(): Promise<SweepResult> {
    const startTime = performance.now();
    const now = getMonotonicNow();
    const tables = await this.getTablesToSweep();

    let totalExpiredRowsDeleted = 0;
    let totalBlobsDeleted = 0;
    const tablesProcessed: string[] = [];

    for (const table of tables) {
      try {
        const { rowsDeleted, blobsDeleted } = await this.sweepTable(table, now);
        totalExpiredRowsDeleted += rowsDeleted;
        totalBlobsDeleted += blobsDeleted;
        tablesProcessed.push(table);
      } catch {
        // Continue sweeping remaining tables
      }
    }

    let totalOrphanBlobsDeleted = 0;
    if (this.sweepOrphans && this.r2Bucket) {
      try {
        totalOrphanBlobsDeleted = await this.sweepOrphanBlobs();
      } catch {
        // Continue
      }
    }

    const durationMs = Math.round(performance.now() - startTime);

    return {
      expiredRowsDeleted: totalExpiredRowsDeleted,
      blobsDeleted: totalBlobsDeleted,
      orphanedBlobsDeleted: totalOrphanBlobsDeleted,
      tablesProcessed,
      durationMs,
    };
  }

  private async sweepTable(
    tableName: string,
    now: number
  ): Promise<{ rowsDeleted: number; blobsDeleted: number }> {
    let rowsDeleted = 0;
    let blobsDeleted = 0;
    let batchIndex = 0;

    const safeTableName = `"${tableName.replace(/"/g, '""')}"`;
    let cols: Array<{ name: string; pk: number }> = [];
    try {
      cols = await this.adapter.query<{ name: string; pk: number }>(`PRAGMA table_info(${safeTableName});`);
    } catch {
      return { rowsDeleted: 0, blobsDeleted: 0 };
    }

    const hasValue = cols.some((c) => c.name === "value");
    const pkCols = cols.filter((c) => c.pk > 0).map((c) => c.name);

    while (batchIndex < this.maxBatchesPerTable) {
      batchIndex++;

      let rows: Array<{ rowid?: number; value?: string; [k: string]: any }> = [];
      let useRowid = true;

      try {
        const selectSql = hasValue
          ? `SELECT rowid, value FROM ${safeTableName} WHERE expires_at IS NOT NULL AND expires_at <= ? LIMIT ?;`
          : `SELECT rowid FROM ${safeTableName} WHERE expires_at IS NOT NULL AND expires_at <= ? LIMIT ?;`;
        rows = await this.adapter.query(selectSql, now, this.batchSize);
      } catch {
        useRowid = false;
        const selectCols = hasValue && pkCols.length > 0
          ? [...pkCols, "value"].join(", ")
          : pkCols.length > 0
          ? pkCols.join(", ")
          : "*";
        const selectSql = `SELECT ${selectCols} FROM ${safeTableName} WHERE expires_at IS NOT NULL AND expires_at <= ? LIMIT ?;`;
        rows = await this.adapter.query(selectSql, now, this.batchSize);
      }

      if (rows.length === 0) {
        break;
      }

      // 1. Cascading R2 overflow blob cleanup
      if (this.r2Bucket && hasValue) {
        const blobKeysToDelete: string[] = [];
        for (const row of rows) {
          if (row.value) {
            try {
              const parsed = deserialize<unknown>(row.value);
              if (isBlobDescriptor(parsed)) {
                blobKeysToDelete.push(parsed.r2Key);
              }
            } catch {
              // Ignore JSON parse errors
            }
          }
        }

        if (blobKeysToDelete.length > 0) {
          try {
            await this.r2Bucket.delete(blobKeysToDelete);
            blobsDeleted += blobKeysToDelete.length;
          } catch {
            // Ignore R2 delete failure
          }
        }
      }

      // 2. Chunked deletion of database rows (parameter-safe for D1 <= 100 limit)
      if (useRowid) {
        const rowids = rows.map((r) => r.rowid).filter((id): id is number => id !== undefined);
        const rowidChunks = chunkArray(rowids, 75);
        for (const chunk of rowidChunks) {
          const placeholders = chunk.map(() => "?").join(", ");
          const deleteSql = `DELETE FROM ${safeTableName} WHERE rowid IN (${placeholders});`;
          const res = await this.adapter.exec(deleteSql, ...chunk);
          rowsDeleted += res.changes;
        }
      } else if (pkCols.length === 1) {
        const pkCol = pkCols[0]!;
        const keys = rows.map((r) => r[pkCol]);
        const keyChunks = chunkArray(keys, 75);
        for (const chunk of keyChunks) {
          const placeholders = chunk.map(() => "?").join(", ");
          const deleteSql = `DELETE FROM ${safeTableName} WHERE ${pkCol} IN (${placeholders});`;
          const res = await this.adapter.exec(deleteSql, ...chunk);
          rowsDeleted += res.changes;
        }
      } else if (pkCols.length > 1) {
        const rowChunks = chunkArray(rows, 25);
        for (const chunk of rowChunks) {
          const orConditions: string[] = [];
          const params: unknown[] = [];
          for (const r of chunk) {
            orConditions.push(`(${pkCols.map((col) => `${col} = ?`).join(" AND ")})`);
            for (const col of pkCols) {
              params.push(r[col]);
            }
          }
          const deleteSql = `DELETE FROM ${safeTableName} WHERE ${orConditions.join(" OR ")};`;
          const res = await this.adapter.exec(deleteSql, ...params);
          rowsDeleted += res.changes;
        }
      }

      if (rows.length < this.batchSize) {
        break;
      }
    }

    return { rowsDeleted, blobsDeleted };
  }

  /**
   * Executes a database VACUUM command to defragment B-Tree pages and reclaim disk space.
   */
  async vacuum(): Promise<void> {
    try {
      await this.adapter.exec("VACUUM;");
    } catch {
      // Disallowed inside some environments/transactions
    }
  }
}
