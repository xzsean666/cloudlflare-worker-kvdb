export interface SweeperOptions {
  /**
   * Target database instance: Cloudflare D1 (D1Database) or
   * Durable Objects SQLite (SqlStorage).
   */
  db: D1Database | SqlStorage;

  /**
   * Optional Cloudflare R2 bucket binding for cascading cleanup of orphaned overflow blobs.
   */
  r2Bucket?: R2Bucket;

  /**
   * Explicit list of table names to sweep.
   * If omitted and autoDiscoverTables is true, scans _cf_kvdb and all tables with an expires_at column.
   */
  tableNames?: string[];

  /**
   * Whether to automatically inspect SQLite schema for tables containing an expires_at column.
   * Default: true.
   */
  autoDiscoverTables?: boolean;

  /**
   * Maximum records to delete in a single batch (safe under parameter limit and lock contention).
   * Default: 100.
   */
  batchSize?: number;

  /**
   * Maximum number of batches to process per table in a single run to protect against CPU timeouts.
   * Default: 50.
   */
  maxBatchesPerTable?: number;
}

export interface SweepResult {
  expiredRowsDeleted: number;
  blobsDeleted: number;
  tablesProcessed: string[];
  durationMs: number;
}
