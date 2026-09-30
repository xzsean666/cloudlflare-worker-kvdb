import type { Job, JobOptions, JobState, QueueConfig, QueueStats } from "./types.js";
import { createQueueSqlAdapter, type QueueSqlAdapter, type QueueSqlStatement } from "./adapter.js";
import { getMonotonicNow } from "../core/clock.js";
import { serialize, deserialize } from "../core/serializer.js";
import { chunkArray } from "../core/chunker.js";
import { StorageError, KVDBError } from "../core/errors.js";

export const DEFAULT_QUEUE_TABLE = "_cf_queue";
const IDENTIFIER_REGEX = /^[A-Za-z_][A-Za-z0-9_]*$/;

interface DbJobRow {
  id: string;
  queue: string;
  payload: string;
  state: string;
  priority: number;
  attempts: number;
  max_attempts: number;
  available_at: number;
  leased_until: number | null;
  lock_token: string | null;
  dedup_key: string | null;
  last_error: string | null;
  created_at: number;
  updated_at: number;
}

/**
 * Serverless Reliable Job Queue on Cloudflare D1 or Durable Objects SQLite.
 *
 * Implements atomic lock token lease acquisition, exponential backoff, dead-letter queues (DLQ),
 * deduplication, and visibility timeout recovery.
 */
export class JobQueue<T = unknown> {
  private readonly adapter: QueueSqlAdapter;
  public readonly queueName: string;
  public readonly tableName: string;
  public readonly leaseSeconds: number;
  public readonly defaultMaxAttempts: number;
  public readonly baseBackoffMs: number;
  private isInitialized = false;

  constructor(config: QueueConfig) {
    this.adapter = createQueueSqlAdapter(config.db);
    this.queueName = config.queueName ?? "default";
    const tName = config.tableName ?? DEFAULT_QUEUE_TABLE;
    if (!IDENTIFIER_REGEX.test(tName)) {
      throw new KVDBError(`Invalid queue table name identifier "${tName}"`, "INVALID_SCHEMA");
    }
    this.tableName = tName;
    this.leaseSeconds = config.leaseSeconds ?? 30;
    this.defaultMaxAttempts = config.defaultMaxAttempts ?? 3;
    this.baseBackoffMs = config.baseBackoffMs ?? 1000;
  }

  /**
   * Initializes the queue table and composite performance indexes.
   */
  async init(): Promise<void> {
    if (this.isInitialized) return;
    try {
      await this.adapter.exec(`
        CREATE TABLE IF NOT EXISTS ${this.tableName} (
          id TEXT PRIMARY KEY,
          queue TEXT NOT NULL,
          payload TEXT NOT NULL,
          state TEXT NOT NULL,
          priority INTEGER NOT NULL,
          attempts INTEGER NOT NULL,
          max_attempts INTEGER NOT NULL,
          available_at INTEGER NOT NULL,
          leased_until INTEGER,
          lock_token TEXT,
          dedup_key TEXT,
          last_error TEXT,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );
      `);

      await this.adapter.exec(
        `CREATE INDEX IF NOT EXISTS idx_${this.tableName}_poll ON ${this.tableName} (queue, state, available_at, priority DESC);`
      );
      await this.adapter.exec(
        `CREATE INDEX IF NOT EXISTS idx_${this.tableName}_lease ON ${this.tableName} (queue, state, leased_until);`
      );
      await this.adapter.exec(
        `CREATE UNIQUE INDEX IF NOT EXISTS idx_${this.tableName}_dedup ON ${this.tableName} (queue, dedup_key)
         WHERE dedup_key IS NOT NULL AND state NOT IN ('completed', 'failed');`
      );

      this.isInitialized = true;
    } catch (err: any) {
      throw new StorageError(`Failed to initialize JobQueue storage: ${err.message}`, err);
    }
  }

  private mapRowToJob(row: DbJobRow): Job<T> {
    return {
      id: row.id,
      queue: row.queue,
      payload: deserialize<T>(row.payload),
      state: row.state as JobState,
      priority: row.priority,
      attempts: row.attempts,
      maxAttempts: row.max_attempts,
      availableAt: row.available_at,
      leasedUntil: row.leased_until,
      lockToken: row.lock_token,
      dedupKey: row.dedup_key,
      lastError: row.last_error,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    };
  }

  /**
   * Enqueues a new job with optional delay, priority, max attempts, and deduplication.
   */
  async push(payload: T, options?: JobOptions): Promise<Job<T>> {
    await this.init();
    const now = getMonotonicNow();
    const delayMs = options?.delayMs ?? 0;
    const availableAt = now + delayMs;
    const state: JobState = delayMs > 0 ? "delayed" : "ready";
    const priority = options?.priority ?? 0;
    const maxAttempts = options?.maxAttempts ?? this.defaultMaxAttempts;
    const dedupKey = options?.dedupKey ?? null;

    if (dedupKey) {
      // Check if an existing uncompleted job with this dedupKey exists
      const existing = await this.adapter.query<DbJobRow>(
        `SELECT * FROM ${this.tableName}
         WHERE queue = ? AND dedup_key = ? AND state NOT IN ('completed', 'failed')
         LIMIT 1;`,
        this.queueName,
        dedupKey
      );
      if (existing.length > 0) {
        return this.mapRowToJob(existing[0]!);
      }
    }

    const id = crypto.randomUUID();
    const serializedPayload = serialize(payload);

    try {
      await this.adapter.exec(
        `INSERT INTO ${this.tableName} (
          id, queue, payload, state, priority, attempts, max_attempts,
          available_at, leased_until, lock_token, dedup_key, last_error,
          created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
        id,
        this.queueName,
        serializedPayload,
        state,
        priority,
        0, // initial attempts
        maxAttempts,
        availableAt,
        null, // leased_until
        null, // lock_token
        dedupKey,
        null, // last_error
        now,
        now
      );
    } catch (err: any) {
      if (dedupKey && (err.message?.includes("UNIQUE") || err.message?.includes("constraint"))) {
        const existing = await this.adapter.query<DbJobRow>(
          `SELECT * FROM ${this.tableName}
           WHERE queue = ? AND dedup_key = ? AND state NOT IN ('completed', 'failed')
           LIMIT 1;`,
          this.queueName,
          dedupKey
        );
        if (existing.length > 0) {
          return this.mapRowToJob(existing[0]!);
        }
      }
      throw err;
    }

    return {
      id,
      queue: this.queueName,
      payload,
      state,
      priority,
      attempts: 0,
      maxAttempts,
      availableAt,
      leasedUntil: null,
      lockToken: null,
      dedupKey,
      lastError: null,
      createdAt: now,
      updatedAt: now,
    };
  }

  /**
   * Enqueues multiple jobs in a single call with batch SQL execution.
   */
  async pushMany(
    items: readonly { payload: T; options?: JobOptions }[]
  ): Promise<Job<T>[]> {
    if (items.length === 0) return [];
    await this.init();

    const hasDedup = items.some((i) => Boolean(i.options?.dedupKey));
    if (!hasDedup && typeof this.adapter.batch === "function") {
      const now = getMonotonicNow();
      const jobs: Job<T>[] = [];
      const stmts: QueueSqlStatement[] = [];

      for (const item of items) {
        const id = crypto.randomUUID();
        const serializedPayload = serialize(item.payload);
        const delayMs = item.options?.delayMs ?? 0;
        const availableAt = now + delayMs;
        const state: JobState = delayMs > 0 ? "delayed" : "ready";
        const priority = item.options?.priority ?? 0;
        const maxAttempts = item.options?.maxAttempts ?? this.defaultMaxAttempts;

        stmts.push({
          sql: `INSERT INTO ${this.tableName} (
            id, queue, payload, state, priority, attempts, max_attempts,
            available_at, leased_until, lock_token, dedup_key, last_error,
            created_at, updated_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
          params: [
            id,
            this.queueName,
            serializedPayload,
            state,
            priority,
            0,
            maxAttempts,
            availableAt,
            null,
            null,
            null,
            null,
            now,
            now,
          ],
        });

        jobs.push({
          id,
          queue: this.queueName,
          payload: item.payload,
          state,
          priority,
          attempts: 0,
          maxAttempts,
          availableAt,
          leasedUntil: null,
          lockToken: null,
          dedupKey: null,
          lastError: null,
          createdAt: now,
          updatedAt: now,
        });
      }

      const chunks = chunkArray(stmts, 50);
      for (const chunk of chunks) {
        await this.adapter.batch(chunk);
      }
      return jobs;
    }

    const results: Job<T>[] = [];
    for (const item of items) {
      results.push(await this.push(item.payload, item.options));
    }
    return results;
  }

  /**
   * Atomically leases ready jobs, locking them with a unique lockToken to prevent duplicate delivery.
   */
  async popMany(limit = 1, leaseSeconds?: number): Promise<Job<T>[]> {
    if (limit <= 0) return [];
    await this.init();

    const now = getMonotonicNow();
    const leaseDuration = (leaseSeconds ?? this.leaseSeconds) * 1000;
    const leasedUntil = now + leaseDuration;
    const lockToken = crypto.randomUUID();

    // Atomic subquery lease lock:
    // Selects candidate jobs and marks them active with lock_token in a single statement
    const updateSql = `
      UPDATE ${this.tableName}
      SET state = 'active',
          lock_token = ?,
          leased_until = ?,
          attempts = attempts + 1,
          updated_at = ?
      WHERE id IN (
        SELECT id FROM ${this.tableName}
        WHERE queue = ?
          AND (state = 'ready' OR (state = 'delayed' AND available_at <= ?))
          AND available_at <= ?
        ORDER BY priority DESC, available_at ASC, id ASC
        LIMIT ?
      );
    `;

    const updateRes = await this.adapter.exec(
      updateSql,
      lockToken,
      leasedUntil,
      now,
      this.queueName,
      now,
      now,
      limit
    );

    if (updateRes.changes === 0) {
      return [];
    }

    // Retrieve the jobs assigned to this worker's lock token
    const rows = await this.adapter.query<DbJobRow>(
      `SELECT * FROM ${this.tableName}
       WHERE queue = ? AND lock_token = ? AND state = 'active'
       ORDER BY priority DESC, available_at ASC;`,
      this.queueName,
      lockToken
    );

    return rows.map((r) => this.mapRowToJob(r));
  }

  /**
   * Atomically leases a single ready job.
   */
  async pop(leaseSeconds?: number): Promise<Job<T> | null> {
    const jobs = await this.popMany(1, leaseSeconds);
    return jobs[0] ?? null;
  }

  /**
   * Acknowledges successful completion of a job.
   */
  async ack(
    jobOrId: Job<T> | string,
    lockToken?: string,
    options?: { removeOnComplete?: boolean }
  ): Promise<boolean> {
    await this.init();
    const id = typeof jobOrId === "string" ? jobOrId : jobOrId.id;
    const token = typeof jobOrId === "string" ? lockToken : jobOrId.lockToken;
    const now = getMonotonicNow();

    if (options?.removeOnComplete) {
      const res = await this.adapter.exec(
        `DELETE FROM ${this.tableName}
         WHERE id = ? AND lock_token = ? AND state = 'active';`,
        id,
        token
      );
      return res.changes > 0;
    }

    const res = await this.adapter.exec(
      `UPDATE ${this.tableName}
       SET state = 'completed',
           lock_token = NULL,
           leased_until = NULL,
           updated_at = ?
       WHERE id = ? AND lock_token = ? AND state = 'active';`,
      now,
      id,
      token
    );

    return res.changes > 0;
  }

  /**
   * Batch acknowledges successful completion of multiple jobs atomically.
   */
  async ackMany(
    jobs: readonly (Job<T> | { id: string; lockToken?: string | null } | string)[],
    options?: { removeOnComplete?: boolean }
  ): Promise<number> {
    if (jobs.length === 0) return 0;
    await this.init();
    const now = getMonotonicNow();

    const normalized = jobs.map((j) => {
      if (typeof j === "string") return { id: j, lockToken: undefined };
      return { id: j.id, lockToken: j.lockToken ?? undefined };
    });

    if (typeof this.adapter.batch === "function") {
      const stmts: QueueSqlStatement[] = [];
      for (const item of normalized) {
        if (options?.removeOnComplete) {
          if (item.lockToken !== undefined) {
            stmts.push({
              sql: `DELETE FROM ${this.tableName} WHERE id = ? AND lock_token = ? AND state = 'active';`,
              params: [item.id, item.lockToken],
            });
          } else {
            stmts.push({
              sql: `DELETE FROM ${this.tableName} WHERE id = ? AND state = 'active';`,
              params: [item.id],
            });
          }
        } else {
          if (item.lockToken !== undefined) {
            stmts.push({
              sql: `UPDATE ${this.tableName} SET state = 'completed', lock_token = NULL, leased_until = NULL, updated_at = ? WHERE id = ? AND lock_token = ? AND state = 'active';`,
              params: [now, item.id, item.lockToken],
            });
          } else {
            stmts.push({
              sql: `UPDATE ${this.tableName} SET state = 'completed', lock_token = NULL, leased_until = NULL, updated_at = ? WHERE id = ? AND state = 'active';`,
              params: [now, item.id],
            });
          }
        }
      }

      let totalChanges = 0;
      const chunks = chunkArray(stmts, 50);
      for (const chunk of chunks) {
        const results = await this.adapter.batch(chunk);
        for (const res of results) {
          totalChanges += res.changes;
        }
      }
      return totalChanges;
    }

    let totalChanges = 0;
    for (const item of normalized) {
      const success = await this.ack(item.id, item.lockToken, options);
      if (success) totalChanges++;
    }
    return totalChanges;
  }

  /**
   * Negatively acknowledges a job upon failure, calculating exponential backoff or moving to DLQ.
   */
  async nack(
    jobOrId: Job<T> | string,
    errorOrMessage?: unknown,
    lockToken?: string
  ): Promise<void> {
    await this.init();
    const id = typeof jobOrId === "string" ? jobOrId : jobOrId.id;
    const token = typeof jobOrId === "string" ? lockToken : jobOrId.lockToken;
    const now = getMonotonicNow();
    const errorMsg =
      errorOrMessage instanceof Error
        ? errorOrMessage.message
        : String(errorOrMessage ?? "Job processing failed");

    // Fetch attempt count and max attempts
    const rows = await this.adapter.query<DbJobRow>(
      `SELECT attempts, max_attempts FROM ${this.tableName} WHERE id = ? LIMIT 1;`,
      id
    );

    if (rows.length === 0) return;
    const row = rows[0]!;

    if (row.attempts >= row.max_attempts) {
      // Transition to Dead-Letter Queue (DLQ)
      await this.adapter.exec(
        `UPDATE ${this.tableName}
         SET state = 'failed',
             lock_token = NULL,
             leased_until = NULL,
             last_error = ?,
             updated_at = ?
         WHERE id = ? AND lock_token = ?;`,
        errorMsg,
        now,
        id,
        token
      );
    } else {
      // Exponential backoff: baseBackoffMs * 2^(attempts - 1)
      const backoffMs = this.baseBackoffMs * Math.pow(2, Math.max(0, row.attempts - 1));
      const nextAvailableAt = now + backoffMs;

      await this.adapter.exec(
        `UPDATE ${this.tableName}
         SET state = 'delayed',
             available_at = ?,
             lock_token = NULL,
             leased_until = NULL,
             last_error = ?,
             updated_at = ?
         WHERE id = ? AND lock_token = ?;`,
        nextAvailableAt,
        errorMsg,
        now,
        id,
        token
      );
    }
  }

  /**
   * Extends the active lease of a running job (heartbeat).
   */
  async heartbeat(
    jobOrId: Job<T> | string,
    lockToken?: string,
    extendSeconds?: number
  ): Promise<boolean> {
    await this.init();
    const id = typeof jobOrId === "string" ? jobOrId : jobOrId.id;
    const token = typeof jobOrId === "string" ? lockToken : jobOrId.lockToken;
    const now = getMonotonicNow();
    const extensionMs = (extendSeconds ?? this.leaseSeconds) * 1000;
    const newLeasedUntil = now + extensionMs;

    const res = await this.adapter.exec(
      `UPDATE ${this.tableName}
       SET leased_until = ?, updated_at = ?
       WHERE id = ? AND lock_token = ? AND state = 'active';`,
      newLeasedUntil,
      now,
      id,
      token
    );

    return res.changes > 0;
  }

  /**
   * Retrieves a job by ID.
   */
  async getJob(id: string): Promise<Job<T> | null> {
    await this.init();
    const rows = await this.adapter.query<DbJobRow>(
      `SELECT * FROM ${this.tableName} WHERE id = ? LIMIT 1;`,
      id
    );
    return rows.length > 0 ? this.mapRowToJob(rows[0]!) : null;
  }

  /**
   * Aggregates queue statistics across job states.
   */
  async stats(): Promise<QueueStats> {
    await this.init();
    const rows = await this.adapter.query<{ state: string; count: number }>(
      `SELECT state, COUNT(*) as count FROM ${this.tableName} WHERE queue = ? GROUP BY state;`,
      this.queueName
    );

    const counts: Record<JobState, number> = {
      ready: 0,
      active: 0,
      delayed: 0,
      completed: 0,
      failed: 0,
    };

    let total = 0;
    for (const r of rows) {
      if (r.state in counts) {
        counts[r.state as JobState] = Number(r.count);
        total += Number(r.count);
      }
    }

    return {
      ...counts,
      total,
    };
  }

  /**
   * Alias for stats().
   */
  async getStats(): Promise<QueueStats> {
    return this.stats();
  }

  /**
   * Purges all jobs in this queue.
   */
  async clear(): Promise<void> {
    await this.init();
    await this.adapter.exec(
      `DELETE FROM ${this.tableName} WHERE queue = ?;`,
      this.queueName
    );
  }

  /**
   * Returns the underlying SQL adapter.
   */
  getAdapter(): QueueSqlAdapter {
    return this.adapter;
  }
}
