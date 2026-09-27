import type { QueueSqlAdapter } from "./adapter.js";
import { getMonotonicNow } from "../core/clock.js";
import { DEFAULT_QUEUE_TABLE } from "./queue.js";

export interface ReaperOptions {
  adapter: QueueSqlAdapter;
  tableName?: string;
}

/**
 * Queue Reaper.
 *
 * Recovers orphaned active jobs whose worker processes crashed, exceeded CPU time limits,
 * or lost connectivity before completing or extending their lease.
 */
export class QueueReaper {
  private readonly adapter: QueueSqlAdapter;
  private readonly tableName: string;

  constructor(options: ReaperOptions) {
    this.adapter = options.adapter;
    this.tableName = options.tableName ?? DEFAULT_QUEUE_TABLE;
  }

  /**
   * Recovers orphaned jobs across all queues (or a specific queue) whose visibility lease expired.
   * Resets their state to 'ready' and clears their lock tokens so other workers can process them.
   */
  async reap(queueName?: string): Promise<number> {
    const now = getMonotonicNow();

    let sql = `
      UPDATE ${this.tableName}
      SET state = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'ready' END,
          last_error = CASE WHEN attempts >= max_attempts THEN 'Lease expired and max attempts exceeded' ELSE last_error END,
          lock_token = NULL,
          leased_until = NULL,
          updated_at = ?
      WHERE state = 'active'
        AND leased_until IS NOT NULL
        AND leased_until < ?
    `;
    const params: unknown[] = [now, now];

    if (queueName) {
      sql += ` AND queue = ?`;
      params.push(queueName);
    }

    const res = await this.adapter.exec(sql, ...params);
    return res.changes;
  }
}
