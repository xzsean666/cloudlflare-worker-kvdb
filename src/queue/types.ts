export type JobState = "ready" | "active" | "delayed" | "completed" | "failed";

export interface Job<T = unknown> {
  id: string;
  queue: string;
  payload: T;
  state: JobState;
  priority: number;
  attempts: number;
  maxAttempts: number;
  availableAt: number;
  leasedUntil: number | null;
  lockToken: string | null;
  dedupKey?: string | null;
  lastError?: string | null;
  createdAt: number;
  updatedAt: number;
}

export interface JobOptions {
  /**
   * Priority of the job. Higher values are dequeued first.
   * Default: 0.
   */
  priority?: number;

  /**
   * Delay in milliseconds before the job becomes available for processing.
   * Default: 0.
   */
  delayMs?: number;

  /**
   * Maximum retry attempts before moving to Dead-Letter Queue (state: 'failed').
   * Default: 3.
   */
  maxAttempts?: number;

  /**
   * Unique deduplication key. If another active/ready/delayed job with this key exists,
   * enqueueing will be skipped or ignored.
   */
  dedupKey?: string;

  /**
   * Whether to permanently delete the job record upon successful acknowledgment.
   * Default: false.
   */
  removeOnComplete?: boolean;
}

export interface QueueConfig {
  /**
   * Target database instance: either Cloudflare D1 (D1Database) or
   * Durable Objects SQLite (SqlStorage).
   */
  db: D1Database | SqlStorage;

  /**
   * Queue namespace name. Default: "default".
   */
  queueName?: string;

  /**
   * Queue storage table name. Default: "_cf_queue".
   */
  tableName?: string;

  /**
   * Visibility lease duration in seconds. Default: 30.
   */
  leaseSeconds?: number;

  /**
   * Default max retry attempts. Default: 3.
   */
  defaultMaxAttempts?: number;

  /**
   * Base delay in milliseconds for exponential backoff retries. Default: 1000ms.
   */
  baseBackoffMs?: number;
}

export interface QueueStats {
  ready: number;
  active: number;
  delayed: number;
  completed: number;
  failed: number;
  total: number;
}

export interface WorkerOptions {
  /**
   * Number of concurrent jobs to process in parallel.
   * Default: 1.
   */
  concurrency?: number;

  /**
   * Milliseconds to wait before polling again when the queue is empty.
   * Default: 500ms.
   */
  pollIntervalMs?: number;

  /**
   * Lease duration in seconds requested by worker. Default: 30.
   */
  leaseSeconds?: number;

  /**
   * Whether to automatically send heartbeats extending the lease while handler is running.
   * Default: true.
   */
  autoHeartbeat?: boolean;

  /**
   * Interval in milliseconds for lease extension heartbeats. Default: 10000ms.
   */
  heartbeatIntervalMs?: number;
}
