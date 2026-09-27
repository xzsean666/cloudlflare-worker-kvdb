import type { Job, WorkerOptions } from "./types.js";
import type { JobQueue } from "./queue.js";

export type JobHandler<T = unknown> = (job: Job<T>) => Promise<void> | void;

/**
 * Managed serverless queue worker runner with concurrency control, automatic heartbeats,
 * and graceful shutdown.
 */
export class QueueWorker<T = unknown> {
  public readonly concurrency: number;
  public readonly pollIntervalMs: number;
  public readonly leaseSeconds: number;
  public readonly autoHeartbeat: boolean;
  public readonly heartbeatIntervalMs: number;

  private isRunning = false;
  private inFlight = 0;
  private pollTimeout?: any;

  constructor(
    private readonly queue: JobQueue<T>,
    private readonly handler: JobHandler<T>,
    options: WorkerOptions = {}
  ) {
    this.concurrency = Math.max(1, options.concurrency ?? 1);
    this.pollIntervalMs = options.pollIntervalMs ?? 500;
    this.leaseSeconds = options.leaseSeconds ?? 30;
    this.autoHeartbeat = options.autoHeartbeat ?? true;
    this.heartbeatIntervalMs = options.heartbeatIntervalMs ?? 10000;
  }

  /**
   * Processes a single available job.
   * Returns true if a job was processed, false if the queue was empty.
   */
  async processOne(): Promise<boolean> {
    const job = await this.queue.pop(this.leaseSeconds);
    if (!job) return false;

    let heartbeatTimer: any = null;

    if (this.autoHeartbeat && job.lockToken) {
      heartbeatTimer = setInterval(async () => {
        try {
          await this.queue.heartbeat(job);
        } catch {
          // Ignore heartbeat error if job finished
        }
      }, this.heartbeatIntervalMs);
    }

    try {
      await this.handler(job);
      await this.queue.ack(job);
    } catch (err) {
      await this.queue.nack(job, err);
    } finally {
      if (heartbeatTimer) {
        clearInterval(heartbeatTimer);
      }
    }

    return true;
  }

  /**
   * Processes up to `concurrency` jobs currently ready in the queue.
   * Returns the number of jobs processed.
   */
  async processBatch(): Promise<number> {
    const jobs = await this.queue.popMany(this.concurrency, this.leaseSeconds);
    if (jobs.length === 0) return 0;

    await Promise.allSettled(
      jobs.map(async (job) => {
        let heartbeatTimer: any = null;
        if (this.autoHeartbeat && job.lockToken) {
          heartbeatTimer = setInterval(async () => {
            try {
              await this.queue.heartbeat(job);
            } catch {
              // Ignore
            }
          }, this.heartbeatIntervalMs);
        }

        try {
          await this.handler(job);
          await this.queue.ack(job);
        } catch (err) {
          await this.queue.nack(job, err);
        } finally {
          if (heartbeatTimer) {
            clearInterval(heartbeatTimer);
          }
        }
      })
    );

    return jobs.length;
  }

  /**
   * Starts the continuous background polling loop.
   */
  start(): void {
    if (this.isRunning) return;
    this.isRunning = true;
    this.scheduleNextTick(0);
  }

  /**
   * Stops the background polling loop.
   */
  stop(): void {
    this.isRunning = false;
    if (this.pollTimeout) {
      clearTimeout(this.pollTimeout);
      this.pollTimeout = undefined;
    }
  }

  private scheduleNextTick(delayMs: number): void {
    if (!this.isRunning) return;
    this.pollTimeout = setTimeout(async () => {
      if (!this.isRunning) return;
      try {
        const processed = await this.processBatch();
        // If we processed items, poll immediately; otherwise wait pollIntervalMs
        this.scheduleNextTick(processed > 0 ? 0 : this.pollIntervalMs);
      } catch {
        this.scheduleNextTick(this.pollIntervalMs);
      }
    }, delayMs);
  }

  /**
   * Drains ready jobs from the queue up to maxJobs (default 100).
   * Ideal for serverless cron execution (e.g. Worker scheduled event).
   */
  async drain(maxJobs = 100): Promise<number> {
    let totalProcessed = 0;
    while (totalProcessed < maxJobs) {
      const batchLimit = Math.min(this.concurrency, maxJobs - totalProcessed);
      const jobs = await this.queue.popMany(batchLimit, this.leaseSeconds);
      if (jobs.length === 0) break;

      await Promise.allSettled(
        jobs.map(async (job) => {
          let heartbeatTimer: any = null;
          if (this.autoHeartbeat && job.lockToken) {
            heartbeatTimer = setInterval(async () => {
              try {
                await this.queue.heartbeat(job);
              } catch {
                // Ignore
              }
            }, this.heartbeatIntervalMs);
          }

          try {
            await this.handler(job);
            await this.queue.ack(job);
          } catch (err) {
            await this.queue.nack(job, err);
          } finally {
            if (heartbeatTimer) {
              clearInterval(heartbeatTimer);
            }
          }
        })
      );

      totalProcessed += jobs.length;
    }
    return totalProcessed;
  }

  /**
   * Indicates whether the worker is currently polling.
   */
  get active(): boolean {
    return this.isRunning;
  }
}
