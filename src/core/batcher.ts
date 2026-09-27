import type { TableSetItem } from "./table.js";

export interface AutoBatchConfig {
  /**
   * Maximum number of write items to accumulate before triggering an immediate batch flush.
   * Default: 50.
   */
  maxBatchSize?: number;

  /**
   * Maximum milliseconds to wait before flushing accumulated writes if maxBatchSize is not reached.
   * Default: 10.
   */
  maxWaitMs?: number;

  /**
   * Optional Cloudflare ExecutionContext to register background flushes with ctx.waitUntil().
   */
  ctx?: ExecutionContext;
}

export interface PendingWrite<V> {
  item: TableSetItem<V>;
  resolve: () => void;
  reject: (err: unknown) => void;
}

/**
 * Micro-Batch Write Buffer for Cloudflare Workers.
 *
 * Automatically coalesces concurrent discrete `set()` operations occurring within
 * the same isolate and time window into a single atomic `setMany()` / `db.batch()`.
 */
export class WriteBatcher<V = unknown> {
  private queue: PendingWrite<V>[] = [];
  private timer: any = null;
  private readonly maxBatchSize: number;
  private readonly maxWaitMs: number;
  private readonly ctx?: ExecutionContext;
  private isFlushing = false;
  private activeFlushPromise?: Promise<void>;

  constructor(
    private readonly flushHandler: (items: readonly TableSetItem<V>[]) => Promise<void>,
    config?: AutoBatchConfig
  ) {
    this.maxBatchSize = config?.maxBatchSize ?? 50;
    this.maxWaitMs = config?.maxWaitMs ?? 10;
    this.ctx = config?.ctx;
  }

  /**
   * Enqueues a write item into the micro-batch buffer.
   * Returns a promise that resolves once the entire batch containing this write
   * has been successfully committed to the database.
   */
  async enqueue(item: TableSetItem<V>): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.queue.push({ item, resolve, reject });

      if (this.queue.length >= this.maxBatchSize) {
        void this.triggerFlush();
      } else if (!this.timer) {
        this.timer = setTimeout(() => {
          this.timer = null;
          void this.triggerFlush();
        }, this.maxWaitMs);

        // In Cloudflare Worker, register background flush promise with ctx.waitUntil to prevent premature isolate suspension
        if (this.ctx && typeof this.ctx.waitUntil === "function") {
          this.ctx.waitUntil(this.waitForPendingDrain());
        }
      }
    });
  }

  private async waitForPendingDrain(): Promise<void> {
    while (this.queue.length > 0 || this.isFlushing) {
      if (this.activeFlushPromise) {
        try {
          await this.activeFlushPromise;
        } catch {
          // Handled per individual item
        }
      } else {
        await new Promise((r) => setTimeout(r, this.maxWaitMs));
      }
    }
  }

  private async triggerFlush(): Promise<void> {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    await this.flush();
  }

  /**
   * Immediately flushes all queued writes to the database.
   */
  async flush(): Promise<void> {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }

    if (this.queue.length === 0) {
      return;
    }

    // If already flushing, wait for current flush to finish then proceed
    if (this.isFlushing && this.activeFlushPromise) {
      await this.activeFlushPromise;
      if (this.queue.length === 0) return;
    }

    this.isFlushing = true;
    const batch = this.queue.splice(0, this.queue.length);

    this.activeFlushPromise = (async () => {
      try {
        await this.flushHandler(batch.map((b) => b.item));
        for (const entry of batch) {
          entry.resolve();
        }
      } catch (err) {
        for (const entry of batch) {
          entry.reject(err);
        }
      } finally {
        this.isFlushing = false;
        this.activeFlushPromise = undefined;
        // If more writes arrived during flush execution, schedule follow-up flush
        if (this.queue.length > 0) {
          if (this.queue.length >= this.maxBatchSize) {
            void this.triggerFlush();
          } else if (!this.timer) {
            this.timer = setTimeout(() => {
              this.timer = null;
              void this.triggerFlush();
            }, this.maxWaitMs);
          }
        }
      }
    })();

    await this.activeFlushPromise;
  }

  /**
   * Number of items currently queued in the buffer.
   */
  get pendingCount(): number {
    return this.queue.length;
  }

  /**
   * Whether a batch flush is currently executing.
   */
  get isFlushingNow(): boolean {
    return this.isFlushing;
  }

  /**
   * Clears any buffered items without committing (cancels pending promises).
   */
  clear(error?: Error): void {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    const err = error ?? new Error("Write buffer cleared");
    const items = this.queue.splice(0, this.queue.length);
    for (const item of items) {
      item.reject(err);
    }
  }
}
