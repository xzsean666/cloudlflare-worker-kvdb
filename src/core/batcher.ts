import type { TableSetItem } from "./table.js";

export interface AutoBatchConfig {
  /**
   * Maximum number of write items to accumulate before triggering an immediate batch flush.
   * Default: 50.
   */
  maxBatchSize?: number;

  /**
   * Maximum milliseconds to wait before flushing accumulated writes if maxBatchSize is not reached.
   * Default: 200.
   */
  maxWaitMs?: number;

  /**
   * Optional Cloudflare ExecutionContext to register background flushes with ctx.waitUntil().
   */
  ctx?: ExecutionContext;
}

export type BatchOpType = "set" | "delete";

export interface PendingBatchItem<V = unknown> {
  key: string | number;
  type: BatchOpType;
  item?: TableSetItem<V>;
  hadPriorSetInSameWindow?: boolean;
  resolves: Array<(val?: any) => void>;
  rejects: Array<(err: unknown) => void>;
}

export interface BatchFlushPayload<V = unknown> {
  sets: readonly TableSetItem<V>[];
  deletes: readonly (string | number)[];
}

export interface BatchFlushResult {
  /**
   * Set of keys that actually existed and were deleted in the database.
   */
  deletedKeys?: Set<string | number>;
}

export type FlushHandler<V = unknown> = (
  payload: BatchFlushPayload<V>
) => Promise<BatchFlushResult | void>;

/**
 * In-Memory Aggregating Micro-Batch Write Buffer for Cloudflare Workers.
 *
 * Automatically coalesces concurrent discrete `set()`, `update()`, and `delete()` operations
 * occurring within the same isolate and debounce window (default 200ms) into an aggregated in-memory
 * state before flushing an atomic net-delta batch to the storage layer (`setMany()` / `deleteMany()`).
 */
export class WriteBatcher<V = unknown> {
  private pendingOps = new Map<string, PendingBatchItem<V>>();
  private timer: any = null;
  private readonly maxBatchSize: number;
  private readonly maxWaitMs: number;
  private readonly ctx?: ExecutionContext;
  private isFlushing = false;
  private activeFlushPromise?: Promise<void>;

  constructor(
    private readonly flushHandler: FlushHandler<V>,
    config?: AutoBatchConfig
  ) {
    this.maxBatchSize = config?.maxBatchSize ?? 50;
    this.maxWaitMs = config?.maxWaitMs ?? 200;
    this.ctx = config?.ctx;
  }

  /**
   * Enqueues or merges a write item into the micro-batch buffer.
   * If the key already has a pending write in the buffer, it coalesces in memory.
   * Returns a promise that resolves once the entire batch containing this write
   * has been successfully committed to the database.
   */
  async enqueue(item: TableSetItem<V>): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const strKey = String(item.key);
      const existing = this.pendingOps.get(strKey);
      if (existing) {
        // Coalesce / Aggregate in memory with secondary keys preserved!
        existing.type = "set";
        const mergedKeys = existing.item?.keys || item.keys
          ? { ...(existing.item?.keys ?? {}), ...(item.keys ?? {}) }
          : undefined;
        existing.item = { ...item, keys: mergedKeys };
        existing.resolves.push(resolve);
        existing.rejects.push(reject);
      } else {
        this.pendingOps.set(strKey, {
          key: item.key,
          type: "set",
          item,
          resolves: [resolve],
          rejects: [reject],
        });
      }

      this.checkFlushConditions();
    });
  }

  /**
   * Enqueues or merges a delete operation for a key into the micro-batch buffer.
   * If the key was previously pending as a write in this window, it cancels the write in memory.
   */
  async enqueueDelete(key: string | number): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      const strKey = String(key);
      const existing = this.pendingOps.get(strKey);
      if (existing) {
        // A prior set in this window is canceled in memory!
        const hadPriorSet = existing.type === "set" || Boolean(existing.hadPriorSetInSameWindow);
        for (const prevResolve of existing.resolves) {
          prevResolve();
        }
        existing.type = "delete";
        existing.item = undefined;
        existing.hadPriorSetInSameWindow = hadPriorSet;
        existing.resolves = [(delResult?: boolean) => resolve(delResult ?? true)];
        existing.rejects = [reject];
      } else {
        this.pendingOps.set(strKey, {
          key,
          type: "delete",
          hadPriorSetInSameWindow: false,
          resolves: [(delResult?: boolean) => resolve(delResult ?? false)],
          rejects: [reject],
        });
      }

      this.checkFlushConditions();
    });
  }

  /**
   * Gets the in-memory buffered operation for a key, if any.
   */
  getPending(key: string | number): PendingBatchItem<V> | undefined {
    return this.pendingOps.get(String(key));
  }

  /**
   * Checks if a key has a pending operation in memory.
   */
  hasPending(key: string | number): boolean {
    return this.pendingOps.has(String(key));
  }

  /**
   * Cancels any pending set for a key directly in memory.
   */
  deletePending(key: string | number): boolean {
    const existing = this.pendingOps.get(String(key));
    if (existing) {
      for (const r of existing.resolves) r();
      this.pendingOps.delete(String(key));
      return true;
    }
    return false;
  }

  private checkFlushConditions(): void {
    if (this.pendingOps.size >= this.maxBatchSize) {
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
  }

  private async waitForPendingDrain(): Promise<void> {
    while (this.pendingOps.size > 0 || this.isFlushing) {
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
   * Immediately flushes all queued writes/deletes to the database.
   */
  async flush(): Promise<void> {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }

    // 1. If already flushing, wait for active flush to complete FIRST
    if (this.isFlushing && this.activeFlushPromise) {
      await this.activeFlushPromise;
    }

    // 2. If nothing is pending after waiting, return cleanly
    if (this.pendingOps.size === 0) {
      return;
    }

    this.isFlushing = true;
    const snapshot = Array.from(this.pendingOps.values());
    this.pendingOps.clear();

    const sets: TableSetItem<V>[] = [];
    const deletes: (string | number)[] = [];

    for (const op of snapshot) {
      if (op.type === "set" && op.item) {
        sets.push(op.item);
      } else if (op.type === "delete") {
        deletes.push(op.key);
      }
    }

    this.activeFlushPromise = (async () => {
      try {
        const result = await this.flushHandler({ sets, deletes });
        const deletedSet =
          result && typeof result === "object" && result.deletedKeys
            ? result.deletedKeys
            : undefined;

        for (const op of snapshot) {
          if (op.type === "set") {
            for (const resolve of op.resolves) {
              resolve();
            }
          } else if (op.type === "delete") {
            const wasDeleted =
              op.hadPriorSetInSameWindow || (deletedSet ? deletedSet.has(op.key) : true);
            for (const resolve of op.resolves) {
              resolve(wasDeleted);
            }
          }
        }
      } catch (err) {
        for (const op of snapshot) {
          for (const reject of op.rejects) {
            reject(err);
          }
        }
      } finally {
        this.isFlushing = false;
        this.activeFlushPromise = undefined;

        // If more writes arrived during flush execution, schedule follow-up flush
        if (this.pendingOps.size > 0) {
          if (this.pendingOps.size >= this.maxBatchSize) {
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
    return this.pendingOps.size;
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
    const items = Array.from(this.pendingOps.values());
    this.pendingOps.clear();
    for (const op of items) {
      for (const rej of op.rejects) {
        rej(err);
      }
    }
  }
}
