import {
  JobQueue,
  QueueWorker,
  QueueReaper,
  type Job,
  type QueueStats,
} from "cloudflare-worker-kvdb";

export interface Env {
  QUEUE_DO: DurableObjectNamespace;
}

export interface TaskPayload {
  task: string;
  email?: string;
  payload?: Record<string, unknown>;
}

/**
 * QueueActor: A transactional, persistent Queue Runner inside Cloudflare Durable Objects.
 * Powered by Durable Objects SQLite (ctx.storage.sql).
 */
export class QueueActor {
  private state: DurableObjectState;
  private queue: JobQueue<TaskPayload>;
  private reaper: QueueReaper;
  private worker: QueueWorker<TaskPayload>;
  private initialized = false;

  constructor(state: DurableObjectState, _env: Env) {
    this.state = state;

    // Initialize JobQueue backed directly by Durable Objects SQLite storage
    this.queue = new JobQueue<TaskPayload>({
      db: this.state.storage.sql,
      queueName: "do-tasks",
      leaseSeconds: 30, // 30-second visibility lease
      defaultMaxAttempts: 3,
      baseBackoffMs: 1000,
    });

    // Reaper automatically detects and recovers orphaned jobs if an executor crashes
    this.reaper = new QueueReaper({
      adapter: this.queue.getAdapter(),
      tableName: this.queue.tableName,
    });

    // Worker executor handling job execution with heartbeat support
    this.worker = new QueueWorker<TaskPayload>(
      this.queue,
      async (job: Job<TaskPayload>) => {
        // Business logic execution
        console.log(`[DO Queue] Processing job ${job.id}:`, job.payload.task);

        if (job.payload.task === "error_simulation") {
          throw new Error("Simulated task failure to demonstrate DLQ & exponential backoff");
        }

        // Simulate async operation
        await new Promise((resolve) => setTimeout(resolve, 50));
      },
      {
        concurrency: 5,
        heartbeatIntervalMs: 5000, // extend lease every 5s for long tasks
      }
    );
  }

  private async ensureInitialized(): Promise<void> {
    if (!this.initialized) {
      await this.queue.init();
      this.initialized = true;
    }
  }

  /**
   * Cloudflare Durable Objects alarm handler.
   * Periodically recovers expired leases and processes outstanding jobs.
   */
  async alarm(): Promise<void> {
    await this.ensureInitialized();
    const reaped = await this.reaper.reap();
    if (reaped > 0) {
      console.log(`[DO Queue] Reaped ${reaped} timed-out jobs`);
    }

    const processed = await this.worker.processBatch();
    if (processed > 0) {
      console.log(`[DO Queue] Processed ${processed} jobs on alarm trigger`);
    }

    // Schedule next alarm in 60 seconds if jobs remain
    const stats = await this.queue.stats();
    if (stats.ready > 0 || stats.delayed > 0) {
      await this.state.storage.setAlarm(Date.now() + 60000);
    }
  }

  async fetch(request: Request): Promise<Response> {
    await this.ensureInitialized();
    const url = new URL(request.url);

    // Route: POST /push - Enqueue a new job
    if (request.method === "POST" && url.pathname === "/push") {
      const body = (await request.json()) as {
        payload: TaskPayload;
        priority?: number;
        delayMs?: number;
        dedupKey?: string;
      };

      const job = await this.queue.push(body.payload, {
        priority: body.priority,
        delayMs: body.delayMs,
        dedupKey: body.dedupKey,
      });

      // Schedule background processing alarm if not already scheduled
      const currentAlarm = await this.state.storage.getAlarm();
      if (!currentAlarm) {
        await this.state.storage.setAlarm(Date.now() + 100);
      }

      return Response.json({ success: true, job }, { status: 201 });
    }

    // Route: POST /process - Trigger batch execution manually
    if (request.method === "POST" && url.pathname === "/process") {
      const processedCount = await this.worker.processBatch();
      return Response.json({ success: true, processedCount });
    }

    // Route: POST /reap - Recover expired locks manually
    if (request.method === "POST" && url.pathname === "/reap") {
      const reapedCount = await this.reaper.reap();
      return Response.json({ success: true, reapedCount });
    }

    // Route: GET /stats - Get queue statistics
    if (request.method === "GET" && url.pathname === "/stats") {
      const stats: QueueStats = await this.queue.stats();
      return Response.json(stats);
    }

    return new Response("Not Found", { status: 404 });
  }
}

/**
 * Worker Entrypoint: Routes incoming HTTP requests to the Durable Object singleton.
 */
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const id = env.QUEUE_DO.idFromName("global-queue-actor");
    const stub = env.QUEUE_DO.get(id);
    return stub.fetch(request);
  },
};
