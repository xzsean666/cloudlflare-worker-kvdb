import {
  TTLSweeper,
  createScheduledHandler,
  type SweepResult,
} from "cloudflare-worker-kvdb";

export interface Env {
  DB: D1Database;
  BLOBS?: R2Bucket;
}

/**
 * Cloudflare Worker with Scheduled Cron Trigger for Database Maintenance.
 * Sweeps expired records across all KVDB tables and cascades deletion to orphaned R2 overflow blobs.
 */
export default {
  // Cron handler: Triggers on scheduled cron events (e.g., every 5 minutes)
  async scheduled(
    event: ScheduledController,
    env: Env,
    ctx: ExecutionContext
  ): Promise<void> {
    const handler = createScheduledHandler({
      db: env.DB,
      r2Bucket: env.BLOBS,
      autoDiscoverTables: true,
      batchSize: 200,
      maxBatchesPerTable: 50,
      onSuccess: (result: SweepResult) => {
        console.log(
          `[CRON CLEANER] Cleaned ${result.expiredRowsDeleted} rows and ` +
          `${result.blobsDeleted} R2 blobs across tables [${result.tablesProcessed.join(", ")}] ` +
          `in ${result.durationMs}ms (Cron: ${event.cron})`
        );
      },
      onError: (err: unknown) => {
        console.error("[CRON CLEANER ERROR]", err);
      },
    });

    await handler(event, env, ctx);
  },

  // HTTP Handler: Optional manual trigger and health endpoint
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "POST" && url.pathname === "/cleanup") {
      const sweeper = new TTLSweeper({
        db: env.DB,
        r2Bucket: env.BLOBS,
        autoDiscoverTables: true,
      });

      const result = await sweeper.sweepExpired();
      return Response.json({ success: true, result });
    }

    if (request.method === "POST" && url.pathname === "/vacuum") {
      const sweeper = new TTLSweeper({
        db: env.DB,
      });
      await sweeper.vacuum();
      return Response.json({ success: true, message: "Database vacuum executed" });
    }

    if (request.method === "GET" && url.pathname === "/health") {
      return Response.json({
        status: "ok",
        service: "kvdb-cron-cleaner",
        timestamp: Date.now(),
      });
    }

    return new Response("Not Found", { status: 404 });
  },
};
