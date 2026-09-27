import type { SweeperOptions, SweepResult } from "./types.js";
import { TTLSweeper } from "./sweeper.js";

export interface ScheduledHandlerOptions extends SweeperOptions {
  onSuccess?: (result: SweepResult) => void | Promise<void>;
  onError?: (error: unknown) => void | Promise<void>;
}

/**
 * Creates a ready-to-use Cloudflare Worker `scheduled(event, env, ctx)` cron trigger handler.
 *
 * Usage in worker index.ts:
 * ```ts
 * export default {
 *   fetch: app.fetch,
 *   scheduled: createScheduledHandler({ db: env.DB, r2Bucket: env.R2 }),
 * };
 * ```
 */
export function createScheduledHandler(
  sweeperOrOptions: TTLSweeper | ScheduledHandlerOptions
): (event: ScheduledController, env?: any, ctx?: ExecutionContext) => Promise<void> {
  const sweeper =
    sweeperOrOptions instanceof TTLSweeper
      ? sweeperOrOptions
      : new TTLSweeper(sweeperOrOptions);

  const options = sweeperOrOptions instanceof TTLSweeper ? undefined : sweeperOrOptions;

  return async (
    _event: ScheduledController,
    _env?: any,
    ctx?: ExecutionContext
  ): Promise<void> => {
    const runSweep = async () => {
      try {
        const result = await sweeper.sweepExpired();
        if (options?.onSuccess) {
          await options.onSuccess(result);
        }
      } catch (err) {
        if (options?.onError) {
          await options.onError(err);
        } else {
          console.error("[cf-kvdb] Scheduled TTL sweeper error:", err);
        }
      }
    };

    if (ctx && typeof ctx.waitUntil === "function") {
      ctx.waitUntil(runSweep());
    } else {
      await runSweep();
    }
  };
}
