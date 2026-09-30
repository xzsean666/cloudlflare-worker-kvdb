import { describe, it, expect, vi, beforeEach } from "vitest";
import { createMockD1Database } from "../helpers/mock-d1.js";
import { TTLSweeper } from "../../src/maintenance/sweeper.js";
import { createScheduledHandler } from "../../src/maintenance/handler.js";
import type { SweepResult } from "../../src/maintenance/types.js";

describe("createScheduledHandler (Cron Trigger Handler)", () => {
  let rawDb: D1Database;

  beforeEach(() => {
    rawDb = createMockD1Database();
  });

  it("creates a scheduled handler from TTLSweeper instance and executes sweep", async () => {
    const sweeper = new TTLSweeper({ db: rawDb });
    const sweepSpy = vi.spyOn(sweeper, "sweepExpired").mockResolvedValue({
      tablesProcessed: ["t_items"],
      expiredRowsDeleted: 4,
      blobsDeleted: 0,
      orphanedBlobsDeleted: 0,
      durationMs: 12,
    });

    const handler = createScheduledHandler(sweeper);

    const event = {
      cron: "*/5 * * * *",
      scheduledTime: Date.now(),
      type: "scheduled",
    } as unknown as ScheduledController;

    await handler(event, {}, undefined);

    expect(sweepSpy).toHaveBeenCalledTimes(1);
  });

  it("handles ExecutionContext.waitUntil if ctx is provided", async () => {
    const sweeper = new TTLSweeper({ db: rawDb });
    let awaitedPromise: Promise<unknown> | null = null;

    const ctx = {
      waitUntil: vi.fn((promise: Promise<unknown>) => {
        awaitedPromise = promise;
      }),
      passThroughOnException: vi.fn(),
    } as unknown as ExecutionContext;

    const handler = createScheduledHandler(sweeper);
    const event = { cron: "0 0 * * *", scheduledTime: Date.now(), type: "scheduled" } as unknown as ScheduledController;

    await handler(event, {}, ctx);

    expect(ctx.waitUntil).toHaveBeenCalledTimes(1);
    expect(awaitedPromise).not.toBeNull();
    await awaitedPromise;
  });

  it("invokes onSuccess callback upon successful sweep", async () => {
    let receivedResult: SweepResult | null = null;
    const onSuccess = vi.fn((result: SweepResult) => {
      receivedResult = result;
    });

    const handler = createScheduledHandler({
      db: rawDb,
      onSuccess,
    });

    const event = { cron: "*/10 * * * *", scheduledTime: Date.now(), type: "scheduled" } as unknown as ScheduledController;
    await handler(event);

    expect(onSuccess).toHaveBeenCalledTimes(1);
    expect(receivedResult).not.toBeNull();
    expect(receivedResult!.durationMs).toBeGreaterThanOrEqual(0);
  });

  it("invokes onError callback when sweep fails", async () => {
    const sweeper = new TTLSweeper({ db: rawDb });
    const sweepError = new Error("D1 connection lost");
    vi.spyOn(sweeper, "sweepExpired").mockRejectedValue(sweepError);

    const onError = vi.fn();
    const handler = createScheduledHandler({
      db: rawDb,
      onError,
    });

    // Replace the internal sweeper mock
    const originalSweeperSweep = TTLSweeper.prototype.sweepExpired;
    TTLSweeper.prototype.sweepExpired = vi.fn().mockRejectedValue(sweepError);

    try {
      const event = { cron: "0 * * * *", scheduledTime: Date.now(), type: "scheduled" } as unknown as ScheduledController;
      await handler(event);
      expect(onError).toHaveBeenCalledWith(sweepError);
    } finally {
      TTLSweeper.prototype.sweepExpired = originalSweeperSweep;
    }
  });

  it("falls back to console.error when onError is omitted and sweep fails", async () => {
    const consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const sweepError = new Error("Database timeout");

    const originalSweeperSweep = TTLSweeper.prototype.sweepExpired;
    TTLSweeper.prototype.sweepExpired = vi.fn().mockRejectedValue(sweepError);

    try {
      const handler = createScheduledHandler({ db: rawDb });
      const event = { cron: "0 * * * *", scheduledTime: Date.now(), type: "scheduled" } as unknown as ScheduledController;
      await handler(event);
      expect(consoleErrorSpy).toHaveBeenCalledWith(
        "[cf-kvdb] Scheduled TTL sweeper error:",
        sweepError
      );
    } finally {
      TTLSweeper.prototype.sweepExpired = originalSweeperSweep;
      consoleErrorSpy.mockRestore();
    }
  });
});
