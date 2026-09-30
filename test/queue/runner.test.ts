import { describe, it, expect, beforeEach, vi } from "vitest";
import { createMockD1Database } from "../helpers/mock-d1.js";
import { JobQueue } from "../../src/queue/queue.js";
import { QueueWorker } from "../../src/queue/runner.js";
import type { Job } from "../../src/queue/types.js";

describe("QueueWorker (Managed Queue Runner)", () => {
  let rawDb: D1Database;
  let queue: JobQueue<{ task: string; num: number }>;

  beforeEach(async () => {
    rawDb = createMockD1Database();
    queue = new JobQueue({
      db: rawDb,
      queueName: "runner-queue",
      leaseSeconds: 5,
      defaultMaxAttempts: 3,
    });
    await queue.init();
  });

  it("processes a single job with processOne() and acknowledges on success", async () => {
    await queue.push({ task: "send_email", num: 1 });

    const processed: string[] = [];
    const worker = new QueueWorker(queue, async (job) => {
      processed.push(job.payload.task);
    });

    const ran = await worker.processOne();
    expect(ran).toBe(true);
    expect(processed).toEqual(["send_email"]);

    // Queue should now be empty
    const stats = await queue.stats();
    expect(stats.completed).toBe(1);
    expect(stats.ready).toBe(0);

    // Running processOne() on empty queue returns false
    const ranAgain = await worker.processOne();
    expect(ranAgain).toBe(false);
  });

  it("catches errors in processOne() and nacks the job", async () => {
    await queue.push({ task: "failing_task", num: 2 });

    const worker = new QueueWorker(queue, async () => {
      throw new Error("Worker task crashed");
    });

    const ran = await worker.processOne();
    expect(ran).toBe(true);

    // Job should be failed or delayed back into queue with backoff
    const stats = await queue.stats();
    expect(stats.active).toBe(0);
    // Attempts was incremented
    expect(stats.completed).toBe(0);
  });

  it("handles autoHeartbeat during processOne() execution", async () => {
    await queue.push({ task: "long_task", num: 3 });

    const heartbeatSpy = vi.spyOn(queue, "heartbeat");

    const worker = new QueueWorker(
      queue,
      async () => {
        // Sleep 40ms to trigger at least one heartbeat with 20ms interval
        await new Promise((r) => setTimeout(r, 45));
      },
      {
        autoHeartbeat: true,
        heartbeatIntervalMs: 20,
      }
    );

    const ran = await worker.processOne();
    expect(ran).toBe(true);
    expect(heartbeatSpy).toHaveBeenCalled();
    heartbeatSpy.mockRestore();
  });

  it("processes batch jobs and handles partial failures via Promise.allSettled", async () => {
    await queue.push({ task: "task_ok_1", num: 10 });
    await queue.push({ task: "task_fail", num: 20 });
    await queue.push({ task: "task_ok_2", num: 30 });

    const processed: string[] = [];

    const worker = new QueueWorker(
      queue,
      async (job) => {
        if (job.payload.task === "task_fail") {
          throw new Error("Job failed intentional");
        }
        processed.push(job.payload.task);
      },
      { concurrency: 5 }
    );

    const count = await worker.processBatch();
    expect(count).toBe(3);
    expect(processed).toContain("task_ok_1");
    expect(processed).toContain("task_ok_2");

    const stats = await queue.stats();
    expect(stats.completed).toBe(2);
  });

  it("drains all ready jobs up to maxJobs limit", async () => {
    // Push 8 jobs
    for (let i = 1; i <= 8; i++) {
      await queue.push({ task: `drain_${i}`, num: i });
    }

    const processed: number[] = [];
    const worker = new QueueWorker(
      queue,
      async (job) => {
        processed.push(job.payload.num);
      },
      { concurrency: 3 }
    );

    // Drain with maxJobs = 5
    const drained = await worker.drain(5);
    expect(drained).toBe(5);
    expect(processed.length).toBe(5);

    const stats = await queue.stats();
    expect(stats.completed).toBe(5);
    expect(stats.ready).toBe(3);

    // Drain the remaining jobs (up to default 100)
    const remainingDrained = await worker.drain();
    expect(remainingDrained).toBe(3);
    expect(processed.length).toBe(8);

    // Drain on empty queue returns 0
    const emptyDrain = await worker.drain();
    expect(emptyDrain).toBe(0);
  });

  it("manages start, active, and stop background polling lifecycle", async () => {
    vi.useFakeTimers();

    const processed: string[] = [];
    const worker = new QueueWorker(
      queue,
      async (job) => {
        processed.push(job.payload.task);
      },
      { pollIntervalMs: 100, concurrency: 2 }
    );

    expect(worker.active).toBe(false);

    worker.start();
    expect(worker.active).toBe(true);

    // Starting again does not duplicate timers
    worker.start();
    expect(worker.active).toBe(true);

    // Stop runner
    worker.stop();
    expect(worker.active).toBe(false);

    vi.useRealTimers();
  });
});
