import { describe, it, expect, beforeEach } from "vitest";
import { createMockD1Database } from "../helpers/mock-d1.js";
import { createMockSqlStorage } from "../helpers/mock-do-sql.js";
import { JobQueue } from "../../src/queue/queue.js";
import { QueueReaper } from "../../src/queue/reaper.js";
import { QueueWorker } from "../../src/queue/runner.js";

describe("JobQueue on Cloudflare D1", () => {
  let rawDb: D1Database;
  let queue: JobQueue<{ task: string; count: number }>;

  beforeEach(async () => {
    rawDb = createMockD1Database();
    queue = new JobQueue({
      db: rawDb,
      queueName: "email-queue",
      leaseSeconds: 10,
      defaultMaxAttempts: 3,
      baseBackoffMs: 200,
    });
    await queue.init();
  });

  it("handles standard push, pop, and ack lifecycle", async () => {
    const job = await queue.push({ task: "send_welcome", count: 1 });
    expect(job.id).toBeDefined();
    expect(job.state).toBe("ready");
    expect(job.attempts).toBe(0);

    // Pop the job
    const leased = await queue.pop();
    expect(leased).not.toBeNull();
    expect(leased!.id).toBe(job.id);
    expect(leased!.state).toBe("active");
    expect(leased!.attempts).toBe(1);
    expect(leased!.lockToken).toBeDefined();
    expect(leased!.payload).toEqual({ task: "send_welcome", count: 1 });

    // Acknowledge completion
    const acked = await queue.ack(leased!);
    expect(acked).toBe(true);

    // No more ready jobs
    expect(await queue.pop()).toBeNull();

    // Check stats
    const stats = await queue.stats();
    expect(stats.completed).toBe(1);
    expect(stats.ready).toBe(0);
    expect(stats.active).toBe(0);
  });

  it("respects priority ordering (higher priority first)", async () => {
    await queue.push({ task: "low", count: 1 }, { priority: 0 });
    await queue.push({ task: "high", count: 2 }, { priority: 100 });
    await queue.push({ task: "medium", count: 3 }, { priority: 50 });

    const job1 = await queue.pop();
    expect(job1!.payload.task).toBe("high");

    const job2 = await queue.pop();
    expect(job2!.payload.task).toBe("medium");

    const job3 = await queue.pop();
    expect(job3!.payload.task).toBe("low");
  });

  it("respects delayMs before making jobs available", async () => {
    // 5 seconds delay
    await queue.push({ task: "delayed_task", count: 1 }, { delayMs: 5000 });

    // Should not be available immediately
    expect(await queue.pop()).toBeNull();

    const stats = await queue.stats();
    expect(stats.delayed).toBe(1);
  });

  it("deduplicates jobs using dedupKey", async () => {
    const job1 = await queue.push(
      { task: "sync_user", count: 1 },
      { dedupKey: "user:123" }
    );

    const job2 = await queue.push(
      { task: "sync_user", count: 2 },
      { dedupKey: "user:123" }
    );

    expect(job1.id).toBe(job2.id);

    const stats = await queue.stats();
    expect(stats.ready).toBe(1);
    expect(stats.total).toBe(1);
  });

  it("atomically leases batches without overlapping between concurrent workers", async () => {
    for (let i = 1; i <= 6; i++) {
      await queue.push({ task: `task_${i}`, count: i });
    }

    // Two concurrent workers pop 3 items each simultaneously
    const [batch1, batch2] = await Promise.all([
      queue.popMany(3),
      queue.popMany(3),
    ]);

    expect(batch1).toHaveLength(3);
    expect(batch2).toHaveLength(3);

    const ids1 = new Set(batch1.map((j) => j.id));
    const ids2 = new Set(batch2.map((j) => j.id));

    // Zero overlap
    for (const id of ids1) {
      expect(ids2.has(id)).toBe(false);
    }
  });

  it("retries with exponential backoff on nack and moves to DLQ when maxAttempts is exceeded", async () => {
    const job = await queue.push({ task: "fragile_task", count: 1 }, { maxAttempts: 2 });

    // Attempt 1
    const attempt1 = (await queue.pop())!;
    expect(attempt1.attempts).toBe(1);
    await queue.nack(attempt1, "First failure");

    // Job is now in 'delayed' state waiting for backoff
    const stats1 = await queue.stats();
    expect(stats1.delayed).toBe(1);
    expect(stats1.active).toBe(0);

    // Force job to become available by advancing available_at in DB
    await rawDb
      .prepare(`UPDATE ${queue.tableName} SET available_at = 0 WHERE id = ?`)
      .bind(job.id)
      .run();

    // Attempt 2 (reaches maxAttempts = 2)
    const attempt2 = (await queue.pop())!;
    expect(attempt2.attempts).toBe(2);
    await queue.nack(attempt2, "Fatal second failure");

    // Job must now be moved to Dead-Letter Queue (state: 'failed')
    const finalJob = await queue.getJob(job.id);
    expect(finalJob!.state).toBe("failed");
    expect(finalJob!.lastError).toBe("Fatal second failure");

    const stats2 = await queue.stats();
    expect(stats2.failed).toBe(1);
    expect(stats2.ready).toBe(0);
  });

  it("extends lease via heartbeat", async () => {
    await queue.push({ task: "long_job", count: 1 });
    const leased = (await queue.pop(5))!;

    const initialLease = leased.leasedUntil!;
    expect(initialLease).toBeDefined();

    // Extend lease by 20 seconds
    const ok = await queue.heartbeat(leased, leased.lockToken!, 20);
    expect(ok).toBe(true);

    const refreshed = (await queue.getJob(leased.id))!;
    expect(refreshed.leasedUntil!).toBeGreaterThan(initialLease);
  });

  it("recovers orphaned expired leases via QueueReaper", async () => {
    await queue.push({ task: "crashed_worker_job", count: 1 });
    const leased = (await queue.pop(1))!;

    // Simulate worker crashing and lease expiring in the past
    await rawDb
      .prepare(`UPDATE ${queue.tableName} SET leased_until = 1000 WHERE id = ?`)
      .bind(leased.id)
      .run();

    const reaper = new QueueReaper({
      adapter: queue.getAdapter(),
      tableName: queue.tableName,
    });

    const reapedCount = await reaper.reap();
    expect(reapedCount).toBe(1);

    // Job is now 'ready' again and can be picked up by another worker
    const recovered = await queue.pop();
    expect(recovered).not.toBeNull();
    expect(recovered!.id).toBe(leased.id);
  });
});

describe("JobQueue on Durable Objects SQLite (SqlStorage)", () => {
  let mockSql: SqlStorage;
  let queue: JobQueue<{ action: string }>;

  beforeEach(async () => {
    mockSql = createMockSqlStorage();
    queue = new JobQueue({
      db: mockSql,
      queueName: "do-tasks",
      leaseSeconds: 15,
    });
    await queue.init();
  });

  it("processes queue operations correctly in Durable Objects SQLite", async () => {
    await queue.push({ action: "compute" });
    const job = await queue.pop();
    expect(job).not.toBeNull();
    expect(job!.payload.action).toBe("compute");

    await queue.ack(job!);

    const stats = await queue.stats();
    expect(stats.completed).toBe(1);
  });

  it("processes jobs with QueueWorker managed runner", async () => {
    await queue.push({ action: "item_1" });
    await queue.push({ action: "item_2" });
    await queue.push({ action: "item_3" });

    const processed: string[] = [];

    const worker = new QueueWorker(
      queue,
      async (job) => {
        processed.push(job.payload.action);
      },
      { concurrency: 5 }
    );

    const count = await worker.processBatch();
    expect(count).toBe(3);
    expect(processed).toEqual(["item_1", "item_2", "item_3"]);

    const stats = await queue.stats();
    expect(stats.completed).toBe(3);
    expect(stats.ready).toBe(0);
  });
});
