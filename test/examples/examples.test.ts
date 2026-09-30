import { describe, it, expect, beforeEach, vi } from "vitest";
import { createMockD1Database } from "../helpers/mock-d1.js";
import { createMockKVNamespace } from "../helpers/mock-kv.js";
import { createMockR2Bucket } from "../helpers/mock-r2.js";
import { createMockSqlStorage } from "../helpers/mock-do-sql.js";

import workerApiApp from "../../examples/worker-api/index.js";
import cronCleanerApp from "../../examples/cron-cleaner/index.js";
import queueDoApp, { QueueActor } from "../../examples/durable-object-queue/index.js";

describe("Example Applications Integration Tests", () => {
  describe("examples/worker-api (REST API with L1/L2/L3 Cache & R2 Overflow)", () => {
    let env: any;
    let ctx: ExecutionContext;

    beforeEach(() => {
      env = {
        DB: createMockD1Database(),
        CACHE_KV: createMockKVNamespace(),
        BLOBS: createMockR2Bucket(),
      };
      ctx = {
        waitUntil: vi.fn((p) => p),
        passThroughOnException: vi.fn(),
      } as unknown as ExecutionContext;
    });

    it("creates, reads, filters, and deletes articles", async () => {
      // 1. Validation error on missing id/title
      const badReq = new Request("http://localhost/articles", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ category: "tech" }),
      });
      const badRes = await workerApiApp.fetch(badReq, env, ctx);
      expect(badRes.status).toBe(400);

      // 2. Create article 1
      const createReq1 = new Request("http://localhost/articles", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          id: "art-1",
          title: "Introduction to Workers KVDB",
          category: "tech",
          content: "Cloudflare D1 is SQLite at the edge...",
          views: 100,
        }),
      });
      const createRes1 = await workerApiApp.fetch(createReq1, env, ctx);
      expect(createRes1.status).toBe(201);
      const createdData1 = (await createRes1.json()) as any;
      expect(createdData1.success).toBe(true);

      // 3. Create article 2 (finance category)
      const createReq2 = new Request("http://localhost/articles", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          id: "art-2",
          title: "Serverless Economics",
          category: "finance",
          content: "Pay as you go...",
          views: 250,
        }),
      });
      await workerApiApp.fetch(createReq2, env, ctx);

      // 4. Read article through cache (L1/L2/L3)
      const getReq = new Request("http://localhost/articles/art-1", { method: "GET" });
      const getRes = await workerApiApp.fetch(getReq, env, ctx);
      expect(getRes.status).toBe(200);
      const fetchedArticle = (await getRes.json()) as any;
      expect(fetchedArticle.id).toBe("art-1");
      expect(fetchedArticle.title).toBe("Introduction to Workers KVDB");

      // 5. Read non-existent article returns 404
      const notFoundReq = new Request("http://localhost/articles/art-999", { method: "GET" });
      const notFoundRes = await workerApiApp.fetch(notFoundReq, env, ctx);
      expect(notFoundRes.status).toBe(404);

      // 6. Filter articles by category
      const filterReq = new Request("http://localhost/articles?category=tech", { method: "GET" });
      const filterRes = await workerApiApp.fetch(filterReq, env, ctx);
      expect(filterRes.status).toBe(200);
      const filterData = (await filterRes.json()) as any;
      expect(filterData.articles.length).toBe(1);
      expect(filterData.articles[0].id).toBe("art-1");

      // 7. Delete article
      const deleteReq = new Request("http://localhost/articles/art-1", { method: "DELETE" });
      const deleteRes = await workerApiApp.fetch(deleteReq, env, ctx);
      expect(deleteRes.status).toBe(200);
      const deleteData = (await deleteRes.json()) as any;
      expect(deleteData.deleted).toBe(true);

      // Verify gone
      const verifyGet = await workerApiApp.fetch(getReq, env, ctx);
      expect(verifyGet.status).toBe(404);

      // 8. Unknown route returns 404
      const unknownRes = await workerApiApp.fetch(new Request("http://localhost/unknown"), env, ctx);
      expect(unknownRes.status).toBe(404);
    });
  });

  describe("examples/cron-cleaner (Scheduled Cron & HTTP Cleaner)", () => {
    let env: any;
    let ctx: ExecutionContext;

    beforeEach(() => {
      env = {
        DB: createMockD1Database(),
        BLOBS: createMockR2Bucket(),
      };
      ctx = {
        waitUntil: vi.fn((p) => p),
        passThroughOnException: vi.fn(),
      } as unknown as ExecutionContext;
    });

    it("handles scheduled cron trigger", async () => {
      const scheduledEvent = {
        cron: "*/5 * * * *",
        scheduledTime: Date.now(),
        type: "scheduled",
      } as unknown as ScheduledController;

      await cronCleanerApp.scheduled(scheduledEvent, env, ctx);
      expect(ctx.waitUntil).toHaveBeenCalledTimes(1);
    });

    it("handles HTTP /cleanup, /vacuum, and /health endpoints", async () => {
      // 1. /health
      const healthRes = await cronCleanerApp.fetch(new Request("http://localhost/health"), env, ctx);
      expect(healthRes.status).toBe(200);
      const healthData = (await healthRes.json()) as any;
      expect(healthData.status).toBe("ok");
      expect(healthData.service).toBe("kvdb-cron-cleaner");

      // 2. /cleanup
      const cleanupRes = await cronCleanerApp.fetch(
        new Request("http://localhost/cleanup", { method: "POST" }),
        env,
        ctx
      );
      expect(cleanupRes.status).toBe(200);
      const cleanupData = (await cleanupRes.json()) as any;
      expect(cleanupData.success).toBe(true);
      expect(cleanupData.result).toBeDefined();

      // 3. /vacuum
      const vacuumRes = await cronCleanerApp.fetch(
        new Request("http://localhost/vacuum", { method: "POST" }),
        env,
        ctx
      );
      expect(vacuumRes.status).toBe(200);
      const vacuumData = (await vacuumRes.json()) as any;
      expect(vacuumData.success).toBe(true);
      expect(vacuumData.message).toBe("Database vacuum executed");

      // 4. Unknown route
      const unknownRes = await cronCleanerApp.fetch(new Request("http://localhost/other"), env, ctx);
      expect(unknownRes.status).toBe(404);
    });
  });

  describe("examples/durable-object-queue (QueueActor on DO SqlStorage)", () => {
    let mockSql: SqlStorage;
    let alarmTimestamp: number | null = null;
    let stateMock: DurableObjectState;
    let actor: QueueActor;

    beforeEach(() => {
      mockSql = createMockSqlStorage();
      alarmTimestamp = null;

      stateMock = {
        storage: {
          sql: mockSql,
          getAlarm: vi.fn(async () => alarmTimestamp),
          setAlarm: vi.fn(async (t: number) => {
            alarmTimestamp = t;
          }),
          deleteAlarm: vi.fn(async () => {
            alarmTimestamp = null;
          }),
        },
      } as unknown as DurableObjectState;

      actor = new QueueActor(stateMock, { QUEUE_DO: {} as any });
    });

    it("enqueues jobs, checks stats, processes jobs, and handles alarms", async () => {
      // 1. Enqueue job
      const pushReq = new Request("http://localhost/push", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          payload: { task: "render_video", payload: { resolution: "4k" } },
          priority: 50,
        }),
      });
      const pushRes = await actor.fetch(pushReq);
      expect(pushRes.status).toBe(201);
      const pushData = (await pushRes.json()) as any;
      expect(pushData.success).toBe(true);
      expect(pushData.job.id).toBeDefined();

      // Alarm should have been scheduled
      expect(alarmTimestamp).not.toBeNull();

      // 2. Check stats
      const statsReq = new Request("http://localhost/stats", { method: "GET" });
      const statsRes = await actor.fetch(statsReq);
      expect(statsRes.status).toBe(200);
      const stats = (await statsRes.json()) as any;
      expect(stats.ready).toBe(1);

      // 3. Process jobs
      const processReq = new Request("http://localhost/process", { method: "POST" });
      const processRes = await actor.fetch(processReq);
      expect(processRes.status).toBe(200);
      const processData = (await processRes.json()) as any;
      expect(processData.processedCount).toBe(1);

      // 4. Reap route
      const reapReq = new Request("http://localhost/reap", { method: "POST" });
      const reapRes = await actor.fetch(reapReq);
      expect(reapRes.status).toBe(200);

      // 5. Test alarm execution
      // Push another job first
      await actor.fetch(
        new Request("http://localhost/push", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ payload: { task: "send_report" } }),
        })
      );
      await actor.alarm();

      const finalStatsRes = await actor.fetch(statsReq);
      const finalStats = (await finalStatsRes.json()) as any;
      expect(finalStats.completed).toBe(2);

      // 6. Unknown route in DO returns 404
      const notFoundRes = await actor.fetch(new Request("http://localhost/unknown"));
      expect(notFoundRes.status).toBe(404);
    });

    it("routes incoming Worker fetch requests to DO stub", async () => {
      const mockStub = {
        fetch: vi.fn(async (_req: Request) => new Response("From DO Stub")),
      };
      const mockEnv = {
        QUEUE_DO: {
          idFromName: vi.fn(() => ({ toString: () => "mock-id" })),
          get: vi.fn(() => mockStub),
        },
      } as unknown as any;

      const res = await queueDoApp.fetch(new Request("http://localhost/stats"), mockEnv);
      expect(mockStub.fetch).toHaveBeenCalledTimes(1);
      expect(await res.text()).toBe("From DO Stub");
    });
  });
});
