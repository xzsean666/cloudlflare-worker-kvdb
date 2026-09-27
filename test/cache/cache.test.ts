import { describe, it, expect, vi } from "vitest";
import { TieredCache } from "../../src/cache/cache.js";
import { createMockKVNamespace } from "../helpers/mock-kv.js";

describe("Multi-Tier Caching System", () => {
  it("stores and retrieves from L1 memory cache", async () => {
    const cache = new TieredCache({
      l1: { max: 100, defaultTTLMs: 60000 },
    });

    expect(await cache.get("k1")).toBeNull();
    await cache.set("k1", { score: 100 });

    const val = await cache.get<{ score: number }>("k1");
    expect(val).toEqual({ score: 100 });

    await cache.delete("k1");
    expect(await cache.get("k1")).toBeNull();
  });

  it("falls back to L2 KV on L1 miss and repopulates L1", async () => {
    const kv = createMockKVNamespace();
    const cache = new TieredCache({
      l1: { max: 100 },
      l2: { namespace: kv, prefix: "test_cache:" },
    });

    await cache.set("user:123", { name: "Bob", role: "author" });

    // Verify written to both L1 and L2
    expect(await cache.l1!.has("user:123")).toBe(true);
    expect(await cache.l2!.has("user:123")).toBe(true);

    // Clear L1 memory to simulate isolate restart
    await cache.l1!.clear();
    expect(await cache.l1!.has("user:123")).toBe(false);

    // Get should hit L2 and repopulate L1
    const retrieved = await cache.get<{ name: string; role: string }>("user:123");
    expect(retrieved).toEqual({ name: "Bob", role: "author" });
    expect(await cache.l1!.has("user:123")).toBe(true);
  });

  it("handles Stale-While-Revalidate (SWR) transparently", async () => {
    const waitPromises: Promise<any>[] = [];
    const mockCtx = {
      waitUntil: vi.fn((p: Promise<any>) => {
        waitPromises.push(p);
      }),
    } as unknown as ExecutionContext;

    const cache = new TieredCache({
      l1: { max: 50 },
      ctx: mockCtx,
    });

    let fetchCount = 0;
    const fetcher = async () => {
      fetchCount++;
      return { timestamp: Date.now(), counter: fetchCount };
    };

    // 1. Initial call (cache miss)
    const res1 = await cache.wrap("metrics", fetcher, { ttlMs: 50, swrMs: 100 });
    expect(res1.counter).toBe(1);
    expect(fetchCount).toBe(1);

    // 2. Call while fresh (< 50ms)
    const res2 = await cache.wrap("metrics", fetcher, { ttlMs: 50, swrMs: 100 });
    expect(res2.counter).toBe(1);
    expect(fetchCount).toBe(1);

    // 3. Fast-forward past fresh window to enter stale window
    await new Promise((r) => setTimeout(r, 60));

    // Stale call: returns old counter (1) immediately and schedules background refresh via waitUntil
    const res3 = await cache.wrap("metrics", fetcher, { ttlMs: 50, swrMs: 100 });
    expect(res3.counter).toBe(1); // Immediate stale return
    expect(mockCtx.waitUntil).toHaveBeenCalled();

    // Await background revalidation promise
    await Promise.all(waitPromises);
    expect(fetchCount).toBe(2);

    // 4. Next call should now reflect updated counter (2)
    const res4 = await cache.wrap("metrics", fetcher, { ttlMs: 50, swrMs: 100 });
    expect(res4.counter).toBe(2);
  });

  it("clears all cache tiers simultaneously", async () => {
    const kv = createMockKVNamespace();
    const cache = new TieredCache({
      l1: { max: 100 },
      l2: { namespace: kv },
    });

    await cache.set("itemA", "valA");
    await cache.set("itemB", "valB");

    expect(await cache.has("itemA")).toBe(true);
    await cache.clear();

    expect(await cache.has("itemA")).toBe(false);
    expect(await cache.has("itemB")).toBe(false);
    expect(await cache.l1!.has("itemA")).toBe(false);
    expect(await cache.l2!.has("itemA")).toBe(false);
  });
});
