import { describe, it, expect, beforeEach } from "vitest";
import { TieredCache } from "../../src/cache/cache.js";
import { Cacheable, setDefaultCache, clearDefaultCache } from "../../src/decorators/cacheable.js";
import { CacheClear } from "../../src/decorators/cache-clear.js";

describe("Method Caching Decorators (@Cacheable, @CacheClear)", () => {
  let cache: TieredCache;

  beforeEach(() => {
    cache = new TieredCache({ l1: { max: 100 } });
    setDefaultCache(cache);
  });

  it("caches method execution and returns cached result on subsequent invocations", async () => {
    let executionCount = 0;

    class UserService {
      @Cacheable({ ttlMs: 10000 })
      async getUser(id: string): Promise<{ id: string; name: string; count: number }> {
        executionCount++;
        return { id, name: `User_${id}`, count: executionCount };
      }
    }

    const service = new UserService();

    // Call 1: executes method
    const user1 = await service.getUser("u1");
    expect(user1.count).toBe(1);
    expect(executionCount).toBe(1);

    // Call 2: hits cache
    const user2 = await service.getUser("u1");
    expect(user2.count).toBe(1);
    expect(executionCount).toBe(1);

    // Call with different argument: executes method
    const userB = await service.getUser("u2");
    expect(userB.count).toBe(2);
    expect(executionCount).toBe(2);
  });

  it("evicts cached method results using @CacheClear", async () => {
    let executionCount = 0;

    class ProductService {
      @Cacheable({ cacheKey: (args) => `product:${args[0]}` })
      async getProduct(sku: string): Promise<{ sku: string; call: number }> {
        executionCount++;
        return { sku, call: executionCount };
      }

      @CacheClear({ cacheKey: (args) => `product:${args[0]}` })
      async updateProduct(sku: string, _price: number): Promise<void> {
        // updates product
      }
    }

    const service = new ProductService();

    const p1 = await service.getProduct("sku-999");
    expect(p1.call).toBe(1);
    expect(executionCount).toBe(1);

    // Call again -> cached
    const p2 = await service.getProduct("sku-999");
    expect(p2.call).toBe(1);
    expect(executionCount).toBe(1);

    // Update product -> triggers @CacheClear
    await service.updateProduct("sku-999", 49.99);

    // Call after clear -> re-executes
    const p3 = await service.getProduct("sku-999");
    expect(p3.call).toBe(2);
    expect(executionCount).toBe(2);
  });

  it("throws error when no cache instance is configured", async () => {
    clearDefaultCache();

    class UncachedService {
      @Cacheable()
      async doWork(): Promise<string> {
        return "done";
      }
    }

    const service = new UncachedService();
    await expect(service.doWork()).rejects.toThrow("No cache instance available");
  });
});
