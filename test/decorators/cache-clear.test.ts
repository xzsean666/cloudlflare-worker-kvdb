import { describe, it, expect, beforeEach } from "vitest";
import { TieredCache } from "../../src/cache/cache.js";
import { setDefaultCache, clearDefaultCache } from "../../src/decorators/cacheable.js";
import { CacheClear } from "../../src/decorators/cache-clear.js";
import { KVDBError } from "../../src/core/errors.js";

describe("CacheClear Decorator Edge Cases", () => {
  let cache: TieredCache;

  beforeEach(() => {
    cache = new TieredCache({ l1: { max: 100 } });
    setDefaultCache(cache);
  });

  it("clears multiple cache keys specified as an array", async () => {
    await cache.set("user:10", { name: "Alice" });
    await cache.set("user:summary:10", { count: 5 });
    await cache.set("user:other", { flag: true });

    class AccountService {
      @CacheClear({
        cacheKey: [
          (args) => `user:${args[0]}`,
          (args) => `user:summary:${args[0]}`,
        ],
      })
      async updateAccount(id: number): Promise<void> {
        // Updated account
      }
    }

    const service = new AccountService();
    await service.updateAccount(10);

    expect(await cache.get("user:10")).toBeNull();
    expect(await cache.get("user:summary:10")).toBeNull();
    // Unrelated key remains intact
    expect(await cache.get("user:other")).not.toBeNull();
  });

  it("throws MISSING_CACHE_INSTANCE if cache is not provided in Stage 3 mode", async () => {
    clearDefaultCache();

    class Service {
      @CacheClear({ cacheKey: "some_key" })
      async doAction(): Promise<void> {}
    }

    const service = new Service();
    await expect(service.doAction()).rejects.toThrow(KVDBError);
    await expect(service.doAction()).rejects.toThrow("No cache instance available for @CacheClear decorator.");
  });

  it("supports legacy TypeScript property descriptor decoration", async () => {
    await cache.set("item:legacy", { value: 123 });

    const descriptor: PropertyDescriptor = {
      value: async function (itemId: string) {
        return `done_${itemId}`;
      },
      writable: true,
      enumerable: false,
      configurable: true,
    };

    // Decorate method via legacy decorator signature
    const decoratorFn = CacheClear({ cacheKey: (args) => `item:${args[0]}` });
    const decoratedDescriptor = decoratorFn({}, "testMethod", descriptor);

    const result = await decoratedDescriptor.value("legacy");
    expect(result).toBe("done_legacy");
    expect(await cache.get("item:legacy")).toBeNull();
  });

  it("throws MISSING_CACHE_INSTANCE in legacy mode when no cache is configured", async () => {
    clearDefaultCache();

    const descriptor: PropertyDescriptor = {
      value: async function () {
        return "val";
      },
      writable: true,
      enumerable: false,
      configurable: true,
    };

    const decoratorFn = CacheClear({ cacheKey: "any_key" });
    const decoratedDescriptor = decoratorFn({}, "testMethod", descriptor);

    await expect(decoratedDescriptor.value()).rejects.toThrow(
      "No cache instance available for @CacheClear decorator."
    );
  });
});
