import { describe, it, expect } from "vitest";
import { MonotonicClock, getMonotonicNow } from "../../src/core/clock.js";

describe("MonotonicClock", () => {
  it("generates strictly increasing numbers across 10,000 iterations", () => {
    const clock = new MonotonicClock();
    const timestamps: number[] = new Array(10000);

    for (let i = 0; i < 10000; i++) {
      timestamps[i] = clock.now();
    }

    for (let i = 1; i < 10000; i++) {
      expect(timestamps[i]).toBeGreaterThan(timestamps[i - 1]!);
    }
  });

  it("global getMonotonicNow generates strictly increasing numbers", () => {
    const t1 = getMonotonicNow();
    const t2 = getMonotonicNow();
    const t3 = getMonotonicNow();

    expect(t2).toBeGreaterThan(t1);
    expect(t3).toBeGreaterThan(t2);
  });

  it("reset resets the clock state", () => {
    const clock = new MonotonicClock();
    clock.reset(500);
    expect(clock.peek()).toBe(500);
  });
});
