/**
 * Monotonic Clock for Cloudflare Workers.
 *
 * Cloudflare Worker isolates executing parallel or sequential tasks in the same millisecond
 * can cause identical `Date.now()` timestamps.
 * `MonotonicClock` guarantees strictly increasing millisecond timestamps to preserve total ordering.
 */
export class MonotonicClock {
  private lastTimestamp = 0;

  constructor(initialTimestamp = 0) {
    this.lastTimestamp = initialTimestamp;
  }

  /**
   * Returns a strictly increasing millisecond timestamp.
   */
  now(): number {
    let current = Date.now();
    if (current <= this.lastTimestamp) {
      current = this.lastTimestamp + 1;
    }
    this.lastTimestamp = current;
    return current;
  }

  /**
   * Resets the clock (useful for isolated tests).
   */
  reset(timestamp = 0): void {
    this.lastTimestamp = timestamp;
  }

  /**
   * Returns the last generated timestamp without incrementing.
   */
  peek(): number {
    return this.lastTimestamp;
  }
}

// Global default singleton clock for worker isolate
const defaultClock = new MonotonicClock();

/**
 * Convenience function returning a strictly increasing millisecond timestamp.
 */
export function getMonotonicNow(): number {
  return defaultClock.now();
}
