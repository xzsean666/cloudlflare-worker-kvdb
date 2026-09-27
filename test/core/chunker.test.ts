import { describe, it, expect } from "vitest";
import {
  chunkArray,
  chunkByParamCount,
  buildInClausePlaceholders,
  buildMultiRowPlaceholders,
  assertD1ParamLimit,
  D1_MAX_PARAMS_LIMIT,
} from "../../src/core/chunker.js";
import { D1LimitError } from "../../src/core/errors.js";

describe("Chunker", () => {
  it("splits array into equal chunks", () => {
    const items = [1, 2, 3, 4, 5, 6, 7];
    const chunks = chunkArray(items, 3);
    expect(chunks).toEqual([[1, 2, 3], [4, 5, 6], [7]]);
  });

  it("returns empty array for empty items", () => {
    expect(chunkArray([], 5)).toEqual([]);
  });

  it("throws error for chunkSize <= 0", () => {
    expect(() => chunkArray([1], 0)).toThrow();
  });

  it("splits 500 items into chunks respecting maxParams of 80 with 1 param per item", () => {
    const items = Array.from({ length: 500 }, (_, i) => i);
    const chunks = chunkByParamCount(items, 1, 80);

    expect(chunks.length).toBe(Math.ceil(500 / 80)); // 7 chunks
    for (const chunk of chunks) {
      expect(chunk.length).toBeLessThanOrEqual(80);
    }
  });

  it("splits items respecting multi-parameter per item limits", () => {
    // 4 params per item with maxParams = 80 -> 20 items per chunk
    const items = Array.from({ length: 50 }, (_, i) => ({ id: i, val: `v${i}` }));
    const chunks = chunkByParamCount(items, 4, 80);

    expect(chunks[0]!.length).toBe(20);
    expect(chunks[1]!.length).toBe(20);
    expect(chunks[2]!.length).toBe(10);
  });

  it("throws D1LimitError when maxParams > 100", () => {
    expect(() => chunkByParamCount([1], 1, 105)).toThrow(D1LimitError);
  });

  it("builds correct IN clause placeholders", () => {
    expect(buildInClausePlaceholders(0)).toBe("()");
    expect(buildInClausePlaceholders(1)).toBe("(?)");
    expect(buildInClausePlaceholders(3)).toBe("(?, ?, ?)");
  });

  it("builds correct multi-row placeholders", () => {
    expect(buildMultiRowPlaceholders(0, 3)).toBe("");
    expect(buildMultiRowPlaceholders(2, 3)).toBe("(?, ?, ?), (?, ?, ?)");
  });

  it("assertD1ParamLimit validates parameter boundaries", () => {
    expect(() => assertD1ParamLimit(50)).not.toThrow();
    expect(() => assertD1ParamLimit(D1_MAX_PARAMS_LIMIT)).not.toThrow();
    expect(() => assertD1ParamLimit(101)).toThrow(D1LimitError);
  });
});
