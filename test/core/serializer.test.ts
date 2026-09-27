import { describe, it, expect } from "vitest";
import {
  serialize,
  deserialize,
  canonicalStringify,
  isBlobDescriptor,
  createBlobDescriptor,
} from "../../src/core/serializer.js";

describe("Serializer", () => {
  it("produces identical canonical string regardless of key ordering", () => {
    const objA = { b: 2, a: 1, c: { z: 26, y: 25 } };
    const objB = { a: 1, c: { y: 25, z: 26 }, b: 2 };

    const strA = canonicalStringify(objA);
    const strB = canonicalStringify(objB);

    expect(strA).toBe(strB);
    expect(strA).toBe('{"a":1,"b":2,"c":{"y":25,"z":26}}');
  });

  it("roundtrips primitives, objects, and arrays", () => {
    const data = {
      str: "hello",
      num: 42,
      bool: true,
      nil: null,
      arr: [1, "two", false],
    };

    const str = serialize(data);
    const recovered = deserialize<typeof data>(str);
    expect(recovered).toEqual(data);
  });

  it("handles BigInt safely", () => {
    const original = { amount: 12345678901234567890n };
    const serialized = serialize(original);
    const restored = deserialize<{ amount: bigint }>(serialized);

    expect(restored.amount).toBe(12345678901234567890n);
  });

  it("handles Date safely", () => {
    const now = new Date();
    const original = { timestamp: now };
    const serialized = serialize(original);
    const restored = deserialize<{ timestamp: Date }>(serialized);

    expect(restored.timestamp instanceof Date).toBe(true);
    expect(restored.timestamp.getTime()).toBe(now.getTime());
  });

  it("handles Uint8Array binary data safely", () => {
    const bytes = new Uint8Array([1, 2, 3, 255, 128]);
    const original = { data: bytes };
    const serialized = serialize(original);
    const restored = deserialize<{ data: Uint8Array }>(serialized);

    expect(restored.data instanceof Uint8Array).toBe(true);
    expect(Array.from(restored.data)).toEqual([1, 2, 3, 255, 128]);
  });

  it("detects and creates blob descriptors", () => {
    const desc = createBlobDescriptor("blobs/user_avatar_123.jpg", 1024 * 1024 * 5, {
      mimeType: "image/jpeg",
    });

    expect(isBlobDescriptor(desc)).toBe(true);
    expect(isBlobDescriptor({ foo: "bar" })).toBe(false);
    expect(isBlobDescriptor(null)).toBe(false);

    const serialized = serialize(desc);
    const parsed = deserialize(serialized);
    expect(isBlobDescriptor(parsed)).toBe(true);
  });
});
