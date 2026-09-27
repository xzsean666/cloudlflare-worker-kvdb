import { describe, it, expect } from "vitest";
import {
  encodeKey,
  decodeKey,
  buildPrefix,
  joinKeyPath,
  validateKey,
  validateNamespace,
} from "../../src/core/key.js";
import { KVDBError } from "../../src/core/errors.js";

describe("Key Utilities", () => {
  it("encodes and decodes keys accurately", () => {
    const encoded = encodeKey("users", "user_123");
    expect(encoded).toBe("users:user_123");

    const decoded = decodeKey(encoded);
    expect(decoded).toEqual({ namespace: "users", key: "user_123" });
  });

  it("supports custom delimiter", () => {
    const encoded = encodeKey("orders", "ord-999", "/");
    expect(encoded).toBe("orders/ord-999");

    const decoded = decodeKey(encoded, "/");
    expect(decoded).toEqual({ namespace: "orders", key: "ord-999" });
  });

  it("builds prefix with delimiter", () => {
    expect(buildPrefix("users")).toBe("users:");
    expect(buildPrefix("sessions", "/")).toBe("sessions/");
  });

  it("joins key paths cleanly", () => {
    expect(joinKeyPath(["users", "123", "settings"])).toBe("users:123:settings");
    expect(joinKeyPath(["a", "b"], "/")).toBe("a/b");
  });

  it("validates keys and namespaces", () => {
    expect(() => validateKey("")).toThrow(KVDBError);
    expect(() => validateKey("  ")).toThrow(KVDBError);
    expect(() => validateNamespace("")).toThrow(KVDBError);
    expect(() => decodeKey("no-delimiter")).toThrow(KVDBError);
  });
});
