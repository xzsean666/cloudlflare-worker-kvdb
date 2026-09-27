import { describe, it, expect, beforeEach } from "vitest";
import { createMockD1Database } from "../helpers/mock-d1.js";
import { createMockR2Bucket } from "../helpers/mock-r2.js";
import { createMockKVNamespace } from "../helpers/mock-kv.js";
import { D1Driver } from "../../src/drivers/d1/driver.js";
import { KVDriver } from "../../src/drivers/kv/driver.js";
import { Table } from "../../src/core/table.js";
import { CloudflareKVDB } from "../../src/core/kvdb.js";
import { R2BlobOverflowManager } from "../../src/drivers/r2/overflow.js";
import { isBlobDescriptor } from "../../src/core/serializer.js";

describe("R2BlobOverflowManager", () => {
  let r2Bucket: R2Bucket;
  let overflowManager: R2BlobOverflowManager;

  beforeEach(() => {
    r2Bucket = createMockR2Bucket();
    overflowManager = new R2BlobOverflowManager({
      bucket: r2Bucket,
      thresholdBytes: 100, // 100 bytes threshold for testing
      prefix: "__blobs/test",
    });
  });

  it("checks threshold correctly based on UTF-8 byte length", () => {
    const smallPayload = JSON.stringify({ a: "small" });
    expect(overflowManager.shouldOverflow(smallPayload)).toBe(false);

    // 120 'x' characters exceeds 100 bytes
    const largePayload = JSON.stringify({ data: "x".repeat(120) });
    expect(overflowManager.shouldOverflow(largePayload)).toBe(true);
  });

  it("writes blob to R2 and returns descriptor with SHA-256 hash key", async () => {
    const content = JSON.stringify({ large: "content-".repeat(20) });
    const descriptor = await overflowManager.writeBlob(content, { mimeType: "application/json" });

    expect(descriptor.__isBlob).toBe(true);
    expect(descriptor.__cf_blob_overflow).toBe(true);
    expect(descriptor.size).toBe(new TextEncoder().encode(content).byteLength);
    expect(descriptor.mimeType).toBe("application/json");
    expect(descriptor.r2Key.startsWith("__blobs/test/")).toBe(true);
    expect(isBlobDescriptor(descriptor)).toBe(true);

    // Verify object actually exists in R2
    const stored = await r2Bucket.get(descriptor.r2Key);
    expect(stored).not.toBeNull();
    const storedText = await stored!.text();
    expect(storedText).toBe(content);
  });

  it("reads blob back from R2", async () => {
    const content = JSON.stringify({ nested: { message: "hello r2 overflow" } });
    const descriptor = await overflowManager.writeBlob(content);

    const reconstituted = await overflowManager.readBlob(descriptor);
    expect(reconstituted).toBe(content);
  });

  it("deletes blob from R2", async () => {
    const content = "payload-to-delete";
    const descriptor = await overflowManager.writeBlob(content);

    expect(await r2Bucket.get(descriptor.r2Key)).not.toBeNull();
    await overflowManager.deleteBlob(descriptor);
    expect(await r2Bucket.get(descriptor.r2Key)).toBeNull();
  });

  it("throws error when reading non-existent blob", async () => {
    const danglingDescriptor = {
      __isBlob: true as const,
      r2Key: "__blobs/test/does_not_exist",
      size: 123,
      createdAt: Date.now(),
    };

    await expect(overflowManager.readBlob(danglingDescriptor)).rejects.toThrow(
      "Blob object '__blobs/test/does_not_exist' not found in R2 bucket"
    );
  });
});

describe("Table with Transparent R2 Blob Overflow", () => {
  let rawDb: D1Database;
  let r2Bucket: R2Bucket;
  let driver: D1Driver;

  beforeEach(async () => {
    rawDb = createMockD1Database();
    r2Bucket = createMockR2Bucket();
    driver = new D1Driver(rawDb);
    await driver.init();
  });

  it("stores small payloads in D1 without writing to R2", async () => {
    const table = new Table<{ id: string; name: string }>("users", driver, {
      r2Bucket,
      overflowThresholdBytes: 200,
    });

    const smallUser = { id: "u1", name: "Alice" };
    await table.set("u1", smallUser);

    const retrieved = await table.get("u1");
    expect(retrieved).toEqual(smallUser);

    // Ensure R2 bucket is completely empty
    const r2List = await r2Bucket.list();
    expect(r2List.objects.length).toBe(0);
  });

  it("transparently offloads large payloads to R2 and reconstitutes on get", async () => {
    const table = new Table<{ id: string; payload: string }>("documents", driver, {
      r2Bucket,
      overflowThresholdBytes: 150,
    });

    const largeDoc = {
      id: "doc-1",
      payload: "large-document-content-block-".repeat(15), // well exceeds 150 bytes
    };

    await table.set("doc-1", largeDoc);

    // 1. R2 bucket now holds 1 blob object
    const r2List = await r2Bucket.list();
    expect(r2List.objects.length).toBe(1);
    expect(r2List.objects[0]!.key.startsWith("__blobs/documents/")).toBe(true);

    // 2. Underlying driver stores only the small descriptor
    const rawStored = await driver.get("documents", "doc-1");
    expect(rawStored).not.toBeNull();
    const parsedStored = JSON.parse(rawStored!);
    expect(parsedStored.__isBlob).toBe(true);
    expect(parsedStored.__cf_blob_overflow).toBe(true);
    expect(parsedStored.r2Key).toBe(r2List.objects[0]!.key);

    // 3. table.get() transparently reconstitutes the full payload
    const retrieved = await table.get("doc-1");
    expect(retrieved).toEqual(largeDoc);
  });

  it("transparently handles batch getMany with mixed small and overflow records", async () => {
    const table = new Table<{ id: string; data: string }>("mixed", driver, {
      r2Bucket,
      overflowThresholdBytes: 100,
    });

    const smallItem = { id: "item-1", data: "small" };
    const largeItem = { id: "item-2", data: "large-payload-chunk-".repeat(10) };

    await table.set("item-1", smallItem);
    await table.set("item-2", largeItem);

    const results = await table.getMany(["item-1", "item-2", "item-missing"]);
    expect(results).toHaveLength(3);
    expect(results[0]).toEqual(smallItem);
    expect(results[1]).toEqual(largeItem);
    expect(results[2]).toBeNull();
  });

  it("cleans up both D1 record and R2 object on table.delete()", async () => {
    const table = new Table<{ id: string; blobData: string }>("files", driver, {
      r2Bucket,
      overflowThresholdBytes: 80,
    });

    const largeFile = {
      id: "f1",
      blobData: "binary-or-json-stream-overflow-data-".repeat(8),
    };

    await table.set("f1", largeFile);

    // Verify stored in R2
    let r2List = await r2Bucket.list();
    expect(r2List.objects.length).toBe(1);
    const r2Key = r2List.objects[0]!.key;

    // Delete record
    const deleted = await table.delete("f1");
    expect(deleted).toBe(true);

    // Verify removed from table
    expect(await table.get("f1")).toBeNull();
    expect(await table.has("f1")).toBe(false);

    // Verify removed from R2
    const r2Obj = await r2Bucket.get(r2Key);
    expect(r2Obj).toBeNull();
  });

  it("handles batch deleteMany with R2 cleanup", async () => {
    const table = new Table<{ id: string; text: string }>("batch_del", driver, {
      r2Bucket,
      overflowThresholdBytes: 50,
    });

    await table.set("b1", { id: "b1", text: "overflow-content-alpha-".repeat(5) });
    await table.set("b2", { id: "b2", text: "overflow-content-beta-".repeat(5) });

    let r2List = await r2Bucket.list();
    expect(r2List.objects.length).toBe(2);

    const deletedCount = await table.deleteMany(["b1", "b2"]);
    expect(deletedCount).toBe(2);

    r2List = await r2Bucket.list();
    expect(r2List.objects.length).toBe(0);
  });

  it("works with physical schema tables and index lookups", async () => {
    const table = new Table<{ id: string; category: string; content: string }>(
      "articles",
      driver,
      {
        r2Bucket,
        overflowThresholdBytes: 100,
        schema: {
          tableName: "t_articles",
          primaryKey: { name: "id" },
          columns: {
            category: { type: "string", index: true },
          },
        },
      }
    );

    await table.init();

    const article = {
      id: "art-1",
      category: "tech",
      content: "substantial-long-article-body-text-content-".repeat(10),
    };

    await table.set("art-1", article);

    // Lookup by primary key
    const byPk = await table.get("art-1");
    expect(byPk).toEqual(article);

    // Point lookup by indexed secondary column
    const byIndex = await table.getBy("category", "tech");
    expect(byIndex).toEqual(article);

    // Query via find()
    const queryResults = await table.find({ category: "tech" });
    expect(queryResults).toHaveLength(1);
    expect(queryResults[0]).toEqual(article);

    // Delete schema row cleans up R2 object
    const deleted = await table.delete("art-1");
    expect(deleted).toBe(true);
    const r2List = await r2Bucket.list();
    expect(r2List.objects.length).toBe(0);
  });
});

describe("CloudflareKVDB with R2 Binding", () => {
  it("automatically passes r2 binding to table instances", async () => {
    const rawDb = createMockD1Database();
    const r2Bucket = createMockR2Bucket();

    const kvdb = new CloudflareKVDB({
      d1: rawDb,
      r2: r2Bucket,
    });

    const table = kvdb.table<{ id: string; big: string }>("profiles", {
      overflowThresholdBytes: 80,
    });

    expect(table.getBlobOverflow()).toBeDefined();

    const profile = { id: "p1", big: "big-profile-description-".repeat(10) };
    await table.set("p1", profile);

    const r2List = await r2Bucket.list();
    expect(r2List.objects.length).toBe(1);

    const fetched = await table.get("p1");
    expect(fetched).toEqual(profile);
  });

  it("works with Workers KV driver and R2 overflow", async () => {
    const kvNamespace = createMockKVNamespace();
    const r2Bucket = createMockR2Bucket();

    const kvdb = new CloudflareKVDB({
      kv: kvNamespace,
      r2: r2Bucket,
    });

    const table = kvdb.table<{ id: string; data: string }>("kv_large", {
      overflowThresholdBytes: 50,
    });

    const item = { id: "k1", data: "large-kv-payload-string-".repeat(5) };
    await table.set("k1", item);

    const r2List = await r2Bucket.list();
    expect(r2List.objects.length).toBe(1);

    const fetched = await table.get("k1");
    expect(fetched).toEqual(item);

    await table.delete("k1");
    expect(await r2Bucket.get(r2List.objects[0]!.key)).toBeNull();
  });
});
