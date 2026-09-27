import type { BlobDescriptor } from "../../core/serializer.js";
import { createBlobDescriptor } from "../../core/serializer.js";
import { StorageError } from "../../core/errors.js";

export interface R2BlobOverflowOptions {
  /**
   * Target Cloudflare R2 bucket binding.
   */
  bucket: R2Bucket;

  /**
   * Byte threshold above which serialized payloads are offloaded to R2.
   * Default: 65536 bytes (64 KB).
   */
  thresholdBytes?: number;

  /**
   * Path prefix for storing blob objects in R2.
   * Default: "__blobs".
   */
  prefix?: string;
}

/**
 * Transparent Cloudflare R2 Blob Overflow Manager.
 *
 * Automatically inspects payload byte sizes to bypass Cloudflare D1's 2MB row limit
 * and prevent SQLite B-Tree page bloat by offloading large documents and binary blobs to R2.
 */
export class R2BlobOverflowManager {
  public readonly bucket: R2Bucket;
  public readonly thresholdBytes: number;
  public readonly prefix: string;

  constructor(options: R2BlobOverflowOptions) {
    if (!options.bucket) {
      throw new StorageError("R2BlobOverflowManager requires a valid R2Bucket binding", "INVALID_CONFIG");
    }
    this.bucket = options.bucket;
    this.thresholdBytes = options.thresholdBytes ?? 64 * 1024;
    this.prefix = options.prefix ?? "__blobs";
  }

  /**
   * Calculates the UTF-8 byte length of a string.
   */
  private getByteLength(text: string): number {
    return new TextEncoder().encode(text).byteLength;
  }

  /**
   * Determines if a serialized string payload exceeds the overflow threshold.
   */
  shouldOverflow(serializedText: string): boolean {
    if (serializedText.length > this.thresholdBytes) {
      return true;
    }
    if (serializedText.length * 3 <= this.thresholdBytes) {
      return false;
    }
    return this.getByteLength(serializedText) > this.thresholdBytes;
  }

  /**
   * Offloads serialized payload bytes to R2, returning a compact BlobDescriptor metadata pointer.
   */
  async writeBlob(
    serializedText: string,
    options?: { mimeType?: string; key?: string }
  ): Promise<BlobDescriptor> {
    const bytes = new TextEncoder().encode(serializedText);

    // Compute SHA-256 hash using standard Web Crypto API
    const hashBuffer = await crypto.subtle.digest("SHA-256", bytes);
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    const hashHex = hashArray.map((b) => b.toString(16).padStart(2, "0")).join("");

    const r2Key = options?.key
      ? `${this.prefix}/${encodeURIComponent(options.key)}_${hashHex}`
      : `${this.prefix}/${hashHex}`;

    const putResult = await this.bucket.put(r2Key, bytes, {
      httpMetadata: options?.mimeType ? { contentType: options.mimeType } : undefined,
    });

    return createBlobDescriptor(r2Key, bytes.byteLength, {
      mimeType: options?.mimeType,
      etag: putResult?.etag,
      createdAt: Date.now(),
    });
  }

  /**
   * Reads and reconstitutes serialized payload string from R2.
   */
  async readBlob(descriptor: BlobDescriptor): Promise<string> {
    if (!descriptor.r2Key.startsWith(`${this.prefix}/`)) {
      throw new StorageError(
        `Unauthorized blob key access: '${descriptor.r2Key}' is outside table prefix '${this.prefix}/'`,
        "UNAUTHORIZED_BLOB_ACCESS"
      );
    }
    const obj = await this.bucket.get(descriptor.r2Key);
    if (!obj) {
      throw new StorageError(
        `Blob object '${descriptor.r2Key}' not found in R2 bucket`,
        "R2_BLOB_NOT_FOUND"
      );
    }
    return await obj.text();
  }

  /**
   * Reads raw body stream from R2.
   */
  async readBlobStream(descriptor: BlobDescriptor): Promise<ReadableStream> {
    if (!descriptor.r2Key.startsWith(`${this.prefix}/`)) {
      throw new StorageError(
        `Unauthorized blob stream access: '${descriptor.r2Key}' is outside table prefix '${this.prefix}/'`,
        "UNAUTHORIZED_BLOB_ACCESS"
      );
    }
    const obj = await this.bucket.get(descriptor.r2Key);
    if (!obj) {
      throw new StorageError(
        `Blob object '${descriptor.r2Key}' not found in R2 bucket`,
        "R2_BLOB_NOT_FOUND"
      );
    }
    return obj.body;
  }

  /**
   * Deletes an overflow blob from R2.
   */
  async deleteBlob(r2KeyOrDescriptor: string | BlobDescriptor): Promise<void> {
    const key = typeof r2KeyOrDescriptor === "string" ? r2KeyOrDescriptor : r2KeyOrDescriptor.r2Key;
    if (!key.startsWith(`${this.prefix}/`)) {
      throw new StorageError(
        `Unauthorized blob deletion: '${key}' is outside table prefix '${this.prefix}/'`,
        "UNAUTHORIZED_BLOB_ACCESS"
      );
    }
    await this.bucket.delete(key);
  }

  /**
   * Deletes multiple overflow blobs from R2 in chunks of 1000.
   */
  async deleteBlobs(r2KeysOrDescriptors: (string | BlobDescriptor)[]): Promise<void> {
    if (r2KeysOrDescriptors.length === 0) return;
    const keys = r2KeysOrDescriptors.map((item) =>
      typeof item === "string" ? item : item.r2Key
    );
    for (const key of keys) {
      if (!key.startsWith(`${this.prefix}/`)) {
        throw new StorageError(
          `Unauthorized blob deletion: '${key}' is outside table prefix '${this.prefix}/'`,
          "UNAUTHORIZED_BLOB_ACCESS"
        );
      }
    }
    // R2 bucket.delete accepts string[] (up to 1000 keys per call)
    for (let i = 0; i < keys.length; i += 1000) {
      const chunk = keys.slice(i, i + 1000);
      await this.bucket.delete(chunk);
    }
  }

  /**
   * Deletes all blobs stored under this manager's prefix.
   */
  async clearBlobs(): Promise<void> {
    let cursor: string | undefined = undefined;
    const prefix = `${this.prefix}/`;
    do {
      const list = await this.bucket.list({ prefix, cursor, limit: 1000 });
      if (list.objects.length > 0) {
        const keys = list.objects.map((o) => o.key);
        await this.bucket.delete(keys);
      }
      cursor = list.truncated ? list.cursor : undefined;
    } while (cursor);
  }
}

