interface StoredR2Object {
  data: Uint8Array;
  key: string;
  size: number;
  etag: string;
  httpMetadata?: R2HTTPMetadata | Headers;
  customMetadata?: Record<string, string>;
  uploaded: Date;
}

export class MockR2Bucket {
  private store = new Map<string, StoredR2Object>();

  async put(
    key: string,
    value: any,
    options?: R2PutOptions
  ): Promise<R2Object> {
    let bytes: Uint8Array;
    if (value instanceof Uint8Array) {
      bytes = value;
    } else if (value instanceof ArrayBuffer) {
      bytes = new Uint8Array(value);
    } else if (typeof value === "string") {
      bytes = new TextEncoder().encode(value);
    } else if (value && typeof value.arrayBuffer === "function") {
      const buf = await value.arrayBuffer();
      bytes = new Uint8Array(buf);
    } else {
      bytes = new Uint8Array();
    }

    const stored: StoredR2Object = {
      key,
      data: bytes,
      size: bytes.byteLength,
      etag: `etag-${Date.now()}-${Math.random()}`,
      httpMetadata: options?.httpMetadata,
      customMetadata: options?.customMetadata,
      uploaded: new Date(),
    };

    this.store.set(key, stored);

    return {
      key: stored.key,
      version: "1",
      size: stored.size,
      etag: stored.etag,
      httpEtag: `"${stored.etag}"`,
      uploaded: stored.uploaded,
      httpMetadata: stored.httpMetadata ?? {},
      customMetadata: stored.customMetadata ?? {},
      writeHttpMetadata: () => {},
      checksums: { md5: undefined, sha1: undefined, sha256: undefined, sha384: undefined, sha512: undefined },
    } as any;
  }

  async get(key: string): Promise<R2ObjectBody | null> {
    const stored = this.store.get(key);
    if (!stored) return null;

    const stream = new ReadableStream({
      start(controller) {
        controller.enqueue(stored.data);
        controller.close();
      },
    });

    return {
      key: stored.key,
      version: "1",
      size: stored.size,
      etag: stored.etag,
      httpEtag: `"${stored.etag}"`,
      uploaded: stored.uploaded,
      httpMetadata: stored.httpMetadata ?? {},
      customMetadata: stored.customMetadata ?? {},
      body: stream,
      bodyUsed: false,
      arrayBuffer: async () => stored.data.buffer.slice(stored.data.byteOffset, stored.data.byteOffset + stored.data.byteLength),
      text: async () => new TextDecoder().decode(stored.data),
      json: async () => JSON.parse(new TextDecoder().decode(stored.data)),
      blob: async () => new Blob([stored.data]),
      writeHttpMetadata: () => {},
      checksums: { md5: undefined, sha1: undefined, sha256: undefined, sha384: undefined, sha512: undefined },
    } as any;
  }

  async head(key: string): Promise<R2Object | null> {
    const stored = this.store.get(key);
    if (!stored) return null;
    return {
      key: stored.key,
      version: "1",
      size: stored.size,
      etag: stored.etag,
      httpEtag: `"${stored.etag}"`,
      uploaded: stored.uploaded,
      httpMetadata: stored.httpMetadata ?? {},
      customMetadata: stored.customMetadata ?? {},
      writeHttpMetadata: () => {},
      checksums: { md5: undefined, sha1: undefined, sha256: undefined, sha384: undefined, sha512: undefined },
    } as any;
  }

  async delete(keys: string | string[]): Promise<void> {
    const keyList = Array.isArray(keys) ? keys : [keys];
    for (const k of keyList) {
      this.store.delete(k);
    }
  }

  async list(options?: R2ListOptions): Promise<R2Objects> {
    const objects: R2Object[] = [];
    for (const stored of this.store.values()) {
      if (options?.prefix && !stored.key.startsWith(options.prefix)) {
        continue;
      }
      objects.push({
        key: stored.key,
        version: "1",
        size: stored.size,
        etag: stored.etag,
        httpEtag: `"${stored.etag}"`,
        uploaded: stored.uploaded,
        httpMetadata: stored.httpMetadata ?? {},
        customMetadata: stored.customMetadata ?? {},
        writeHttpMetadata: () => {},
        checksums: { md5: undefined, sha1: undefined, sha256: undefined, sha384: undefined, sha512: undefined },
      } as any);
    }

    return {
      objects,
      truncated: false,
      delimitedPrefixes: [],
    };
  }
}

export function createMockR2Bucket(): R2Bucket {
  return new MockR2Bucket() as unknown as R2Bucket;
}
