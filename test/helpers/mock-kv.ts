interface StoredKVValue {
  value: string;
  metadata?: any;
  expiration?: number; // epoch seconds
}

export class MockKVNamespace {
  private store = new Map<string, StoredKVValue>();

  private isExpired(entry: StoredKVValue): boolean {
    if (entry.expiration && entry.expiration <= Math.floor(Date.now() / 1000)) {
      return true;
    }
    return false;
  }

  async get(key: string, options?: any): Promise<any> {
    const entry = this.store.get(key);
    if (!entry || this.isExpired(entry)) {
      if (entry) this.store.delete(key);
      return null;
    }

    const type = typeof options === "string" ? options : options?.type;
    if (type === "json") {
      try {
        return JSON.parse(entry.value);
      } catch {
        return null;
      }
    } else if (type === "arrayBuffer") {
      return new TextEncoder().encode(entry.value).buffer;
    } else if (type === "stream") {
      const data = new TextEncoder().encode(entry.value);
      return new ReadableStream({
        start(controller) {
          controller.enqueue(data);
          controller.close();
        },
      });
    }
    return entry.value;
  }

  async getWithMetadata<Metadata = unknown>(key: string, options?: any): Promise<{ value: any; metadata: Metadata | null }> {
    const val = await this.get(key, options);
    const entry = this.store.get(key);
    return {
      value: val,
      metadata: entry ? (entry.metadata as Metadata) : null,
    };
  }

  async put(
    key: string,
    value: string | ArrayBuffer | ReadableStream,
    options?: { expiration?: number; expirationTtl?: number; metadata?: any }
  ): Promise<void> {
    let strValue = "";
    if (typeof value === "string") {
      strValue = value;
    } else if (value instanceof ArrayBuffer) {
      strValue = new TextDecoder().decode(value);
    } else if (value && typeof (value as any).getReader === "function") {
      const reader = (value as ReadableStream).getReader();
      const chunks: Uint8Array[] = [];
      let totalLength = 0;
      while (true) {
        const { done, value: chunk } = await reader.read();
        if (done) break;
        chunks.push(chunk);
        totalLength += chunk.length;
      }
      const combined = new Uint8Array(totalLength);
      let offset = 0;
      for (const chunk of chunks) {
        combined.set(chunk, offset);
        offset += chunk.length;
      }
      strValue = new TextDecoder().decode(combined);
    }

    let expiration: number | undefined = options?.expiration;
    if (options?.expirationTtl) {
      expiration = Math.floor(Date.now() / 1000) + options.expirationTtl;
    }

    this.store.set(key, {
      value: strValue,
      metadata: options?.metadata,
      expiration,
    });
  }

  async delete(key: string): Promise<void> {
    this.store.delete(key);
  }

  async list(options?: { prefix?: string; limit?: number; cursor?: string }): Promise<{
    keys: { name: string; expiration?: number; metadata?: unknown }[];
    list_complete: boolean;
    cursor?: string;
  }> {
    const prefix = options?.prefix ?? "";
    const limit = options?.limit ?? 1000;
    const allKeys = Array.from(this.store.keys())
      .filter((k) => k.startsWith(prefix))
      .filter((k) => {
        const entry = this.store.get(k)!;
        if (this.isExpired(entry)) {
          this.store.delete(k);
          return false;
        }
        return true;
      })
      .sort();

    let startIndex = 0;
    if (options?.cursor) {
      const idx = allKeys.indexOf(options.cursor);
      if (idx !== -1) {
        startIndex = idx + 1;
      }
    }

    const selectedKeys = allKeys.slice(startIndex, startIndex + limit);
    const hasMore = startIndex + limit < allKeys.length;
    const nextCursor = hasMore ? selectedKeys[selectedKeys.length - 1] : undefined;

    return {
      keys: selectedKeys.map((name) => {
        const item = this.store.get(name);
        return {
          name,
          expiration: item?.expiration,
          metadata: item?.metadata,
        };
      }),
      list_complete: !hasMore,
      cursor: nextCursor,
    };
  }
}

export function createMockKVNamespace(): KVNamespace {
  return new MockKVNamespace() as unknown as KVNamespace;
}
