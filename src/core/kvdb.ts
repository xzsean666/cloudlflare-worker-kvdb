import type { Driver } from "../drivers/types.js";
import { D1Driver } from "../drivers/d1/driver.js";
import { KVDriver } from "../drivers/kv/driver.js";
import { DurableObjectSqlDriver } from "../drivers/do-sql/driver.js";
import { HyperdriveDriver, type HyperdriveClient } from "../drivers/hyperdrive/driver.js";
import { Table, type TableOptions } from "./table.js";
import type { AutoBatchConfig } from "./batcher.js";
import { KVDBError } from "./errors.js";

export interface CloudflareKVDBOptions {
  d1?: D1Database;
  kv?: KVNamespace;
  r2?: R2Bucket;
  doSql?: SqlStorage;
  hyperdrive?: Hyperdrive;
  hyperdriveClient?: HyperdriveClient;
  driver?: Driver;
  defaultDriver?: "d1" | "kv" | "do-sql" | "hyperdrive";
  sessionBookmark?: string | null;
  tableName?: string;
  ctx?: ExecutionContext;
  autoBatch?: boolean | AutoBatchConfig;
}

/**
 * Top-level Cloudflare KVDB client.
 */
export class CloudflareKVDB {
  private readonly driver: Driver;
  private readonly tables = new Map<string, Table<any>>();
  private readonly options: CloudflareKVDBOptions;

  constructor(options: CloudflareKVDBOptions = {}) {
    this.options = options;

    if (options.driver) {
      this.driver = options.driver;
    } else if (options.hyperdrive || options.hyperdriveClient) {
      this.driver = new HyperdriveDriver({
        hyperdrive: options.hyperdrive,
        client: options.hyperdriveClient,
        tableName: options.tableName,
      });
    } else if (options.doSql) {
      this.driver = new DurableObjectSqlDriver(options.doSql, {
        tableName: options.tableName,
      });
    } else if (options.d1) {
      this.driver = new D1Driver(options.d1, {
        bookmark: options.sessionBookmark,
        tableName: options.tableName,
      });
    } else if (options.kv) {
      this.driver = new KVDriver(options.kv);
    } else {
      throw new KVDBError(
        "No storage binding provided. Please supply { d1 }, { kv }, { doSql }, { hyperdrive }, or a custom { driver }.",
        "MISSING_STORAGE_ENGINE"
      );
    }
  }

  /**
   * Returns the underlying storage driver.
   */
  getDriver(): Driver {
    return this.driver;
  }

  /**
   * Returns a typed Table facade for a given namespace.
   */
  table<V = unknown>(name: string, tableOptions?: TableOptions): Table<V> {
    const existing = this.tables.get(name);
    if (existing) {
      return existing as Table<V>;
    }
    const mergedOptions: TableOptions = {
      r2Bucket: this.options.r2,
      ctx: this.options.ctx,
      autoBatch: this.options.autoBatch,
      ...tableOptions,
    };
    const table = new Table<V>(name, this.driver, mergedOptions);
    this.tables.set(name, table);
    return table;
  }

  /**
   * Flushes any pending auto-batch writes across all initialized tables.
   */
  async flush(): Promise<void> {
    await Promise.all(
      Array.from(this.tables.values()).map((t) => t.flush())
    );
  }

  /**
   * Returns the active D1 session bookmark (if using D1Driver).
   */
  getBookmark(): string | null {
    if (this.driver instanceof D1Driver) {
      return this.driver.getBookmark();
    }
    return null;
  }

  /**
   * Alias for getBookmark() to match documentation and D1 Sessions conventions.
   */
  getSessionBookmark(): string | null {
    return this.getBookmark();
  }

  /**
   * Advances or sets the D1 session bookmark.
   */
  setBookmark(bookmark: string | null): void {
    if (this.driver instanceof D1Driver) {
      this.driver.setBookmark(bookmark);
    }
  }

  /**
   * Clones this client with a specific D1 session bookmark for Read-Your-Own-Writes (RYW).
   */
  withSession(bookmark?: string | null): CloudflareKVDB {
    return new CloudflareKVDB({
      ...this.options,
      sessionBookmark: bookmark ?? this.getBookmark(),
    });
  }

  /**
   * Shuts down drivers or flushes remaining buffers if applicable.
   */
  async close(): Promise<void> {
    await this.flush();
    if (typeof this.driver.close === "function") {
      await this.driver.close();
    }
    this.tables.clear();
  }
}
