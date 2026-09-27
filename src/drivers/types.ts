export interface DriverCapabilities {
  supportsTTL: boolean;
  supportsBatch: boolean;
  supportsSessions: boolean;
  supportsJSONQuery: boolean;
  supportsIndexes: boolean;
  supportsTransactions?: boolean;
}

export interface DriverSetItem {
  key: string;
  value: string;
  ttlSeconds?: number;
}

export interface DriverListOptions {
  prefix?: string;
  limit?: number;
  cursor?: string;
}

export interface DriverListResult {
  keys: string[];
  cursor?: string;
  complete: boolean;
}

/**
 * Universal Storage Driver interface for Cloudflare storage engines.
 */
export interface Driver {
  readonly name: string;
  readonly capabilities: DriverCapabilities;

  init(): Promise<void>;
  get(namespace: string, key: string): Promise<string | null>;
  getMany(namespace: string, keys: readonly string[]): Promise<(string | null)[]>;
  set(namespace: string, key: string, value: string, ttlSeconds?: number): Promise<void>;
  setMany(namespace: string, entries: readonly DriverSetItem[]): Promise<void>;
  delete(namespace: string, key: string): Promise<boolean>;
  deleteMany(namespace: string, keys: readonly string[]): Promise<number>;
  has(namespace: string, key: string): Promise<boolean>;
  clear(namespace: string): Promise<void>;
  list(namespace: string, options?: DriverListOptions): Promise<DriverListResult>;
  close?(): Promise<void>;
}
