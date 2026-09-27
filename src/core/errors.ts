export class KVDBError extends Error {
  public readonly code: string;

  constructor(message: string, code = "KVDB_ERROR", cause?: unknown) {
    super(message, cause !== undefined ? { cause } : undefined);
    this.name = this.constructor.name;
    this.code = code;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class D1LimitError extends KVDBError {
  constructor(message: string) {
    super(message, "D1_LIMIT_ERROR");
  }
}

export class KeyNotFoundError extends KVDBError {
  constructor(key: string, namespace?: string) {
    const nsMsg = namespace ? ` in namespace '${namespace}'` : "";
    super(`Key '${key}' not found${nsMsg}`, "KEY_NOT_FOUND");
  }
}

export class SerializationError extends KVDBError {
  constructor(message: string, cause?: unknown) {
    super(message, "SERIALIZATION_ERROR", cause);
  }
}

export class StorageError extends KVDBError {
  constructor(message: string, cause?: unknown) {
    super(message, "STORAGE_ERROR", cause);
  }
}

