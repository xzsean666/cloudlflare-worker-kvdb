import { DatabaseSync } from "node:sqlite";

export class MockSqlStorageCursor<T extends Record<string, any>> {
  private rows: T[];
  private index = 0;
  public rowsRead: number;
  public rowsWritten: number;
  public columnNames: string[];

  constructor(rows: T[], rowsWritten = 0) {
    this.rows = rows;
    this.rowsRead = rows.length;
    this.rowsWritten = rowsWritten;
    this.columnNames = rows.length > 0 ? Object.keys(rows[0]!) : [];
  }

  next(): { done?: false; value: T } | { done: true; value?: never } {
    if (this.index < this.rows.length) {
      return { done: false, value: this.rows[this.index++]! };
    }
    return { done: true };
  }

  toArray(): T[] {
    return [...this.rows];
  }

  one(): T {
    if (this.rows.length === 0) {
      throw new Error("No rows returned from query");
    }
    return this.rows[0]!;
  }

  *raw<U extends any[]>(): IterableIterator<U> {
    for (const row of this.rows) {
      yield Object.values(row) as U;
    }
  }

  *[Symbol.iterator](): IterableIterator<T> {
    for (const row of this.rows) {
      yield row;
    }
  }
}

export class MockSqlStorage implements SqlStorage {
  private db: DatabaseSync;

  constructor(db?: DatabaseSync) {
    this.db = db ?? new DatabaseSync(":memory:");
  }

  exec<T extends Record<string, any>>(
    query: string,
    ...bindings: any[]
  ): SqlStorageCursor<T> {
    const trimmed = query.trim();
    // Normalize boolean params (SQLite in node:sqlite doesn't accept booleans)
    const normalizedBindings = bindings.map((b) => (typeof b === "boolean" ? (b ? 1 : 0) : b));

    // Handle transaction commands or DDL/DML statements
    const isSelect = /^(SELECT|PRAGMA|EXPLAIN)/i.test(trimmed);

    try {
      const stmt = this.db.prepare(trimmed);
      if (isSelect) {
        const rows = stmt.all(...(normalizedBindings as any[])) as T[];
        return new MockSqlStorageCursor(rows, 0) as unknown as SqlStorageCursor<T>;
      } else {
        const res = stmt.run(...(normalizedBindings as any[]));
        return new MockSqlStorageCursor([] as T[], Number(res.changes)) as unknown as SqlStorageCursor<T>;
      }
    } catch (err: any) {
      // In SQLite, some multiple statements like bootstrap can be executed via exec
      if (trimmed.includes(";")) {
        try {
          this.db.exec(trimmed);
          return new MockSqlStorageCursor([] as T[], 0) as unknown as SqlStorageCursor<T>;
        } catch (innerErr: any) {
          throw new Error(`MockSqlStorage query error: ${innerErr.message} [SQL: ${query}]`);
        }
      }
      throw new Error(`MockSqlStorage query error: ${err.message} [SQL: ${query}]`);
    }
  }

  get databaseSize(): number {
    return 4096;
  }

  Cursor: any = MockSqlStorageCursor;
  Statement: any = class {};
}

export function createMockSqlStorage(): SqlStorage {
  return new MockSqlStorage();
}
