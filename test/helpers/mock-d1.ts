import { DatabaseSync } from "node:sqlite";
import { assertD1ParamLimit } from "../../src/core/chunker.js";

export class MockD1PreparedStatement {
  public db: DatabaseSync;
  public sql: string;
  public params: unknown[] = [];

  constructor(db: DatabaseSync, sql: string, params: unknown[] = []) {
    this.db = db;
    this.sql = sql;
    this.params = params;
  }

  bind(...values: unknown[]): D1PreparedStatement {
    return new MockD1PreparedStatement(this.db, this.sql, values) as unknown as D1PreparedStatement;
  }

  async first<T = unknown>(colName?: string): Promise<T | null> {
    assertD1ParamLimit(this.params.length);
    try {
      const stmt = this.db.prepare(this.sql);
      const row = stmt.get(...(this.params as any[])) as Record<string, unknown> | undefined;
      if (!row) return null;
      if (colName !== undefined) {
        return (row[colName] as unknown as T) ?? null;
      }
      return row as unknown as T;
    } catch (err: any) {
      throw new Error(`D1 query error in first(): ${err.message} [SQL: ${this.sql}]`);
    }
  }

  async run<T = Record<string, unknown>>(): Promise<D1Result<T>> {
    assertD1ParamLimit(this.params.length);
    try {
      const stmt = this.db.prepare(this.sql);
      const result = stmt.run(...(this.params as any[]));
      return {
        results: [],
        success: true,
        meta: {
          duration: 0,
          size_after: 0,
          rows_read: 0,
          rows_written: Number(result.changes),
          last_row_id: Number(result.lastInsertRowid),
          changed_db: Number(result.changes) > 0,
          changes: Number(result.changes),
        },
      };
    } catch (err: any) {
      throw new Error(`D1 query error in run(): ${err.message} [SQL: ${this.sql}]`);
    }
  }

  async all<T = Record<string, unknown>>(): Promise<D1Result<T>> {
    assertD1ParamLimit(this.params.length);
    try {
      const stmt = this.db.prepare(this.sql);
      const rows = stmt.all(...(this.params as any[])) as T[];
      return {
        results: rows,
        success: true,
        meta: {
          duration: 0,
          size_after: 0,
          rows_read: rows.length,
          rows_written: 0,
          last_row_id: 0,
          changed_db: false,
          changes: 0,
        },
      };
    } catch (err: any) {
      throw new Error(`D1 query error in all(): ${err.message} [SQL: ${this.sql}]`);
    }
  }

  async raw<T = unknown[]>(): Promise<T[]> {
    const res = await this.all<Record<string, unknown>>();
    return res.results.map((r) => Object.values(r) as T);
  }
}

export class MockD1Database {
  public rawDb: DatabaseSync;
  public bookmarks: string[] = [];

  constructor(rawDb?: DatabaseSync) {
    this.rawDb = rawDb ?? new DatabaseSync(":memory:");
    this.rawDb.exec("PRAGMA foreign_keys = ON;");
  }

  prepare(query: string): D1PreparedStatement {
    return new MockD1PreparedStatement(this.rawDb, query) as unknown as D1PreparedStatement;
  }

  async batch<T = unknown>(statements: D1PreparedStatement[]): Promise<D1Result<T>[]> {
    const results: D1Result<T>[] = [];
    for (const stmt of statements) {
      const sql = (stmt as any).sql || "";
      const isSelect = sql.trim().toUpperCase().startsWith("SELECT");
      const res = isSelect ? await stmt.all<T>() : await (stmt as any).run();
      results.push(res);
    }
    return results;
  }

  async exec(query: string): Promise<D1ExecResult> {
    this.rawDb.exec(query);
    return {
      count: 1,
      duration: 0,
    };
  }

  async dump(): Promise<ArrayBuffer> {
    throw new Error("dump() is not implemented in mock");
  }

  withSession(bookmark?: string): D1Database {
    if (bookmark) {
      this.bookmarks.push(bookmark);
    }
    return this as unknown as D1Database;
  }
}

export function createMockD1Database(): D1Database {
  return new MockD1Database() as unknown as D1Database;
}
