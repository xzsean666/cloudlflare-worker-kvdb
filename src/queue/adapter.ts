import { StorageError } from "../core/errors.js";

export interface QueueSqlStatement {
  sql: string;
  params?: unknown[];
}

export interface QueueSqlAdapter {
  exec(sql: string, ...params: unknown[]): Promise<{ changes: number }>;
  query<T = Record<string, unknown>>(sql: string, ...params: unknown[]): Promise<T[]>;
  batch(statements: QueueSqlStatement[]): Promise<{ changes: number }[]>;
}

function coerceParams(params: unknown[]): unknown[] {
  return params.map((p) => {
    if (typeof p === "boolean") return p ? 1 : 0;
    return p;
  });
}

/**
 * Creates a unified SQL adapter wrapping either Cloudflare D1 or Durable Objects SqlStorage.
 */
export function createQueueSqlAdapter(db: D1Database | SqlStorage): QueueSqlAdapter {
  if (!db) {
    throw new StorageError("Queue requires a valid D1Database or SqlStorage instance", "INVALID_CONFIG");
  }

  // Detect D1Database (has .prepare)
  if (typeof (db as D1Database).prepare === "function") {
    const d1 = db as D1Database;
    return {
      async exec(sql: string, ...params: unknown[]): Promise<{ changes: number }> {
        const stmt = d1.prepare(sql).bind(...coerceParams(params));
        const res = await stmt.run();
        return { changes: res.meta.changes ?? 0 };
      },
      async query<T = Record<string, unknown>>(sql: string, ...params: unknown[]): Promise<T[]> {
        const stmt = d1.prepare(sql).bind(...coerceParams(params));
        const res = await stmt.all<T>();
        return res.results ?? [];
      },
      async batch(statements: QueueSqlStatement[]): Promise<{ changes: number }[]> {
        if (statements.length === 0) return [];
        const stmts = statements.map((s) =>
          d1.prepare(s.sql).bind(...coerceParams(s.params ?? []))
        );
        const results = await d1.batch(stmts);
        return results.map((r) => ({ changes: r.meta?.changes ?? 0 }));
      },
    };
  }

  // Detect SqlStorage (has .exec and .Cursor)
  if (typeof (db as SqlStorage).exec === "function") {
    const sqlStorage = db as SqlStorage;
    return {
      async exec(sql: string, ...params: unknown[]): Promise<{ changes: number }> {
        const cursor = sqlStorage.exec(sql, ...coerceParams(params));
        return { changes: cursor.rowsWritten ?? 0 };
      },
      async query<T = Record<string, unknown>>(sql: string, ...params: unknown[]): Promise<T[]> {
        const cursor = sqlStorage.exec<T & Record<string, any>>(sql, ...coerceParams(params));
        return cursor.toArray();
      },
      async batch(statements: QueueSqlStatement[]): Promise<{ changes: number }[]> {
        if (statements.length === 0) return [];
        const results: { changes: number }[] = [];
        sqlStorage.exec("BEGIN;");
        try {
          for (const s of statements) {
            const cursor = sqlStorage.exec(s.sql, ...coerceParams(s.params ?? []));
            results.push({ changes: cursor.rowsWritten ?? 0 });
          }
          sqlStorage.exec("COMMIT;");
          return results;
        } catch (err) {
          try {
            sqlStorage.exec("ROLLBACK;");
          } catch {}
          throw err;
        }
      },
    };
  }

  throw new StorageError("Unsupported database object provided to Queue adapter", "INVALID_CONFIG");
}
