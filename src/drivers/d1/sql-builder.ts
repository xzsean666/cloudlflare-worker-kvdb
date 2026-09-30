import { buildInClausePlaceholders, buildMultiRowPlaceholders } from "../../core/chunker.js";
import { KVDBError } from "../../core/errors.js";

export const DEFAULT_KVDB_TABLE = "_kvdb_entries";
const IDENTIFIER_REGEX = /^[A-Za-z_][A-Za-z0-9_]*$/;

export class D1SqlBuilder {
  constructor(public readonly tableName: string = DEFAULT_KVDB_TABLE) {
    if (!IDENTIFIER_REGEX.test(tableName)) {
      throw new KVDBError(`Invalid table name identifier "${tableName}"`, "INVALID_SCHEMA");
    }
  }

  buildBootstrapSql(): string[] {
    return [
      `CREATE TABLE IF NOT EXISTS ${this.tableName} (
        namespace TEXT NOT NULL,
        key TEXT NOT NULL,
        value TEXT NOT NULL,
        expires_at INTEGER,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY (namespace, key)
      );`,
      `CREATE INDEX IF NOT EXISTS idx_${this.tableName}_ns_expires ON ${this.tableName} (namespace, expires_at);`,
      `CREATE INDEX IF NOT EXISTS idx_${this.tableName}_ns_created ON ${this.tableName} (namespace, created_at, key);`,
    ];
  }

  buildGetSql(): string {
    return `SELECT value FROM ${this.tableName} WHERE namespace = ? AND key = ? AND (expires_at IS NULL OR expires_at > ?) LIMIT 1;`;
  }

  buildGetManySql(keysCount: number): string {
    const inClause = buildInClausePlaceholders(keysCount);
    return `SELECT key, value FROM ${this.tableName} WHERE namespace = ? AND key IN ${inClause} AND (expires_at IS NULL OR expires_at > ?);`;
  }

  buildSetSql(): string {
    return `INSERT INTO ${this.tableName} (namespace, key, value, expires_at, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(namespace, key) DO UPDATE SET
        value = excluded.value,
        expires_at = excluded.expires_at,
        updated_at = excluded.updated_at;`;
  }

  buildMultiRowSetSql(rowCount: number): string {
    const placeholders = buildMultiRowPlaceholders(rowCount, 6);
    return `INSERT INTO ${this.tableName} (namespace, key, value, expires_at, created_at, updated_at)
      VALUES ${placeholders}
      ON CONFLICT(namespace, key) DO UPDATE SET
        value = excluded.value,
        expires_at = excluded.expires_at,
        updated_at = excluded.updated_at;`;
  }

  buildDeleteSql(): string {
    return `DELETE FROM ${this.tableName} WHERE namespace = ? AND key = ?;`;
  }

  buildDeleteManySql(keysCount: number): string {
    const inClause = buildInClausePlaceholders(keysCount);
    return `DELETE FROM ${this.tableName} WHERE namespace = ? AND key IN ${inClause};`;
  }

  buildHasSql(): string {
    return `SELECT 1 FROM ${this.tableName} WHERE namespace = ? AND key = ? AND (expires_at IS NULL OR expires_at > ?) LIMIT 1;`;
  }

  buildClearSql(): string {
    return `DELETE FROM ${this.tableName} WHERE namespace = ?;`;
  }

  buildListQuery(
    hasPrefix: boolean,
    hasCursor: boolean
  ): { sql: string } {
    let sql = `SELECT key FROM ${this.tableName} WHERE namespace = ? AND (expires_at IS NULL OR expires_at > ?)`;
    if (hasPrefix) {
      sql += ` AND key LIKE ? ESCAPE '\\'`;
    }
    if (hasCursor) {
      sql += ` AND key > ?`;
    }
    sql += ` ORDER BY key ASC LIMIT ?;`;
    return { sql };
  }
}
