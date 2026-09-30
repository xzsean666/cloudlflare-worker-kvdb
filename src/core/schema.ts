import { KVDBError } from "./errors.js";

export type KeyType = "string" | "integer" | "number" | "boolean" | "json";
export type PrimaryKeyType = "string" | "integer";

export interface KeyIndexOptions {
  name?: string;
  unique?: boolean;
}

export interface KeyDefinition<T extends KeyType = KeyType> {
  type: T;
  nullable?: boolean;
  default?: unknown;
  index?: boolean | KeyIndexOptions;
}

export interface PrimaryKeyDefinition<T extends PrimaryKeyType = PrimaryKeyType> {
  name: string;
  type?: T;
}

export interface MultiKeyIndexDefinition {
  name?: string;
  keys?: string[];
  columns?: string[];
  unique?: boolean;
}

export interface TableIndexDefinition {
  name?: string;
  columns?: string[];
  keys?: string[];
  unique?: boolean;
}

export interface MultiKeySchema<
  Keys extends Record<string, unknown> = Record<string, unknown>,
  PKType extends PrimaryKeyType = PrimaryKeyType
> {
  primaryKey?: PrimaryKeyDefinition<PKType>;
  keys?: { [Name in keyof Keys]?: KeyDefinition } & Record<string, KeyDefinition>;
  columns?: { [Name in keyof Keys]?: KeyDefinition } & Record<string, KeyDefinition>;
  indexes?: (TableIndexDefinition | MultiKeyIndexDefinition)[];
  tableName?: string;
  version?: number;
}

export interface TableSchema<Columns extends Record<string, unknown> = Record<string, unknown>> {
  primaryKey?: PrimaryKeyDefinition;
  columns?: { [Name in keyof Columns]?: KeyDefinition } & Record<string, KeyDefinition>;
  keys?: { [Name in keyof Columns]?: KeyDefinition } & Record<string, KeyDefinition>;
  indexes?: (TableIndexDefinition | MultiKeyIndexDefinition)[];
  tableName?: string;
  version?: number;
}

export interface PhysicalRecord<
  Columns extends Record<string, unknown> = Record<string, unknown>,
  Value = unknown,
  PK = string | number
> {
  key: PK;
  columns: Partial<Columns>;
  keys: Partial<Columns>;
  value: Value;
}

export interface NormalizedSchema {
  primaryKey: Required<PrimaryKeyDefinition>;
  columns: Record<string, KeyDefinition>;
  indexes: TableIndexDefinition[];
  tableName?: string;
  version: number;
}

const IDENTIFIER_REGEX = /^[A-Za-z_][A-Za-z0-9_]*$/;
const RESERVED_WORDS = new Set(["value", "expires_at", "created_at", "updated_at", "namespace", "key"]);

/**
 * Normalizes and validates a TableSchema or MultiKeySchema.
 */
export function normalizeTableSchema(schema: TableSchema | MultiKeySchema): NormalizedSchema {
  const pk: Required<PrimaryKeyDefinition> = {
    name: schema.primaryKey?.name ?? "id",
    type: schema.primaryKey?.type ?? "string",
  };

  if (!IDENTIFIER_REGEX.test(pk.name)) {
    throw new KVDBError(`Invalid primary key identifier: ${pk.name}`, "INVALID_SCHEMA");
  }

  if (schema.tableName && !IDENTIFIER_REGEX.test(schema.tableName)) {
    throw new KVDBError(`Invalid table name identifier: ${schema.tableName}`, "INVALID_SCHEMA");
  }

  const rawColumns = schema.keys ?? schema.columns ?? {};
  const columns: Record<string, KeyDefinition> = {};
  for (const [colName, colDef] of Object.entries(rawColumns)) {
    if (!IDENTIFIER_REGEX.test(colName)) {
      throw new KVDBError(`Invalid column identifier: ${colName}`, "INVALID_SCHEMA");
    }
    if (RESERVED_WORDS.has(colName)) {
      throw new KVDBError(`Column identifier "${colName}" is a reserved system keyword`, "INVALID_SCHEMA");
    }
    if (colName === pk.name) {
      throw new KVDBError(`Column "${colName}" conflicts with primary key name`, "INVALID_SCHEMA");
    }
    columns[colName] = { ...colDef };
  }

  const indexes: TableIndexDefinition[] = [];
  if (schema.indexes) {
    for (const idx of schema.indexes) {
      if (idx.name && !IDENTIFIER_REGEX.test(idx.name)) {
        throw new KVDBError(`Invalid index identifier: ${idx.name}`, "INVALID_SCHEMA");
      }
      const cols = idx.keys ?? idx.columns ?? [];
      for (const col of cols) {
        if (!IDENTIFIER_REGEX.test(col)) {
          throw new KVDBError(`Invalid index column identifier: ${col}`, "INVALID_SCHEMA");
        }
      }
      indexes.push({
        name: idx.name,
        columns: [...cols],
        keys: [...cols],
        unique: idx.unique,
      });
    }
  }

  // Also extract single-column indexes declared on KeyDefinition
  for (const [colName, colDef] of Object.entries(columns)) {
    if (colDef.index) {
      const isUnique = typeof colDef.index === "object" ? Boolean(colDef.index.unique) : false;
      const idxName = typeof colDef.index === "object" && colDef.index.name ? colDef.index.name : undefined;
      if (idxName && !IDENTIFIER_REGEX.test(idxName)) {
        throw new KVDBError(`Invalid index identifier: ${idxName}`, "INVALID_SCHEMA");
      }
      indexes.push({
        name: idxName,
        columns: [colName],
        keys: [colName],
        unique: isUnique,
      });
    }
  }

  return {
    primaryKey: pk,
    columns,
    indexes,
    tableName: schema.tableName,
    version: schema.version ?? 1,
  };
}

/**
 * Maps logical KeyType to SQLite column type.
 */
export function keyTypeToSqliteType(type: KeyType | PrimaryKeyType): string {
  switch (type) {
    case "string":
      return "TEXT";
    case "integer":
      return "INTEGER";
    case "number":
      return "REAL";
    case "boolean":
      return "INTEGER";
    case "json":
      return "TEXT";
    default:
      return "TEXT";
  }
}

/**
 * Generates SQL DDL to create the physical table for the schema.
 */
export function generateCreateTableSql(
  tableName: string,
  schema: NormalizedSchema
): string {
  const pkSqlType = keyTypeToSqliteType(schema.primaryKey.type);
  const columnDefs: string[] = [
    `${schema.primaryKey.name} ${pkSqlType} PRIMARY KEY`,
  ];

  for (const [colName, colDef] of Object.entries(schema.columns)) {
    const sqlType = keyTypeToSqliteType(colDef.type);
    let defSql = `${colName} ${sqlType}`;
    if (colDef.nullable === false) {
      defSql += " NOT NULL";
    }
    if (colDef.default !== undefined) {
      const defaultVal = typeof colDef.default === "string"
        ? `'${colDef.default.replace(/'/g, "''")}'`
        : colDef.default;
      defSql += ` DEFAULT ${defaultVal}`;
    }
    columnDefs.push(defSql);
  }

  // Standard payload columns
  columnDefs.push("value TEXT NOT NULL");
  columnDefs.push("expires_at INTEGER");
  columnDefs.push("created_at INTEGER NOT NULL");
  columnDefs.push("updated_at INTEGER NOT NULL");

  return `CREATE TABLE IF NOT EXISTS ${tableName} (\n  ${columnDefs.join(",\n  ")}\n);`;
}

/**
 * Generates SQL DDL to create B-Tree indexes on the physical table.
 */
export function generateIndexSqls(
  tableName: string,
  schema: NormalizedSchema
): string[] {
  const sqls: string[] = [];

  // Default system indexes for TTL and timestamps
  sqls.push(`CREATE INDEX IF NOT EXISTS idx_${tableName}_expires ON ${tableName} (expires_at);`);
  sqls.push(`CREATE INDEX IF NOT EXISTS idx_${tableName}_created ON ${tableName} (created_at);`);

  for (const idx of schema.indexes) {
    const cols = idx.columns ?? idx.keys ?? [];
    const colList = cols.join("_");
    const idxName = idx.name ?? `idx_${tableName}_${colList}`;
    const unique = idx.unique ? "UNIQUE " : "";
    sqls.push(
      `CREATE ${unique}INDEX IF NOT EXISTS ${idxName} ON ${tableName} (${cols.join(", ")});`
    );
  }

  return sqls;
}

/**
 * Generates SQL DDL to dynamically add a column to an existing table.
 */
export function generateAddColumnSql(
  tableName: string,
  colName: string,
  colDef: KeyDefinition
): { alterSql: string; indexSql?: string } {
  const sqlType = keyTypeToSqliteType(colDef.type);
  let alterSql = `ALTER TABLE ${tableName} ADD COLUMN ${colName} ${sqlType}`;

  if (colDef.default !== undefined) {
    const defaultVal = typeof colDef.default === "string"
      ? `'${colDef.default.replace(/'/g, "''")}'`
      : colDef.default;
    alterSql += ` DEFAULT ${defaultVal}`;
  }

  let indexSql: string | undefined;
  if (colDef.index) {
    const isUnique = typeof colDef.index === "object" ? Boolean(colDef.index.unique) : false;
    const idxName = typeof colDef.index === "object" && colDef.index.name
      ? colDef.index.name
      : `idx_${tableName}_${colName}`;
    const unique = isUnique ? "UNIQUE " : "";
    indexSql = `CREATE ${unique}INDEX IF NOT EXISTS ${idxName} ON ${tableName} (${colName});`;
  }

  return { alterSql, indexSql };
}

/**
 * Generates SQL DDL to dynamically create an index on an existing table.
 */
export function generateAddIndexSql(
  tableName: string,
  indexDef: TableIndexDefinition | MultiKeyIndexDefinition
): string {
  const cols = indexDef.keys ?? indexDef.columns ?? [];
  if (cols.length === 0) {
    throw new KVDBError("Index definition must contain at least one column/key", "INVALID_INDEX");
  }
  for (const col of cols) {
    if (!IDENTIFIER_REGEX.test(col)) {
      throw new KVDBError(`Invalid index column identifier: ${col}`, "INVALID_SCHEMA");
    }
  }
  const colList = cols.join("_");
  const idxName = indexDef.name ?? `idx_${tableName}_${colList}`;
  if (!IDENTIFIER_REGEX.test(idxName)) {
    throw new KVDBError(`Invalid index identifier: ${idxName}`, "INVALID_SCHEMA");
  }
  const unique = indexDef.unique ? "UNIQUE " : "";
  return `CREATE ${unique}INDEX IF NOT EXISTS ${idxName} ON ${tableName} (${cols.join(", ")});`;
}
