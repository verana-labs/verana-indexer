import knex from "./db_connection";

const tableExistenceCache = new Map<string, { exists: boolean; timestamp: number }>();
const CACHE_TTL_MS = 60000;

export async function tableExists(tableName: string, useCache = true): Promise<boolean> {
  if (useCache) {
    const cached = tableExistenceCache.get(tableName);
    const now = Date.now();
    if (cached && (now - cached.timestamp) < CACHE_TTL_MS) {
      return cached.exists;
    }
  }

  try {
    const exists = await knex.schema.hasTable(tableName);
    tableExistenceCache.set(tableName, { exists, timestamp: Date.now() });
    return exists;
  } catch (err) {
    console.error(`Error checking table existence for ${tableName}:`, err);
    return false;
  }
}

export async function tablesExist(tableNames: string[]): Promise<boolean> {
  const checks = await Promise.all(tableNames.map(name => tableExists(name)));
  return checks.every(exists => exists);
}

export function clearTableExistenceCache(tableName?: string): void {
  if (tableName) {
    tableExistenceCache.delete(tableName);
  } else {
    tableExistenceCache.clear();
  }
}

export function isTableMissingError(error: any): boolean {
  return error?.nativeError?.code === '42P01' || 
         error?.code === '42P01' ||
         (error?.message && error.message.includes('does not exist'));
}

export async function waitForTables(
  tableNames: string[],
  maxRetries = 30,
  delayMs = 2000
): Promise<boolean> {
  for (let i = 0; i < maxRetries; i++) {
    const allExist = await tablesExist(tableNames);
    if (allExist) {
      return true;
    }
    if (i < maxRetries - 1) {
      await new Promise<void>(resolve => {
        setTimeout(() => resolve(), delayMs);
      });
    }
  }
  return false;
}

