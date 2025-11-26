import { Knex } from "knex";
import path from "node:path";
import { Config } from "./common";
import { Network } from "./network";


const DEFAULT_POOL_MAX = 50;
const DEFAULT_STATEMENT_TIMEOUT = 20000;
const DEFAULT_CONNECTION_TIMEOUT = 60000;

const databaseName = Network.databaseName;

const isProduction = process.env.NODE_ENV === "production";
const migrationExtension = isProduction ? ".js" : ".ts";
function getMigrationsDirectory(): string {
  const baseDir = process.cwd();
  if (isProduction) {
    return path.join(baseDir, "dist", "src", "migrations");
  }
  return path.join(baseDir, "src", "migrations");
}

const migrationsDirectory = getMigrationsDirectory();

const toInt = (value: string | number | undefined, fallback: number): number =>
  parseInt(String(value ?? fallback), 10);

const connectionTimeoutSource = (Config as Record<string, unknown>).POSTGRES_CONNECTION_TIMEOUT;
const getBaseConnection = () => ({
  host: Config.POSTGRES_HOST || "localhost",
  user: Config.POSTGRES_USER,
  password: String(Config.POSTGRES_PASSWORD ?? ""),
  port: Config.POSTGRES_PORT,
  connectionTimeoutMillis: isLightweightMode()
    ? 120000
    : toInt(connectionTimeoutSource as string | number | undefined, DEFAULT_CONNECTION_TIMEOUT),
  statement_timeout: toInt(Config.POSTGRES_STATEMENT_TIMEOUT, DEFAULT_STATEMENT_TIMEOUT),
});

const baseMigrations = {
  directory: migrationsDirectory,
  loadExtensions: [migrationExtension],
};

const createPool = (min: number, max: number, acquireTimeout: number = 120000) => ({
  min,
  max,
  acquireTimeoutMillis: acquireTimeout,
  idleTimeoutMillis: 10000,
  reapIntervalMillis: 1000,
  createTimeoutMillis: 30000,
  destroyTimeoutMillis: 5000,
  propagateCreateError: false,
  createRetryIntervalMillis: 200,
});

const isLightweightMode = () => process.env.MIGRATION_MODE === "lightweight";
const lightweightAcquireTimeout = 120000;

export const knexConfig: Record<string, Knex.Config> = {
  development: {
    client: "pg",
    migrations: {
      ...baseMigrations,
      tableName: "knex_migrations",
      disableTransactions: false,
    },
    connection: { ...getBaseConnection(), database: databaseName },
    pool: isLightweightMode()
      ? createPool(1, 2, lightweightAcquireTimeout)
      : createPool(2, toInt(Config.POSTGRES_POOL_MAX, DEFAULT_POOL_MAX), 120000),
    acquireConnectionTimeout: isLightweightMode() ? lightweightAcquireTimeout : 120000,
    asyncStackTraces: false,
  },
  test: {
    client: "pg",
    migrations: {
      ...baseMigrations,
      tableName: "knex_migrations",
      disableTransactions: false,
    },
    connection: { ...getBaseConnection(), database: Config.POSTGRES_DB_TEST, host: "localhost" },
    pool: isLightweightMode()
      ? createPool(1, 2, lightweightAcquireTimeout)
      : createPool(1, 5, 120000),
    acquireConnectionTimeout: isLightweightMode() ? lightweightAcquireTimeout : 120000,
    asyncStackTraces: false,
  },
  production: {
    client: "pg",
    migrations: {
      ...baseMigrations,
      tableName: "knex_migrations",
      disableTransactions: false,
    },
    connection: { ...getBaseConnection(), database: databaseName },
    pool: isLightweightMode()
      ? createPool(1, 2, lightweightAcquireTimeout)
      : createPool(2, toInt(Config.POSTGRES_POOL_MAX, DEFAULT_POOL_MAX), 120000),
    acquireConnectionTimeout: isLightweightMode() ? lightweightAcquireTimeout : 120000,
    asyncStackTraces: false,
  },
};

export const getConfigForEnv = (env?: string): Knex.Config => {
  const resolvedEnv = env || process.env.NODE_ENV || "development";
  const envConfig = knexConfig[resolvedEnv];

  if (!envConfig) {
    throw new Error(`Invalid NODE_ENV: ${resolvedEnv}. Must be one of: development, test, production`);
  }

  return envConfig;
};

export default getConfigForEnv;

