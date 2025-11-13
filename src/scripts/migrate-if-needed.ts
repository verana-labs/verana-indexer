import knex, { Knex } from "knex";
import { loadEnvFiles } from "../common/utils/loadEnv";
import * as dotenv from "dotenv";
import { getConfigForEnv } from "../knexfile";

loadEnvFiles();
dotenv.config();


async function waitForDatabase(config: any, maxRetries = 30, delayMs = 2000): Promise<void> {
  for (let i = 0; i < maxRetries; i++) {
    let testDb: Knex | undefined;
    try {
      const testConfig: Knex.Config = {
        client: config.client,
        connection: config.connection,
        pool: {
          min: 0,
          max: 10,
          acquireTimeoutMillis: 10000,
          createTimeoutMillis: 10000,
          idleTimeoutMillis: 1000,
          reapIntervalMillis: 1000,
          destroyTimeoutMillis: 5000,
          propagateCreateError: false,
        }
      };

      testDb = knex(testConfig);
      await testDb.raw("SELECT 1");
      await testDb.destroy().catch(() => undefined);
      return;
    } catch (error: any) {
      if (testDb) {
        await testDb.destroy().catch(() => undefined);
      }

      if (i === maxRetries - 1) {
        const errorMsg = error?.message || String(error);
        const errorCode = error?.code || 'UNKNOWN';
        throw new Error(
          `Database not ready after ${maxRetries} attempts.\n` +
          `Error: ${errorMsg}\n` +
          `Code: ${errorCode}\n` +
          `Make sure PostgreSQL container is running: docker ps --filter "name=psql_erascope"\n` +
          `Start containers: pnpm run up\n` +
          `Check container logs: docker logs psql_erascope`
        );
      }

      const errorMsg = error?.message || String(error);
      const shortMsg = errorMsg.length > 100 ? errorMsg.substring(0, 100) + '...' : errorMsg;
      console.log(`Waiting for database... (attempt ${i + 1}/${maxRetries}) - ${shortMsg}`);

      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
}

(async function runMigrations(): Promise<void> {
  const environment = process.env.NODE_ENV || "production";
  process.env.NODE_ENV = environment;
  process.env.MIGRATION_MODE = "lightweight";

  let db: Knex | undefined;

  try {
    console.log(`Connecting to database (env: ${environment})...`);
    const config = getConfigForEnv();

    // Log connection details (without password)
    const connInfo = config.connection as any;
    console.log(`Host: ${connInfo.host}, Port: ${connInfo.port}, User: ${connInfo.user}, Database: ${connInfo.database}`);

    await waitForDatabase(config);
    console.log("Database connection established.");

    db = knex(config);

    const [completed, pending] = await db.migrate.list();

    if (!pending.length) {
      console.log(`Database is up to date (env: ${environment}). No migrations required.`);
    } else {
      console.log(`Applying ${pending.length} pending migration(s) (env: ${environment})...`);
      await db.migrate.latest();
      console.log("Migrations finished successfully.");
    }
  } catch (error) {
    console.error("Migration run failed:", error);
    process.exitCode = 1;
  } finally {
    if (db) {
      await db.destroy().catch(() => undefined);
    }
    process.exit(process.exitCode || 0);
  }
})();
