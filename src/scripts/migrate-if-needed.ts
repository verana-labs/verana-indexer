import * as dotenv from "dotenv";
import knex, { Knex } from "knex";
import { loadEnvFiles } from "../common/utils/loadEnv";
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
      const shortMsg = errorMsg.length > 100 ? `${errorMsg.substring(0, 100)}...` : errorMsg;
      console.log(`Waiting for database... (attempt ${i + 1}/${maxRetries}) - ${shortMsg}`);
      await new Promise<void>((resolve) => {
        setTimeout(resolve, delayMs);
      });
    }
  }
}

(async function runMigrations(): Promise<void> {
  const environment = process.env.NODE_ENV || "production";
  process.env.NODE_ENV = environment;
  process.env.MIGRATION_MODE = "lightweight";

  const isTestMode = environment === "test";
  
  if (isTestMode) {
    console.log("Test mode detected - running migrations only (no destructive operations).");
  }

  let db: Knex | undefined;

  try {
    console.log(`Connecting to database (env: ${environment})...`);
    const config = getConfigForEnv();

    const connInfo = config.connection as any;
    console.log(`Host: ${connInfo.host}, Port: ${connInfo.port}, User: ${connInfo.user}, Database: ${connInfo.database}`);

    await waitForDatabase(config);
    console.log("Database connection established.");

    db = knex(config);

    const blockExists = await db.schema.hasTable("block");
    let migrationsTableExists = await db.schema.hasTable("knex_migrations");
    
    try {
      let pending: any[] = [];
      let completed: any[] = [];
      
      if (migrationsTableExists) {
        try {
          [completed, pending] = await db.migrate.list();
        } catch (listError: any) {
          const errorMsg = listError?.message || String(listError);
          if (errorMsg.includes("does not exist") || errorMsg.includes("relation") || errorMsg.includes("knex_migrations")) {
            console.log("Migration table exists but list() failed - treating as empty and will run migrations.");
            migrationsTableExists = false;
          } else {
            throw listError;
          }
        }
      } else {
        console.log("Migration table does not exist - will run all migrations from scratch.");
      }
      
      const shouldRunMigrations = !blockExists || !migrationsTableExists || (pending && pending.length > 0);
      
      if (!shouldRunMigrations) {
        console.log(`Database is up to date (env: ${environment}). No migrations required.`);
        console.log(`  Completed migrations: ${completed?.length || 0}`);
        console.log(`  Pending migrations: ${pending?.length || 0}`);
        if (isTestMode) {
          console.log(`  Test mode: Database ready for tests.`);
        }
      } else {
        if (pending && pending.length > 0) {
          console.log(`Found ${pending.length} pending migration(s) (env: ${environment})...`);
        } else if (!migrationsTableExists) {
          console.log(`Migration table missing - running all migrations from scratch (env: ${environment})...`);
          if (isTestMode) {
            console.log(`  Test mode: This is expected after database cleanup in CI.`);
          }
        } else if (!blockExists) {
          console.log(`Database tables missing - running all migrations from scratch (env: ${environment})...`);
          if (isTestMode) {
            console.log(`  Test mode: This is expected after database cleanup in CI.`);
          }
        }
        
        if (blockExists && pending && pending.length > 0) {
          const initMigration = pending.find((m: any) => m && m.name && m.name.includes("init_horoscope_layer_1_model"));
          if (initMigration) {
            console.log(`    Init migration detected but block table exists, will skip it`);
            console.log(`  Using Knex migrate.latest() - it will handle skipping init migration automatically`);
          }
        }
        
        console.log(`  Running migrations using Knex migrate.latest()...`);
        await db.migrate.latest();
        console.log("Migrations finished successfully.");
      }
    } catch (migrateError: any) {
      const errorMsg = migrateError.message || String(migrateError);
      
      if (errorMsg.includes("corrupt") || errorMsg.includes("missing")) {
        console.log("  Migration validation error detected (this is normal after reindexing)");
        console.log("  Migration state may be inconsistent after reindexing operations.");
        console.log("  If you're running reindex, migrations will be handled by the reindex script.");
        console.log("  If this is a fresh database, you may need to run: pnpm run reindex");
        
        const hasTables = await db.schema.hasTable("block");
        if (hasTables) {
          console.log("  Database tables exist - continuing without running migrations.");
          console.log("  This is expected behavior after reindexing.");
        } else {
          console.log("  No tables found - you may need to run migrations manually.");
          console.log("  Try running: pnpm run reindex");
          process.exitCode = 1;
        }
      } else if (errorMsg.includes("already exists") && errorMsg.includes("block")) {
        console.log("  Init migration tried to create block table (skipping)");
        console.log("  Continuing with remaining migrations...");
        try {
          await db.migrate.latest();
          console.log("Migrations finished successfully.");
        } catch (retryError: any) {
          console.error("Migration retry failed:", retryError.message);
          throw retryError;
        }
      } else {
        console.error("Migration run failed:", migrateError);
        throw migrateError;
      }
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
