/* eslint-disable no-console */
import * as dotenv from "dotenv";
import knex, { Knex } from "knex";
import * as fs from "fs";
import * as path from "path";
import { loadEnvFiles } from "../common/utils/loadEnv";
import { getConfigForEnv } from "../knexfile";

loadEnvFiles();
dotenv.config();

const TABLES_TO_DROP = [
  "transaction_message",
  "transaction",
  "credential_schema_history",
  "credential_schemas",
  "governance_framework_document_history",
  "governance_framework_version_history",
  "trust_registry_history",
  "governance_framework_document",
  "governance_framework_version",
  "trust_registry",
  "permission_session_history",
  "permission_history",
  "permission_sessions",
  "permissions",
  "trust_deposit_history",
  "trust_deposits",
  "did_history",
  "dids",
  "module_params_history",
  "module_params",
  "account_balance",
  "account_vesting",
  "account_statistics",
  "daily_statistics",
  "account",
];

const SEQUENCES_TO_RESET = [
  "transaction_id_seq",
  "credential_schema_history_id_seq",
  "credential_schemas_id_seq",
  "governance_framework_document_history_id_seq",
  "governance_framework_version_history_id_seq",
  "trust_registry_history_id_seq",
  "governance_framework_document_id_seq",
  "governance_framework_version_id_seq",
  "trust_registry_id_seq",
  "permission_session_history_id_seq",
  "permission_history_id_seq",
  "permission_sessions_id_seq",
  "permissions_id_seq",
  "trust_deposit_history_id_seq",
  "trust_deposits_id_seq",
  "did_history_id_seq",
  "dids_id_seq",
  "module_params_history_id_seq",
  "module_params_id_seq",
  "account_balance_id_seq",
  "account_vesting_id_seq",
  "account_statistics_id_seq",
  "daily_statistics_id_seq",
  "account_id_seq",
];

async function waitForDatabase(config: Knex.Config, maxRetries = 30, delayMs = 2000): Promise<void> {
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
    } catch (error: unknown) {
      if (testDb) {
        await testDb.destroy().catch(() => undefined);
      }

      const err = error as NodeJS.ErrnoException;
      if (i === maxRetries - 1) {
        const errorMsg = err?.message || String(error);
        const errorCode = err?.code || 'UNKNOWN';
        throw new Error(
          `Database not ready after ${maxRetries} attempts.\n` +
          `Error: ${errorMsg}\n` +
          `Code: ${errorCode}`
        );
      }

      const errorMsg = err?.message || String(error);
      const shortMsg = errorMsg.length > 100 ? `${errorMsg.substring(0, 100)}...` : errorMsg;
      console.log(`Waiting for database... (attempt ${i + 1}/${maxRetries}) - ${shortMsg}`);
      await new Promise<void>((resolve) => {
        setTimeout(resolve, delayMs);
      });
    }
  }
}

async function checkTableExists(db: Knex, tableName: string): Promise<boolean> {
  const result = await db.raw(`
    SELECT EXISTS (
      SELECT FROM information_schema.tables 
      WHERE table_schema = 'public' 
      AND table_name = ?
    );
  `, [tableName]);
  return result.rows[0]?.exists || false;
}

async function dropTables(db: Knex): Promise<void> {
  console.log("\nStep 2: Dropping transaction and module tables...");
  
  for (const tableName of TABLES_TO_DROP) {
    const exists = await checkTableExists(db, tableName);
    if (exists) {
      try {
        console.log(`  Dropping table: ${tableName}`);
        await db.schema.dropTableIfExists(tableName).catch((err: unknown) => {
          const error = err as NodeJS.ErrnoException;
          return db.raw(`DROP TABLE IF EXISTS ${tableName} CASCADE`).catch(() => {
            console.warn(`    Could not drop ${tableName}: ${error.message}`);
          });
        });
        console.log(`   Dropped: ${tableName}`);
      } catch (error: unknown) {
        const err = error as NodeJS.ErrnoException;
        console.warn(`    Error dropping ${tableName}: ${err.message}`);
      }
    } else {
      console.log(`   Table ${tableName} does not exist, skipping`);
    }
  }
  
  console.log(" All tables dropped successfully\n");
}

let migrationCheckpointsBackup: Array<{ job_name: string; height: number }> = [];

async function clearCheckpoints(db: Knex): Promise<void> {
  console.log(" Step 3: Clearing checkpoints and setting block checkpoint to highest block...");
  
  let highestBlock = 0;
  try {
    const blockExists = await checkTableExists(db, "block");
    if (blockExists) {
      const result = await db("block").max("height as max").first();
      highestBlock = result && (result as { max: string | number | null }).max 
        ? parseInt(String((result as { max: string | number }).max), 10) 
        : 0;
      console.log(`   Highest block in database: ${highestBlock}`);
    }
    } catch (error: unknown) {
      const err = error as NodeJS.ErrnoException;
      console.warn(`    Could not get highest block: ${err.message}`);
    }
  
  try {
    const blockCheckpointExists = await checkTableExists(db, "block_checkpoint");
    if (blockCheckpointExists) {
      const migrationJobNames = [
        "job:create-event-attr-partition",
      ];
      
      const migrationCheckpoints = await db("block_checkpoint")
        .whereIn("job_name", migrationJobNames)
        .select("job_name", "height");
      
      migrationCheckpointsBackup = migrationCheckpoints.map((cp: { job_name: string; height: number }) => ({
        job_name: cp.job_name,
        height: cp.height
      }));
      
      if (migrationCheckpointsBackup.length > 0) {
        console.log(`   Backed up ${migrationCheckpointsBackup.length} migration checkpoints:`, migrationCheckpointsBackup);
      }
      
      const genesisJobNames = [
        "crawl:genesis",
        "crawl:genesis-account",
        "crawl:genesis-validator",
        "crawl:genesis-proposal",
        "crawl:genesis-code",
        "crawl:genesis-contract",
        "crawl:genesis-feegrant",
        "crawl:genesis-ibc-tao"
      ];
      
      await db("block_checkpoint")
        .whereIn("job_name", genesisJobNames)
        .delete();
      console.log(`   Cleared genesis job checkpoints`);
      
      const deleted = await db("block_checkpoint").delete();
      console.log(`   Cleared ${deleted} rows from block_checkpoint`);
      
      if (highestBlock > 0) {
        await db("block_checkpoint").insert({
          job_name: "crawl:block",
          height: highestBlock
        });
        console.log(`   Set crawl:block checkpoint to ${highestBlock} (highest block in database)`);
        console.log(`   Block crawler will skip fetching blocks 0-${highestBlock} and only fetch new blocks`);
      }
      
      const tableInfo = await db.raw(`
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'block_checkpoint' 
        AND column_name IN ('job_name', 'height')
      `);
      
      if (tableInfo.rows.length >= 2) {
        console.log("   block_checkpoint table structure verified");
      }
    } else {
      console.log("   block_checkpoint table does not exist (will be created by migrations)");
      if (highestBlock > 0) {
        console.log(`   After migrations, set crawl:block checkpoint to ${highestBlock} manually if needed`);
      }
    }
  } catch (error: unknown) {
      const err = error as NodeJS.ErrnoException;
      console.warn(`    Error clearing block_checkpoint: ${err.message}`);
    }

  try {
    const checkpointExists = await checkTableExists(db, "checkpoint");
    if (checkpointExists) {
      const deleted = await db("checkpoint").delete();
      console.log(`   Cleared ${deleted} rows from checkpoint`);
    } else {
      console.log("   checkpoint table does not exist (will be created by migrations)");
    }
  } catch (error: unknown) {
    const err = error as NodeJS.ErrnoException;
    console.warn(`    Error clearing checkpoint: ${err.message}`);
  }
  
  console.log(" Checkpoints cleared and block checkpoint set successfully\n");
}

async function restoreMigrationCheckpoints(db: Knex): Promise<void> {
  console.log("Step 7: Restoring migration checkpoints...");
  
  if (migrationCheckpointsBackup.length === 0) {
    console.log("   No migration checkpoints to restore");
    return;
  }
  
  try {
    const blockCheckpointExists = await checkTableExists(db, "block_checkpoint");
    if (!blockCheckpointExists) {
      console.log("   block_checkpoint table does not exist yet, skipping restore");
      return;
    }
    
    for (const checkpoint of migrationCheckpointsBackup) {
      try {
        await db("block_checkpoint")
          .insert(checkpoint)
          .onConflict("job_name")
          .merge();
        console.log(`   Restored checkpoint: ${checkpoint.job_name} = ${checkpoint.height}`);
      } catch (error: unknown) {
        const err = error as NodeJS.ErrnoException;
        console.warn(`    Failed to restore checkpoint ${checkpoint.job_name}: ${err.message}`);
      }
    }
    
    console.log(` Restored ${migrationCheckpointsBackup.length} migration checkpoints\n`);
  } catch (error: unknown) {
    const err = error as NodeJS.ErrnoException;
    console.warn(`    Error restoring migration checkpoints: ${err.message}`);
  }
}

async function recreateTransactionTables(db: Knex): Promise<void> {
  const transactionExists = await checkTableExists(db, "transaction");
  const transactionMessageExists = await checkTableExists(db, "transaction_message");
  
  if (!transactionExists) {
    console.log("  Creating base transaction table...");
    try {
      await db.schema.createTable("transaction", (table: Knex.TableBuilder) => {
        table.increments("id").primary();
        table.integer("height").index().notNullable();
        table.string("hash").unique().notNullable();
        table.string("codespace").notNullable();
        table.integer("code").notNullable();
        table.bigInteger("gas_used").notNullable();
        table.bigInteger("gas_wanted").notNullable();
        table.bigInteger("gas_limit").notNullable();
        table.jsonb("fee").notNullable();
        table.timestamp("timestamp").notNullable();
        table.jsonb("data");
        table.text("memo");
        table.integer("index");
        table.foreign("height").references("block.height");
      });
      console.log("     Created transaction table");
    } catch (err: unknown) {
      const error = err as NodeJS.ErrnoException;
      if (error.message?.includes("already exists")) {
        console.log("     Transaction table already exists");
      } else {
        throw err;
      }
    }
  }
  
  if (!transactionMessageExists) {
    console.log("  Creating base transaction_message table...");
    try {
      await db.schema.createTable("transaction_message", (table: Knex.TableBuilder) => {
        table.increments("id").primary();
        table.integer("tx_id").index().notNullable();
        table.integer("index").notNullable();
        table.string("type").index().notNullable();
        table.string("sender").index().notNullable();
        table.jsonb("content").notNullable();
        table.integer("parent_id").index();
        table.foreign("tx_id").references("transaction.id");
      });
      console.log("     Created transaction_message table");
    } catch (err: unknown) {
      const error = err as NodeJS.ErrnoException;
      if (error.message?.includes("already exists")) {
        console.log("     Transaction_message table already exists");
      } else {
        throw err;
      }
    }
  }
}

interface Migration {
  name: string;
  [key: string]: unknown;
}

async function skipInitMigrationIfPending(db: Knex, pendingMigrations: Migration[]): Promise<Migration[]> {
  const initMigrationName = "20230301021633_init_horoscope_layer_1_model";
  const blockExists = await checkTableExists(db, "block");
  
  if (blockExists) {
    return pendingMigrations.filter((m: Migration) => {
      if (!m || !m.name || typeof m.name !== 'string') {
        return true;
      }
      return !m.name.includes(initMigrationName);
    });
  }
  
  return pendingMigrations;
}

async function runMigrations(db: Knex): Promise<void> {
  console.log("Step 4: Running migrations to recreate tables...");
  
  try {
    const moduleOnlyMigrationNames = [
      "20230317040849_create_account_table",
      "20230317041447_create_account_vesting_table",
      "20230704102018_create_table_daily_stats_account_stats",
      "20231226102519_add_balances_to_account",
      "20250915120000_create_module_params",
      "20250919_create_credential_schema",
      "20250905_create_trust_registry_tables",
      "20240924_create_permissions_table",
      "0123_create_trust_deposit_tables",
      "20241201000000_create_did_records_table",
      "20250919_create_credential_schema_history",
      "20250922_create_trust_registry_history",
      "20251125000000_create_permission_history",
      "20251125000001_create_permission_session_history",
      "20251125000002_create_trust_deposit_history",
      "20251125000003_create_module_params_history",
      "123456765_Create_did_histry"
    ];
    
    const transactionPartitionMigrationNames = [
      "partition-transaction-table",
      "transaction_message_partition"
    ];
    
    const [completed] = await db.migrate.list();
    
    const missingTables = [];
    for (const tableName of TABLES_TO_DROP) {
      const exists = await checkTableExists(db, tableName);
      if (!exists) {
        missingTables.push(tableName);
      }
    }
    
    if (missingTables.length > 0) {
      console.log(`  Found ${missingTables.length} missing tables, will run migrations to recreate them...`);
      
      const needsTransactionTables = !await checkTableExists(db, "transaction") || !await checkTableExists(db, "transaction_message");
      
      if (needsTransactionTables) {
        console.log("  Transaction tables are missing, will recreate base tables first...");
        await recreateTransactionTables(db);
      }
      
      const transactionPartitionMigrations = completed.filter((m: Migration) => 
        m && m.name && typeof m.name === 'string' && transactionPartitionMigrationNames.some(name => m.name.includes(name))
      );
      
      if (transactionPartitionMigrations.length > 0) {
        console.log(`  Removing ${transactionPartitionMigrations.length} transaction partition migration records to re-run...`);
        for (const migration of transactionPartitionMigrations) {
          await db("knex_migrations")
            .where("name", migration.name)
            .delete();
          console.log(`     Removed: ${migration.name}`);
        }
      }
      
      const moduleMigrations = completed.filter((m: Migration) => 
        m && m.name && moduleOnlyMigrationNames.some(name => m.name.includes(name))
      );
      
      if (moduleMigrations.length > 0) {
        console.log(`  Removing ${moduleMigrations.length} module migration records to force re-run...`);
        for (const migration of moduleMigrations) {
          const isInit = migration.name && migration.name.includes("init_horoscope_layer_1_model");
          if (!isInit) {
            await db("knex_migrations")
              .where("name", migration.name)
              .delete();
            console.log(`     Removed: ${migration.name}`);
          } else {
            console.log(`     Skipping init migration: ${migration.name}`);
          }
        }
      }
    }
    
    try {
      console.log(`  Running migrations using Knex migrate.latest()...`);
      await db.migrate.latest();
      console.log("   Migrations completed successfully");
    } catch (migrateError: unknown) {
      const err = migrateError as Error;
      if (err.message?.includes("already exists") && err.message.includes("block")) {
        console.log("    Init migration tried to create block table (skipping)");
        console.log("  Continuing with remaining migrations...");
        try {
          await db.migrate.latest();
          console.log("   Migrations completed successfully");
        } catch (retryError: unknown) {
          const retryErr = retryError as Error;
          console.error("  ❌ Migration retry failed:", retryErr.message);
          throw retryError;
        }
      } else if (err.message?.includes("corrupt") || err.message?.includes("missing")) {
        console.log("    Migration validation error detected");
        console.log("  This usually happens after reindexing. Trying to continue...");
        try {
          await db.migrate.latest();
          console.log("   Migrations completed successfully");
        } catch (retryError: unknown) {
          const retryErr = retryError as Error;
          console.error("  ❌ Migration retry failed:", retryErr.message);
          throw retryError;
        }
      } else {
        throw migrateError;
      }
    }
    
    console.log(" Tables recreated successfully\n");
  } catch (error: unknown) {
    const err = error as NodeJS.ErrnoException;
    if (err.message?.includes("already exists") && err.message.includes("block")) {
      console.log("    Migration tried to recreate block table (skipping)");
    } else if (err.message?.includes("corrupt") || err.message?.includes("missing")) {
      console.log("    Migration validation error, checking if init migration needs to be skipped...");
      const [completedCheck, pendingCheck] = await db.migrate.list().catch(() => [[], []]);
      const migrationsToRun = await skipInitMigrationIfPending(db, pendingCheck);
      if (migrationsToRun.length > 0) {
        console.log(`  Found ${migrationsToRun.length} migrations to run (excluding init)`);
        for (const migration of migrationsToRun) {
          try {
            const migrationPath = `../migrations/${migration.name}.ts`;
            const migrationFile = await import(migrationPath);
            if (migrationFile.up) {
              await migrationFile.up(db);
              const maxBatch = await db("knex_migrations").max("batch as max").first();
              const nextBatch = (maxBatch && (maxBatch as { max: number | null }).max) 
                ? ((maxBatch as { max: number }).max + 1) 
                : 1;
              await db("knex_migrations").insert({
                name: migration.name,
                batch: nextBatch,
                migration_time: new Date()
              });
              console.log(`     Applied: ${migration.name}`);
            }
          } catch (err: unknown) {
            const error = err as NodeJS.ErrnoException;
            if (!error.message?.includes("already exists") || !error.message.includes("block")) {
              console.error(`    ❌ Failed to apply ${migration.name}: ${error.message}`);
              throw err;
            }
          }
        }
      }
    } else {
      console.error(`  ❌ Migration failed: ${err.message}`);
      throw error;
    }
  }
  
  console.log(" Tables recreated successfully\n");
}

async function resetSequences(db: Knex): Promise<void> {
  console.log("Step 5: Resetting ID sequences...");
  
  for (const sequenceName of SEQUENCES_TO_RESET) {
    try {
      const exists = await db.raw(`
        SELECT EXISTS (
          SELECT FROM pg_sequences 
          WHERE sequencename = ?
        );
      `, [sequenceName]);
      
      if (exists.rows[0]?.exists) {
        await db.raw(`ALTER SEQUENCE ${sequenceName} RESTART WITH 1`);
        console.log(`   Reset ${sequenceName}`);
      } else {
        console.log(`   Sequence ${sequenceName} does not exist, skipping`);
      }
    } catch (error: unknown) {
      const err = error as NodeJS.ErrnoException;
      console.warn(`    Could not reset ${sequenceName}: ${err.message}`);
    }
  }
  
  console.log(" Sequences reset successfully\n");
}

async function handleGenesisFile(): Promise<void> {
  console.log("Step 6: Handling genesis.json file...");
  
  const genesisPath = path.resolve("genesis.json");
  const genesisBackupPath = path.resolve("genesis.json.backup");
  
  try {
    if (fs.existsSync(genesisPath)) {
      fs.copyFileSync(genesisPath, genesisBackupPath);
      console.log(`   Backed up genesis.json to genesis.json.backup`);
      
      fs.unlinkSync(genesisPath);
      console.log(`   Removed genesis.json (will be re-fetched on next start)`);
      console.log(`   To restore: cp genesis.json.backup genesis.json`);
    } else {
      console.log(`   genesis.json not found, skipping`);
    }
  } catch (error: unknown) {
    const err = error as NodeJS.ErrnoException;
    console.warn(`    Error handling genesis.json: ${err.message}`);
  }
  
  console.log(" Genesis file handled successfully\n");
}

async function verifyBlocksTable(db: Knex): Promise<void> {
  console.log("Verifying block table is preserved...");
  
  const blocksExists = await checkTableExists(db, "block");
  if (!blocksExists) {
    throw new Error("❌ CRITICAL: block table does not exist! This should never be dropped.");
  }
  
  const blockCount = await db("block").count("* as count").first();
  console.log(`   Block table exists with ${blockCount?.count || 0} rows`);
  console.log(" Block table verified\n");
}

(async function reindexModules(): Promise<void> {
  const environment = process.env.NODE_ENV || "production";
  process.env.NODE_ENV = environment;
  process.env.MIGRATION_MODE = "lightweight";

  let db: Knex | undefined;

  try {
    console.log(" Starting Module Reindexing Process");
    console.log(`Environment: ${environment}\n`);

    const config = getConfigForEnv();
    const connInfo = config.connection as { database?: string; host?: string; port?: number };
    console.log(`Database: ${connInfo.database || 'unknown'} @ ${connInfo.host || 'unknown'}:${connInfo.port || 'unknown'}\n`);

    console.log("Step 1: Connecting to database...");
    await waitForDatabase(config);
    console.log(" Database connection established\n");

    db = knex(config);

    await verifyBlocksTable(db);

    await dropTables(db);

    await clearCheckpoints(db);

    await runMigrations(db);

    await resetSequences(db);

    await handleGenesisFile();

    await restoreMigrationCheckpoints(db);

    console.log(" Reindexing preparation completed successfully!");
    console.log("\n Next steps:");
    console.log("  1. Start the indexer services");
    console.log("  2. Services will process existing blocks from the database");
    console.log("  3. Block crawler will only fetch new blocks (not existing ones)");
    console.log("  4. Monitor checkpoint progress");
    console.log("\n Optimization tips:");
    console.log("  - During reindexing, services process existing blocks faster");
    console.log("  - Block crawler skips fetching blocks that already exist");
    console.log("  - After reindexing completes, normal block crawling resumes");
    console.log("\nTo start indexer:");
    console.log("  Development: pnpm run dev");
    console.log("  Production:  pnpm run start");

  } catch (error: unknown) {
    const err = error as Error;
    console.error("\n❌ Reindexing failed:", err.message);
    if (err.stack) {
      console.error(err.stack);
    }
    process.exitCode = 1;
  } finally {
    if (db) {
      await db.destroy().catch(() => undefined);
    }
    process.exit(process.exitCode || 0);
  }
})();
