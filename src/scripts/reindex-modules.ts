/* eslint-disable no-console */
import * as dotenv from "dotenv";
import knex, { Knex } from "knex";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath, pathToFileURL } from "url";
import { loadEnvFiles } from "../common/utils/loadEnv";
import { getConfigForEnv } from "../knexfile";

loadEnvFiles();
dotenv.config();

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));

const TABLES_TO_DROP = [
  "transaction_message",
  "transaction",
  "global_metrics",
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
  "global_metrics_id_seq",
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

async function importMigrationByName(migrationName: string): Promise<any | null> {
  const candidates = [
    path.join(SCRIPT_DIR, "..", "migrations", `${migrationName}.js`),
    path.join(SCRIPT_DIR, "..", "migrations", `${migrationName}.ts`),
    path.join(process.cwd(), "dist", "src", "migrations", `${migrationName}.js`),
    path.join(process.cwd(), "src", "migrations", `${migrationName}.ts`)
  ];

  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) {
      return import(pathToFileURL(candidate).href);
    }
  }

  return null;
}

async function removeErrorsLog(): Promise<void> {
  const logPath = path.join(process.cwd(), "logs", "errors.log");
  try {
    await fs.promises.unlink(logPath);
    console.log(" Removed logs/errors.log");
  } catch (err: unknown) {
    const error = err as NodeJS.ErrnoException;
    if (error?.code === "ENOENT") {
      console.log(" logs/errors.log not found; nothing to remove");
    } else {
      console.warn(` Could not remove logs/errors.log: ${error?.message || String(error)}`);
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
  console.log(" Step 3: Resetting all checkpoints to 0 and setting block checkpoint to highest block...");
  
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
        .update({ height: 0 });
      console.log(`   Reset ${genesisJobNames.length} genesis job checkpoints to 0`);
      
      const updated = await db("block_checkpoint")
        .whereNotIn("job_name", [...migrationJobNames, ...genesisJobNames, "crawl:block"])
        .update({ height: 0 });
      console.log(`   Reset ${updated} module checkpoints to 0`);
      
      if (highestBlock > 0) {
        const crawlBlockUpdated = await db("block_checkpoint")
          .where("job_name", "crawl:block")
          .update({ height: highestBlock });
        if (crawlBlockUpdated === 0) {
          await db("block_checkpoint").insert({
            job_name: "crawl:block",
            height: highestBlock
          });
        }
        console.log(`   Set crawl:block checkpoint to ${highestBlock} (highest block in database)`);
        console.log(`   Block crawler will skip fetching blocks 0-${highestBlock} and only fetch new blocks`);
      } else {
        const crawlBlockUpdated = await db("block_checkpoint")
          .where("job_name", "crawl:block")
          .update({ height: 0 });
        if (crawlBlockUpdated === 0) {
          await db("block_checkpoint").insert({
            job_name: "crawl:block",
            height: 0
          });
        }
        console.log(`   Set crawl:block checkpoint to 0`);
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
      console.warn(`    Error resetting block_checkpoint: ${err.message}`);
    }

  try {
    const checkpointExists = await checkTableExists(db, "checkpoint");
    if (checkpointExists) {
      const updated = await db("checkpoint").update({ data: null });
      console.log(`   Reset ${updated} rows in checkpoint table`);
    } else {
      console.log("   checkpoint table does not exist (will be created by migrations)");
    }
  } catch (error: unknown) {
    const err = error as NodeJS.ErrnoException;
    console.warn(`    Error resetting checkpoint: ${err.message}`);
  }
  
  console.log(" All checkpoints reset to 0 (except crawl:block and migration checkpoints)\n");
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

const MIGRATION_TO_TABLES: Record<string, string[]> = {
  "20230317040849_create_account_table": ["account"],
  "20230317041447_create_account_vesting_table": ["account_vesting"],
  "20230704102018_create_table_daily_stats_account_stats": ["daily_statistics", "account_statistics"],
  "20231226102519_add_balances_to_account": ["account_balance"],
  "20250915120000_create_module_params": ["module_params"],
  "20250919_create_credential_schema": ["credential_schemas"],
  "20250905_create_trust_registry_tables": ["trust_registry", "governance_framework_version", "governance_framework_document"],
  "20240924_create_permissions_table": ["permissions"],
  "0123_create_trust_deposit_tables": ["trust_deposits"],
  "20241201000000_create_did_records_table": ["dids"],
  "20250919_create_credential_schema_history": ["credential_schema_history"],
  "20250922_create_trust_registry_history": ["trust_registry_history", "governance_framework_version_history", "governance_framework_document_history"],
  "20251125000000_create_permission_history": ["permission_history"],
  "20251125000001_create_permission_session_history": ["permission_session_history"],
  "20251125000002_create_trust_deposit_history": ["trust_deposit_history"],
  "20251125000003_create_module_params_history": ["module_params_history"],
  "20260126000001_create_did_history": ["did_history"],
  "20260202020000_create_global_metrics": ["global_metrics"],
  "partition-transaction-table": ["transaction"],
  "transaction_message_partition": ["transaction_message"]
};

const ALTER_MIGRATIONS = [
  "20251124120000_add_height_to_credential_schema_history",
  "20251125113000_add_height_indexes_to_history_tables",
  "20251210000000_add_permission_statistics",
  "20250115000000_add_permission_new_attributes",
  "20260126000000_add_trust_registry_statistics",
  "20260126000002_add_credential_schema_statistics",
  "20260130000004_alter_gfv_combined",
  "20260202000000_add_title_description_to_credential_schema",
  "20260203000000_add_indexes_permissions_history"
];

async function runMigrations(db: Knex): Promise<void> {
  console.log("Step 4: Running migrations to recreate tables...");
  
  try {
    console.log("  Checking which tables exist...");
    const missingTables: string[] = [];
    const existingTables: string[] = [];
    
    for (const tableName of TABLES_TO_DROP) {
      const exists = await checkTableExists(db, tableName);
      if (!exists) {
        missingTables.push(tableName);
      } else {
        existingTables.push(tableName);
      }
    }
    
    console.log(`  ✓ Found ${existingTables.length} existing tables`);
    console.log(`  ✗ Found ${missingTables.length} missing tables: ${missingTables.join(", ")}`);
    
    if (missingTables.length === 0) {
      console.log("  All tables exist, no migrations needed!");
      return;
    }
    
    const neededMigrations = new Set<string>();
    
    for (const [migrationName, tables] of Object.entries(MIGRATION_TO_TABLES)) {
      const hasMissingTable = tables.some(table => missingTables.includes(table));
      if (hasMissingTable) {
        neededMigrations.add(migrationName);
        console.log(`  → Migration "${migrationName}" needed (creates: ${tables.filter(t => missingTables.includes(t)).join(", ")})`);
      }
    }
    
    for (const alterMigration of ALTER_MIGRATIONS) {
      neededMigrations.add(alterMigration);
    }
    
    console.log(`  Will run ${neededMigrations.size} migration(s) to create missing tables...`);
    
    const [completed] = await db.migrate.list();
    
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
      "20260126000001_create_did_history",
      "20260202020000_create_global_metrics",
      "20251124120000_add_height_to_credential_schema_history",
      "20251125113000_add_height_indexes_to_history_tables",
      "20251210000000_add_permission_statistics",
      "20250115000000_add_permission_new_attributes",
      "20260126000000_add_trust_registry_statistics",
      "20260126000002_add_credential_schema_statistics"
    ];
    
    const transactionPartitionMigrationNames = [
      "partition-transaction-table",
      "transaction_message_partition"
    ];
    
    const needsTransactionTables = missingTables.includes("transaction") || missingTables.includes("transaction_message");
    
    if (needsTransactionTables) {
      console.log("  Transaction tables are missing, will recreate base tables first...");
      await recreateTransactionTables(db);
    }
    
    console.log(`  Clearing migration records for needed migrations...`);
    const allMigrationRecords = await db("knex_migrations").select("name");
    const allMigrationNames = new Set(allMigrationRecords.map((m: any) => m.name));
    const transactionPartitionMigrationsToClear: string[] = [];
    for (const migName of transactionPartitionMigrationNames) {
      for (const recordedName of allMigrationNames) {
        if (recordedName.includes(migName) && neededMigrations.has(migName)) {
          transactionPartitionMigrationsToClear.push(recordedName);
        }
      }
    }
    
    if (transactionPartitionMigrationsToClear.length > 0) {
      console.log(`  Removing ${transactionPartitionMigrationsToClear.length} transaction partition migration record(s)...`);
      for (const migName of transactionPartitionMigrationsToClear) {
        try {
          const deleted = await db("knex_migrations")
            .where("name", migName)
            .delete();
          if (deleted > 0) {
            console.log(`     Removed: ${migName}`);
          }
        } catch (err: unknown) {
          const error = err as NodeJS.ErrnoException;
          console.warn(`     Could not remove ${migName}: ${error.message}`);
        }
      }
    }
    
    const moduleMigrationsToClear: string[] = [];
    const allModuleMigrationNames = [...moduleOnlyMigrationNames, ...ALTER_MIGRATIONS];
    
    for (const migName of allModuleMigrationNames) {
      if (neededMigrations.has(migName)) {
        for (const recordedName of allMigrationNames) {
          if (recordedName.includes(migName)) {
            const isInit = recordedName.includes("init_horoscope_layer_1_model");
            if (!isInit) {
              moduleMigrationsToClear.push(recordedName);
            }
          }
        }
      }
    }
    
    if (moduleMigrationsToClear.length > 0) {
      console.log(`  Removing ${moduleMigrationsToClear.length} module migration record(s) to force re-run...`);
      for (const migName of moduleMigrationsToClear) {
        try {
          const deleted = await db("knex_migrations")
            .where("name", migName)
            .delete();
          if (deleted > 0) {
            console.log(`     Removed ${deleted} record(s): ${migName}`);
          }
        } catch (err: unknown) {
          const error = err as NodeJS.ErrnoException;
          console.warn(`     Could not remove ${migName}: ${error.message}`);
        }
      }
    } else {
      console.log(`  No module migrations found in database to clear`);
    }
    console.log(`  Checking for ALTER migrations that need to be cleared...`);
    for (const alterMig of ALTER_MIGRATIONS) {
      for (const recordedName of allMigrationNames) {
        if (recordedName.includes(alterMig)) {
          let shouldClear = false;
          if (recordedName.includes("gfd_id") || recordedName.includes("document_history")) {
            const tableExists = await checkTableExists(db, "governance_framework_document_history");
            if (!tableExists) {
              shouldClear = true;
            }
          } else if (recordedName.includes("permission") && !recordedName.includes("history")) {
            const tableExists = await checkTableExists(db, "permissions");
            if (!tableExists) {
              shouldClear = true;
            }
          } else if (recordedName.includes("trust_registry") && !recordedName.includes("history")) {
            const tableExists = await checkTableExists(db, "trust_registry");
            if (!tableExists) {
              shouldClear = true;
            }
          } else if (recordedName.includes("credential_schema") && !recordedName.includes("history")) {
            const tableExists = await checkTableExists(db, "credential_schemas");
            if (!tableExists) {
              shouldClear = true;
            }
          }
          
          if (shouldClear) {
            try {
              const deleted = await db("knex_migrations")
                .where("name", recordedName)
                .delete();
              if (deleted > 0) {
                console.log(`     Cleared ALTER migration (table doesn't exist): ${recordedName}`);
              }
            } catch (e) {
              // Ignore
            }
          }
        }
      }
    }
    
    // Clean up any remaining duplicates before running migrations
    try {
      console.log(`  Cleaning up duplicate migration entries...`);
      const duplicates = await db.raw(`
        DELETE FROM knex_migrations 
        WHERE id NOT IN (
          SELECT DISTINCT ON (name) id 
          FROM knex_migrations 
          ORDER BY name, migration_time DESC, id DESC
        )
      `);
      if (duplicates.rowCount > 0) {
        console.log(`     Removed ${duplicates.rowCount} duplicate migration record(s)`);
      }
    } catch (err: unknown) {
      const error = err as NodeJS.ErrnoException;
      console.warn(`     Could not clean duplicates: ${error.message}`);
    }
    
    console.log(`  Running migrations using Knex migrate.latest()...`);
    try {
      console.log("  Ensuring base transaction tables exist before running migrations...");
      await recreateTransactionTables(db);
      console.log("  Base transaction tables verified/created (if needed).");
      
      const [, pendingBefore] = await db.migrate.list();
      if (pendingBefore && pendingBefore.length > 0) {
        console.log(`   Found ${pendingBefore.length} pending migration(s) to run`);
        for (const mig of pendingBefore) {
          let migName: string;
          if (typeof mig === 'string') {
            migName = mig;
          } else if (mig && typeof mig === 'object') {
            migName = (mig as any).name || (mig as any).file || JSON.stringify(mig);
          } else {
            migName = String(mig);
          }
          console.log(`     - ${migName}`);
        }
      } else {
        console.log(`   No pending migrations found in Knex list, but tables are missing.`);
        console.log(`   This may indicate migration records exist but tables don't.`);
        console.log(`   Will attempt to run migrations anyway...`);
      }
      await db.migrate.latest();
      console.log("   Migrations completed successfully");
      const tablesAfterMigration = [];
      for (const tableName of TABLES_TO_DROP) {
        const exists = await checkTableExists(db, tableName);
        if (!exists) {
          tablesAfterMigration.push(tableName);
        }
      }
      
      if (tablesAfterMigration.length > 0) {
        console.warn(`   Warning: ${tablesAfterMigration.length} tables still missing after migrate.latest(): ${tablesAfterMigration.join(", ")}`);
        console.warn(`   This indicates migrations may not have run properly. Will try manual migration...`);
        throw new Error(`Tables still missing after migrations: ${tablesAfterMigration.join(", ")}`);
      }
      
      const hasParticipantsColumn = await db.schema.hasColumn("permissions", "participants");
      if (hasParticipantsColumn) {
        console.log("   ✓ New permission attributes (participants, slash stats) verified in permissions table");
      } else {
        console.warn("   ⚠ Warning: participants column not found in permissions table after migrations");
      }

      const hasTrustRegistryParticipantsColumn = await db.schema.hasColumn("trust_registry", "participants");
      if (hasTrustRegistryParticipantsColumn) {
        console.log("   ✓ New trust registry statistics attributes (participants, active_schemas, weight, etc.) verified in trust_registry table");
      } else {
        console.warn("   ⚠ Warning: participants column not found in trust_registry table after migrations");
      }
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
          console.error("  Migration retry failed:", retryErr.message);
          throw retryError;
        }
      } else if (err.message?.includes("corrupt") || err.message?.includes("missing") || err.message?.includes("Tables still missing")) {
        console.log("    Migration validation error or missing tables detected");
        console.log("  This usually happens after reindexing. Will force re-run migrations...");
        console.log("  Clearing migration records for module tables to force re-creation...");
        const moduleMigrationsToClear = completed.filter((m: Migration) => {
          if (!m || !m.name || typeof m.name !== "string") {
            return false;
          }
          const isModuleMigration = moduleOnlyMigrationNames.some(name => m.name.includes(name));
          if (!isModuleMigration) {
            return false;
          }
          const matchedEntry = Object.entries(MIGRATION_TO_TABLES)
            .find(([migrationName]) => m.name.includes(migrationName));
          if (!matchedEntry) {
            return true;
          }
          const [, tables] = matchedEntry;
          return tables.some(table => missingTables.includes(table));
        });
        
        for (const migration of moduleMigrationsToClear) {
          try {
            const deleted = await db("knex_migrations")
              .where("name", migration.name)
              .delete();
            if (deleted > 0) {
              console.log(`     Cleared migration record: ${migration.name}`);
            }
          } catch (clearErr: unknown) {
            const clearError = clearErr as NodeJS.ErrnoException;
            console.warn(`     Could not clear ${migration.name}: ${clearError.message}`);
          }
        }
        
        try {
          await recreateTransactionTables(db);
          await db.migrate.latest();
          console.log("   Migrations completed successfully after clearing records");
          const tablesAfterRetry = [];
          for (const tableName of TABLES_TO_DROP) {
            const exists = await checkTableExists(db, tableName);
            if (!exists) {
              tablesAfterRetry.push(tableName);
            }
          }
          
          if (tablesAfterRetry.length > 0) {
            console.error(`   ❌ CRITICAL: ${tablesAfterRetry.length} tables still missing: ${tablesAfterRetry.join(", ")}`);
            console.error("   This is a critical error - migrations did not create required tables!");
            throw new Error(`Critical: Tables still missing after migration retry: ${tablesAfterRetry.join(", ")}`);
          }
        } catch (retryError: unknown) {
          const retryErr = retryError as Error;
          console.error("  Migration retry failed:", retryErr.message);
          throw retryError;
        }
      } else {
        console.error("  Migration error:", err.message);
        throw migrateError;
      }
    }
    
    const finalMissingTables = [];
    for (const tableName of TABLES_TO_DROP) {
      const exists = await checkTableExists(db, tableName);
      if (!exists) {
        finalMissingTables.push(tableName);
      }
    }
    
    if (finalMissingTables.length > 0) {
      console.error(`  ❌ CRITICAL ERROR: ${finalMissingTables.length} tables still missing after migrations: ${finalMissingTables.join(", ")}`);
      console.error("  This indicates migrations did not run properly or migration files are missing!");
      console.error("  The indexer will fail to start without these tables.");
      console.error("  Attempting to force run migrations one more time...");
      try {
        await db.migrate.latest();
        console.log("  Retry migrations completed");
        const stillMissing: string[] = [];
        for (const tableName of finalMissingTables) {
          const exists = await checkTableExists(db, tableName);
          if (!exists) {
            stillMissing.push(tableName);
          }
        }
        
        if (stillMissing.length > 0) {
          console.error(`  ❌ Still missing ${stillMissing.length} table(s): ${stillMissing.join(", ")}`);
          console.error("  Please check migration files and try running: pnpm run migrate:dev");
          throw new Error(`Critical: ${stillMissing.length} tables still missing after migration retry: ${stillMissing.join(", ")}`);
        } else {
          console.log("  ✓ All tables verified after retry");
        }
      } catch (retryError: unknown) {
        const retryErr = retryError as Error;
        console.error(`  ❌ Migration retry failed: ${retryErr.message}`);
        throw new Error(`Critical: ${finalMissingTables.length} tables still missing after migrations: ${finalMissingTables.join(", ")}`);
      }
    } else {
      console.log("  ✓ All tables verified and recreated successfully");
    }
    
    const permissionsTableExists = await checkTableExists(db, "permissions");
    if (permissionsTableExists) {
      const hasParticipants = await db.schema.hasColumn("permissions", "participants");
      const hasEcosystemSlashEvents = await db.schema.hasColumn("permissions", "ecosystem_slash_events");
      const hasNetworkSlashEvents = await db.schema.hasColumn("permissions", "network_slash_events");
      
      if (hasParticipants && hasEcosystemSlashEvents && hasNetworkSlashEvents) {
        console.log("  ✓ Permission table has all new attributes (participants, ecosystem_slash_events, network_slash_events, etc.)");
      } else {
        console.warn("  ⚠ Warning: Permission table is missing some new attributes:");
        if (!hasParticipants) console.warn("     - Missing: participants");
        if (!hasEcosystemSlashEvents) console.warn("     - Missing: ecosystem_slash_events");
        if (!hasNetworkSlashEvents) console.warn("     - Missing: network_slash_events");
        console.warn("  You may need to manually run: npm run migrate:dev");
      }
    }

    const trustRegistryTableExists = await checkTableExists(db, "trust_registry");
    if (trustRegistryTableExists) {
      const hasParticipants = await db.schema.hasColumn("trust_registry", "participants");
      const hasActiveSchemas = await db.schema.hasColumn("trust_registry", "active_schemas");
      const hasWeight = await db.schema.hasColumn("trust_registry", "weight");
      const hasEcosystemSlashEvents = await db.schema.hasColumn("trust_registry", "ecosystem_slash_events");
      const hasNetworkSlashEvents = await db.schema.hasColumn("trust_registry", "network_slash_events");
      
      if (hasParticipants && hasActiveSchemas && hasWeight && hasEcosystemSlashEvents && hasNetworkSlashEvents) {
        console.log("  ✓ Trust registry table has all new statistics attributes (participants, active_schemas, archived_schemas, weight, issued, verified, slash stats, etc.)");
      } else {
        console.warn("  ⚠ Warning: Trust registry table is missing some new statistics attributes:");
        if (!hasParticipants) console.warn("     - Missing: participants");
        if (!hasActiveSchemas) console.warn("     - Missing: active_schemas");
        if (!hasWeight) console.warn("     - Missing: weight");
        if (!hasEcosystemSlashEvents) console.warn("     - Missing: ecosystem_slash_events");
        if (!hasNetworkSlashEvents) console.warn("     - Missing: network_slash_events");
        console.warn("  You may need to manually run: npm run migrate:dev");
      }
    }

    const credentialSchemaTableExists = await checkTableExists(db, "credential_schemas");
    if (credentialSchemaTableExists) {
      const hasParticipants = await db.schema.hasColumn("credential_schemas", "participants");
      const hasWeight = await db.schema.hasColumn("credential_schemas", "weight");
      const hasEcosystemSlashEvents = await db.schema.hasColumn("credential_schemas", "ecosystem_slash_events");
      const hasNetworkSlashEvents = await db.schema.hasColumn("credential_schemas", "network_slash_events");
      
      if (hasParticipants && hasWeight && hasEcosystemSlashEvents && hasNetworkSlashEvents) {
        console.log("  ✓ Credential schema table has all new statistics attributes (participants, weight, issued, verified, slash stats, etc.)");
      } else {
        console.warn("  ⚠ Warning: Credential schema table is missing some new statistics attributes:");
        if (!hasParticipants) console.warn("     - Missing: participants");
        if (!hasWeight) console.warn("     - Missing: weight");
        if (!hasEcosystemSlashEvents) console.warn("     - Missing: ecosystem_slash_events");
        if (!hasNetworkSlashEvents) console.warn("     - Missing: network_slash_events");
        console.warn("  You may need to manually run: npm run migrate:dev");
      }
    }
    
    console.log(" Tables recreated successfully\n");
  } catch (error: unknown) {
    const err = error as NodeJS.ErrnoException;
    if (err.message?.includes("already exists") && err.message.includes("block")) {
      console.log("    Migration tried to recreate block table (skipping)");
    } else if (err.message?.includes("corrupt") || err.message?.includes("missing")) {
      console.log("    Migration validation error, checking if init migration needs to be skipped...");
      const [, pendingCheck] = await db.migrate.list().catch(() => [[], []]);
      const migrationsToRun = await skipInitMigrationIfPending(db, pendingCheck);
      if (migrationsToRun.length > 0) {
        console.log(`  Found ${migrationsToRun.length} migrations to run (excluding init)`);
        for (const migration of migrationsToRun) {
          try {
            const existing = await db("knex_migrations")
              .where("name", migration.name)
              .first();
            
            if (existing) {
              console.log(`     Skipping ${migration.name} - already recorded`);
              continue;
            }

            const migrationFile = await importMigrationByName(migration.name);
            if (!migrationFile) {
              console.warn(`     Migration file not found for ${migration.name}, skipping`);
              continue;
            }
            if (migrationFile.up) {
              await migrationFile.up(db);
              const maxBatch = await db("knex_migrations").max("batch as max").first();
              const nextBatch = (maxBatch && (maxBatch as { max: number | null }).max) 
                ? ((maxBatch as { max: number }).max + 1) 
                : 1;
              
              const stillExists = await db("knex_migrations")
                .where("name", migration.name)
                .first();
              
              if (!stillExists) {
                await db("knex_migrations").insert({
                  name: migration.name,
                  batch: nextBatch,
                  migration_time: new Date()
                });
                console.log(`     Applied: ${migration.name}`);
              } else {
                console.log(`     Skipping ${migration.name} - was added concurrently`);
              }
            }
          } catch (err: unknown) {
            const error = err as NodeJS.ErrnoException;
            if (error.message?.includes("duplicate key") || error.message?.includes("unique constraint")) {
              console.log(`     Skipping ${migration.name} - duplicate entry detected`);
              continue;
            }
            if (!error.message?.includes("already exists") || !error.message.includes("block")) {
              console.error(`    Failed to apply ${migration.name}: ${error.message}`);
              throw err;
            }
          }
        }
      }
    } else {
      console.error(`  Migration failed: ${err.message}`);
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
    throw new Error("CRITICAL: block table does not exist! This should never be dropped.");
  }
  
  const blockCount = await db("block").count("* as count").first();
  console.log(`   Block table exists with ${blockCount?.count || 0} rows`);
  console.log(" Block table verified\n");
}

(async function reindexModules(): Promise<void> {
  const environment = process.env.NODE_ENV || "production";
  process.env.NODE_ENV = environment;
  process.env.MIGRATION_MODE = "lightweight";

  if (environment === "test") {
    console.error("ERROR: Reindex script cannot run in test mode.");
    console.error("Reindexing is only for development and production environments.");
    console.error("Test environment should only run migrations, not reindex operations.");
    process.exit(1);
  }

  let db: Knex | undefined;

  function logMemoryUsage(stage: string) {
    if (global.gc) {
      global.gc();
    }
    const memUsage = process.memoryUsage();
    const formatMB = (bytes: number) => (bytes / 1024 / 1024).toFixed(2);
    console.log(`\n[Memory ${stage}]`);
    console.log(`  Heap Used: ${formatMB(memUsage.heapUsed)} MB`);
    console.log(`  Heap Total: ${formatMB(memUsage.heapTotal)} MB`);
    console.log(`  External: ${formatMB(memUsage.external)} MB`);
    console.log(`  RSS: ${formatMB(memUsage.rss)} MB\n`);
  }

  const checkpointFile = path.join(process.cwd(), '.reindex-checkpoint.json');
  let checkpointData: { completedSteps: string[]; lastCompletedStep?: string; attemptCount?: number; timestamp?: string } | null = null;
  
  try {
    if (process.env.REINDEX_CHECKPOINT) {
      checkpointData = JSON.parse(process.env.REINDEX_CHECKPOINT);
    } else if (fs.existsSync(checkpointFile)) {
      const content = fs.readFileSync(checkpointFile, 'utf-8');
      checkpointData = JSON.parse(content);
    }
  } catch (error) {
    console.warn(`⚠️  Could not load checkpoint: ${error}`);
  }
  
  if (!checkpointData) {
    checkpointData = { completedSteps: [], attemptCount: 0 };
  }
  
  const completedSteps: string[] = checkpointData.completedSteps || [];

  function markStepComplete(stepName: string): void {
    if (!completedSteps.includes(stepName)) {
      completedSteps.push(stepName);
      checkpointData!.completedSteps = completedSteps;
      checkpointData!.lastCompletedStep = stepName;
      checkpointData!.timestamp = new Date().toISOString();
      process.env.REINDEX_CHECKPOINT = JSON.stringify(checkpointData);
      
      try {
        fs.writeFileSync(checkpointFile, JSON.stringify(checkpointData, null, 2));
      } catch (error) {
        console.warn(`⚠️  Could not save checkpoint to file: ${error}`);
      }
    }
  }

  function isStepComplete(stepName: string): boolean {
    return completedSteps.includes(stepName);
  }

  try {
    logMemoryUsage("Start");
    console.log(" Starting Module Reindexing Process");
    console.log(`Environment: ${environment}\n`);
    
    if (checkpointData && completedSteps.length > 0) {
      console.log(`📌 Resuming from checkpoint. Completed steps: ${completedSteps.join(', ')}\n`);
    }

    if (!isStepComplete("remove-errors-log")) {
      console.log("Step 0: Removing errors.log...");
      await removeErrorsLog();
      markStepComplete("remove-errors-log");
      console.log("");
    } else {
      console.log("Step 0: Removing errors.log... [SKIPPED - already completed]\n");
    }

    const config = getConfigForEnv();
    const connInfo = config.connection as { database?: string; host?: string; port?: number };
    console.log(`Database: ${connInfo.database || 'unknown'} @ ${connInfo.host || 'unknown'}:${connInfo.port || 'unknown'}\n`);

    if (!isStepComplete('connect')) {
      console.log("Step 1: Connecting to database...");
      await waitForDatabase(config);
      console.log(" Database connection established\n");
      markStepComplete('connect');
    } else {
      console.log("Step 1: Connecting to database... [SKIPPED - already completed]\n");
    }

    db = knex(config);

    if (!isStepComplete('verify-blocks')) {
      await verifyBlocksTable(db);
      markStepComplete('verify-blocks');
    } else {
      console.log("Step: Verify blocks table... [SKIPPED - already completed]\n");
    }

    if (!isStepComplete('drop-tables')) {
      await dropTables(db);
      markStepComplete('drop-tables');
    } else {
      console.log("Step: Drop tables... [SKIPPED - already completed]\n");
    }

    if (!isStepComplete('clear-checkpoints')) {
      await clearCheckpoints(db);
      markStepComplete('clear-checkpoints');
    } else {
      console.log("Step: Clear checkpoints... [SKIPPED - already completed]\n");
    }

    if (!isStepComplete('migrations')) {
      await runMigrations(db);
      markStepComplete('migrations');
    } else {
      console.log("Step: Run migrations... [SKIPPED - already completed]\n");
    }
    
    if (!isStepComplete('indexes')) {
      console.log("Step X: Applying index migration (if columns exist)...");
      try {
        const migrationFile = await importMigrationByName("20260203000000_add_indexes_permissions_history");
        if (migrationFile && typeof migrationFile.up === "function") {
          await migrationFile.up(db);
          console.log("  Index migration applied (or skipped for missing columns).");
        } else {
          console.log("  Index migration file not found or invalid; skipping.");
        }
        markStepComplete('indexes');
      } catch (err: any) {
        console.error("  Failed to apply index migration:", err?.message || err);
        throw err;
      }
    } else {
      console.log("Step X: Applying index migration... [SKIPPED - already completed]\n");
    }

    if (!isStepComplete('reset-sequences')) {
      await resetSequences(db);
      markStepComplete('reset-sequences');
    } else {
      console.log("Step: Reset sequences... [SKIPPED - already completed]\n");
    }

    if (!isStepComplete('genesis')) {
      await handleGenesisFile();
      markStepComplete('genesis');
    } else {
      console.log("Step: Handle genesis file... [SKIPPED - already completed]\n");
    }

    if (!isStepComplete('restore-checkpoints')) {
      await restoreMigrationCheckpoints(db);
      markStepComplete('restore-checkpoints');
    } else {
      console.log("Step: Restore migration checkpoints... [SKIPPED - already completed]\n");
    }

    logMemoryUsage("End");
    
    try {
      if (fs.existsSync(checkpointFile)) {
        fs.unlinkSync(checkpointFile);
        console.log(" ✅ Checkpoint file cleared\n");
      }
    } catch (error) {
      console.warn(` ⚠️  Could not clear checkpoint file: ${error}\n`);
    }
    
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
    logMemoryUsage("Error");
    console.error("\n❌ Reindexing failed:", err.message);
    if (err.stack) {
      console.error(err.stack);
    }
    
    if (err.message?.includes("heap") || err.message?.includes("memory") || err.message?.includes("Allocation failed")) {
      console.error("\n⚠️  Memory error detected. The process will be restarted automatically.");
      console.error("   If this persists, try:");
      console.error("   - Increasing NODE_OPTIONS='--max-old-space-size=12288'");
      console.error("   - Processing data in smaller batches");
    }
    
    process.exitCode = 1;
  } finally {
    if (db) {
      await db.destroy().catch(() => undefined);
    }
    process.exit(process.exitCode || 0);
  }
})();
