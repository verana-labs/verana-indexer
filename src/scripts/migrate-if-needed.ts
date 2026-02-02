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
    
    const criticalTables = [
      "account",
      "transaction",
      "trust_registry",
      "credential_schemas",
      "permissions",
      "dids"
    ];
    
    const missingCriticalTables: string[] = [];
    for (const tableName of criticalTables) {
      const exists = await db.schema.hasTable(tableName);
      if (!exists) {
        missingCriticalTables.push(tableName);
      }
    }
    
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
      
        const shouldRunMigrations = !blockExists || !migrationsTableExists || (pending && pending.length > 0) || missingCriticalTables.length > 0;
      
      if (!shouldRunMigrations) {
        console.log(`Database is up to date (env: ${environment}). No migrations required.`);
        console.log(`  Completed migrations: ${completed?.length || 0}`);
        console.log(`  Pending migrations: ${pending?.length || 0}`);
        if (isTestMode) {
          console.log(`  Test mode: Database ready for tests.`);
        }
      } else {
        if (missingCriticalTables.length > 0) {
          console.log(` Found ${missingCriticalTables.length} missing critical table(s) (env: ${environment}): ${missingCriticalTables.join(", ")}`);
          console.log(`  This usually happens after reindexing. Will run migrations to create missing tables...`);
        } else if (pending && pending.length > 0) {
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
        console.log("  Checking if critical tables exist...");
        
        const criticalTables = [
          "account",
          "transaction",
          "trust_registry",
          "credential_schemas",
          "permissions",
          "dids"
        ];
        
        const existingCriticalTables: string[] = [];
        const missingCriticalTables: string[] = [];
        
        for (const tableName of criticalTables) {
          const exists = await db.schema.hasTable(tableName);
          if (exists) {
            existingCriticalTables.push(tableName);
          } else {
            missingCriticalTables.push(tableName);
          }
        }
        
        if (missingCriticalTables.length > 0) {
          console.log(`  ❌ CRITICAL: ${missingCriticalTables.length} critical table(s) are missing: ${missingCriticalTables.join(", ")}`);
          console.log("  Migrations must be run to create these tables!");
          console.log("  Attempting to force run migrations...");
          
          try {
            const moduleMigrationNames = [
              "20230317040849_create_account_table",
              "20250905_create_trust_registry_tables",
              "20250919_create_credential_schema",
              "20240924_create_permissions_table",
              "20241201000000_create_did_records_table",
              "20250919_create_credential_schema_history",
              "20250922_create_trust_registry_history",
              "20251125000000_create_permission_history",
              "20251125000001_create_permission_session_history",
              "20251125000002_create_trust_deposit_history",
              "20251125000003_create_module_params_history",
              "20260126000001_create_did_history",
              "0123_create_trust_deposit_tables",
              "20250915120000_create_module_params",
              "20230317041447_create_account_vesting_table",
              "20230704102018_create_table_daily_stats_account_stats",
              "20231226102519_add_balances_to_account"
            ];
            
            console.log("  Clearing migration records for missing tables...");
            for (const migName of moduleMigrationNames) {
              try {
                const deleted = await db("knex_migrations").where("name", "like", `%${migName}%`).delete();
                if (deleted > 0) {
                  console.log(`    Cleared: ${migName}`);
                }
              } catch (e) {
              }
            }
            
            try {
              const allMigrations = await db("knex_migrations").select("name");
              const migrationsToClear: string[] = [];
              
              for (const mig of allMigrations) {
                const migName = mig.name;
                if (migName.includes("account") && missingCriticalTables.includes("account")) {
                  migrationsToClear.push(migName);
                } else if (migName.includes("trust_registry") && missingCriticalTables.includes("trust_registry")) {
                  migrationsToClear.push(migName);
                } else if (migName.includes("credential_schema") && missingCriticalTables.includes("credential_schemas")) {
                  migrationsToClear.push(migName);
                } else if (migName.includes("permission") && missingCriticalTables.includes("permissions")) {
                  migrationsToClear.push(migName);
                } else if (migName.includes("did") && missingCriticalTables.includes("dids")) {
                  migrationsToClear.push(migName);
                } else if (migName.includes("transaction") && missingCriticalTables.includes("transaction")) {
                  migrationsToClear.push(migName);
                }
              }
              
              if (migrationsToClear.length > 0) {
                console.log(`  Clearing ${migrationsToClear.length} additional migration record(s) that should create missing tables...`);
                await db("knex_migrations").whereIn("name", migrationsToClear).delete();
              }
            } catch (e) {
              console.warn(`  Could not check additional migrations: ${e}`);
            }
            
            console.log("  Running migrations...");
            await db.migrate.latest();
            console.log("  ✓ Migrations completed successfully after clearing records");
            
            const stillMissing: string[] = [];
            for (const tableName of missingCriticalTables) {
              const exists = await db.schema.hasTable(tableName);
              if (!exists) {
                stillMissing.push(tableName);
              }
            }
            
            if (stillMissing.length > 0) {
              console.error(`  ❌ CRITICAL ERROR: ${stillMissing.length} table(s) still missing after migration: ${stillMissing.join(", ")}`);
              console.error("  Please run: pnpm run reindex:dev");
              process.exitCode = 1;
            } else {
              console.log("  ✓ All critical tables verified and created");
            }
          } catch (forceError: any) {
            const forceErrorMsg = forceError.message || String(forceError);
            
            if (forceErrorMsg.includes("corrupt") || forceErrorMsg.includes("missing") || forceErrorMsg.includes("files are missing")) {
              console.log("  Migration directory validation error detected.");
              console.log("  This can happen when migration records exist but files are missing or tables don't exist.");
              console.log("  Attempting to clear problematic migration records...");
              
              try {
                const missingFileMatch = forceErrorMsg.match(/files are missing: (.+)/);
                if (missingFileMatch) {
                  const missingFiles = missingFileMatch[1].split(',').map((f: string) => f.trim());
                  console.log(`  Found ${missingFiles.length} migration(s) marked as missing: ${missingFiles.join(", ")}`);
                  
                  for (const fileName of missingFiles) {
                    try {
                      const nameWithoutExt = fileName.replace(/\.(ts|js)$/, '');
                      
                      const isAlterMigration = nameWithoutExt.includes("add_") || 
                                               nameWithoutExt.includes("alter_") || 
                                               nameWithoutExt.includes("update_") ||
                                               nameWithoutExt.includes("modify_");
                      
                      if (isAlterMigration) {
                        let shouldClear = true;
                        
                        if (nameWithoutExt.includes("gfd_id") || nameWithoutExt.includes("document_history")) {
                          const tableExists = await db.schema.hasTable("governance_framework_document_history");
                          if (!tableExists) {
                            console.log(`    Table 'governance_framework_document_history' doesn't exist, clearing ALTER migration record`);
                            shouldClear = true;
                          } else {
                            console.log(`    Table exists, migration might have failed - clearing anyway to retry`);
                          }
                        }
                        
                        if (shouldClear) {
                          const deleted = await db("knex_migrations")
                            .where("name", nameWithoutExt)
                            .orWhere("name", "like", `%${nameWithoutExt}%`)
                            .delete();
                          if (deleted > 0) {
                            console.log(`    Cleared ALTER migration record: ${nameWithoutExt}`);
                          }
                        }
                      } else {
                        const deleted = await db("knex_migrations")
                          .where("name", nameWithoutExt)
                          .orWhere("name", "like", `%${nameWithoutExt}%`)
                          .delete();
                        if (deleted > 0) {
                          console.log(`    Cleared CREATE migration record: ${nameWithoutExt}`);
                        }
                      }
                    } catch (e) {
                      console.warn(`    Could not clear ${fileName}: ${e}`);
                    }
                  }
                } else {
                  console.log("  Clearing ALTER migrations that might be causing issues...");
                  const alterMigrations = [
                    "20251124120000_add_height_to_credential_schema_history",
                    "20251210000000_add_permission_statistics",
                    "20250115000000_add_permission_new_attributes",
                    "20260126000000_add_trust_registry_statistics",
                    "20260126000002_add_credential_schema_statistics"
                  ];
                  
                  for (const migName of alterMigrations) {
                    try {
                      await db("knex_migrations").where("name", "like", `%${migName}%`).delete();
                    } catch (e) {
                    }
                  }
                }
                
                console.log("  Retrying migrations after clearing problematic records...");
                await db.migrate.latest();
                console.log("  ✓ Migrations completed successfully after clearing problematic records");
                
                const stillMissing: string[] = [];
                for (const tableName of missingCriticalTables) {
                  const exists = await db.schema.hasTable(tableName);
                  if (!exists) {
                    stillMissing.push(tableName);
                  }
                }
                
                if (stillMissing.length > 0) {
                  console.error(`  ❌ CRITICAL ERROR: ${stillMissing.length} table(s) still missing: ${stillMissing.join(", ")}`);
                  console.error("  Please run: pnpm run reindex:dev");
                  process.exitCode = 1;
                } else {
                  console.log("  ✓ All critical tables verified and created");
                }
              } catch (retryError: any) {
                console.error(`  ❌ Retry failed: ${retryError.message}`);
                console.error("  Please run: pnpm run reindex:dev");
                process.exitCode = 1;
              }
            } else {
              console.error(`  ❌ Failed to force run migrations: ${forceErrorMsg}`);
              console.error("  Please run: pnpm run reindex:dev");
              process.exitCode = 1;
            }
          }
        } else {
          console.log("  ✓ All critical tables exist - continuing without running migrations.");
          console.log("  This is expected behavior after reindexing.");
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
