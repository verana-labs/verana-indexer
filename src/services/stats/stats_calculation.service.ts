import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { ServiceBroker } from "moleculer";
import BullableService, { QueueHandler } from "../../base/bullable.service";
import { BULL_JOB_NAME, SERVICE } from "../../common";
import Stats, { Granularity, EntityType } from "../../models/stats";
import { BlockCheckpoint } from "../../models/block_checkpoint";
import { Block } from "../../models/block";
import knex from "../../common/utils/db_connection";
import * as fs from "fs";
import * as path from "path";

@Service({
  name: SERVICE.V1.StatsCalculationService.key,
  version: 1,
})
export default class StatsCalculationService extends BullableService {
  private statsLogFile: string;
  private statsLogStream: fs.WriteStream | null = null;
  private processingInterval: NodeJS.Timeout | null = null;
  private readonly PROCESS_INTERVAL_MS = 30000;
  private isProcessing: boolean = false;

  public constructor(public broker: ServiceBroker) {
    super(broker);
    
    const logDir = path.join(process.cwd(), "logs");
    if (!fs.existsSync(logDir)) {
      fs.mkdirSync(logDir, { recursive: true });
    }
    
    this.statsLogFile = path.join(logDir, `stats-calculation-${new Date().toISOString().split("T")[0]}.log`);
    
    this.statsLogStream = fs.createWriteStream(this.statsLogFile, { flags: "a" });
    this.writeToFile(`\n${"=".repeat(80)}\n`);
    this.writeToFile(`Stats Calculation Log - ${new Date().toISOString()}\n`);
    this.writeToFile(`${"=".repeat(80)}\n\n`);
  }

  private writeToFile(message: string): void {
    if (this.statsLogStream) {
      const timestamp = new Date().toISOString();
      this.statsLogStream.write(`[${timestamp}] ${message}\n`);
    }
  }

  private logToFile(level: string, ...args: any[]): void {
    const message = args.map(arg => {
      if (typeof arg === "object") {
        try {
          return JSON.stringify(arg, null, 2);
        } catch {
          return String(arg);
        }
      }
      return String(arg);
    }).join(" ");
    
    this.writeToFile(`[${level}] ${message}`);
  }

  @Action({
    name: "calculateStats",
  })
  @QueueHandler({
    queueName: BULL_JOB_NAME.CALCULATE_STATS,
    jobName: BULL_JOB_NAME.CALCULATE_STATS,
  })
  public async calculateStats(): Promise<void> {
    await this.processStatsFromCheckpoint();
  }


  private async calculateForGranularity(granularity: Granularity, now: Date): Promise<void> {
    try {
      const timestamp = this.getGranularityTimestamp(granularity, now);
      this.logger.info(`[${granularity}] Timestamp: ${timestamp.toISOString()}`);

      this.logger.info(`[${granularity}] Calculating GLOBAL stats...`);
      await this.calculateGlobalStats(granularity, timestamp);
      
      this.logger.info(`[${granularity}] Calculating TRUST_REGISTRY stats...`);
      await this.calculateTrustRegistryStats(granularity, timestamp);
      
      this.logger.info(`[${granularity}] Calculating CREDENTIAL_SCHEMA stats...`);
      await this.calculateCredentialSchemaStats(granularity, timestamp);
      
      this.logger.info(`[${granularity}] Calculating PERMISSION stats...`);
      await this.calculatePermissionStats(granularity, timestamp);
    } catch (error: any) {
      this.logger.error(`[${granularity}] Error in calculateForGranularity:`, error?.message || error, error?.stack);
      throw error;
    }
  }

  private getGranularityTimestamp(granularity: Granularity, date: Date): Date {
    const d = new Date(date);
    d.setUTCSeconds(0, 0);

    if (granularity === "HOUR") {
      d.setUTCMinutes(0);
    } else if (granularity === "DAY") {
      d.setUTCHours(0);
      d.setUTCMinutes(0);
    } else if (granularity === "MONTH") {
      d.setUTCDate(1);
      d.setUTCHours(0);
      d.setUTCMinutes(0);
    }

    return d;
  }

  private async calculateGlobalStats(granularity: Granularity, timestamp: Date): Promise<void> {
    try {
      this.logger.info(`[GLOBAL][${granularity}] Starting calculation for timestamp: ${timestamp.toISOString()}`);
      
      this.logger.info(`[GLOBAL][${granularity}] Checking existing stats in database...`);
      const existing = await Stats.query()
        .where("granularity", granularity)
        .where("timestamp", timestamp.toISOString())
        .where("entity_type", "GLOBAL")
        .whereNull("entity_id")
        .first();
      this.logger.info(`[GLOBAL][${granularity}] Existing stats check result: ${existing ? `Found existing entry (id: ${existing.id})` : "No existing entry"}`);

      this.logger.info(`[GLOBAL][${granularity}] Querying permissions table...`);
      const permCount = await knex("permissions")
        .where("created", "<=", timestamp)
        .count("* as count")
        .first();
      this.logger.info(`[GLOBAL][${granularity}] Permissions query result:`, permCount);

      this.logger.info(`[GLOBAL][${granularity}] Querying credential_schemas table...`);
      const schemaCount = await knex("credential_schemas")
        .where("created", "<=", timestamp)
        .count("* as count")
        .first();
      this.logger.info(`[GLOBAL][${granularity}] Schemas query result:`, schemaCount);

      const permCountNum = Number(permCount?.count || 0);
      const schemaCountNum = Number(schemaCount?.count || 0);
      this.logger.info(`[GLOBAL][${granularity}] Found ${permCountNum} permissions and ${schemaCountNum} schemas`);

      const hasEntities = permCountNum > 0 || schemaCountNum > 0;

      if (!hasEntities) {
        const skipMsg = `[GLOBAL][${granularity}] SKIPPING - no entities exist (permissions: ${permCountNum}, schemas: ${schemaCountNum})`;
        this.logger.warn(skipMsg);
        this.logToFile("WARN", skipMsg);
        return;
      }

      this.logger.info(`[GLOBAL][${granularity}] Computing stats from database...`);
      const stats = await this.computeGlobalStats(timestamp);
      this.logger.info(`[GLOBAL][${granularity}] Stats computed successfully:`, {
        cumulative_participants: stats.cumulative_participants,
        cumulative_active_schemas: stats.cumulative_active_schemas,
        cumulative_archived_schemas: stats.cumulative_archived_schemas,
        cumulative_weight: stats.cumulative_weight,
        cumulative_issued: stats.cumulative_issued,
        cumulative_verified: stats.cumulative_verified,
      });

      if (existing) {
        this.logger.info(`[GLOBAL][${granularity}] Updating existing entry (id: ${existing.id})...`);
        const updateResult = await Stats.query()
          .findById(existing.id)
          .patch(stats);
        this.logger.info(`[GLOBAL][${granularity}] Update query executed. Result:`, updateResult);
        this.logger.info(`[GLOBAL][${granularity}] ✅ Successfully updated stats for ${granularity} at ${timestamp.toISOString()}`);
        this.logToFile("INFO", `[GLOBAL][${granularity}] ✅ Successfully updated stats for ${granularity} at ${timestamp.toISOString()}`);
      } else {
        this.logger.info(`[GLOBAL][${granularity}] Preparing to insert new entry with data:`, {
          granularity,
          timestamp: timestamp.toISOString(),
          entity_type: "GLOBAL",
          entity_id: null,
          ...stats,
        });
        
        try {
          const inserted = await Stats.query().insert({
            granularity,
            timestamp: timestamp.toISOString(),
            entity_type: "GLOBAL",
            entity_id: null,
            ...stats,
          });
          this.logger.info(`[GLOBAL][${granularity}] ✅ Successfully created stats for ${granularity} at ${timestamp.toISOString()} (id: ${inserted.id})`);
        this.logToFile("INFO", `[GLOBAL][${granularity}] ✅ Successfully created stats for ${granularity} at ${timestamp.toISOString()} (id: ${inserted.id})`);
        } catch (insertError: any) {
          this.logger.error(`[GLOBAL][${granularity}] ❌ INSERT FAILED:`, {
            error: insertError?.message,
            stack: insertError?.stack,
            code: insertError?.code,
            detail: insertError?.detail,
          });
          throw insertError;
        }
      }
    } catch (error: any) {
      const fatalErrorDetails = {
        message: error?.message,
        stack: error?.stack,
        code: error?.code,
        detail: error?.detail,
        name: error?.name,
      };
      this.logger.error(`[GLOBAL][${granularity}] ❌ FATAL ERROR in calculateGlobalStats:`, fatalErrorDetails);
      this.logToFile("ERROR", `[GLOBAL][${granularity}] ❌ FATAL ERROR in calculateGlobalStats:`, fatalErrorDetails);
      throw error;
    }
  }

  private async calculateTrustRegistryStats(granularity: Granularity, timestamp: Date): Promise<void> {
    try {
      const trustRegistries = await knex("trust_registry").select("id");
      this.logger.info(`[TRUST_REGISTRY] Found ${trustRegistries.length} trust registries`);

      for (const tr of trustRegistries) {
        try {
          const existing = await Stats.query()
            .where("granularity", granularity)
            .where("timestamp", timestamp.toISOString())
            .where("entity_type", "TRUST_REGISTRY")
            .where("entity_id", String(tr.id))
            .first();

          const stats = await this.computeTrustRegistryStats(String(tr.id), timestamp);

          if (!stats) {
            this.logger.debug(`[TRUST_REGISTRY] No stats computed for TR ${tr.id} (no schemas) - skipping`);
            continue;
          }

      if (existing) {
        this.logger.info(`[TRUST_REGISTRY][${granularity}] Updating existing entry for TR ${tr.id} (id: ${existing.id})`);
        await Stats.query()
          .findById(existing.id)
          .patch(stats);
        this.logger.info(`[TRUST_REGISTRY][${granularity}] ✅ Updated stats for TR ${tr.id}`);
      } else {
        this.logger.info(`[TRUST_REGISTRY][${granularity}] Inserting new entry for TR ${tr.id}`);
        try {
          const inserted = await Stats.query().insert({
            granularity,
            timestamp: timestamp.toISOString(),
            entity_type: "TRUST_REGISTRY",
            entity_id: String(tr.id),
            ...stats,
          });
          this.logger.info(`[TRUST_REGISTRY][${granularity}] ✅ Created stats for TR ${tr.id} (id: ${inserted.id})`);
        } catch (insertError: any) {
          this.logger.error(`[TRUST_REGISTRY][${granularity}] ❌ INSERT FAILED for TR ${tr.id}:`, {
            error: insertError?.message,
            stack: insertError?.stack,
            code: insertError?.code,
          });
          throw insertError;
        }
      }
        } catch (error: any) {
          this.logger.error(`[TRUST_REGISTRY] Error processing TR ${tr.id}:`, error?.message || error, error?.stack);
        }
      }
    } catch (error: any) {
      this.logger.error(`[TRUST_REGISTRY] Error in calculateTrustRegistryStats:`, error?.message || error, error?.stack);
      throw error;
    }
  }

  private async calculateCredentialSchemaStats(granularity: Granularity, timestamp: Date): Promise<void> {
    try {
      const schemas = await knex("credential_schemas").select("id");
      this.logger.info(`[CREDENTIAL_SCHEMA] Found ${schemas.length} schemas`);

      let created = 0;
      let updated = 0;
      let skipped = 0;

      for (const schema of schemas) {
        try {
          const existing = await Stats.query()
            .where("granularity", granularity)
            .where("timestamp", timestamp.toISOString())
            .where("entity_type", "CREDENTIAL_SCHEMA")
            .where("entity_id", String(schema.id))
            .first();

          const stats = await this.computeCredentialSchemaStats(String(schema.id), timestamp);

          if (!stats) {
            skipped++;
            continue;
          }

          if (existing) {
            this.logger.info(`[CREDENTIAL_SCHEMA][${granularity}] Updating existing entry for schema ${schema.id} (id: ${existing.id})`);
            await Stats.query()
              .findById(existing.id)
              .patch(stats);
            updated++;
            this.logger.info(`[CREDENTIAL_SCHEMA][${granularity}] ✅ Updated stats for schema ${schema.id}`);
          } else {
            this.logger.info(`[CREDENTIAL_SCHEMA][${granularity}] Inserting new entry for schema ${schema.id}`);
            try {
              const inserted = await Stats.query().insert({
                granularity,
                timestamp: timestamp.toISOString(),
                entity_type: "CREDENTIAL_SCHEMA",
                entity_id: String(schema.id),
                ...stats,
              });
              created++;
              this.logger.info(`[CREDENTIAL_SCHEMA][${granularity}] ✅ Created stats for schema ${schema.id} (id: ${inserted.id})`);
            } catch (insertError: any) {
              this.logger.error(`[CREDENTIAL_SCHEMA][${granularity}] ❌ INSERT FAILED for schema ${schema.id}:`, {
                error: insertError?.message,
                stack: insertError?.stack,
                code: insertError?.code,
              });
              throw insertError;
            }
          }
        } catch (error: any) {
          this.logger.error(`[CREDENTIAL_SCHEMA] Error processing schema ${schema.id}:`, error?.message || error, error?.stack);
        }
      }

      this.logger.info(`[CREDENTIAL_SCHEMA] Completed: created=${created}, updated=${updated}, skipped=${skipped}`);
    } catch (error: any) {
      this.logger.error(`[CREDENTIAL_SCHEMA] Error in calculateCredentialSchemaStats:`, error?.message || error, error?.stack);
      throw error;
    }
  }

  private async calculatePermissionStats(granularity: Granularity, timestamp: Date): Promise<void> {
    try {
      const permissions = await knex("permissions").select("id", "schema_id");
      this.logger.info(`[PERMISSION] Found ${permissions.length} permissions`);

      let created = 0;
      let updated = 0;
      let skipped = 0;

      for (const perm of permissions) {
        try {
          const existing = await Stats.query()
            .where("granularity", granularity)
            .where("timestamp", timestamp.toISOString())
            .where("entity_type", "PERMISSION")
            .where("entity_id", String(perm.id))
            .first();

          const stats = await this.computePermissionStats(String(perm.id), String(perm.schema_id), timestamp);

          if (!stats) {
            skipped++;
            this.logger.debug(`[PERMISSION] No stats for permission ${perm.id} - skipping`);
            continue;
          }

          if (existing) {
            this.logger.info(`[PERMISSION][${granularity}] Updating existing entry for permission ${perm.id} (id: ${existing.id})`);
            await Stats.query()
              .findById(existing.id)
              .patch(stats);
            updated++;
            this.logger.info(`[PERMISSION][${granularity}] ✅ Updated stats for permission ${perm.id}`);
          } else {
            this.logger.info(`[PERMISSION][${granularity}] Inserting new entry for permission ${perm.id}`);
            try {
              const inserted = await Stats.query().insert({
                granularity,
                timestamp: timestamp.toISOString(),
                entity_type: "PERMISSION",
                entity_id: String(perm.id),
                ...stats,
              });
              created++;
              this.logger.info(`[PERMISSION][${granularity}] ✅ Created stats for permission ${perm.id} (id: ${inserted.id})`);
            } catch (insertError: any) {
              this.logger.error(`[PERMISSION][${granularity}] ❌ INSERT FAILED for permission ${perm.id}:`, {
                error: insertError?.message,
                stack: insertError?.stack,
                code: insertError?.code,
              });
              throw insertError;
            }
          }
        } catch (error: any) {
          this.logger.error(`[PERMISSION] Error processing permission ${perm.id}:`, error?.message || error, error?.stack);
        }
      }

      this.logger.info(`[PERMISSION] Completed: created=${created}, updated=${updated}, skipped=${skipped}`);
    } catch (error: any) {
      this.logger.error(`[PERMISSION] Error in calculatePermissionStats:`, error?.message || error, error?.stack);
      throw error;
    }
  }

  private async computeGlobalStats(timestamp: Date): Promise<any> {
    this.logger.info(`[GLOBAL] Computing stats: Querying permissions table...`);
    const allPerms = await knex("permissions")
      .where("created", "<=", timestamp)
      .select("participants", "weight", "issued", "verified", "ecosystem_slash_events", "ecosystem_slashed_amount", "ecosystem_slashed_amount_repaid", "network_slash_events", "network_slashed_amount", "network_slashed_amount_repaid");
    this.logger.info(`[GLOBAL] Found ${allPerms.length} permissions to process`);

    const cumulative = {
      participants: 0,
      active_schemas: 0,
      archived_schemas: 0,
      weight: BigInt(0),
      issued: BigInt(0),
      verified: BigInt(0),
      ecosystem_slash_events: 0,
      ecosystem_slashed_amount: BigInt(0),
      ecosystem_slashed_amount_repaid: BigInt(0),
      network_slash_events: 0,
      network_slashed_amount: BigInt(0),
      network_slashed_amount_repaid: BigInt(0),
    };

    for (const perm of allPerms) {
      cumulative.participants += Number(perm.participants || 0);
      cumulative.weight += BigInt(perm.weight || "0");
      cumulative.issued += BigInt(perm.issued || "0");
      cumulative.verified += BigInt(perm.verified || "0");
      cumulative.ecosystem_slash_events += Number(perm.ecosystem_slash_events || 0);
      cumulative.ecosystem_slashed_amount += BigInt(perm.ecosystem_slashed_amount || "0");
      cumulative.ecosystem_slashed_amount_repaid += BigInt(perm.ecosystem_slashed_amount_repaid || "0");
      cumulative.network_slash_events += Number(perm.network_slash_events || 0);
      cumulative.network_slashed_amount += BigInt(perm.network_slashed_amount || "0");
      cumulative.network_slashed_amount_repaid += BigInt(perm.network_slashed_amount_repaid || "0");
    }

    const activeSchemas = await knex("credential_schemas")
      .whereNull("archived")
      .where("created", "<=", timestamp)
      .count("* as count")
      .first();

    const archivedSchemas = await knex("credential_schemas")
      .whereNotNull("archived")
      .where("created", "<=", timestamp)
      .count("* as count")
      .first();

    cumulative.active_schemas = Number(activeSchemas?.count || 0);
    cumulative.archived_schemas = Number(archivedSchemas?.count || 0);

    const prevStats = await Stats.query()
      .where("entity_type", "GLOBAL")
      .whereNull("entity_id")
      .where("timestamp", "<", timestamp)
      .orderBy("timestamp", "desc")
      .first();

    const delta = {
      participants: cumulative.participants - (prevStats?.cumulative_participants || 0),
      active_schemas: cumulative.active_schemas - (prevStats?.cumulative_active_schemas || 0),
      archived_schemas: cumulative.archived_schemas - (prevStats?.cumulative_archived_schemas || 0),
      weight: cumulative.weight - BigInt(prevStats?.cumulative_weight || "0"),
      issued: cumulative.issued - BigInt(prevStats?.cumulative_issued || "0"),
      verified: cumulative.verified - BigInt(prevStats?.cumulative_verified || "0"),
      ecosystem_slash_events: cumulative.ecosystem_slash_events - (prevStats?.cumulative_ecosystem_slash_events || 0),
      ecosystem_slashed_amount: cumulative.ecosystem_slashed_amount - BigInt(prevStats?.cumulative_ecosystem_slashed_amount || "0"),
      ecosystem_slashed_amount_repaid: cumulative.ecosystem_slashed_amount_repaid - BigInt(prevStats?.cumulative_ecosystem_slashed_amount_repaid || "0"),
      network_slash_events: cumulative.network_slash_events - (prevStats?.cumulative_network_slash_events || 0),
      network_slashed_amount: cumulative.network_slashed_amount - BigInt(prevStats?.cumulative_network_slashed_amount || "0"),
      network_slashed_amount_repaid: cumulative.network_slashed_amount_repaid - BigInt(prevStats?.cumulative_network_slashed_amount_repaid || "0"),
    };

    return {
      cumulative_participants: cumulative.participants,
      cumulative_active_schemas: cumulative.active_schemas,
      cumulative_archived_schemas: cumulative.archived_schemas,
      cumulative_weight: cumulative.weight.toString(),
      cumulative_issued: cumulative.issued.toString(),
      cumulative_verified: cumulative.verified.toString(),
      cumulative_ecosystem_slash_events: cumulative.ecosystem_slash_events,
      cumulative_ecosystem_slashed_amount: cumulative.ecosystem_slashed_amount.toString(),
      cumulative_ecosystem_slashed_amount_repaid: cumulative.ecosystem_slashed_amount_repaid.toString(),
      cumulative_network_slash_events: cumulative.network_slash_events,
      cumulative_network_slashed_amount: cumulative.network_slashed_amount.toString(),
      cumulative_network_slashed_amount_repaid: cumulative.network_slashed_amount_repaid.toString(),
      delta_participants: delta.participants,
      delta_active_schemas: delta.active_schemas,
      delta_archived_schemas: delta.archived_schemas,
      delta_weight: delta.weight.toString(),
      delta_issued: delta.issued.toString(),
      delta_verified: delta.verified.toString(),
      delta_ecosystem_slash_events: delta.ecosystem_slash_events,
      delta_ecosystem_slashed_amount: delta.ecosystem_slashed_amount.toString(),
      delta_ecosystem_slashed_amount_repaid: delta.ecosystem_slashed_amount_repaid.toString(),
      delta_network_slash_events: delta.network_slash_events,
      delta_network_slashed_amount: delta.network_slashed_amount.toString(),
      delta_network_slashed_amount_repaid: delta.network_slashed_amount_repaid.toString(),
    };
  }

  private async computeTrustRegistryStats(trId: string, timestamp: Date): Promise<any> {
    const schemas = await knex("credential_schemas")
      .where("tr_id", trId)
      .where("created", "<=", timestamp)
      .select("id");

    const schemaIds = schemas.map((s) => String(s.id));

    if (schemaIds.length === 0) {
      return null;
    }

    const perms = await knex("permissions")
      .whereIn("schema_id", schemaIds)
      .where("created", "<=", timestamp)
      .select("participants", "weight", "issued", "verified", "ecosystem_slash_events", "ecosystem_slashed_amount", "ecosystem_slashed_amount_repaid", "network_slash_events", "network_slashed_amount", "network_slashed_amount_repaid");

    const cumulative = {
      participants: 0,
      active_schemas: 0,
      archived_schemas: 0,
      weight: BigInt(0),
      issued: BigInt(0),
      verified: BigInt(0),
      ecosystem_slash_events: 0,
      ecosystem_slashed_amount: BigInt(0),
      ecosystem_slashed_amount_repaid: BigInt(0),
      network_slash_events: 0,
      network_slashed_amount: BigInt(0),
      network_slashed_amount_repaid: BigInt(0),
    };

    for (const perm of perms) {
      cumulative.participants += Number(perm.participants || 0);
      cumulative.weight += BigInt(perm.weight || "0");
      cumulative.issued += BigInt(perm.issued || "0");
      cumulative.verified += BigInt(perm.verified || "0");
      cumulative.ecosystem_slash_events += Number(perm.ecosystem_slash_events || 0);
      cumulative.ecosystem_slashed_amount += BigInt(perm.ecosystem_slashed_amount || "0");
      cumulative.ecosystem_slashed_amount_repaid += BigInt(perm.ecosystem_slashed_amount_repaid || "0");
      cumulative.network_slash_events += Number(perm.network_slash_events || 0);
      cumulative.network_slashed_amount += BigInt(perm.network_slashed_amount || "0");
      cumulative.network_slashed_amount_repaid += BigInt(perm.network_slashed_amount_repaid || "0");
    }

    const activeSchemas = await knex("credential_schemas")
      .whereIn("id", schemaIds)
      .whereNull("archived")
      .where("created", "<=", timestamp)
      .count("* as count")
      .first();

    const archivedSchemas = await knex("credential_schemas")
      .whereIn("id", schemaIds)
      .whereNotNull("archived")
      .where("created", "<=", timestamp)
      .count("* as count")
      .first();

    cumulative.active_schemas = Number(activeSchemas?.count || 0);
    cumulative.archived_schemas = Number(archivedSchemas?.count || 0);

    const prevStats = await Stats.query()
      .where("entity_type", "TRUST_REGISTRY")
      .where("entity_id", trId)
      .where("timestamp", "<", timestamp)
      .orderBy("timestamp", "desc")
      .first();

    const delta = {
      participants: cumulative.participants - (prevStats?.cumulative_participants || 0),
      active_schemas: cumulative.active_schemas - (prevStats?.cumulative_active_schemas || 0),
      archived_schemas: cumulative.archived_schemas - (prevStats?.cumulative_archived_schemas || 0),
      weight: cumulative.weight - BigInt(prevStats?.cumulative_weight || "0"),
      issued: cumulative.issued - BigInt(prevStats?.cumulative_issued || "0"),
      verified: cumulative.verified - BigInt(prevStats?.cumulative_verified || "0"),
      ecosystem_slash_events: cumulative.ecosystem_slash_events - (prevStats?.cumulative_ecosystem_slash_events || 0),
      ecosystem_slashed_amount: cumulative.ecosystem_slashed_amount - BigInt(prevStats?.cumulative_ecosystem_slashed_amount || "0"),
      ecosystem_slashed_amount_repaid: cumulative.ecosystem_slashed_amount_repaid - BigInt(prevStats?.cumulative_ecosystem_slashed_amount_repaid || "0"),
      network_slash_events: cumulative.network_slash_events - (prevStats?.cumulative_network_slash_events || 0),
      network_slashed_amount: cumulative.network_slashed_amount - BigInt(prevStats?.cumulative_network_slashed_amount || "0"),
      network_slashed_amount_repaid: cumulative.network_slashed_amount_repaid - BigInt(prevStats?.cumulative_network_slashed_amount_repaid || "0"),
    };

    return {
      cumulative_participants: cumulative.participants,
      cumulative_active_schemas: cumulative.active_schemas,
      cumulative_archived_schemas: cumulative.archived_schemas,
      cumulative_weight: cumulative.weight.toString(),
      cumulative_issued: cumulative.issued.toString(),
      cumulative_verified: cumulative.verified.toString(),
      cumulative_ecosystem_slash_events: cumulative.ecosystem_slash_events,
      cumulative_ecosystem_slashed_amount: cumulative.ecosystem_slashed_amount.toString(),
      cumulative_ecosystem_slashed_amount_repaid: cumulative.ecosystem_slashed_amount_repaid.toString(),
      cumulative_network_slash_events: cumulative.network_slash_events,
      cumulative_network_slashed_amount: cumulative.network_slashed_amount.toString(),
      cumulative_network_slashed_amount_repaid: cumulative.network_slashed_amount_repaid.toString(),
      delta_participants: delta.participants,
      delta_active_schemas: delta.active_schemas,
      delta_archived_schemas: delta.archived_schemas,
      delta_weight: delta.weight.toString(),
      delta_issued: delta.issued.toString(),
      delta_verified: delta.verified.toString(),
      delta_ecosystem_slash_events: delta.ecosystem_slash_events,
      delta_ecosystem_slashed_amount: delta.ecosystem_slashed_amount.toString(),
      delta_ecosystem_slashed_amount_repaid: delta.ecosystem_slashed_amount_repaid.toString(),
      delta_network_slash_events: delta.network_slash_events,
      delta_network_slashed_amount: delta.network_slashed_amount.toString(),
      delta_network_slashed_amount_repaid: delta.network_slashed_amount_repaid.toString(),
    };
  }

  private async computeCredentialSchemaStats(schemaId: string, timestamp: Date): Promise<any> {
    const perms = await knex("permissions")
      .where("schema_id", schemaId)
      .where("created", "<=", timestamp)
      .select("participants", "weight", "issued", "verified", "ecosystem_slash_events", "ecosystem_slashed_amount", "ecosystem_slashed_amount_repaid", "network_slash_events", "network_slashed_amount", "network_slashed_amount_repaid");

    const cumulative = {
      participants: 0,
      active_schemas: 0,
      archived_schemas: 0,
      weight: BigInt(0),
      issued: BigInt(0),
      verified: BigInt(0),
      ecosystem_slash_events: 0,
      ecosystem_slashed_amount: BigInt(0),
      ecosystem_slashed_amount_repaid: BigInt(0),
      network_slash_events: 0,
      network_slashed_amount: BigInt(0),
      network_slashed_amount_repaid: BigInt(0),
    };

    for (const perm of perms) {
      cumulative.participants += Number(perm.participants || 0);
      cumulative.weight += BigInt(perm.weight || "0");
      cumulative.issued += BigInt(perm.issued || "0");
      cumulative.verified += BigInt(perm.verified || "0");
      cumulative.ecosystem_slash_events += Number(perm.ecosystem_slash_events || 0);
      cumulative.ecosystem_slashed_amount += BigInt(perm.ecosystem_slashed_amount || "0");
      cumulative.ecosystem_slashed_amount_repaid += BigInt(perm.ecosystem_slashed_amount_repaid || "0");
      cumulative.network_slash_events += Number(perm.network_slash_events || 0);
      cumulative.network_slashed_amount += BigInt(perm.network_slashed_amount || "0");
      cumulative.network_slashed_amount_repaid += BigInt(perm.network_slashed_amount_repaid || "0");
    }

    const schema = await knex("credential_schemas")
      .where("id", schemaId)
      .where("created", "<=", timestamp)
      .select("archived")
      .first();

    if (schema) {
      cumulative.active_schemas = schema.archived === null ? 1 : 0;
      cumulative.archived_schemas = schema.archived !== null ? 1 : 0;
    }

    const prevStats = await Stats.query()
      .where("entity_type", "CREDENTIAL_SCHEMA")
      .where("entity_id", schemaId)
      .where("timestamp", "<", timestamp)
      .orderBy("timestamp", "desc")
      .first();

    const delta = {
      participants: cumulative.participants - (prevStats?.cumulative_participants || 0),
      active_schemas: cumulative.active_schemas - (prevStats?.cumulative_active_schemas || 0),
      archived_schemas: cumulative.archived_schemas - (prevStats?.cumulative_archived_schemas || 0),
      weight: cumulative.weight - BigInt(prevStats?.cumulative_weight || "0"),
      issued: cumulative.issued - BigInt(prevStats?.cumulative_issued || "0"),
      verified: cumulative.verified - BigInt(prevStats?.cumulative_verified || "0"),
      ecosystem_slash_events: cumulative.ecosystem_slash_events - (prevStats?.cumulative_ecosystem_slash_events || 0),
      ecosystem_slashed_amount: cumulative.ecosystem_slashed_amount - BigInt(prevStats?.cumulative_ecosystem_slashed_amount || "0"),
      ecosystem_slashed_amount_repaid: cumulative.ecosystem_slashed_amount_repaid - BigInt(prevStats?.cumulative_ecosystem_slashed_amount_repaid || "0"),
      network_slash_events: cumulative.network_slash_events - (prevStats?.cumulative_network_slash_events || 0),
      network_slashed_amount: cumulative.network_slashed_amount - BigInt(prevStats?.cumulative_network_slashed_amount || "0"),
      network_slashed_amount_repaid: cumulative.network_slashed_amount_repaid - BigInt(prevStats?.cumulative_network_slashed_amount_repaid || "0"),
    };

    return {
      cumulative_participants: cumulative.participants,
      cumulative_active_schemas: cumulative.active_schemas,
      cumulative_archived_schemas: cumulative.archived_schemas,
      cumulative_weight: cumulative.weight.toString(),
      cumulative_issued: cumulative.issued.toString(),
      cumulative_verified: cumulative.verified.toString(),
      cumulative_ecosystem_slash_events: cumulative.ecosystem_slash_events,
      cumulative_ecosystem_slashed_amount: cumulative.ecosystem_slashed_amount.toString(),
      cumulative_ecosystem_slashed_amount_repaid: cumulative.ecosystem_slashed_amount_repaid.toString(),
      cumulative_network_slash_events: cumulative.network_slash_events,
      cumulative_network_slashed_amount: cumulative.network_slashed_amount.toString(),
      cumulative_network_slashed_amount_repaid: cumulative.network_slashed_amount_repaid.toString(),
      delta_participants: delta.participants,
      delta_active_schemas: delta.active_schemas,
      delta_archived_schemas: delta.archived_schemas,
      delta_weight: delta.weight.toString(),
      delta_issued: delta.issued.toString(),
      delta_verified: delta.verified.toString(),
      delta_ecosystem_slash_events: delta.ecosystem_slash_events,
      delta_ecosystem_slashed_amount: delta.ecosystem_slashed_amount.toString(),
      delta_ecosystem_slashed_amount_repaid: delta.ecosystem_slashed_amount_repaid.toString(),
      delta_network_slash_events: delta.network_slash_events,
      delta_network_slashed_amount: delta.network_slashed_amount.toString(),
      delta_network_slashed_amount_repaid: delta.network_slashed_amount_repaid.toString(),
    };
  }

  private async computePermissionStats(permId: string, schemaId: string, timestamp: Date): Promise<any> {
    const perm = await knex("permissions")
      .where("id", permId)
      .where("created", "<=", timestamp)
      .select("participants", "weight", "issued", "verified", "ecosystem_slash_events", "ecosystem_slashed_amount", "ecosystem_slashed_amount_repaid", "network_slash_events", "network_slashed_amount", "network_slashed_amount_repaid")
      .first();

    if (!perm) {
      return null;
    }

    const schema = await knex("credential_schemas")
      .where("id", schemaId)
      .where("created", "<=", timestamp)
      .select("archived")
      .first();

    const cumulative = {
      participants: Number(perm.participants || 0),
      active_schemas: schema && schema.archived === null ? 1 : 0,
      archived_schemas: schema && schema.archived !== null ? 1 : 0,
      weight: BigInt(perm.weight || "0"),
      issued: BigInt(perm.issued || "0"),
      verified: BigInt(perm.verified || "0"),
      ecosystem_slash_events: Number(perm.ecosystem_slash_events || 0),
      ecosystem_slashed_amount: BigInt(perm.ecosystem_slashed_amount || "0"),
      ecosystem_slashed_amount_repaid: BigInt(perm.ecosystem_slashed_amount_repaid || "0"),
      network_slash_events: Number(perm.network_slash_events || 0),
      network_slashed_amount: BigInt(perm.network_slashed_amount || "0"),
      network_slashed_amount_repaid: BigInt(perm.network_slashed_amount_repaid || "0"),
    };

    const prevStats = await Stats.query()
      .where("entity_type", "PERMISSION")
      .where("entity_id", permId)
      .where("timestamp", "<", timestamp)
      .orderBy("timestamp", "desc")
      .first();

    const delta = {
      participants: cumulative.participants - (prevStats?.cumulative_participants || 0),
      active_schemas: cumulative.active_schemas - (prevStats?.cumulative_active_schemas || 0),
      archived_schemas: cumulative.archived_schemas - (prevStats?.cumulative_archived_schemas || 0),
      weight: cumulative.weight - BigInt(prevStats?.cumulative_weight || "0"),
      issued: cumulative.issued - BigInt(prevStats?.cumulative_issued || "0"),
      verified: cumulative.verified - BigInt(prevStats?.cumulative_verified || "0"),
      ecosystem_slash_events: cumulative.ecosystem_slash_events - (prevStats?.cumulative_ecosystem_slash_events || 0),
      ecosystem_slashed_amount: cumulative.ecosystem_slashed_amount - BigInt(prevStats?.cumulative_ecosystem_slashed_amount || "0"),
      ecosystem_slashed_amount_repaid: cumulative.ecosystem_slashed_amount_repaid - BigInt(prevStats?.cumulative_ecosystem_slashed_amount_repaid || "0"),
      network_slash_events: cumulative.network_slash_events - (prevStats?.cumulative_network_slash_events || 0),
      network_slashed_amount: cumulative.network_slashed_amount - BigInt(prevStats?.cumulative_network_slashed_amount || "0"),
      network_slashed_amount_repaid: cumulative.network_slashed_amount_repaid - BigInt(prevStats?.cumulative_network_slashed_amount_repaid || "0"),
    };

    return {
      cumulative_participants: cumulative.participants,
      cumulative_active_schemas: cumulative.active_schemas,
      cumulative_archived_schemas: cumulative.archived_schemas,
      cumulative_weight: cumulative.weight.toString(),
      cumulative_issued: cumulative.issued.toString(),
      cumulative_verified: cumulative.verified.toString(),
      cumulative_ecosystem_slash_events: cumulative.ecosystem_slash_events,
      cumulative_ecosystem_slashed_amount: cumulative.ecosystem_slashed_amount.toString(),
      cumulative_ecosystem_slashed_amount_repaid: cumulative.ecosystem_slashed_amount_repaid.toString(),
      cumulative_network_slash_events: cumulative.network_slash_events,
      cumulative_network_slashed_amount: cumulative.network_slashed_amount.toString(),
      cumulative_network_slashed_amount_repaid: cumulative.network_slashed_amount_repaid.toString(),
      delta_participants: delta.participants,
      delta_active_schemas: delta.active_schemas,
      delta_archived_schemas: delta.archived_schemas,
      delta_weight: delta.weight.toString(),
      delta_issued: delta.issued.toString(),
      delta_verified: delta.verified.toString(),
      delta_ecosystem_slash_events: delta.ecosystem_slash_events,
      delta_ecosystem_slashed_amount: delta.ecosystem_slashed_amount.toString(),
      delta_ecosystem_slashed_amount_repaid: delta.ecosystem_slashed_amount_repaid.toString(),
      delta_network_slash_events: delta.network_slash_events,
      delta_network_slashed_amount: delta.network_slashed_amount.toString(),
      delta_network_slashed_amount_repaid: delta.network_slashed_amount_repaid.toString(),
    };
  }

  private hasData(stats: any): boolean {
    if (!stats) {
      return false;
    }

    return true;
  }

  async _start(): Promise<void> {
    this.logger.info("🚀 StatsCalculationService._start() called");
    this.logToFile("INFO", "🚀 StatsCalculationService._start() called");
    this.logger.info(`📝 Stats calculation logs will be written to: ${this.statsLogFile}`);
    
    try {
      await super._start();
      this.logger.info("✅ Super._start() completed");
      this.logToFile("INFO", "✅ Super._start() completed");
    } catch (error: any) {
      this.logger.error("❌ Error in super._start():", error?.message || error, error?.stack);
      this.logToFile("ERROR", "❌ Error in super._start():", error?.message || error, error?.stack);
      throw error;
    }

    this.logger.info("📅 Scheduling recurring stats calculation job...");
    this.logToFile("INFO", "📅 Scheduling recurring stats calculation job...");

    try {
      await this.createJob(
        BULL_JOB_NAME.CALCULATE_STATS,
        BULL_JOB_NAME.CALCULATE_STATS,
        {},
        {
          removeOnComplete: true,
          removeOnFail: {
            count: 3,
          },
          repeat: {
            every: this.PROCESS_INTERVAL_MS,
          },
        }
      );
      this.logger.info(`✅ Stats calculation job scheduled (interval: ${this.PROCESS_INTERVAL_MS}ms)`);
      this.logToFile("INFO", `✅ Stats calculation job scheduled (interval: ${this.PROCESS_INTERVAL_MS}ms)`);
    } catch (error: any) {
      this.logger.error("❌ Error scheduling stats calculation job:", error?.message || error, error?.stack);
      this.logToFile("ERROR", "❌ Error scheduling stats calculation job:", error?.message || error, error?.stack);
    }

    this.logger.info(`⏰ Starting interval-based stats processing (every ${this.PROCESS_INTERVAL_MS}ms)...`);
    this.logToFile("INFO", `⏰ Starting interval-based stats processing (every ${this.PROCESS_INTERVAL_MS}ms)...`);
    
    this.processingInterval = setInterval(async () => {
      if (this.isProcessing) {
        this.logger.debug("⏭️ Stats calculation already in progress, skipping...");
        return;
      }
      
      try {
        await this.processStatsFromCheckpoint();
      } catch (error: any) {
        const errorDetails = {
          message: error?.message,
          stack: error?.stack,
          code: error?.code,
          detail: error?.detail,
          name: error?.name,
        };
        this.logger.error("❌ Error during interval stats calculation:", errorDetails);
        this.logToFile("ERROR", "❌ Error during interval stats calculation:", errorDetails);
        this.isProcessing = false;
      }
    }, this.PROCESS_INTERVAL_MS);

    setTimeout(async () => {
      this.logger.info("🔄 Running initial stats calculation after service startup...");
      this.logToFile("INFO", "🔄 Running initial stats calculation after service startup...");
      try {
        await this.processStatsFromCheckpoint();
        this.logger.info("✅ Initial stats calculation completed successfully");
        this.logToFile("INFO", "✅ Initial stats calculation completed successfully");
      } catch (error: any) {
        const errorDetails = {
          message: error?.message,
          stack: error?.stack,
          code: error?.code,
          detail: error?.detail,
          name: error?.name,
        };
        this.logger.error("❌ Error during initial stats calculation:", errorDetails);
        this.logToFile("ERROR", "❌ Error during initial stats calculation:", errorDetails);
        this.isProcessing = false;
      }
    }, 2000);
    
    this.logger.info("✅ StatsCalculationService._start() completed");
    this.logToFile("INFO", "✅ StatsCalculationService._start() completed");
  }

  async stopped(): Promise<void> {
    if (this.processingInterval) {
      clearInterval(this.processingInterval);
      this.processingInterval = null;
    }
    
    if (this.statsLogStream) {
      this.writeToFile(`\n${"=".repeat(80)}\n`);
      this.writeToFile(`Service stopped - ${new Date().toISOString()}\n`);
      this.writeToFile(`${"=".repeat(80)}\n`);
      this.statsLogStream.end();
      this.statsLogStream = null;
    }
    await super.stopped();
  }

  private async processStatsFromCheckpoint(): Promise<void> {
    if (this.isProcessing) {
      return;
    }

    this.isProcessing = true;

    try {
      const handleTxCheckpoint = await BlockCheckpoint.query()
        .where("job_name", BULL_JOB_NAME.HANDLE_TRANSACTION)
        .first();

      if (!handleTxCheckpoint) {
        this.logger.debug("[CHECKPOINT] HANDLE_TRANSACTION checkpoint not found, waiting for transactions to be processed...");
        this.logToFile("DEBUG", "[CHECKPOINT] HANDLE_TRANSACTION checkpoint not found, waiting for transactions to be processed...");
        return;
      }

      const handleTxHeight = handleTxCheckpoint.height;
      this.logger.info(`[CHECKPOINT] HANDLE_TRANSACTION checkpoint height: ${handleTxHeight}`);
      this.logToFile("INFO", `[CHECKPOINT] HANDLE_TRANSACTION checkpoint height: ${handleTxHeight}`);

      const statsCheckpoint = await BlockCheckpoint.query()
        .where("job_name", BULL_JOB_NAME.CALCULATE_STATS)
        .first();

      const statsHeight = statsCheckpoint ? statsCheckpoint.height : 0;
      this.logger.info(`[CHECKPOINT] Stats checkpoint height: ${statsHeight}`);
      this.logToFile("INFO", `[CHECKPOINT] Stats checkpoint height: ${statsHeight}`);

      if (statsHeight >= handleTxHeight) {
        this.logger.debug(`[CHECKPOINT] Stats already up to date (${statsHeight} >= ${handleTxHeight}), no new transactions to process`);
        this.logToFile("DEBUG", `[CHECKPOINT] Stats already up to date (${statsHeight} >= ${handleTxHeight}), no new transactions to process`);
        return;
      }

      const block = await Block.query()
        .where("height", handleTxHeight)
        .first();

      if (!block) {
        this.logger.warn(`[CHECKPOINT] Block at height ${handleTxHeight} not found, waiting...`);
        this.logToFile("WARN", `[CHECKPOINT] Block at height ${handleTxHeight} not found, waiting...`);
        return;
      }

      const blockTimestamp = new Date(block.time);
      this.logger.info(`[CHECKPOINT] Processing stats for HANDLE_TRANSACTION checkpoint height: ${handleTxHeight}, block timestamp: ${blockTimestamp.toISOString()}`);
      this.logToFile("INFO", `[CHECKPOINT] Processing stats for HANDLE_TRANSACTION checkpoint height: ${handleTxHeight}, block timestamp: ${blockTimestamp.toISOString()}`);

      try {
        await this.calculateStatsForTimestamp(blockTimestamp);
        
        if (statsCheckpoint) {
          statsCheckpoint.height = handleTxHeight;
          await BlockCheckpoint.query()
            .insert(statsCheckpoint)
            .onConflict("job_name")
            .merge()
            .returning("id");
        } else {
          await BlockCheckpoint.query().insert({
            job_name: BULL_JOB_NAME.CALCULATE_STATS,
            height: handleTxHeight,
          });
        }
        
        this.logger.info(`[CHECKPOINT] Updated stats checkpoint to height ${handleTxHeight} (synced with HANDLE_TRANSACTION)`);
        this.logToFile("INFO", `[CHECKPOINT] Updated stats checkpoint to height ${handleTxHeight} (synced with HANDLE_TRANSACTION)`);
      } catch (calcError: any) {
        this.logger.error(`[CHECKPOINT] Error calculating stats for height ${handleTxHeight}:`, {
          message: calcError?.message,
          stack: calcError?.stack,
          code: calcError?.code,
        });
        this.logToFile("ERROR", `[CHECKPOINT] Error calculating stats for height ${handleTxHeight}:`, calcError?.message || calcError);
        throw calcError;
      }
    } catch (error: any) {
      const errorDetails = {
        message: error?.message,
        stack: error?.stack,
        code: error?.code,
        detail: error?.detail,
        name: error?.name,
      };
      this.logger.error("[CHECKPOINT] Error processing stats from checkpoint:", errorDetails);
      this.logToFile("ERROR", "[CHECKPOINT] Error processing stats from checkpoint:", errorDetails);
      throw error;
    } finally {
      this.isProcessing = false;
    }
  }

  private async calculateStatsForTimestamp(timestamp: Date): Promise<void> {
    const logMessage = `═══════════════════════════════════════════════════════════\n📊 STARTING STATS CALCULATION\nTimestamp: ${timestamp.toISOString()}\n═══════════════════════════════════════════════════════════`;
    this.logger.info(logMessage);
    this.logToFile("INFO", logMessage);

    try {
      this.logger.info("🔍 Testing database connection...");
      this.logToFile("INFO", "🔍 Testing database connection...");
      const dbTest = await knex.raw("SELECT 1 as connection_test");
      this.logger.info("✅ Database connection OK");
      this.logToFile("INFO", "✅ Database connection OK");

      this.logger.info(`⏰ Processing timestamp: ${timestamp.toISOString()}`);
      const granularities: Granularity[] = ["HOUR", "DAY", "MONTH"];
      this.logger.info(`📋 Processing granularities: ${granularities.join(", ")}`);
      this.logToFile("INFO", `📋 Processing granularities: ${granularities.join(", ")}`);

      for (const granularity of granularities) {
        const separator = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━";
        this.logger.info(`\n${separator}`);
        this.logger.info(`🔄 [${granularity}] Starting calculation...`);
        this.logger.info(`${separator}`);
        this.logToFile("INFO", `\n${separator}`);
        this.logToFile("INFO", `🔄 [${granularity}] Starting calculation...`);
        this.logToFile("INFO", `${separator}`);
        try {
          await this.calculateForGranularity(granularity, timestamp);
          this.logger.info(`✅ [${granularity}] Calculation completed successfully`);
          this.logToFile("INFO", `✅ [${granularity}] Calculation completed successfully`);
        } catch (error: any) {
          const errorDetails = {
            message: error?.message,
            stack: error?.stack,
            code: error?.code,
            detail: error?.detail,
          };
          this.logger.error(`❌ [${granularity}] Error during calculation:`, errorDetails);
          this.logToFile("ERROR", `❌ [${granularity}] Error during calculation:`, errorDetails);
          throw error;
        }
      }

      this.logger.info(`\n📊 Checking final stats count in database...`);
      this.logToFile("INFO", `\n📊 Checking final stats count in database...`);
      const statsCountResult = await knex("stats").count("* as count").first() as any;
      const count = Number(statsCountResult?.count || 0);
      this.logger.info(`✅ Stats calculation completed. Total stats entries in database: ${count}`);
      this.logToFile("INFO", `✅ Stats calculation completed. Total stats entries in database: ${count}`);
      
      if (count === 0) {
        const warnings = [
          "⚠️  WARNING: No stats entries found in database after calculation!",
          "⚠️  This might indicate:",
          "   1. No entities exist in permissions/credential_schemas tables",
          "   2. Database insert operations failed silently",
          "   3. Transaction rollback occurred"
        ];
        warnings.forEach(w => {
          this.logger.warn(w);
          this.logToFile("WARN", w);
        });
      }
      
      const finishMessage = "═══════════════════════════════════════════════════════════\n📊 STATS CALCULATION FINISHED\n═══════════════════════════════════════════════════════════";
      this.logger.info(finishMessage);
      this.logToFile("INFO", finishMessage);
      this.logger.info(`\n📝 Full log saved to: ${this.statsLogFile}\n`);
    } catch (error: any) {
      const errorDetails = {
        message: error?.message,
        stack: error?.stack,
        code: error?.code,
        detail: error?.detail,
        name: error?.name,
      };
      const errorMessage = "═══════════════════════════════════════════════════════════\n❌ FATAL ERROR during stats calculation:\n═══════════════════════════════════════════════════════════";
      this.logger.error(errorMessage);
      this.logger.error(errorDetails);
      this.logToFile("ERROR", errorMessage);
      this.logToFile("ERROR", JSON.stringify(errorDetails, null, 2));
      this.logger.error("═══════════════════════════════════════════════════════════\n");
      this.logger.error(`📝 Error log saved to: ${this.statsLogFile}\n`);
      throw error;
    }
  }
}
