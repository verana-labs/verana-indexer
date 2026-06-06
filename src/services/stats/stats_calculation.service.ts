import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { ServiceBroker } from "moleculer";
import BullableService, { QueueHandler } from "../../base/bullable.service";
import { BULL_JOB_NAME, SERVICE } from "../../common";
import knex from "../../common/utils/db_connection";
import { Block } from "../../models/block";
import { BlockCheckpoint } from "../../models/block_checkpoint";
import Stats, { Granularity } from "../../models/stats";

@Service({
  name: SERVICE.V1.StatsCalculationService.key,
  version: 1,
})
export default class StatsCalculationService extends BullableService {
  private processingInterval: NodeJS.Timeout | null = null;
  private readonly PROCESS_INTERVAL_MS = 30000;
  private isProcessing: boolean = false;

  public constructor(public broker: ServiceBroker) {
    super(broker);
  }

  private sumParticipantsByRole(source: {
    participants_ecosystem?: number | string | null;
    participants_issuer_grantor?: number | string | null;
    participants_issuer?: number | string | null;
    participants_verifier_grantor?: number | string | null;
    participants_verifier?: number | string | null;
    participants_holder?: number | string | null;
  }): number {
    return Number(source.participants_ecosystem || 0)
      + Number(source.participants_issuer_grantor || 0)
      + Number(source.participants_issuer || 0)
      + Number(source.participants_verifier_grantor || 0)
      + Number(source.participants_verifier || 0)
      + Number(source.participants_holder || 0);
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

      this.logger.info(`[${granularity}] Calculating ECOSYSTEM stats...`);
      await this.calculateEcosystemStats(granularity, timestamp);

      this.logger.info(`[${granularity}] Calculating CREDENTIAL_SCHEMA stats...`);
      await this.calculateCredentialSchemaStats(granularity, timestamp);

      this.logger.info(`[${granularity}] Calculating PARTICIPANT stats...`);
      await this.calculateParticipantStats(granularity, timestamp);
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
      this.logger.info(`GLOBAL [${granularity}] Starting calculation for timestamp: ${timestamp.toISOString()}`);

      this.logger.info(`GLOBAL [${granularity}] Checking existing stats in database...`);
      const existing = await Stats.query()
        .where("granularity", granularity)
        .where("timestamp", timestamp.toISOString())
        .where("entity_type", "GLOBAL")
        .whereNull("entity_id")
        .first();
      this.logger.info(`GLOBAL [${granularity}] Existing stats check result: ${existing ? `Found existing entry (id: ${existing.id})` : "No existing entry"}`);

      this.logger.info(`GLOBAL [${granularity}] Querying participants table...`);
      const participantCount = await knex("participants")
        .where("created", "<=", timestamp)
        .count("* as count")
        .first();
      this.logger.info(`GLOBAL [${granularity}] Participants query result:`, participantCount);

      this.logger.info(`GLOBAL [${granularity}] Querying credential_schemas table...`);
      const schemaCount = await knex("credential_schemas")
        .where("created", "<=", timestamp)
        .count("* as count")
        .first();
      this.logger.info(`GLOBAL [${granularity}] Schemas query result:`, schemaCount);

      const participantCountNum = Number(participantCount?.count || 0);
      const schemaCountNum = Number(schemaCount?.count || 0);
      this.logger.info(`GLOBAL [${granularity}] Found ${participantCountNum} participants and ${schemaCountNum} schemas`);

      const hasEntities = participantCountNum > 0 || schemaCountNum > 0;

      if (!hasEntities) {
        const skipMsg = `GLOBAL [${granularity}] SKIPPING - no entities exist (participants: ${participantCountNum}, schemas: ${schemaCountNum})`;
        this.logger.warn(skipMsg);
        return;
      }

      this.logger.info(`GLOBAL [${granularity}] Computing stats from database...`);
      const stats = await this.computeGlobalStats(timestamp);
      this.logger.info(`GLOBAL [${granularity}] Stats computed successfully:`, {
        cumulative_participants: stats.cumulative_participants,
        cumulative_active_schemas: stats.cumulative_active_schemas,
        cumulative_archived_schemas: stats.cumulative_archived_schemas,
        cumulative_weight: stats.cumulative_weight,
        cumulative_issued: stats.cumulative_issued,
        cumulative_verified: stats.cumulative_verified,
      });

      if (existing) {
        this.logger.info(`GLOBAL [${granularity}] Updating existing entry (id: ${existing.id})...`);
        if (!this.hasAnyDelta(stats)) {
          const skipMsg = `GLOBAL [${granularity}] SKIPPING UPDATE - all delta fields are zero for timestamp ${timestamp.toISOString()}`;
          this.logger.info(skipMsg);
        } else if (this.isStatsEqual(existing, stats)) {
          const skipMsg = `GLOBAL [${granularity}] SKIPPING UPDATE - computed stats identical to existing for id ${existing.id}`;
          this.logger.info(skipMsg);
        } else {
          const updateResult = await Stats.query()
            .findById(existing.id)
            .patch(stats);
          this.logger.info(`GLOBAL [${granularity}] Update query executed. Result:`, updateResult);
          this.logger.info(`GLOBAL [${granularity}]  Successfully updated stats for ${granularity} at ${timestamp.toISOString()}`);
        }
      } else {
        this.logger.info(`GLOBAL [${granularity}] Preparing to insert new entry with data:`, {
          granularity,
          timestamp: timestamp.toISOString(),
          entity_type: "GLOBAL",
          entity_id: null,
          ...stats,
        });

        const hasAnyDelta = this.hasAnyDelta(stats);
        if (!hasAnyDelta) {
          const skipMsg = `GLOBAL [${granularity}] SKIPPING INSERT - all delta fields are zero for timestamp ${timestamp.toISOString()}`;
          this.logger.info(skipMsg);
        } else {
          try {
            const inserted = await Stats.query().insert({
              granularity,
              timestamp: timestamp.toISOString(),
              entity_type: "GLOBAL",
              entity_id: null,
              ...stats,
            });
            this.logger.info(`GLOBAL [${granularity}]  Successfully created stats for ${granularity} at ${timestamp.toISOString()} (id: ${inserted.id})`);
          } catch (insertError: any) {
            const isUniqueViolation = insertError?.nativeError?.code === '23505' 
              || insertError?.code === '23505'
              || insertError?.message?.includes('duplicate key')
              || insertError?.message?.includes('UNIQUE constraint')
              || insertError?.message?.includes('stats_unique_key');
            
            if (isUniqueViolation) {
              this.logger.warn(`GLOBAL [${granularity}]  Duplicate key detected (race condition), fetching existing record and updating...`);
              const existingRecord = await Stats.query()
                .where("granularity", granularity)
                .where("timestamp", timestamp.toISOString())
                .where("entity_type", "GLOBAL")
                .whereNull("entity_id")
                .first();
              
              if (existingRecord) {
                if (!this.hasAnyDelta(stats)) {
                  this.logger.info(`GLOBAL [${granularity}]  SKIPPING UPDATE - all delta fields are zero`);
                } else if (this.isStatsEqual(existingRecord, stats)) {
                  this.logger.info(`GLOBAL [${granularity}]  SKIPPING UPDATE - computed stats identical to existing`);
                } else {
                  await Stats.query()
                    .findById(existingRecord.id)
                    .patch(stats);
                  this.logger.info(`GLOBAL [${granularity}]  Updated existing stats record (id: ${existingRecord.id}) after duplicate key detection`);
                }
              } else {
                this.logger.warn(`GLOBAL [${granularity}]  Duplicate key detected but record not found (likely race condition - non-critical)`);
              }
            } else {
              this.logger.error(`GLOBAL [${granularity}]  INSERT FAILED:`, {
                error: insertError?.message,
                stack: insertError?.stack,
                code: insertError?.code,
                detail: insertError?.detail,
              });
              throw insertError;
            }
          }
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
      this.logger.error(`GLOBAL [${granularity}]  FATAL ERROR in calculateGlobalStats:`, fatalErrorDetails);
      throw error;
    }
  }

  private async calculateEcosystemStats(granularity: Granularity, timestamp: Date): Promise<void> {
    try {
      const trustRegistries = await knex("ecosystem").select("id");
      this.logger.info(`[ECOSYSTEM] Found ${trustRegistries.length} trust registries`);

      for (const ec of trustRegistries) {
        try {
          const existing = await Stats.query()
            .where("granularity", granularity)
            .where("timestamp", timestamp.toISOString())
            .where("entity_type", "ECOSYSTEM")
            .where("entity_id", String(ec.id))
            .first();

          const stats = await this.computeEcosystemStats(String(ec.id), timestamp);

          if (!stats) {
            this.logger.debug(`[ECOSYSTEM] No stats computed for EC ${ec.id} (no schemas) - skipping`);
            continue;
          }

          if (existing) {
            this.logger.info(`[ECOSYSTEM][${granularity}] Updating existing entry for EC ${ec.id} (id: ${existing.id})`);
            if (!this.hasAnyDelta(stats)) {
              this.logger.info(`[ECOSYSTEM][${granularity}] SKIPPING UPDATE for EC ${ec.id} - all delta fields are zero`);
            } else if (this.isStatsEqual(existing, stats)) {
              this.logger.info(`[ECOSYSTEM][${granularity}] SKIPPING UPDATE for EC ${ec.id} - no changes`);
            } else {
              await Stats.query()
                .findById(existing.id)
                .patch(stats);
              this.logger.info(`[ECOSYSTEM][${granularity}]  Updated stats for EC ${ec.id}`);
            }
          } else {
            this.logger.info(`[ECOSYSTEM][${granularity}] Inserting new entry for EC ${ec.id}`);
            if (!this.hasAnyDelta(stats)) {
              this.logger.info(`[ECOSYSTEM][${granularity}] SKIPPING INSERT for EC ${ec.id} - all delta fields are zero`);
              continue;
            }
            try {
              const inserted = await Stats.query().insert({
                granularity,
                timestamp: timestamp.toISOString(),
                entity_type: "ECOSYSTEM",
                entity_id: String(ec.id),
                ...stats,
              });
              this.logger.info(`[ECOSYSTEM][${granularity}]  Created stats for EC ${ec.id} (id: ${inserted.id})`);
            } catch (insertError: any) {
              const isUniqueViolation = insertError?.nativeError?.code === '23505' 
                || insertError?.code === '23505'
                || insertError?.message?.includes('duplicate key')
                || insertError?.message?.includes('UNIQUE constraint')
                || insertError?.message?.includes('stats_unique_key');
              
              if (isUniqueViolation) {
                this.logger.warn(`[ECOSYSTEM][${granularity}]  Duplicate key detected for EC ${ec.id} (race condition), fetching existing record and updating...`);
                const existingRecord = await Stats.query()
                  .where("granularity", granularity)
                  .where("timestamp", timestamp.toISOString())
                  .where("entity_type", "ECOSYSTEM")
                  .where("entity_id", String(ec.id))
                  .first();
                
                if (existingRecord) {
                  if (!this.hasAnyDelta(stats)) {
                    this.logger.info(`[ECOSYSTEM][${granularity}]  SKIPPING UPDATE for EC ${ec.id} - all delta fields are zero`);
                  } else if (this.isStatsEqual(existingRecord, stats)) {
                    this.logger.info(`[ECOSYSTEM][${granularity}]  SKIPPING UPDATE for EC ${ec.id} - no changes`);
                  } else {
                    await Stats.query()
                      .findById(existingRecord.id)
                      .patch(stats);
                    this.logger.info(`[ECOSYSTEM][${granularity}]  Updated existing stats for EC ${ec.id} (id: ${existingRecord.id}) after duplicate key detection`);
                  }
                } else {
                  // Race condition: Another process inserted and possibly deleted the record, or there's a timing issue
                  // This is not critical - the duplicate key violation prevented the duplicate insert, which is the desired behavior
                  this.logger.debug(`[ECOSYSTEM][${granularity}]  Duplicate key detected for EC ${ec.id} but record not found (likely race condition - non-critical)`);
                }
              } else {
                this.logger.error(`[ECOSYSTEM][${granularity}]  INSERT FAILED for EC ${ec.id}:`, {
                  error: insertError?.message,
                  stack: insertError?.stack,
                  code: insertError?.code,
                });
                throw insertError;
              }
            }
          }
        } catch (error: any) {
          this.logger.error(`[ECOSYSTEM] Error processing EC ${ec.id}:`, error?.message || error, error?.stack);
        }
      }
    } catch (error: any) {
      this.logger.error(`[ECOSYSTEM] Error in calculateEcosystemStats:`, error?.message || error, error?.stack);
      throw error;
    }
  }

  private async calculateCredentialSchemaStats(granularity: Granularity, timestamp: Date): Promise<void> {
    try {
      const schemas = await knex("credential_schemas").select("id");
      this.logger.info(`CREDENTIAL_SCHEMA Found ${schemas.length} schemas`);

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
            this.logger.info(`CREDENTIAL_SCHEMA[${granularity}] Updating existing entry for schema ${schema.id} (id: ${existing.id})`);
            if (!this.hasAnyDelta(stats)) {
              this.logger.info(`CREDENTIAL_SCHEMA[${granularity}] SKIPPING UPDATE for schema ${schema.id} - all delta fields are zero`);
            } else if (this.isStatsEqual(existing, stats)) {
              this.logger.info(`CREDENTIAL_SCHEMA[${granularity}] SKIPPING UPDATE for schema ${schema.id} - no changes`);
            } else {
              await Stats.query()
                .findById(existing.id)
                .patch(stats);
              updated++;
              this.logger.info(`CREDENTIAL_SCHEMA[${granularity}]  Updated stats for schema ${schema.id}`);
            }
          } else {
            this.logger.info(`CREDENTIAL_SCHEMA[${granularity}] Inserting new entry for schema ${schema.id}`);
            if (!this.hasAnyDelta(stats)) {
              skipped++;
              this.logger.debug(`CREDENTIAL_SCHEMA SKIPPING INSERT for schema ${schema.id} - all delta fields are zero`);
              continue;
            }
            try {
              const inserted = await Stats.query().insert({
                granularity,
                timestamp: timestamp.toISOString(),
                entity_type: "CREDENTIAL_SCHEMA",
                entity_id: String(schema.id),
                ...stats,
              });
              created++;
              this.logger.info(`CREDENTIAL_SCHEMA[${granularity}]  Created stats for schema ${schema.id} (id: ${inserted.id})`);
            } catch (insertError: any) {
              // Handle race condition: if another process inserted the same record, update it instead
              const isUniqueViolation = insertError?.nativeError?.code === '23505' 
                || insertError?.code === '23505'
                || insertError?.message?.includes('duplicate key')
                || insertError?.message?.includes('UNIQUE constraint')
                || insertError?.message?.includes('stats_unique_key');
              
              if (isUniqueViolation) {
                this.logger.warn(`CREDENTIAL_SCHEMA[${granularity}]  Duplicate key detected for schema ${schema.id} (race condition), fetching existing record and updating...`);
                const existingRecord = await Stats.query()
                  .where("granularity", granularity)
                  .where("timestamp", timestamp.toISOString())
                  .where("entity_type", "CREDENTIAL_SCHEMA")
                  .where("entity_id", String(schema.id))
                  .first();
                
                if (existingRecord) {
                  if (!this.hasAnyDelta(stats)) {
                    this.logger.info(`CREDENTIAL_SCHEMA[${granularity}]  SKIPPING UPDATE for schema ${schema.id} - all delta fields are zero`);
                  } else if (this.isStatsEqual(existingRecord, stats)) {
                    this.logger.info(`CREDENTIAL_SCHEMA[${granularity}]  SKIPPING UPDATE for schema ${schema.id} - no changes`);
                  } else {
                    await Stats.query()
                      .findById(existingRecord.id)
                      .patch(stats);
                    updated++;
                    this.logger.info(`CREDENTIAL_SCHEMA[${granularity}]  Updated existing stats for schema ${schema.id} (id: ${existingRecord.id}) after duplicate key detection`);
                  }
                } else {
                  // Race condition: Another process inserted and possibly deleted the record, or there's a timing issue
                  // This is not critical - the duplicate key violation prevented the duplicate insert, which is the desired behavior
                  this.logger.debug(`CREDENTIAL_SCHEMA[${granularity}]  Duplicate key detected for schema ${schema.id} but record not found (likely race condition - non-critical)`);
                }
              } else {
                this.logger.error(`CREDENTIAL_SCHEMA[${granularity}]  INSERT FAILED for schema ${schema.id}:`, {
                  error: insertError?.message,
                  stack: insertError?.stack,
                  code: insertError?.code,
                });
                throw insertError;
              }
            }
          }
        } catch (error: any) {
          this.logger.error(`CREDENTIAL_SCHEMA Error processing schema ${schema.id}:`, error?.message || error, error?.stack);
        }
      }

      this.logger.info(`CREDENTIAL_SCHEMA Completed: created=${created}, updated=${updated}, skipped=${skipped}`);
    } catch (error: any) {
      this.logger.error(`CREDENTIAL_SCHEMA Error in calculateCredentialSchemaStats:`, error?.message || error, error?.stack);
      throw error;
    }
  }

  private async calculateParticipantStats(granularity: Granularity, timestamp: Date): Promise<void> {
    try {
      const participants = await knex("participants").select("id", "schema_id");
      this.logger.info(`PARTICIPANT Found ${participants.length} participants`);

      let created = 0;
      let updated = 0;
      let skipped = 0;

      for (const participant of participants) {
        try {
          const existing = await Stats.query()
            .where("granularity", granularity)
            .where("timestamp", timestamp.toISOString())
            .where("entity_type", "PARTICIPANT")
            .where("entity_id", String(participant.id))
            .first();

          const stats = await this.computeParticipantStats(String(participant.id), String(participant.schema_id), timestamp);

          if (!stats) {
            skipped++;
            this.logger.debug(`PARTICIPANT No stats for participant ${participant.id} - skipping`);
            continue;
          }

          if (existing) {
            this.logger.info(`PARTICIPANT [${granularity}] Updating existing entry for participant ${participant.id} (id: ${existing.id})`);
            if (!this.hasAnyDelta(stats)) {
              this.logger.info(`PARTICIPANT [${granularity}] SKIPPING UPDATE for participant ${participant.id} - all delta fields are zero`);
            } else if (this.isStatsEqual(existing, stats)) {
              this.logger.info(`PARTICIPANT [${granularity}] SKIPPING UPDATE for participant ${participant.id} - no changes`);
            } else {
              await Stats.query()
                .findById(existing.id)
                .patch(stats);
              updated++;
              this.logger.info(`PARTICIPANT [${granularity}]  Updated stats for participant ${participant.id}`);
            }
          } else {
            this.logger.info(`PARTICIPANT [${granularity}] Inserting new entry for participant ${participant.id}`);
            if (!this.hasAnyDelta(stats)) {
              skipped++;
              this.logger.debug(`PARTICIPANT SKIPPING INSERT for participant ${participant.id} - all delta fields are zero`);
              continue;
            }
            try {
              const inserted = await Stats.query().insert({
                granularity,
                timestamp: timestamp.toISOString(),
                entity_type: "PARTICIPANT",
                entity_id: String(participant.id),
                ...stats,
              });
              created++;
              this.logger.info(`PARTICIPANT [${granularity}]  Created stats for participant ${participant.id} (id: ${inserted.id})`);
            } catch (insertError: any) {
              const isUniqueViolation = insertError?.nativeError?.code === '23505' 
                || insertError?.code === '23505'
                || insertError?.message?.includes('duplicate key')
                || insertError?.message?.includes('UNIQUE constraint')
                || insertError?.message?.includes('stats_unique_key');
              
              if (isUniqueViolation) {
                this.logger.warn(`PARTICIPANT [${granularity}]  Duplicate key detected for participant ${participant.id} (race condition), fetching existing record and updating...`);
                const existingRecord = await Stats.query()
                  .where("granularity", granularity)
                  .where("timestamp", timestamp.toISOString())
                  .where("entity_type", "PARTICIPANT")
                  .where("entity_id", String(participant.id))
                  .first();
                
                if (existingRecord) {
                  if (!this.hasAnyDelta(stats)) {
                    this.logger.info(`PARTICIPANT [${granularity}]  SKIPPING UPDATE for participant ${participant.id} - all delta fields are zero`);
                  } else if (this.isStatsEqual(existingRecord, stats)) {
                    this.logger.info(`PARTICIPANT [${granularity}]  SKIPPING UPDATE for participant ${participant.id} - no changes`);
                  } else {
                    await Stats.query()
                      .findById(existingRecord.id)
                      .patch(stats);
                    updated++;
                    this.logger.info(`PARTICIPANT [${granularity}]  Updated existing stats for participant ${participant.id} (id: ${existingRecord.id}) after duplicate key detection`);
                  }
                } else {
                  // Race condition: Another process inserted and possibly deleted the record, or there's a timing issue
                  // This is not critical - the duplicate key violation prevented the duplicate insert, which is the desired behavior
                  this.logger.debug(`PARTICIPANT [${granularity}]  Duplicate key detected for participant ${participant.id} but record not found (likely race condition - non-critical)`);
                }
              } else {
                this.logger.error(`PARTICIPANT [${granularity}]  INSERT FAILED for participant ${participant.id}:`, {
                  error: insertError?.message,
                  stack: insertError?.stack,
                  code: insertError?.code,
                });
                throw insertError;
              }
            }
          }
        } catch (error: any) {
          this.logger.error(`PARTICIPANT Error processing participant ${participant.id}:`, error?.message || error, error?.stack);
        }
      }

      this.logger.info(`PARTICIPANT Completed: created=${created}, updated=${updated}, skipped=${skipped}`);
    } catch (error: any) {
      this.logger.error(`PARTICIPANT Error in calculateParticipantStats:`, error?.message || error, error?.stack);
      throw error;
    }
  }

  private async computeGlobalStats(timestamp: Date): Promise<any> {
    this.logger.info(`GLOBAL  Computing stats: Querying participants table...`);
    const allParticipants = await knex("participants")
      .where("created", "<=", timestamp)
      .select(
        "participants",
        "participants_ecosystem",
        "participants_issuer_grantor",
        "participants_issuer",
        "participants_verifier_grantor",
        "participants_verifier",
        "participants_holder",
        "weight",
        "issued",
        "verified",
        "ecosystem_slash_events",
        "ecosystem_slashed_amount",
        "ecosystem_slashed_amount_repaid",
        "network_slash_events",
        "network_slashed_amount",
        "network_slashed_amount_repaid"
      );
    this.logger.info(`GLOBAL  Found ${allParticipants.length} participants to process`);

    const cumulative = {
      participants: 0,
      participants_ecosystem: 0,
      participants_issuer_grantor: 0,
      participants_issuer: 0,
      participants_verifier_grantor: 0,
      participants_verifier: 0,
      participants_holder: 0,
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

    for (const participant of allParticipants) {
      cumulative.participants += this.sumParticipantsByRole(participant);
      cumulative.participants_ecosystem += Number(participant.participants_ecosystem || 0);
      cumulative.participants_issuer_grantor += Number(participant.participants_issuer_grantor || 0);
      cumulative.participants_issuer += Number(participant.participants_issuer || 0);
      cumulative.participants_verifier_grantor += Number(participant.participants_verifier_grantor || 0);
      cumulative.participants_verifier += Number(participant.participants_verifier || 0);
      cumulative.participants_holder += Number(participant.participants_holder || 0);
      cumulative.weight += BigInt(participant.weight || "0");
      cumulative.issued += BigInt(participant.issued || "0");
      cumulative.verified += BigInt(participant.verified || "0");
      cumulative.ecosystem_slash_events += Number(participant.ecosystem_slash_events || 0);
      cumulative.ecosystem_slashed_amount += BigInt(participant.ecosystem_slashed_amount || "0");
      cumulative.ecosystem_slashed_amount_repaid += BigInt(participant.ecosystem_slashed_amount_repaid || "0");
      cumulative.network_slash_events += Number(participant.network_slash_events || 0);
      cumulative.network_slashed_amount += BigInt(participant.network_slashed_amount || "0");
      cumulative.network_slashed_amount_repaid += BigInt(participant.network_slashed_amount_repaid || "0");
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
      participants_ecosystem: cumulative.participants_ecosystem - (prevStats?.cumulative_participants_ecosystem || 0),
      participants_issuer_grantor: cumulative.participants_issuer_grantor - (prevStats?.cumulative_participants_issuer_grantor || 0),
      participants_issuer: cumulative.participants_issuer - (prevStats?.cumulative_participants_issuer || 0),
      participants_verifier_grantor: cumulative.participants_verifier_grantor - (prevStats?.cumulative_participants_verifier_grantor || 0),
      participants_verifier: cumulative.participants_verifier - (prevStats?.cumulative_participants_verifier || 0),
      participants_holder: cumulative.participants_holder - (prevStats?.cumulative_participants_holder || 0),
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
      cumulative_participants_ecosystem: cumulative.participants_ecosystem,
      cumulative_participants_issuer_grantor: cumulative.participants_issuer_grantor,
      cumulative_participants_issuer: cumulative.participants_issuer,
      cumulative_participants_verifier_grantor: cumulative.participants_verifier_grantor,
      cumulative_participants_verifier: cumulative.participants_verifier,
      cumulative_participants_holder: cumulative.participants_holder,
      cumulative_active_schemas: cumulative.active_schemas,
      cumulative_archived_schemas: cumulative.archived_schemas,
      cumulative_weight: Number(cumulative.weight),
      cumulative_issued: Number(cumulative.issued),
      cumulative_verified: Number(cumulative.verified),
      cumulative_ecosystem_slash_events: cumulative.ecosystem_slash_events,
      cumulative_ecosystem_slashed_amount: Number(cumulative.ecosystem_slashed_amount),
      cumulative_ecosystem_slashed_amount_repaid: Number(cumulative.ecosystem_slashed_amount_repaid),
      cumulative_network_slash_events: cumulative.network_slash_events,
      cumulative_network_slashed_amount: Number(cumulative.network_slashed_amount),
      cumulative_network_slashed_amount_repaid: Number(cumulative.network_slashed_amount_repaid),
      delta_participants: delta.participants,
      delta_participants_ecosystem: delta.participants_ecosystem,
      delta_participants_issuer_grantor: delta.participants_issuer_grantor,
      delta_participants_issuer: delta.participants_issuer,
      delta_participants_verifier_grantor: delta.participants_verifier_grantor,
      delta_participants_verifier: delta.participants_verifier,
      delta_participants_holder: delta.participants_holder,
      delta_active_schemas: delta.active_schemas,
      delta_archived_schemas: delta.archived_schemas,
      delta_weight: Number(delta.weight),
      delta_issued: Number(delta.issued),
      delta_verified: Number(delta.verified),
      delta_ecosystem_slash_events: delta.ecosystem_slash_events,
      delta_ecosystem_slashed_amount: Number(delta.ecosystem_slashed_amount),
      delta_ecosystem_slashed_amount_repaid: Number(delta.ecosystem_slashed_amount_repaid),
      delta_network_slash_events: delta.network_slash_events,
      delta_network_slashed_amount: Number(delta.network_slashed_amount),
      delta_network_slashed_amount_repaid: Number(delta.network_slashed_amount_repaid),
    };
  }

  private async computeEcosystemStats(ecosystemId: string, timestamp: Date): Promise<any> {
    const schemas = await knex("credential_schemas")
      .where("ecosystem_id", ecosystemId)
      .where("created", "<=", timestamp)
      .select("id");

    const schemaIds = schemas.map((s) => String(s.id));

    if (schemaIds.length === 0) {
      return null;
    }

    const participants = await knex("participants")
      .whereIn("schema_id", schemaIds)
      .where("created", "<=", timestamp)
      .select(
        "participants",
        "participants_ecosystem",
        "participants_issuer_grantor",
        "participants_issuer",
        "participants_verifier_grantor",
        "participants_verifier",
        "participants_holder",
        "weight",
        "issued",
        "verified",
        "ecosystem_slash_events",
        "ecosystem_slashed_amount",
        "ecosystem_slashed_amount_repaid",
        "network_slash_events",
        "network_slashed_amount",
        "network_slashed_amount_repaid"
      );

    const cumulative = {
      participants: 0,
      participants_ecosystem: 0,
      participants_issuer_grantor: 0,
      participants_issuer: 0,
      participants_verifier_grantor: 0,
      participants_verifier: 0,
      participants_holder: 0,
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

    for (const participant of participants) {
      cumulative.participants += this.sumParticipantsByRole(participant);
      cumulative.participants_ecosystem += Number(participant.participants_ecosystem || 0);
      cumulative.participants_issuer_grantor += Number(participant.participants_issuer_grantor || 0);
      cumulative.participants_issuer += Number(participant.participants_issuer || 0);
      cumulative.participants_verifier_grantor += Number(participant.participants_verifier_grantor || 0);
      cumulative.participants_verifier += Number(participant.participants_verifier || 0);
      cumulative.participants_holder += Number(participant.participants_holder || 0);
      cumulative.weight += BigInt(participant.weight || "0");
      cumulative.issued += BigInt(participant.issued || "0");
      cumulative.verified += BigInt(participant.verified || "0");
      cumulative.ecosystem_slash_events += Number(participant.ecosystem_slash_events || 0);
      cumulative.ecosystem_slashed_amount += BigInt(participant.ecosystem_slashed_amount || "0");
      cumulative.ecosystem_slashed_amount_repaid += BigInt(participant.ecosystem_slashed_amount_repaid || "0");
      cumulative.network_slash_events += Number(participant.network_slash_events || 0);
      cumulative.network_slashed_amount += BigInt(participant.network_slashed_amount || "0");
      cumulative.network_slashed_amount_repaid += BigInt(participant.network_slashed_amount_repaid || "0");
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
      .where("entity_type", "ECOSYSTEM")
      .where("entity_id", ecosystemId)
      .where("timestamp", "<", timestamp)
      .orderBy("timestamp", "desc")
      .first();

    const delta = {
      participants: cumulative.participants - (prevStats?.cumulative_participants || 0),
      participants_ecosystem: cumulative.participants_ecosystem - (prevStats?.cumulative_participants_ecosystem || 0),
      participants_issuer_grantor: cumulative.participants_issuer_grantor - (prevStats?.cumulative_participants_issuer_grantor || 0),
      participants_issuer: cumulative.participants_issuer - (prevStats?.cumulative_participants_issuer || 0),
      participants_verifier_grantor: cumulative.participants_verifier_grantor - (prevStats?.cumulative_participants_verifier_grantor || 0),
      participants_verifier: cumulative.participants_verifier - (prevStats?.cumulative_participants_verifier || 0),
      participants_holder: cumulative.participants_holder - (prevStats?.cumulative_participants_holder || 0),
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
      cumulative_participants_ecosystem: cumulative.participants_ecosystem,
      cumulative_participants_issuer_grantor: cumulative.participants_issuer_grantor,
      cumulative_participants_issuer: cumulative.participants_issuer,
      cumulative_participants_verifier_grantor: cumulative.participants_verifier_grantor,
      cumulative_participants_verifier: cumulative.participants_verifier,
      cumulative_participants_holder: cumulative.participants_holder,
      cumulative_active_schemas: cumulative.active_schemas,
      cumulative_archived_schemas: cumulative.archived_schemas,
      cumulative_weight: Number(cumulative.weight),
      cumulative_issued: Number(cumulative.issued),
      cumulative_verified: Number(cumulative.verified),
      cumulative_ecosystem_slash_events: cumulative.ecosystem_slash_events,
      cumulative_ecosystem_slashed_amount: Number(cumulative.ecosystem_slashed_amount),
      cumulative_ecosystem_slashed_amount_repaid: Number(cumulative.ecosystem_slashed_amount_repaid),
      cumulative_network_slash_events: cumulative.network_slash_events,
      cumulative_network_slashed_amount: Number(cumulative.network_slashed_amount),
      cumulative_network_slashed_amount_repaid: Number(cumulative.network_slashed_amount_repaid),
      delta_participants: delta.participants,
      delta_participants_ecosystem: delta.participants_ecosystem,
      delta_participants_issuer_grantor: delta.participants_issuer_grantor,
      delta_participants_issuer: delta.participants_issuer,
      delta_participants_verifier_grantor: delta.participants_verifier_grantor,
      delta_participants_verifier: delta.participants_verifier,
      delta_participants_holder: delta.participants_holder,
      delta_active_schemas: delta.active_schemas,
      delta_archived_schemas: delta.archived_schemas,
      delta_weight: Number(delta.weight),
      delta_issued: Number(delta.issued),
      delta_verified: Number(delta.verified),
      delta_ecosystem_slash_events: delta.ecosystem_slash_events,
      delta_ecosystem_slashed_amount: Number(delta.ecosystem_slashed_amount),
      delta_ecosystem_slashed_amount_repaid: Number(delta.ecosystem_slashed_amount_repaid),
      delta_network_slash_events: delta.network_slash_events,
      delta_network_slashed_amount: Number(delta.network_slashed_amount),
      delta_network_slashed_amount_repaid: Number(delta.network_slashed_amount_repaid),
    };
  }

  private async computeCredentialSchemaStats(schemaId: string, timestamp: Date): Promise<any> {
    const participants = await knex("participants")
      .where("schema_id", schemaId)
      .where("created", "<=", timestamp)
      .select(
        "participants",
        "participants_ecosystem",
        "participants_issuer_grantor",
        "participants_issuer",
        "participants_verifier_grantor",
        "participants_verifier",
        "participants_holder",
        "weight",
        "issued",
        "verified",
        "ecosystem_slash_events",
        "ecosystem_slashed_amount",
        "ecosystem_slashed_amount_repaid",
        "network_slash_events",
        "network_slashed_amount",
        "network_slashed_amount_repaid"
      );

    const cumulative = {
      participants: 0,
      participants_ecosystem: 0,
      participants_issuer_grantor: 0,
      participants_issuer: 0,
      participants_verifier_grantor: 0,
      participants_verifier: 0,
      participants_holder: 0,
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

    for (const participant of participants) {
      cumulative.participants += this.sumParticipantsByRole(participant);
      cumulative.participants_ecosystem += Number(participant.participants_ecosystem || 0);
      cumulative.participants_issuer_grantor += Number(participant.participants_issuer_grantor || 0);
      cumulative.participants_issuer += Number(participant.participants_issuer || 0);
      cumulative.participants_verifier_grantor += Number(participant.participants_verifier_grantor || 0);
      cumulative.participants_verifier += Number(participant.participants_verifier || 0);
      cumulative.participants_holder += Number(participant.participants_holder || 0);
      cumulative.weight += BigInt(participant.weight || "0");
      cumulative.issued += BigInt(participant.issued || "0");
      cumulative.verified += BigInt(participant.verified || "0");
      cumulative.ecosystem_slash_events += Number(participant.ecosystem_slash_events || 0);
      cumulative.ecosystem_slashed_amount += BigInt(participant.ecosystem_slashed_amount || "0");
      cumulative.ecosystem_slashed_amount_repaid += BigInt(participant.ecosystem_slashed_amount_repaid || "0");
      cumulative.network_slash_events += Number(participant.network_slash_events || 0);
      cumulative.network_slashed_amount += BigInt(participant.network_slashed_amount || "0");
      cumulative.network_slashed_amount_repaid += BigInt(participant.network_slashed_amount_repaid || "0");
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
      participants_ecosystem: cumulative.participants_ecosystem - (prevStats?.cumulative_participants_ecosystem || 0),
      participants_issuer_grantor: cumulative.participants_issuer_grantor - (prevStats?.cumulative_participants_issuer_grantor || 0),
      participants_issuer: cumulative.participants_issuer - (prevStats?.cumulative_participants_issuer || 0),
      participants_verifier_grantor: cumulative.participants_verifier_grantor - (prevStats?.cumulative_participants_verifier_grantor || 0),
      participants_verifier: cumulative.participants_verifier - (prevStats?.cumulative_participants_verifier || 0),
      participants_holder: cumulative.participants_holder - (prevStats?.cumulative_participants_holder || 0),
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
      cumulative_participants_ecosystem: cumulative.participants_ecosystem,
      cumulative_participants_issuer_grantor: cumulative.participants_issuer_grantor,
      cumulative_participants_issuer: cumulative.participants_issuer,
      cumulative_participants_verifier_grantor: cumulative.participants_verifier_grantor,
      cumulative_participants_verifier: cumulative.participants_verifier,
      cumulative_participants_holder: cumulative.participants_holder,
      cumulative_active_schemas: cumulative.active_schemas,
      cumulative_archived_schemas: cumulative.archived_schemas,
      cumulative_weight: Number(cumulative.weight),
      cumulative_issued: Number(cumulative.issued),
      cumulative_verified: Number(cumulative.verified),
      cumulative_ecosystem_slash_events: cumulative.ecosystem_slash_events,
      cumulative_ecosystem_slashed_amount: Number(cumulative.ecosystem_slashed_amount),
      cumulative_ecosystem_slashed_amount_repaid: Number(cumulative.ecosystem_slashed_amount_repaid),
      cumulative_network_slash_events: cumulative.network_slash_events,
      cumulative_network_slashed_amount: Number(cumulative.network_slashed_amount),
      cumulative_network_slashed_amount_repaid: Number(cumulative.network_slashed_amount_repaid),
      delta_participants: delta.participants,
      delta_participants_ecosystem: delta.participants_ecosystem,
      delta_participants_issuer_grantor: delta.participants_issuer_grantor,
      delta_participants_issuer: delta.participants_issuer,
      delta_participants_verifier_grantor: delta.participants_verifier_grantor,
      delta_participants_verifier: delta.participants_verifier,
      delta_participants_holder: delta.participants_holder,
      delta_active_schemas: delta.active_schemas,
      delta_archived_schemas: delta.archived_schemas,
      delta_weight: Number(delta.weight),
      delta_issued: Number(delta.issued),
      delta_verified: Number(delta.verified),
      delta_ecosystem_slash_events: delta.ecosystem_slash_events,
      delta_ecosystem_slashed_amount: Number(delta.ecosystem_slashed_amount),
      delta_ecosystem_slashed_amount_repaid: Number(delta.ecosystem_slashed_amount_repaid),
      delta_network_slash_events: delta.network_slash_events,
      delta_network_slashed_amount: Number(delta.network_slashed_amount),
      delta_network_slashed_amount_repaid: Number(delta.network_slashed_amount_repaid),
    };
  }

  private async computeParticipantStats(participantId: string, schemaId: string, timestamp: Date): Promise<any> {
    const participant = await knex("participants")
      .where("id", participantId)
      .where("created", "<=", timestamp)
      .select(
        "participants",
        "participants_ecosystem",
        "participants_issuer_grantor",
        "participants_issuer",
        "participants_verifier_grantor",
        "participants_verifier",
        "participants_holder",
        "weight",
        "issued",
        "verified",
        "ecosystem_slash_events",
        "ecosystem_slashed_amount",
        "ecosystem_slashed_amount_repaid",
        "network_slash_events",
        "network_slashed_amount",
        "network_slashed_amount_repaid"
      )
      .first();

    if (!participant) {
      return null;
    }

    const schema = await knex("credential_schemas")
      .where("id", schemaId)
      .where("created", "<=", timestamp)
      .select("archived")
      .first();

    const cumulative = {
      participants: this.sumParticipantsByRole(participant),
      participants_ecosystem: Number(participant.participants_ecosystem || 0),
      participants_issuer_grantor: Number(participant.participants_issuer_grantor || 0),
      participants_issuer: Number(participant.participants_issuer || 0),
      participants_verifier_grantor: Number(participant.participants_verifier_grantor || 0),
      participants_verifier: Number(participant.participants_verifier || 0),
      participants_holder: Number(participant.participants_holder || 0),
      active_schemas: schema && schema.archived === null ? 1 : 0,
      archived_schemas: schema && schema.archived !== null ? 1 : 0,
      weight: BigInt(participant.weight || "0"),
      issued: BigInt(participant.issued || "0"),
      verified: BigInt(participant.verified || "0"),
      ecosystem_slash_events: Number(participant.ecosystem_slash_events || 0),
      ecosystem_slashed_amount: BigInt(participant.ecosystem_slashed_amount || "0"),
      ecosystem_slashed_amount_repaid: BigInt(participant.ecosystem_slashed_amount_repaid || "0"),
      network_slash_events: Number(participant.network_slash_events || 0),
      network_slashed_amount: BigInt(participant.network_slashed_amount || "0"),
      network_slashed_amount_repaid: BigInt(participant.network_slashed_amount_repaid || "0"),
    };

    const prevStats = await Stats.query()
      .where("entity_type", "PARTICIPANT")
      .where("entity_id", participantId)
      .where("timestamp", "<", timestamp)
      .orderBy("timestamp", "desc")
      .first();

    const delta = {
      participants: cumulative.participants - (prevStats?.cumulative_participants || 0),
      participants_ecosystem: cumulative.participants_ecosystem - (prevStats?.cumulative_participants_ecosystem || 0),
      participants_issuer_grantor: cumulative.participants_issuer_grantor - (prevStats?.cumulative_participants_issuer_grantor || 0),
      participants_issuer: cumulative.participants_issuer - (prevStats?.cumulative_participants_issuer || 0),
      participants_verifier_grantor: cumulative.participants_verifier_grantor - (prevStats?.cumulative_participants_verifier_grantor || 0),
      participants_verifier: cumulative.participants_verifier - (prevStats?.cumulative_participants_verifier || 0),
      participants_holder: cumulative.participants_holder - (prevStats?.cumulative_participants_holder || 0),
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
      cumulative_participants_ecosystem: cumulative.participants_ecosystem,
      cumulative_participants_issuer_grantor: cumulative.participants_issuer_grantor,
      cumulative_participants_issuer: cumulative.participants_issuer,
      cumulative_participants_verifier_grantor: cumulative.participants_verifier_grantor,
      cumulative_participants_verifier: cumulative.participants_verifier,
      cumulative_participants_holder: cumulative.participants_holder,
      cumulative_active_schemas: cumulative.active_schemas,
      cumulative_archived_schemas: cumulative.archived_schemas,
      cumulative_weight: Number(cumulative.weight),
      cumulative_issued: Number(cumulative.issued),
      cumulative_verified: Number(cumulative.verified),
      cumulative_ecosystem_slash_events: cumulative.ecosystem_slash_events,
      cumulative_ecosystem_slashed_amount: Number(cumulative.ecosystem_slashed_amount),
      cumulative_ecosystem_slashed_amount_repaid: Number(cumulative.ecosystem_slashed_amount_repaid),
      cumulative_network_slash_events: cumulative.network_slash_events,
      cumulative_network_slashed_amount: Number(cumulative.network_slashed_amount),
      cumulative_network_slashed_amount_repaid: Number(cumulative.network_slashed_amount_repaid),
      delta_participants: delta.participants,
      delta_participants_ecosystem: delta.participants_ecosystem,
      delta_participants_issuer_grantor: delta.participants_issuer_grantor,
      delta_participants_issuer: delta.participants_issuer,
      delta_participants_verifier_grantor: delta.participants_verifier_grantor,
      delta_participants_verifier: delta.participants_verifier,
      delta_participants_holder: delta.participants_holder,
      delta_active_schemas: delta.active_schemas,
      delta_archived_schemas: delta.archived_schemas,
      delta_weight: Number(delta.weight),
      delta_issued: Number(delta.issued),
      delta_verified: Number(delta.verified),
      delta_ecosystem_slash_events: delta.ecosystem_slash_events,
      delta_ecosystem_slashed_amount: Number(delta.ecosystem_slashed_amount),
      delta_ecosystem_slashed_amount_repaid: Number(delta.ecosystem_slashed_amount_repaid),
      delta_network_slash_events: delta.network_slash_events,
      delta_network_slashed_amount: Number(delta.network_slashed_amount),
      delta_network_slashed_amount_repaid: Number(delta.network_slashed_amount_repaid),
    };
  }

  private hasData(stats: any): boolean {
    if (!stats) {
      return false;
    }

    return true;
  }

  private hasAnyDelta(stats: any): boolean {
    if (!stats) return false;

    const deltaFields = [
      "delta_participants",
      "delta_participants_ecosystem",
      "delta_participants_issuer_grantor",
      "delta_participants_issuer",
      "delta_participants_verifier_grantor",
      "delta_participants_verifier",
      "delta_participants_holder",
      "delta_active_schemas",
      "delta_archived_schemas",
      "delta_weight",
      "delta_issued",
      "delta_verified",
      "delta_ecosystem_slash_events",
      "delta_ecosystem_slashed_amount",
      "delta_ecosystem_slashed_amount_repaid",
      "delta_network_slash_events",
      "delta_network_slashed_amount",
      "delta_network_slashed_amount_repaid",
    ];

    return deltaFields.some((f) => {
      const val = stats[f];
      if (val === undefined || val === null) return false;

      if (typeof val === "string") {
        const s = val.trim();
        if (s.length === 0) return false;
        if (/^-?\d+$/.test(s)) {
          try {
            return BigInt(s) !== BigInt(0);
          } catch {
            return Number(s) !== 0;
          }
        }
        return Number.parseFloat(s) !== 0;
      }

      if (typeof val === "bigint") {
        return val !== BigInt(0);
      }

      return Number(val) !== 0;
    });
  }

  private isStatsEqual(existing: any, stats: any): boolean {
    if (!existing || !stats) return false;
    const fields = [
      "cumulative_participants",
      "cumulative_participants_ecosystem",
      "cumulative_participants_issuer_grantor",
      "cumulative_participants_issuer",
      "cumulative_participants_verifier_grantor",
      "cumulative_participants_verifier",
      "cumulative_participants_holder",
      "cumulative_active_schemas",
      "cumulative_archived_schemas",
      "cumulative_weight",
      "cumulative_issued",
      "cumulative_verified",
      "cumulative_ecosystem_slash_events",
      "cumulative_ecosystem_slashed_amount",
      "cumulative_ecosystem_slashed_amount_repaid",
      "cumulative_network_slash_events",
      "cumulative_network_slashed_amount",
      "cumulative_network_slashed_amount_repaid",
      "delta_participants",
      "delta_participants_ecosystem",
      "delta_participants_issuer_grantor",
      "delta_participants_issuer",
      "delta_participants_verifier_grantor",
      "delta_participants_verifier",
      "delta_participants_holder",
      "delta_active_schemas",
      "delta_archived_schemas",
      "delta_weight",
      "delta_issued",
      "delta_verified",
      "delta_ecosystem_slash_events",
      "delta_ecosystem_slashed_amount",
      "delta_ecosystem_slashed_amount_repaid",
      "delta_network_slash_events",
      "delta_network_slashed_amount",
      "delta_network_slashed_amount_repaid",
    ];

    return fields.every((f) => {
      const a = existing[f];
      const b = stats[f];
      return String(a ?? "") === String(b ?? "");
    });
  }

  async _start(): Promise<void> {
    this.logger.info("🚀 StatsCalculationService._start() called");

    try {
      await super._start();
      this.logger.info(" Super._start() completed");
    } catch (error: any) {
      this.logger.error(" Error in super._start():", error?.message || error, error?.stack);
      throw error;
    }

    this.logger.info("📅 Scheduling recurring stats calculation job...");

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
      this.logger.info(` Stats calculation job scheduled (interval: ${this.PROCESS_INTERVAL_MS}ms)`);
    } catch (error: any) {
      this.logger.error(" Error scheduling stats calculation job:", error?.message || error, error?.stack);
    }

    this.logger.info(` Starting interval-based stats processing (every ${this.PROCESS_INTERVAL_MS}ms)...`);

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
        this.logger.error(" Error during interval stats calculation:", errorDetails);
        this.isProcessing = false;
      }
    }, this.PROCESS_INTERVAL_MS);

    setTimeout(async () => {
      this.logger.info(" Running initial stats calculation after service startup...");
      try {
        await this.processStatsFromCheckpoint();
        this.logger.info(" Initial stats calculation completed successfully");
      } catch (error: any) {
        const errorDetails = {
          message: error?.message,
          stack: error?.stack,
          code: error?.code,
          detail: error?.detail,
          name: error?.name,
        };
        this.logger.error(" Error during initial stats calculation:", errorDetails);
        this.isProcessing = false;
      }
    }, 2000);

    this.logger.info(" StatsCalculationService._start() completed");
  }

  async stopped(): Promise<void> {
    if (this.processingInterval) {
      clearInterval(this.processingInterval);
      this.processingInterval = null;
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
        this.logger.debug("CHECKPOINT HANDLE_TRANSACTION checkpoint not found, waiting for transactions to be processed...");
        return;
      }

      const handleTxHeight = handleTxCheckpoint.height;
      this.logger.debug(`CHECKPOINT HANDLE_TRANSACTION checkpoint height: ${handleTxHeight}`);

      const statsCheckpoint = await BlockCheckpoint.query()
        .where("job_name", BULL_JOB_NAME.CALCULATE_STATS)
        .first();

      const statsHeight = statsCheckpoint ? statsCheckpoint.height : 0;
      this.logger.debug(`CHECKPOINT Stats checkpoint height: ${statsHeight}`);

      if (statsHeight >= handleTxHeight) {
        this.logger.debug(`CHECKPOINT Stats already up to date (${statsHeight} >= ${handleTxHeight}), no new transactions to process`);
        return;
      }

      const block = await Block.query()
        .select('height', 'time')
        .where("height", handleTxHeight)
        .first();

      if (!block) {
        this.logger.warn(`CHECKPOINT Block at height ${handleTxHeight} not found, waiting...`);
        return;
      }

      const blockTimestamp = new Date(block.time);
      this.logger.debug(`CHECKPOINT Processing stats for HANDLE_TRANSACTION checkpoint height: ${handleTxHeight}, block timestamp: ${blockTimestamp.toISOString()}`);

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

        this.logger.debug(`CHECKPOINT Updated stats checkpoint to height ${handleTxHeight} (synced with HANDLE_TRANSACTION)`);
      } catch (calcError: any) {
        this.logger.error(`CHECKPOINT Error calculating stats for height ${handleTxHeight}:`, {
          message: calcError?.message,
          stack: calcError?.stack,
          code: calcError?.code,
        });
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
      this.logger.error("CHECKPOINT Error processing stats from checkpoint:", errorDetails);
      throw error;
    } finally {
      this.isProcessing = false;
    }
  }

  private async calculateStatsForTimestamp(timestamp: Date): Promise<void> {

    try {
      this.logger.debug(" Testing database connection...");
      await knex.raw("SELECT 1 as connection_test");
      this.logger.debug(" Database connection OK");

      this.logger.debug(` Processing timestamp: ${timestamp.toISOString()}`);
      const granularities: Granularity[] = ["HOUR", "DAY", "MONTH"];
      this.logger.debug(`Processing granularities: ${granularities.join(", ")}`);

      for (const granularity of granularities) {
        this.logger.debug(` [${granularity}] Starting calculation...`);
        try {
          await this.calculateForGranularity(granularity, timestamp);
          this.logger.debug(` [${granularity}] Calculation completed successfully`);
        } catch (error: any) {
          const errorDetails = {
            message: error?.message,
            stack: error?.stack,
            code: error?.code,
            detail: error?.detail,
          };
          this.logger.error(` [${granularity}] Error during calculation:`, errorDetails);
          throw error;
        }
      }

      this.logger.debug(`\n Checking final stats count in database...`);
      const statsCountResult = await knex("stats").count("* as count").first() as any;
      const count = Number(statsCountResult?.count || 0);
      this.logger.info(` Stats calculation completed. Total stats entries in database: ${count}`);

      if (count === 0) {
        const warnings = [
          "  WARNING: No stats entries found in database after calculation!",
          "  This might indicate:",
          "   1. No entities exist in participants/credential_schemas tables",
          "   2. Database insert operations failed silently",
          "   3. Transaction rollback occurred"
        ];
        warnings.forEach(w => {
          this.logger.warn(w);
        });
      }

    } catch (error: any) {
      const errorDetails = {
        message: error?.message,
        stack: error?.stack,
        code: error?.code,
        detail: error?.detail,
        name: error?.name,
      };
      this.logger.error(errorDetails);
      throw error;
    }
  }
}
