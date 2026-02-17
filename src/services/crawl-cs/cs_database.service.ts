import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import { ModulesParamsNamesTypes, MODULE_DISPLAY_NAMES, SERVICE } from "../../common";
import { validateParticipantParam } from "../../common/utils/accountValidation";
import ApiResponder from "../../common/utils/apiResponse";
import knex from "../../common/utils/db_connection";
import { applyOrdering, validateSortParameter, sortByStandardAttributes } from "../../common/utils/query_ordering";
import { calculateCredentialSchemaStats } from "./cs_stats";
import { calculateTrustRegistryStats } from "../crawl-tr/tr_stats";

let heightColumnExistsCache: boolean | null = null;

async function checkHeightColumnExists(): Promise<boolean> {
  if (heightColumnExistsCache !== null) {
    return heightColumnExistsCache;
  }
  try {
    const result = await knex.schema.hasColumn('credential_schema_history', 'height');
    heightColumnExistsCache = result;
    return result;
  } catch (error) {
    heightColumnExistsCache = false;
    return false;
  }
}

const JSON_SCHEMA_INDENT = 2;

function sortObjectKeys(value: unknown): unknown {
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map(sortObjectKeys);
  const obj = value as Record<string, unknown>;
  const out: Record<string, unknown> = {};
  for (const key of Object.keys(obj).sort()) {
    out[key] = sortObjectKeys(obj[key]);
  }
  return out;
}

function normalizeJsonSchema(js: unknown): object | null {
  if (js == null) return null;
  let obj: object | null = null;
  if (typeof js === "object") obj = js as object;
  else if (typeof js === "string") {
    try {
      const parsed = JSON.parse(js);
      obj = typeof parsed === "object" && parsed !== null ? parsed : null;
    } catch {
      return null;
    }
  }
  if (!obj) return null;
  return sortObjectKeys(obj) as object;
}

function normalizedJsonSchemaString(js: unknown): string {
  const obj = normalizeJsonSchema(js);
  return obj == null ? "{}" : JSON.stringify(obj, null, JSON_SCHEMA_INDENT);
}

function mapToHistoryRow(row: any, overrides: Partial<any> = {}, includeHeight: boolean = true) {
  if (!row || !row.id) {
    throw new Error(`Invalid row data: missing id. Row: ${JSON.stringify(row)}`);
  }

  const height = overrides.height || 0;
  const baseRow: any = {
    credential_schema_id: row.id,
    tr_id: row.tr_id ?? null,
    json_schema: row.json_schema ?? null,
    title: row.title ?? null,
    description: row.description ?? null,
    deposit: row.deposit ?? 0,
    issuer_grantor_validation_validity_period: row.issuer_grantor_validation_validity_period || 0,
    verifier_grantor_validation_validity_period: row.verifier_grantor_validation_validity_period || 0,
    issuer_validation_validity_period: row.issuer_validation_validity_period || 0,
    verifier_validation_validity_period: row.verifier_validation_validity_period || 0,
    holder_validation_validity_period: row.holder_validation_validity_period || 0,
    issuer_perm_management_mode: row.issuer_perm_management_mode ?? null,
    verifier_perm_management_mode: row.verifier_perm_management_mode ?? null,
    archived: row.archived ?? null,
    is_active: row.is_active ?? false,
    created: row.created ?? null,
    modified: row.modified ?? null,
    changes: overrides.changes ?? null,
    action: overrides.action ?? "unknown",
    created_at: knex.fn.now(),
  };

  if (includeHeight) {
    baseRow.height = height;
  }

  return baseRow;
}

@Service({
  name: SERVICE.V1.CredentialSchemaDatabaseService.key,
  version: 1,
})
export default class CredentialSchemaDatabaseService extends BullableService {
  constructor(broker: ServiceBroker) {
    super(broker);
  }

  @Action({ name: "upsert" })
  async upsert(ctx: Context<{ payload: any }>) {
    try {
      const { payload } = ctx.params;

      const result = await knex.transaction(async (trx) => {
        const {
          height,
          blockchain_schema_id: blockchainSchemaIdSnake,
          blockchainSchemaId: blockchainSchemaIdCamel,
          ...schemaPayload
        } = payload;
        const blockchainSchemaId = blockchainSchemaIdSnake ?? blockchainSchemaIdCamel ?? null;
        const blockHeight = Number(height) || 0;
        
        let existingSchema = null;
        if (blockchainSchemaId != null) {
          const { normalizeSchemaId, extractSchemaIdFromNormalizedId } = await import('../../common/utils/schema_id_normalizer');
          
          const normalizedSchema = normalizeSchemaId(schemaPayload.json_schema, blockchainSchemaId);
          let normalizedId: string | null = null;
          
          if (normalizedSchema) {
            let schemaObj: any;
            if (typeof normalizedSchema === 'string') {
              try {
                schemaObj = JSON.parse(normalizedSchema);
              } catch {
                schemaObj = null;
              }
            } else {
              schemaObj = normalizedSchema;
            }
            
            if (schemaObj && typeof schemaObj === 'object' && schemaObj.$id) {
              normalizedId = schemaObj.$id;
            }
          }
          
          if (normalizedId) {
            const existingSchemas = await trx("credential_schemas")
              .select("*")
              .whereRaw("json_schema->>'$id' = ?", [normalizedId])
              .limit(1);
            
            if (existingSchemas.length > 0) {
              existingSchema = existingSchemas[0];
              this.logger.info(`Found existing schema with blockchain_id=${blockchainSchemaId} (normalized $id=${normalizedId}), database_id=${existingSchema.id}. Updating instead of inserting.`);
            }
          }
        }
        
        let finalRecord: any;
        
        if (existingSchema) {
          const updates: Record<string, any> = {
            ...schemaPayload,
            modified: schemaPayload.modified || new Date(),
          };
          
          if (updates.json_schema && blockchainSchemaId != null) {
            const { normalizeSchemaId } = await import('../../common/utils/schema_id_normalizer');
            updates.json_schema = normalizeSchemaId(updates.json_schema, blockchainSchemaId);
          }
          
          const [updated] = await trx("credential_schemas")
            .where({ id: existingSchema.id })
            .update(updates)
            .returning("*");
          
          if (!updated || updated.id !== existingSchema.id) {
            throw new Error(`Failed to update existing schema id=${existingSchema.id}`);
          }
          
          finalRecord = updated;
        } else {
          const [inserted] = await trx("credential_schemas")
            .insert(schemaPayload)
            .returning("*");

          finalRecord = inserted;

          const jsonSchema = inserted.json_schema;
          if (jsonSchema) {
            const schemaIdForNormalization = blockchainSchemaId ?? inserted.id;
            const { normalizeSchemaId } = await import('../../common/utils/schema_id_normalizer');
            const normalizedSchema = normalizeSchemaId(jsonSchema, schemaIdForNormalization);
            
            if (normalizedSchema !== jsonSchema) {
              const [updated] = await trx("credential_schemas")
                .where({ id: inserted.id })
                .update({ json_schema: normalizedSchema })
                .returning("*");

              if (updated && updated.id && updated.id === inserted.id) {
                finalRecord = updated;
              } else {
                throw new Error(`Failed to update json_schema for schema id=${inserted.id}. Updated record is invalid or ID mismatch.`);
              }
            }
          }
        }
        try {
          let schemaObjForMeta: any = null;
          if (finalRecord?.json_schema) {
            if (typeof finalRecord.json_schema === "string") {
              try {
                schemaObjForMeta = JSON.parse(finalRecord.json_schema);
              } catch {
                schemaObjForMeta = null;
              }
            } else {
              schemaObjForMeta = finalRecord.json_schema;
            }
          }
          if (schemaObjForMeta && typeof schemaObjForMeta === "object") {
            const title = typeof schemaObjForMeta.title === "string" ? schemaObjForMeta.title : null;
            const description = typeof schemaObjForMeta.description === "string" ? schemaObjForMeta.description : null;
            const updates: any = {};
            if (title !== null) updates.title = title;
            if (description !== null) updates.description = description;
            if (Object.keys(updates).length > 0) {
              const [updatedWithMeta] = await trx("credential_schemas").where({ id: finalRecord.id }).update(updates).returning("*");
              if (updatedWithMeta) finalRecord = updatedWithMeta;
            }
          }
        } catch (err: any) {
          this.logger.warn(`Failed to persist title/description for CS ${finalRecord.id}: ${err?.message || err}`);
        }

        const creationChanges: Record<string, any> = {};
        for (const [key, value] of Object.entries(finalRecord)) {
          if (value !== null && value !== undefined && key !== 'id' && key !== 'is_active') {
            creationChanges[key] = value;
          }
        }

        const hasHeightColumn = await checkHeightColumnExists();
        const historyAction = existingSchema ? "update" : "create";
        const historyRow = mapToHistoryRow(finalRecord, {
          changes: Object.keys(creationChanges).length > 0 ? JSON.stringify(creationChanges) : null,
          action: historyAction,
          height: blockHeight,
        }, hasHeightColumn);
        await trx("credential_schema_history").insert(historyRow);

        try {
          const stats = await calculateCredentialSchemaStats(finalRecord.id, blockHeight);
          await trx("credential_schemas")
            .where("id", finalRecord.id)
            .update({
              participants: stats.participants,
              weight: Number(stats.weight ?? 0),
              issued: Number(stats.issued ?? 0),
              verified: Number(stats.verified ?? 0),
              ecosystem_slash_events: stats.ecosystem_slash_events,
              ecosystem_slashed_amount: Number(stats.ecosystem_slashed_amount ?? 0),
              ecosystem_slashed_amount_repaid: Number(stats.ecosystem_slashed_amount_repaid ?? 0),
              network_slash_events: stats.network_slash_events,
              network_slashed_amount: Number(stats.network_slashed_amount ?? 0),
              network_slashed_amount_repaid: Number(stats.network_slashed_amount_repaid ?? 0),
            });

          if (finalRecord.tr_id) {
            const trStats = await calculateTrustRegistryStats(finalRecord.tr_id, blockHeight);
            await trx("trust_registry")
              .where("id", finalRecord.tr_id)
              .update({
                participants: trStats.participants,
                active_schemas: trStats.active_schemas,
                archived_schemas: trStats.archived_schemas,
                weight: trStats.weight,
                issued: trStats.issued,
                verified: trStats.verified,
                ecosystem_slash_events: trStats.ecosystem_slash_events,
                ecosystem_slashed_amount: trStats.ecosystem_slashed_amount,
                ecosystem_slashed_amount_repaid: trStats.ecosystem_slashed_amount_repaid,
                network_slash_events: trStats.network_slash_events,
                network_slashed_amount: trStats.network_slashed_amount,
                network_slashed_amount_repaid: trStats.network_slashed_amount_repaid,
              });
          }
        } catch (statsError: any) {
          this.logger.warn(` Failed to update statistics for CS ${finalRecord.id}: ${statsError?.message || String(statsError)}`);
        }

        return finalRecord;
      });

      return ApiResponder.success(ctx, { success: true, result }, 200);
    } catch (err: any) {
      this.logger.error("Error in CredentialSchema upsert:", err);
      console.error("FATAL CS UPSERT ERROR:", err);
      return ApiResponder.error(ctx, "Internal Server Error", 500);
    }
  }

  @Action({ name: "update" })
  async update(ctx: Context<{ payload: any }>) {
    try {
      const { payload } = ctx.params;

      if (!payload?.id) {
        return ApiResponder.error(ctx, "Missing required field: id", 400);
      }

      let existing = await knex("credential_schemas").where({ id: payload.id }).first();
      
      if (!existing && payload.id) {
        const { normalizeSchemaId } = await import('../../common/utils/schema_id_normalizer');
        const chainId = process.env.CHAIN_ID || "UNKNOWN_CHAIN";
        const expectedNormalizedId = `vpr:verana:${chainId}/cs/v1/js/${payload.id}`;
        
        const existingByBlockchainId = await knex("credential_schemas")
          .whereRaw("json_schema->>'$id' = ?", [expectedNormalizedId])
          .first();
        
        if (existingByBlockchainId) {
          existing = existingByBlockchainId;
          this.logger.info(`Found schema by blockchain_id=${payload.id} (database_id=${existing.id}) for update`);
        }
      }
      
      if (!existing) {
        return ApiResponder.error(ctx, `Credential schema with id=${payload.id} not found`, 404);
      }

      const updates: Record<string, any> = {};
      for (const [key, value] of Object.entries(payload)) {
        if (value !== undefined && key !== "id") {
          updates[key] = value;
        }
      }

      if (Object.keys(updates).length === 0) {
        return ApiResponder.error(ctx, "No valid fields to update", 400);
      }

      const { height, ...updatesWithoutHeight } = updates;
      const blockHeight = Number(height) || 0;

      if (updatesWithoutHeight.json_schema && payload.id) {
        const { normalizeSchemaId } = await import('../../common/utils/schema_id_normalizer');
        updatesWithoutHeight.json_schema = normalizeSchemaId(updatesWithoutHeight.json_schema, payload.id);
      }

      let [updated] = await knex("credential_schemas")
        .where({ id: existing.id })
        .update(updatesWithoutHeight)
        .returning("*");

      // After update, extract title/description from json_schema (if present) and persist them too
      try {
        let schemaObjForMeta: any = null;
        if (updated?.json_schema) {
          if (typeof updated.json_schema === "string") {
            try {
              schemaObjForMeta = JSON.parse(updated.json_schema);
            } catch {
              schemaObjForMeta = null;
            }
          } else {
            schemaObjForMeta = updated.json_schema;
          }
        }
        if (schemaObjForMeta && typeof schemaObjForMeta === "object") {
          const title = typeof schemaObjForMeta.title === "string" ? schemaObjForMeta.title : null;
          const description = typeof schemaObjForMeta.description === "string" ? schemaObjForMeta.description : null;
          const metaUpdates: any = {};
          if (title !== null && title !== updated.title) metaUpdates.title = title;
          if (description !== null && description !== updated.description) metaUpdates.description = description;
          if (Object.keys(metaUpdates).length > 0) {
            const [updatedWithMeta] = await knex("credential_schemas").where({ id: existing.id }).update(metaUpdates).returning("*");
            if (updatedWithMeta) updated = updatedWithMeta;
          }
        }
      } catch (err: any) {
        this.logger.warn(`Failed to persist title/description for CS ${existing.id}: ${err?.message || err}`);
      }

      // Compute changes against existing after any meta persistence
      const changes: Record<string, any> = {};
      const keysToCheck = Object.keys({ ...updatesWithoutHeight, title: updated.title, description: updated.description });
      for (const key of keysToCheck) {
        if (existing[key] !== updated[key] && key !== 'is_active') {
          changes[key] = updated[key];
        }
      }

      if (Object.keys(changes).length > 0) {
        const hasHeightColumn = await checkHeightColumnExists();
        const historyRow = mapToHistoryRow(updated, {
          changes: JSON.stringify(changes),
          action: "update",
          height: blockHeight,
        }, hasHeightColumn);
        await knex("credential_schema_history").insert(historyRow);
      }

      try {
        const stats = await calculateCredentialSchemaStats(existing.id, blockHeight);
        await knex("credential_schemas")
          .where("id", existing.id)
          .update({
            participants: stats.participants,
            weight: Number(stats.weight ?? 0),
            issued: Number(stats.issued ?? 0),
            verified: Number(stats.verified ?? 0),
            ecosystem_slash_events: stats.ecosystem_slash_events,
            ecosystem_slashed_amount: Number(stats.ecosystem_slashed_amount ?? 0),
            ecosystem_slashed_amount_repaid: Number(stats.ecosystem_slashed_amount_repaid ?? 0),
            network_slash_events: stats.network_slash_events,
            network_slashed_amount: Number(stats.network_slashed_amount ?? 0),
            network_slashed_amount_repaid: Number(stats.network_slashed_amount_repaid ?? 0),
          });

        if (updated.tr_id) {
          const trStats = await calculateTrustRegistryStats(Number(updated.tr_id), blockHeight);
          await knex("trust_registry")
            .where("id", updated.tr_id)
            .update({
              participants: trStats.participants,
              active_schemas: trStats.active_schemas,
              archived_schemas: trStats.archived_schemas,
              weight: Number(trStats.weight ?? 0),
              issued: Number(trStats.issued ?? 0),
              verified: Number(trStats.verified ?? 0),
              ecosystem_slash_events: trStats.ecosystem_slash_events,
              ecosystem_slashed_amount: Number(trStats.ecosystem_slashed_amount ?? 0),
              ecosystem_slashed_amount_repaid: Number(trStats.ecosystem_slashed_amount_repaid ?? 0),
              network_slash_events: trStats.network_slash_events,
              network_slashed_amount: Number(trStats.network_slashed_amount ?? 0),
              network_slashed_amount_repaid: Number(trStats.network_slashed_amount_repaid ?? 0),
            });
        }
      } catch (statsError: any) {
        this.logger.warn(` Failed to update statistics for CS ${existing.id}: ${statsError?.message || String(statsError)}`);
      }

      return ApiResponder.success(ctx, { success: true, updated }, 200);
    } catch (err: any) {
      this.logger.error("Error in CredentialSchema update:", err);
      console.error("FATAL CS UPDATE ERROR:", err);
      return ApiResponder.error(ctx, "Internal Server Error", 500);
    }
  }

  @Action({ name: "archive" })
  async archive(ctx: Context<{ payload: any }>) {
    try {
      const { id, archive, modified } = ctx.params.payload;
      if (!id || archive === undefined) {
        return ApiResponder.error(ctx, "Missing required parameters: id and archive", 400);
      }

      const schemaRecord = await knex("credential_schemas").where({ id }).first();
      if (!schemaRecord) {
        return ApiResponder.error(ctx, `Credential schema with id=${id} not found`, 404);
      }

      if (archive && schemaRecord.archived !== null) {
        return ApiResponder.error(ctx, `Credential schema id=${id} is already archived`, 400);
      }
      if (!archive && schemaRecord.archived === null) {
        return ApiResponder.error(ctx, `Credential schema id=${id} is already unarchived`, 400);
      }

      const updates: Record<string, any> = {
        archived: archive ? modified : null,
        is_active: archive ? true : false, // eslint-disable-line no-unneeded-ternary
        modified,
      };

      const { height } = ctx.params.payload;
      const blockHeight = Number(height) || 0;

      const [updated] = await knex("credential_schemas")
        .where({ id })
        .update(updates)
        .returning("*");

      const hasHeightColumn = await checkHeightColumnExists();
      const historyRow = mapToHistoryRow(updated, {
        changes: JSON.stringify({
          archived: updated.archived,
        }),
        action: archive ? "archive" : "unarchive",
        height: blockHeight,
      }, hasHeightColumn);
      await knex("credential_schema_history").insert(historyRow);

      try {
        const stats = await calculateCredentialSchemaStats(id, blockHeight);
        await knex("credential_schemas")
          .where("id", id)
          .update({
            participants: stats.participants,
            weight: Number(stats.weight ?? 0),
            issued: Number(stats.issued ?? 0),
            verified: Number(stats.verified ?? 0),
            ecosystem_slash_events: stats.ecosystem_slash_events,
            ecosystem_slashed_amount: Number(stats.ecosystem_slashed_amount ?? 0),
            ecosystem_slashed_amount_repaid: Number(stats.ecosystem_slashed_amount_repaid ?? 0),
            network_slash_events: stats.network_slash_events,
            network_slashed_amount: Number(stats.network_slashed_amount ?? 0),
            network_slashed_amount_repaid: Number(stats.network_slashed_amount_repaid ?? 0),
          });

        if (updated.tr_id) {
          const trStats = await calculateTrustRegistryStats(Number(updated.tr_id), blockHeight);
          await knex("trust_registry")
            .where("id", updated.tr_id)
            .update({
              participants: trStats.participants,
              active_schemas: trStats.active_schemas,
              archived_schemas: trStats.archived_schemas,
              weight: Number(trStats.weight ?? 0),
              issued: Number(trStats.issued ?? 0),
              verified: Number(trStats.verified ?? 0),
              ecosystem_slash_events: trStats.ecosystem_slash_events,
              ecosystem_slashed_amount: Number(trStats.ecosystem_slashed_amount ?? 0),
              ecosystem_slashed_amount_repaid: Number(trStats.ecosystem_slashed_amount_repaid ?? 0),
              network_slash_events: trStats.network_slash_events,
              network_slashed_amount: Number(trStats.network_slashed_amount ?? 0),
              network_slashed_amount_repaid: Number(trStats.network_slashed_amount_repaid ?? 0),
            });
        }
      } catch (statsError: any) {
        this.logger.warn(` Failed to update statistics for CS ${id}: ${statsError?.message || String(statsError)}`);
      }

      return ApiResponder.success(ctx, { success: true, updated }, 200);
    } catch (err: any) {
      this.logger.error("Error in CredentialSchema archive:", err);
      console.error("FATAL CS ARCHIVE ERROR:", err);
      return ApiResponder.error(ctx, "Internal Server Error", 500);
    }
  }

  @Action({
    name: "get",
    params: {
      id: { type: "number", integer: true, positive: true },
    },
  })
  async get(ctx: Context<{ id: number }>) {
    try {
      const { id } = ctx.params;
      const blockHeight = (ctx.meta as any)?.blockHeight;

      if (typeof blockHeight === "number") {
        const hasHeightColumn = await checkHeightColumnExists();
        let query = knex("credential_schema_history")
          .where({ credential_schema_id: id });

        if (hasHeightColumn) {
          query = query.where("height", "<=", blockHeight)
            .orderBy("height", "desc");
        }
        query = query.orderBy("created_at", "desc");

        const historyRecord = await query.first();

        if (!historyRecord) {
          return ApiResponder.error(ctx, `Credential schema with id=${id} not found`, 404);
        }

        const historyJsonSchemaObj = normalizeJsonSchema(historyRecord.json_schema);
        const historicalSchema = {
          id: historyRecord.credential_schema_id,
          tr_id: historyRecord.tr_id,
          json_schema: historyJsonSchemaObj ?? undefined,
          title: historyRecord.title ?? (historyJsonSchemaObj && "title" in historyJsonSchemaObj ? (historyJsonSchemaObj as any).title : undefined),
          description: historyRecord.description ?? (historyJsonSchemaObj && "description" in historyJsonSchemaObj ? (historyJsonSchemaObj as any).description : undefined),
          deposit: historyRecord.deposit,
          issuer_grantor_validation_validity_period: historyRecord.issuer_grantor_validation_validity_period,
          verifier_grantor_validation_validity_period: historyRecord.verifier_grantor_validation_validity_period,
          issuer_validation_validity_period: historyRecord.issuer_validation_validity_period,
          verifier_validation_validity_period: historyRecord.verifier_validation_validity_period,
          holder_validation_validity_period: historyRecord.holder_validation_validity_period,
          issuer_perm_management_mode: historyRecord.issuer_perm_management_mode,
          verifier_perm_management_mode: historyRecord.verifier_perm_management_mode,
          archived: historyRecord.archived,
          created: historyRecord.created,
          modified: historyRecord.modified,
        };

        let stats;
        try {
          stats = await calculateCredentialSchemaStats(historyRecord.credential_schema_id, blockHeight);
        } catch (statsError: any) {
          this.logger.warn(` Failed to calculate statistics for CS ${historyRecord.credential_schema_id}: ${statsError?.message || String(statsError)}`);
          stats = {
            participants: 0,
            weight: 0,
            issued: 0,
            verified: 0,
            ecosystem_slash_events: 0,
            ecosystem_slashed_amount: 0,
            ecosystem_slashed_amount_repaid: 0,
            network_slash_events: 0,
            network_slashed_amount: 0,
            network_slashed_amount_repaid: 0,
          };
        }

        return ApiResponder.success(ctx, {
          schema: {
            ...historicalSchema,
            participants: stats.participants,
            weight: stats.weight,
            issued: stats.issued,
            verified: stats.verified,
            ecosystem_slash_events: stats.ecosystem_slash_events,
            ecosystem_slashed_amount: stats.ecosystem_slashed_amount,
            ecosystem_slashed_amount_repaid: stats.ecosystem_slashed_amount_repaid,
            network_slash_events: stats.network_slash_events,
            network_slashed_amount: stats.network_slashed_amount,
            network_slashed_amount_repaid: stats.network_slashed_amount_repaid,
          },
        }, 200);
      }

      const schemaRecord = await knex("credential_schemas").where({ id }).first();
      if (!schemaRecord) {
        return ApiResponder.error(ctx, `Credential schema with id=${id} not found`, 404);
      }
      const jsonSchemaObj = normalizeJsonSchema(schemaRecord.json_schema);
      delete schemaRecord?.is_active;

      let stats;
      try {
        stats = await calculateCredentialSchemaStats(id);
      } catch (statsError: any) {
        this.logger.warn(` Failed to calculate statistics for CS ${id}: ${statsError?.message || String(statsError)}`);
        stats = {
          participants: 0,
          weight: 0,
          issued: 0,
          verified: 0,
          ecosystem_slash_events: 0,
          ecosystem_slashed_amount: 0,
          ecosystem_slashed_amount_repaid: 0,
          network_slash_events: 0,
          network_slashed_amount: 0,
          network_slashed_amount_repaid: 0,
        };
      }

      return ApiResponder.success(ctx, {
        schema: {
          ...schemaRecord,
          json_schema: jsonSchemaObj ?? undefined,
          title: schemaRecord.title ?? (jsonSchemaObj && "title" in jsonSchemaObj ? (jsonSchemaObj as any).title : undefined),
          description: schemaRecord.description ?? (jsonSchemaObj && "description" in jsonSchemaObj ? (jsonSchemaObj as any).description : undefined),
          participants: stats.participants,
          weight: stats.weight,
          issued: stats.issued,
          verified: stats.verified,
          ecosystem_slash_events: stats.ecosystem_slash_events,
          ecosystem_slashed_amount: stats.ecosystem_slashed_amount,
          ecosystem_slashed_amount_repaid: stats.ecosystem_slashed_amount_repaid,
          network_slash_events: stats.network_slash_events,
          network_slashed_amount: stats.network_slashed_amount,
          network_slashed_amount_repaid: stats.network_slashed_amount_repaid,
        },
      }, 200);
    } catch (err: any) {
      this.logger.error("Error in CredentialSchema get:", err);
      return ApiResponder.error(ctx, "Internal Server Error", 500);
    }
  }

  @Action({
    rest: "GET list",
    params: {
      tr_id: { type: "number", optional: true },
      participant: { type: "any", optional: true },
      modified_after: { type: "string", optional: true },
      only_active: {
        type: "any",
        optional: true,
        default: false,
      },
      issuer_perm_management_mode: { type: "string", optional: true },
      verifier_perm_management_mode: { type: "string", optional: true },
      response_max_size: { type: "number", optional: true, default: 64 },
      sort: { type: "string", optional: true },
      min_participants: { type: "number", optional: true },
      max_participants: { type: "number", optional: true },
      min_weight: { type: "number", optional: true },
      max_weight: { type: "number", optional: true },
      min_issued: { type: "number", optional: true },
      max_issued: { type: "number", optional: true },
      min_verified: { type: "number", optional: true },
      max_verified: { type: "number", optional: true },
      min_ecosystem_slash_events: { type: "number", optional: true },
      max_ecosystem_slash_events: { type: "number", optional: true },
      min_network_slash_events: { type: "number", optional: true },
      max_network_slash_events: { type: "number", optional: true },
    },
  })
  async list(ctx: Context<{
    tr_id?: number;
    participant?: string;
    modified_after?: string;
    only_active?: any;
    issuer_perm_management_mode?: string;
    verifier_perm_management_mode?: string;
    response_max_size?: number;
    sort?: string;
    min_participants?: number;
    max_participants?: number;
    min_weight?: number;
    max_weight?: number;
    min_issued?: number;
    max_issued?: number;
    min_verified?: number;
    max_verified?: number;
    min_ecosystem_slash_events?: number;
    max_ecosystem_slash_events?: number;
    min_network_slash_events?: number;
    max_network_slash_events?: number;
  }>) {
    try {
      const {
        tr_id: trId,
        participant,
        modified_after: modifiedAfter,
        only_active: onlyActive,
        issuer_perm_management_mode: issuerPerm,
        verifier_perm_management_mode: verifierPerm,
        response_max_size: maxSize,
        sort,
        min_participants: minParticipants,
        max_participants: maxParticipants,
        min_weight: minWeight,
        max_weight: maxWeight,
        min_issued: minIssued,
        max_issued: maxIssued,
        min_verified: minVerified,
        max_verified: maxVerified,
        min_ecosystem_slash_events: minEcosystemSlashEvents,
        max_ecosystem_slash_events: maxEcosystemSlashEvents,
        min_network_slash_events: minNetworkSlashEvents,
        max_network_slash_events: maxNetworkSlashEvents,
      } = ctx.params;

      const participantValidation = validateParticipantParam(participant, "participant");
      if (!participantValidation.valid) {
        return ApiResponder.error(ctx, participantValidation.error, 400);
      }
      const participantAccount = participantValidation.value;

      try {
        validateSortParameter(sort);
      } catch (err: any) {
        return ApiResponder.error(ctx, err.message, 400);
      }

      const blockHeight = (ctx.meta as any)?.blockHeight;
      const limit = Math.min(Math.max(maxSize || 64, 1), 1024);

      if (typeof blockHeight === "number") {
        let schemaIdsAtHeight: number[];
        if (participantAccount) {
          schemaIdsAtHeight = await this.getCredentialSchemaIdsForParticipantAtHeight(participantAccount, blockHeight);
          if (schemaIdsAtHeight.length === 0) {
            return ApiResponder.success(ctx, { schemas: [] }, 200);
          }
        } else {
          const hasHeightColumn = await checkHeightColumnExists();
          let subquery;

          if (hasHeightColumn) {
            subquery = knex("credential_schema_history")
              .select("credential_schema_id")
              .select(
                knex.raw(
                  `ROW_NUMBER() OVER (PARTITION BY credential_schema_id ORDER BY height DESC, created_at DESC) as rn`
                )
              )
              .where("height", "<=", blockHeight)
              .as("ranked");
          } else {
            subquery = knex("credential_schema_history")
              .select("credential_schema_id")
              .select(
                knex.raw(
                  `ROW_NUMBER() OVER (PARTITION BY credential_schema_id ORDER BY created_at DESC) as rn`
                )
              )
              .as("ranked");
          }

          const latestHistory = await knex
            .from(subquery)
            .select("credential_schema_id")
            .where("rn", 1);

          schemaIdsAtHeight = latestHistory.map((r: any) => r.credential_schema_id);
        }

        if (schemaIdsAtHeight.length === 0) {
          return ApiResponder.success(ctx, { schemas: [] }, 200);
        }

        const items = await Promise.all(
          schemaIdsAtHeight.map(async (schemaId: number) => {
            const hasHeightColumn = await checkHeightColumnExists();
            let query = knex("credential_schema_history")
              .where({ credential_schema_id: schemaId });

            if (hasHeightColumn) {
              query = query.where("height", "<=", blockHeight)
                .orderBy("height", "desc");
            }
            query = query.orderBy("created_at", "desc");

            const historyRecord = await query.first();

            if (!historyRecord) return null;

            return historyRecord;
          })
        );

        let filteredItems = items
          .filter((item): item is NonNullable<typeof items[0]> => item !== null)
          .map((historyRecord) => {
            const jsObj = normalizeJsonSchema(historyRecord.json_schema);
            return {
              id: historyRecord.credential_schema_id,
              tr_id: historyRecord.tr_id,
              json_schema: jsObj ?? undefined,
              title: historyRecord.title ?? (jsObj && "title" in jsObj ? (jsObj as any).title : undefined),
              description: historyRecord.description ?? (jsObj && "description" in jsObj ? (jsObj as any).description : undefined),
            deposit: historyRecord.deposit,
            issuer_grantor_validation_validity_period: historyRecord.issuer_grantor_validation_validity_period,
            verifier_grantor_validation_validity_period: historyRecord.verifier_grantor_validation_validity_period,
            issuer_validation_validity_period: historyRecord.issuer_validation_validity_period,
            verifier_validation_validity_period: historyRecord.verifier_validation_validity_period,
            holder_validation_validity_period: historyRecord.holder_validation_validity_period,
              issuer_perm_management_mode: historyRecord.issuer_perm_management_mode,
              verifier_perm_management_mode: historyRecord.verifier_perm_management_mode,
              archived: historyRecord.archived,
              created: historyRecord.created,
              modified: historyRecord.modified,
            };
          });

        if (trId) filteredItems = filteredItems.filter(item => String(item.tr_id) === String(trId));
        if (modifiedAfter) {
          const ts = new Date(modifiedAfter);
          if (Number.isNaN(ts.getTime())) {
            return ApiResponder.error(ctx, "Invalid modified_after timestamp", 400);
          }
          filteredItems = filteredItems.filter(item => new Date(item.modified) > ts);
        }

        let onlyActiveBool: boolean | undefined;
        if (typeof onlyActive === "string") {
          onlyActiveBool = onlyActive.toLowerCase() === "true";
        } else if (typeof onlyActive === "boolean") {
          onlyActiveBool = onlyActive;
        }

        if (onlyActiveBool === true) {
          filteredItems = filteredItems.filter(item => !item.archived);
        }

        if (issuerPerm !== undefined) {
          filteredItems = filteredItems.filter(item => item.issuer_perm_management_mode === issuerPerm);
        }

        if (verifierPerm !== undefined) {
          filteredItems = filteredItems.filter(item => item.verifier_perm_management_mode === verifierPerm);
        }

        type FilteredItem = {
          id: number;
          tr_id: any;
          json_schema: any;
          deposit: any;
          issuer_grantor_validation_validity_period: any;
          verifier_grantor_validation_validity_period: any;
          issuer_validation_validity_period: any;
          verifier_validation_validity_period: any;
          holder_validation_validity_period: any;
          issuer_perm_management_mode: any;
          verifier_perm_management_mode: any;
          archived: any;
          created: string;
          modified: string;
        };
        let schemasWithStats;
        if (typeof blockHeight === "number") {
          schemasWithStats = await Promise.all(
            filteredItems.map(async (item) => {
              const stats = await calculateCredentialSchemaStats(item.id, blockHeight);
              return {
                ...item,
                participants: stats.participants,
                weight: stats.weight,
                issued: stats.issued,
                verified: stats.verified,
                ecosystem_slash_events: stats.ecosystem_slash_events,
                ecosystem_slashed_amount: stats.ecosystem_slashed_amount,
                ecosystem_slashed_amount_repaid: stats.ecosystem_slashed_amount_repaid,
                network_slash_events: stats.network_slash_events,
                network_slashed_amount: stats.network_slashed_amount,
                network_slashed_amount_repaid: stats.network_slashed_amount_repaid,
              };
            })
          );
        } else {
          const schemaIds = filteredItems.map((item) => item.id);
          const schemaStatsMap = new Map<number, any>();

          if (schemaIds.length > 0) {
            const schemaStats = await knex("credential_schemas")
              .whereIn("id", schemaIds)
              .select(
                "id",
                "participants",
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

            for (const stat of schemaStats) {
              schemaStatsMap.set(stat.id, stat);
            }
          }

          schemasWithStats = filteredItems.map((item) => {
            const stats = schemaStatsMap.get(item.id) || {
              participants: 0,
              weight: 0,
              issued: 0,
              verified: 0,
              ecosystem_slash_events: 0,
              ecosystem_slashed_amount: 0,
              ecosystem_slashed_amount_repaid: 0,
              network_slash_events: 0,
              network_slashed_amount: 0,
              network_slashed_amount_repaid: 0,
            };

            return {
              ...item,
              participants: typeof stats.participants === 'number' ? stats.participants : Number(stats.participants || 0),
              weight: typeof stats.weight === 'number' ? stats.weight : Number(stats.weight || 0),
              issued: typeof stats.issued === 'number' ? stats.issued : Number(stats.issued || 0),
              verified: typeof stats.verified === 'number' ? stats.verified : Number(stats.verified || 0),
              ecosystem_slash_events: typeof stats.ecosystem_slash_events === 'number' ? stats.ecosystem_slash_events : Number(stats.ecosystem_slash_events || 0),
              ecosystem_slashed_amount: typeof stats.ecosystem_slashed_amount === 'number' ? stats.ecosystem_slashed_amount : Number(stats.ecosystem_slashed_amount || 0),
              ecosystem_slashed_amount_repaid: typeof stats.ecosystem_slashed_amount_repaid === 'number' ? stats.ecosystem_slashed_amount_repaid : Number(stats.ecosystem_slashed_amount_repaid || 0),
              network_slash_events: typeof stats.network_slash_events === 'number' ? stats.network_slash_events : Number(stats.network_slash_events || 0),
              network_slashed_amount: typeof stats.network_slashed_amount === 'number' ? stats.network_slashed_amount : Number(stats.network_slashed_amount || 0),
              network_slashed_amount_repaid: typeof stats.network_slashed_amount_repaid === 'number' ? stats.network_slashed_amount_repaid : Number(stats.network_slashed_amount_repaid || 0),
            };
          });
        }

        let filteredWithStats = schemasWithStats;

        if (minParticipants !== undefined && maxParticipants !== undefined && minParticipants === maxParticipants) {
          filteredWithStats = [];
        } else {
          if (minParticipants !== undefined) {
            filteredWithStats = filteredWithStats.filter((s) => s.participants >= minParticipants);
          }
          if (maxParticipants !== undefined) {
            filteredWithStats = filteredWithStats.filter((s) => s.participants < maxParticipants);
          }
        }
        if (minWeight !== undefined && maxWeight !== undefined && minWeight === maxWeight) {
          filteredWithStats = [];
        } else {
          if (minWeight !== undefined) {
            const minWeightNum = Number(minWeight);
            filteredWithStats = filteredWithStats.filter((s) => {
              const sWeight = typeof s.weight === 'number' ? s.weight : Number(s.weight || 0);
              return sWeight >= minWeightNum;
            });
          }
          if (maxWeight !== undefined) {
            const maxWeightNum = Number(maxWeight);
            filteredWithStats = filteredWithStats.filter((s) => {
              const sWeight = typeof s.weight === 'number' ? s.weight : Number(s.weight || 0);
              return sWeight < maxWeightNum;
            });
          }
        }
        if (minIssued !== undefined && maxIssued !== undefined && minIssued === maxIssued) {
          filteredWithStats = [];
        } else {
          if (minIssued !== undefined) {
            const minIssuedNum = Number(minIssued);
            filteredWithStats = filteredWithStats.filter((s) => s.issued >= minIssuedNum);
          }
          if (maxIssued !== undefined) {
            const maxIssuedNum = Number(maxIssued);
            filteredWithStats = filteredWithStats.filter((s) => s.issued < maxIssuedNum);
          }
        }
        if (minVerified !== undefined && maxVerified !== undefined && minVerified === maxVerified) {
          filteredWithStats = [];
        } else {
          if (minVerified !== undefined) {
            const minVerifiedNum = Number(minVerified);
            filteredWithStats = filteredWithStats.filter((s) => s.verified >= minVerifiedNum);
          }
          if (maxVerified !== undefined) {
            const maxVerifiedNum = Number(maxVerified);
            filteredWithStats = filteredWithStats.filter((s) => s.verified < maxVerifiedNum);
          }
        }
        if (minEcosystemSlashEvents !== undefined && maxEcosystemSlashEvents !== undefined && minEcosystemSlashEvents === maxEcosystemSlashEvents) {
          filteredWithStats = [];
        } else {
          if (minEcosystemSlashEvents !== undefined) {
            filteredWithStats = filteredWithStats.filter((s) => s.ecosystem_slash_events >= minEcosystemSlashEvents);
          }
          if (maxEcosystemSlashEvents !== undefined) {
            filteredWithStats = filteredWithStats.filter((s) => s.ecosystem_slash_events < maxEcosystemSlashEvents);
          }
        }
        if (minNetworkSlashEvents !== undefined && maxNetworkSlashEvents !== undefined && minNetworkSlashEvents === maxNetworkSlashEvents) {
          filteredWithStats = [];
        } else {
          if (minNetworkSlashEvents !== undefined) {
            filteredWithStats = filteredWithStats.filter((s) => s.network_slash_events >= minNetworkSlashEvents);
          }
          if (maxNetworkSlashEvents !== undefined) {
            filteredWithStats = filteredWithStats.filter((s) => s.network_slash_events < maxNetworkSlashEvents);
          }
        }

        type FilteredItemWithStats = FilteredItem & {
          participants: number;
          weight: number;
          issued: number;
          verified: number;
          ecosystem_slash_events: number;
          ecosystem_slashed_amount: number;
          ecosystem_slashed_amount_repaid: number;
          network_slash_events: number;
          network_slashed_amount: number;
          network_slashed_amount_repaid: number;
        };

        const typedFilteredItems = filteredWithStats as FilteredItemWithStats[];
        const sortedItems = sortByStandardAttributes<FilteredItemWithStats>(typedFilteredItems, sort, {
          getId: (item) => item.id,
          getCreated: (item) => item.created,
          getModified: (item) => item.modified,
          getParticipants: (item) => item.participants,
          getWeight: (item) => item.weight,
          getIssued: (item) => item.issued,
          getVerified: (item) => item.verified,
          getEcosystemSlashEvents: (item) => item.ecosystem_slash_events,
          getEcosystemSlashedAmount: (item) => item.ecosystem_slashed_amount,
          getNetworkSlashEvents: (item) => item.network_slash_events,
          getNetworkSlashedAmount: (item) => item.network_slashed_amount,
          defaultAttribute: "modified",
          defaultDirection: "desc",
        }).slice(0, limit);

        return ApiResponder.success(ctx, { schemas: sortedItems }, 200);
      }

      const query = knex("credential_schemas");
      if (participantAccount) {
        const participantSchemaIds = await this.getCredentialSchemaIdsForParticipant(participantAccount);
        if (participantSchemaIds.length === 0) {
          return ApiResponder.success(ctx, { schemas: [] }, 200);
        }
        query.whereIn("id", participantSchemaIds);
      }
      if (trId) query.where("tr_id", trId);
      if (minParticipants !== undefined && maxParticipants !== undefined && minParticipants === maxParticipants) {
        query.where("participants", minParticipants);
      }

      if (modifiedAfter) {
        const { isValidISO8601UTC } = await import("../../common/utils/date_utils");
        if (!isValidISO8601UTC(modifiedAfter)) {
          return ApiResponder.error(
            ctx,
            "Invalid modified_after format. Must be ISO 8601 UTC format (e.g., '2026-01-18T10:00:00Z' or '2026-01-18T10:00:00.000Z')",
            400
          );
        }
        const ts = new Date(modifiedAfter);
        if (Number.isNaN(ts.getTime())) {
          return ApiResponder.error(ctx, "Invalid modified_after format", 400);
        }
        query.where("modified", ">", ts.toISOString());
      }
      let onlyActiveBool: boolean | undefined;
      if (typeof onlyActive === "string") {
        onlyActiveBool = onlyActive.toLowerCase() === "true";
      } else if (typeof onlyActive === "boolean") {
        onlyActiveBool = onlyActive;
      }

      if (onlyActiveBool === true) {
        query.whereNull("archived");
      }

      if (issuerPerm !== undefined) {
        query.where("issuer_perm_management_mode", issuerPerm);
      }

      if (verifierPerm !== undefined) {
        query.where("verifier_perm_management_mode", verifierPerm);
      }
      const orderedQuery = applyOrdering(query, sort);
      const items = await orderedQuery.limit(limit * 2);

      const schemasWithStats = await Promise.all(
        items.map(async (item) => {
          let stats;
          try {
            stats = await calculateCredentialSchemaStats(item.id);
          } catch (statsError: any) {
            this.logger.warn(` Failed to calculate statistics for CS ${item.id}: ${statsError?.message || String(statsError)}`);
            stats = {
              participants: 0,
              weight: 0,
              issued: 0,
              verified: 0,
              ecosystem_slash_events: 0,
              ecosystem_slashed_amount: 0,
              ecosystem_slashed_amount_repaid: 0,
              network_slash_events: 0,
              network_slashed_amount: 0,
              network_slashed_amount_repaid: 0,
            };
          }
          const listJsonSchemaObj = normalizeJsonSchema(item.json_schema);
          return {
            ...item,
            json_schema: listJsonSchemaObj ?? undefined,
            title: item.title ?? (listJsonSchemaObj && "title" in listJsonSchemaObj ? (listJsonSchemaObj as any).title : undefined),
            description: item.description ?? (listJsonSchemaObj && "description" in listJsonSchemaObj ? (listJsonSchemaObj as any).description : undefined),
            participants: stats.participants,
            weight: stats.weight,
            issued: stats.issued,
            verified: stats.verified,
            ecosystem_slash_events: stats.ecosystem_slash_events,
            ecosystem_slashed_amount: stats.ecosystem_slashed_amount,
            ecosystem_slashed_amount_repaid: stats.ecosystem_slashed_amount_repaid,
            network_slash_events: stats.network_slash_events,
            network_slashed_amount: stats.network_slashed_amount,
            network_slashed_amount_repaid: stats.network_slashed_amount_repaid,
          };
        })
      );

      const cleanItems = schemasWithStats.map(({ is_active, ...rest }) => rest);

      let filteredItems = cleanItems;

      if (minParticipants !== undefined && maxParticipants !== undefined && minParticipants === maxParticipants) {
        filteredItems = [];
      } else {
        if (minParticipants !== undefined) {
          filteredItems = filteredItems.filter((s) => s.participants >= minParticipants);
        }
        if (maxParticipants !== undefined) {
          filteredItems = filteredItems.filter((s) => s.participants < maxParticipants);
        }
      }
      if (minWeight !== undefined && maxWeight !== undefined && minWeight === maxWeight) {
        filteredItems = [];
      } else {
        if (minWeight !== undefined) {
          const minWeightNum = Number(minWeight);
          filteredItems = filteredItems.filter((s) => {
            const sWeight = typeof s.weight === 'number' ? s.weight : Number(s.weight || 0);
            return sWeight >= minWeightNum;
          });
        }
        if (maxWeight !== undefined) {
          const maxWeightNum = Number(maxWeight);
          filteredItems = filteredItems.filter((s) => {
            const sWeight = typeof s.weight === 'number' ? s.weight : Number(s.weight || 0);
            return sWeight < maxWeightNum;
          });
        }
      }
      if (minIssued !== undefined && maxIssued !== undefined && minIssued === maxIssued) {
        filteredItems = [];
      } else {
        if (minIssued !== undefined) {
          const minIssuedNum = Number(minIssued);
          filteredItems = filteredItems.filter((s) => s.issued >= minIssuedNum);
        }
        if (maxIssued !== undefined) {
          const maxIssuedNum = Number(maxIssued);
          filteredItems = filteredItems.filter((s) => s.issued < maxIssuedNum);
        }
      }
      if (minVerified !== undefined && maxVerified !== undefined && minVerified === maxVerified) {
        filteredItems = [];
      } else {
        if (minVerified !== undefined) {
          const minVerifiedNum = Number(minVerified);
          filteredItems = filteredItems.filter((s) => s.verified >= minVerifiedNum);
        }
        if (maxVerified !== undefined) {
          const maxVerifiedNum = Number(maxVerified);
          filteredItems = filteredItems.filter((s) => s.verified < maxVerifiedNum);
        }
      }
      if (minEcosystemSlashEvents !== undefined && maxEcosystemSlashEvents !== undefined && minEcosystemSlashEvents === maxEcosystemSlashEvents) {
        filteredItems = [];
      } else {
        if (minEcosystemSlashEvents !== undefined) {
          filteredItems = filteredItems.filter((s) => s.ecosystem_slash_events >= minEcosystemSlashEvents);
        }
        if (maxEcosystemSlashEvents !== undefined) {
          filteredItems = filteredItems.filter((s) => s.ecosystem_slash_events < maxEcosystemSlashEvents);
        }
      }
      if (minNetworkSlashEvents !== undefined && maxNetworkSlashEvents !== undefined && minNetworkSlashEvents === maxNetworkSlashEvents) {
        filteredItems = [];
      } else {
        if (minNetworkSlashEvents !== undefined) {
          filteredItems = filteredItems.filter((s) => s.network_slash_events >= minNetworkSlashEvents);
        }
        if (maxNetworkSlashEvents !== undefined) {
          filteredItems = filteredItems.filter((s) => s.network_slash_events < maxNetworkSlashEvents);
        }
      }

      type SchemaWithStats = typeof filteredItems[0];
      const sortedItems = sortByStandardAttributes<SchemaWithStats>(filteredItems, sort, {
        getId: (item) => item.id,
        getCreated: (item) => item.created,
        getModified: (item) => item.modified,
        getParticipants: (item) => item.participants,
        getWeight: (item) => item.weight,
        getIssued: (item) => item.issued,
        getVerified: (item) => item.verified,
        getEcosystemSlashEvents: (item) => item.ecosystem_slash_events,
        getEcosystemSlashedAmount: (item) => item.ecosystem_slashed_amount,
        getNetworkSlashEvents: (item) => item.network_slash_events,
        getNetworkSlashedAmount: (item) => item.network_slashed_amount,
        defaultAttribute: "modified",
        defaultDirection: "desc",
      }).slice(0, limit);

      return ApiResponder.success(ctx, { schemas: sortedItems }, 200);
    } catch (err: any) {
      this.logger.error("Error in CredentialSchema list:", err);
      return ApiResponder.error(ctx, "Internal Server Error", 500);
    }
  }

  @Action({
    rest: "GET JsonSchema",
    params: {
      id: { type: "number", integer: true, positive: true },
    },
  })
  async JsonSchema(ctx: Context<{ id: number }>) {
    try {
      const { id } = ctx.params;
      const blockHeight = (ctx.meta as any)?.blockHeight;

      if (typeof blockHeight === "number") {
        const hasHeightColumn = await checkHeightColumnExists();
        let query = knex("credential_schema_history")
          .select("json_schema")
          .where({ credential_schema_id: id });

        if (hasHeightColumn) {
          query = query.where("height", "<=", blockHeight)
            .orderBy("height", "desc");
        }
        query = query.orderBy("created_at", "desc");

        const historyRecord = await query.first();

        if (!historyRecord) {
          return ApiResponder.error(ctx, `Credential schema with id=${id} not found`, 404);
        }

        const historySchemaObj = normalizeJsonSchema(historyRecord.json_schema);
        if (!historySchemaObj) {
          return ApiResponder.error(ctx, `Credential schema with id=${id} has no valid JSON schema`, 404);
        }
        (ctx.meta as Record<string, unknown>).$responseType = "application/json; charset=utf-8";
        return ApiResponder.success(ctx, normalizedJsonSchemaString(historySchemaObj), 200);
      }

      const schemaRecord = await knex("credential_schemas")
        .select("json_schema")
        .where({ id })
        .first();

      if (!schemaRecord) {
        return ApiResponder.error(ctx, `Credential schema with id=${id} not found`, 404);
      }

      const schemaObj = normalizeJsonSchema(schemaRecord.json_schema);
      if (!schemaObj) {
        return ApiResponder.error(ctx, `Credential schema with id=${id} has no valid JSON schema`, 404);
      }
      (ctx.meta as Record<string, unknown>).$responseType = "application/json; charset=utf-8";
      return ApiResponder.success(ctx, normalizedJsonSchemaString(schemaObj), 200);
    } catch (err: any) {
      this.logger.error("Error in renderJsonSchema:", err);
      return ApiResponder.error(ctx, "Internal Server Error", 500);
    }
  }

  @Action()
  public async getParams(ctx: Context) {
    const { getModuleParamsAction } = await import("../../common/utils/params_service");
    return getModuleParamsAction(ctx, ModulesParamsNamesTypes.CS, MODULE_DISPLAY_NAMES.CREDENTIAL_SCHEMA);
  }

  @Action({
    name: "getHistory",
    params: {
      id: { type: "number", integer: true, positive: true },
      response_max_size: { type: "number", optional: true, default: 64 },
      transaction_timestamp_older_than: { type: "string", optional: true },
    },
  })
  async getHistory(ctx: Context<{ id: number; response_max_size?: number; transaction_timestamp_older_than?: string }>) {
    try {
      const { id, response_max_size: responseMaxSize = 64, transaction_timestamp_older_than: transactionTimestampOlderThan } = ctx.params;
      
      if (transactionTimestampOlderThan) {
        const { isValidISO8601UTC } = await import("../../common/utils/date_utils");
        if (!isValidISO8601UTC(transactionTimestampOlderThan)) {
          return ApiResponder.error(
            ctx,
            "Invalid transaction_timestamp_older_than format. Must be ISO 8601 UTC format (e.g., '2026-01-18T10:00:00Z' or '2026-01-18T10:00:00.000Z')",
            400
          );
        }
        const timestampDate = new Date(transactionTimestampOlderThan);
        if (Number.isNaN(timestampDate.getTime())) {
          return ApiResponder.error(ctx, "Invalid transaction_timestamp_older_than format", 400);
        }
      }
      
      const atBlockHeight = (ctx.meta as any)?.$headers?.["at-block-height"] || (ctx.meta as any)?.$headers?.["At-Block-Height"];

      const schemaExists = await knex("credential_schemas").where({ id }).first();
      if (!schemaExists) {
        return ApiResponder.error(ctx, `Credential schema with id=${id} not found`, 404);
      }

      const { buildActivityTimeline } = await import("../../common/utils/activity_timeline_helper");
      const activity = await buildActivityTimeline(
        {
          entityType: "CredentialSchema",
          historyTable: "credential_schema_history",
          idField: "credential_schema_id",
          entityId: id,
          msgTypePrefixes: ["/verana.cs.v1", "/veranablockchain.credentialschema"],
        },
        {
          responseMaxSize,
          transactionTimestampOlderThan,
          atBlockHeight,
        }
      );

      const result = {
        entity_type: "CredentialSchema",
        entity_id: String(id),
        activity: activity || [],
      };

      return ApiResponder.success(ctx, result, 200);
    } catch (err: any) {
      this.logger.error("Error fetching CredentialSchema history:", err);
      return ApiResponder.error(ctx, "Internal Server Error", 500);
    }
  }


  private async getCredentialSchemaIdsForParticipant(account: string): Promise<number[]> {
    const controllerTrRows = await knex("trust_registry")
      .where("controller", account)
      .select("id");
    const controllerTrIds = controllerTrRows.map((r: { id: number }) => r.id);
    const schemaIdsFromController =
      controllerTrIds.length === 0
        ? []
        : (await knex("credential_schemas").whereIn("tr_id", controllerTrIds).select("id")).map((r: { id: number }) => r.id);

    const granteeRows = await knex("permissions").where("grantee", account).distinct("schema_id");
    const schemaIdsFromGrantee = granteeRows
      .map((r: { schema_id: string }) => (r.schema_id != null ? parseFloat(r.schema_id) : null))
      .filter((id): id is number => id != null && !Number.isNaN(id));

    return [...new Set([...schemaIdsFromController, ...schemaIdsFromGrantee])];
  }

  private async getCredentialSchemaIdsForParticipantAtHeight(account: string, blockHeight: number): Promise<number[]> {
    const trHistoryRows = await knex("trust_registry_history")
      .where("height", "<=", blockHeight)
      .where("controller", account)
      .select("tr_id");
    const controllerTrIds = [...new Set(trHistoryRows.map((r: { tr_id: number }) => r.tr_id))];

    let schemaIdsFromController: number[] = [];
    if (controllerTrIds.length > 0) {
      const cshRanked = knex("credential_schema_history")
        .select("credential_schema_id", "tr_id")
        .select(
          knex.raw(
            "ROW_NUMBER() OVER (PARTITION BY credential_schema_id ORDER BY height DESC, created_at DESC) as rn"
          )
        )
        .where("height", "<=", blockHeight)
        .as("ranked");
      const latestCsh = await knex.from(cshRanked).where("rn", 1).whereIn("tr_id", controllerTrIds).select("credential_schema_id");
      schemaIdsFromController = latestCsh.map((r: { credential_schema_id: number }) => r.credential_schema_id);
    }

    const granteePermRows = await knex("permission_history")
      .where("height", "<=", blockHeight)
      .where("grantee", account)
      .distinct("schema_id");
    const schemaIdsFromGrantee = granteePermRows
      .map((r: { schema_id: number }) => r.schema_id)
      .filter((id): id is number => id != null);

    return [...new Set([...schemaIdsFromController, ...schemaIdsFromGrantee])];
  }
}
