import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import { ModulesParamsNamesTypes, MODULE_DISPLAY_NAMES, SERVICE } from "../../common";
import { validateParticipantParam } from "../../common/utils/accountValidation";
import ApiResponder from "../../common/utils/apiResponse";
import knex from "../../common/utils/db_connection";
import { applyOrdering, validateSortParameter, sortByStandardAttributes, parseSortParameter } from "../../common/utils/query_ordering";
import { calculateCredentialSchemaStats, calculateCredentialSchemaStatsBatch } from "./cs_stats";
import { calculateTrustRegistryStats } from "../crawl-tr/tr_stats";
import { extractTitleDescriptionFromJsonSchema } from "../../modules/cs-height-sync/cs_height_sync_helpers";
import { overrideSchemaIdInString } from "../../common/utils/schema_id_normalizer";
import { isValidISO8601UTC } from "../../common/utils/date_utils";
import { getModuleParamsAction } from "../../common/utils/params_service";
import { buildActivityTimeline } from "../../common/utils/activity_timeline_helper";

let heightColumnExistsCache: boolean | null = null;
let historyMetricColumnsExistCache: boolean | null = null;

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

async function checkHistoryMetricColumnsExist(): Promise<boolean> {
  if (historyMetricColumnsExistCache !== null) {
    return historyMetricColumnsExistCache;
  }
  try {
    const requiredColumns = [
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
      "network_slashed_amount_repaid",
    ];
    const checks = await Promise.all(requiredColumns.map((col) => knex.schema.hasColumn("credential_schema_history", col)));
    historyMetricColumnsExistCache = checks.every(Boolean);
    return historyMetricColumnsExistCache;
  } catch {
    historyMetricColumnsExistCache = false;
    return false;
  }
}

function ensureSchemaString(js: unknown): string {
  if (js == null) return "";
  if (typeof js === "string") return js;
  if (typeof js === "object") return JSON.stringify(js);
  return String(js);
}

function getStoredSchemaString(js: unknown): string {
  if (js == null) return "";
  if (typeof js === "string") return js;
  if (typeof js === "object") return JSON.stringify(js);
  return String(js);
}

function normalizeArchivedValue(value: unknown): string | null {
  if (value == null) return null;
  if (value instanceof Date) {
    return Number.isFinite(value.getTime()) ? value.toISOString() : null;
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) return null;
    const lowered = trimmed.toLowerCase();
    if (
      lowered === "null" ||
      lowered === "undefined" ||
      lowered === "none" ||
      lowered === "false" ||
      lowered === "0"
    ) {
      return null;
    }
    if (trimmed.startsWith("0001-01-01T00:00:00")) {
      return null;
    }
    const parsed = new Date(trimmed);
    return Number.isFinite(parsed.getTime()) ? parsed.toISOString() : null;
  }
  return null;
}

function deriveIsActiveFromArchived(value: unknown): boolean {
  return normalizeArchivedValue(value) == null;
}

function normalizeValueForDiff(key: string, value: unknown): unknown {
  if (value === undefined) return null;
  if (value === null) return null;
  if (key === "json_schema") return ensureSchemaString(value);
  if (key === "archived") return normalizeArchivedValue(value);
  if (key === "created" || key === "modified") {
    if (value instanceof Date) {
      return Number.isFinite(value.getTime()) ? value.toISOString() : null;
    }
    if (typeof value === "string") {
      const trimmed = value.trim();
      if (!trimmed) return null;
      const parsed = new Date(trimmed);
      return Number.isFinite(parsed.getTime()) ? parsed.toISOString() : trimmed;
    }
    return String(value);
  }
  if (typeof value === "object") return JSON.stringify(value);
  return value;
}

function buildChangedFields(
  existing: Record<string, unknown>,
  nextValues: Record<string, unknown>
): Record<string, unknown> {
  const changes: Record<string, unknown> = {};
  for (const [key, nextValue] of Object.entries(nextValues)) {
    if (key === "id") continue;
    const prevNorm = normalizeValueForDiff(key, existing[key]);
    const nextNorm = normalizeValueForDiff(key, nextValue);
    if (prevNorm !== nextNorm) {
      changes[key] = nextValue;
    }
  }
  return changes;
}

function toFiniteNumber(value: unknown): number {
  const num = typeof value === "number" ? value : Number(value ?? 0);
  return Number.isFinite(num) ? num : 0;
}

function applyHalfOpenRangeToQuery(qb: any, column: string, minValue?: number, maxValue?: number) {
  if (minValue !== undefined && maxValue !== undefined && minValue === maxValue) {
    qb.whereRaw("1 = 0");
    return;
  }
  if (minValue !== undefined) qb.where(column, ">=", minValue);
  if (maxValue !== undefined) qb.where(column, "<", maxValue);
}

function applyHalfOpenRangeToRows<T>(
  rows: T[],
  minValue: number | string | undefined,
  maxValue: number | string | undefined,
  readValue: (row: T) => number
): T[] {
  if (minValue !== undefined && maxValue !== undefined && minValue === maxValue) {
    return [];
  }

  let filtered = rows;
  if (minValue !== undefined) {
    const minNum = Number(minValue);
    filtered = filtered.filter((row) => readValue(row) >= minNum);
  }
  if (maxValue !== undefined) {
    const maxNum = Number(maxValue);
    filtered = filtered.filter((row) => readValue(row) < maxNum);
  }
  return filtered;
}

function sortCredentialSchemaRows<T extends {
  id: number;
  created: string;
  modified: string;
  participants: number;
  weight: number;
  issued: number;
  verified: number;
  ecosystem_slash_events: number;
  ecosystem_slashed_amount: number;
  network_slash_events: number;
  network_slashed_amount: number;
}>(rows: T[], sort: string | undefined, limit: number): T[] {
  return sortByStandardAttributes<T>(rows, sort, {
    getId: (item) => item.id,
    getCreated: (item) => item.created,
    getModified: (item) => item.modified,
    getParticipants: (item) => item.participants,
    getParticipantsEcosystem: (item: any) => item.participants_ecosystem,
    getParticipantsIssuerGrantor: (item: any) => item.participants_issuer_grantor,
    getParticipantsIssuer: (item: any) => item.participants_issuer,
    getParticipantsVerifierGrantor: (item: any) => item.participants_verifier_grantor,
    getParticipantsVerifier: (item: any) => item.participants_verifier,
    getParticipantsHolder: (item: any) => item.participants_holder,
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
}

const SQL_SORTABLE_CREDENTIAL_SCHEMA_ATTRIBUTES = new Set<string>([
  "id",
  "modified",
  "created",
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
  "network_slash_events",
  "network_slashed_amount",
]);

function applyCredentialSchemaSqlSort(
  query: any,
  sort: string | undefined
): { fullyApplied: boolean } {
  if (!sort || typeof sort !== "string" || !sort.trim()) {
    query.orderBy("modified", "desc").orderBy("id", "desc");
    return { fullyApplied: true };
  }

  const sortOrders = parseSortParameter(sort);
  let hasIdSort = false;
  let fullyApplied = true;
  for (const { attribute, direction } of sortOrders) {
    if (!SQL_SORTABLE_CREDENTIAL_SCHEMA_ATTRIBUTES.has(attribute)) {
      fullyApplied = false;
      continue;
    }
    query.orderBy(attribute, direction);
    if (attribute === "id") hasIdSort = true;
  }

  if (!hasIdSort) {
    query.orderBy("id", "desc");
  }

  return { fullyApplied };
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
        const numericBlockchainId = blockchainSchemaId != null ? Number(blockchainSchemaId) : NaN;
        if (Number.isInteger(numericBlockchainId) && numericBlockchainId > 0) {
          const existingSchemas = await trx("credential_schemas")
            .select("*")
            .where({ id: numericBlockchainId })
            .limit(1);
          if (existingSchemas.length > 0) {
            existingSchema = existingSchemas[0];
            this.logger.info(`Found existing schema with blockchain_id=${blockchainSchemaId} (database_id=${existingSchema.id}). Updating instead of inserting.`);
          }
        }
        
        let finalRecord: any;
        
        if (existingSchema) {
          const updates: Record<string, any> = {
            ...schemaPayload,
            modified: schemaPayload.modified || new Date(),
          };
          if ("archived" in updates) {
            updates.archived = normalizeArchivedValue(updates.archived);
            updates.is_active = deriveIsActiveFromArchived(updates.archived);
          } else if ("is_active" in updates && !("archived" in updates)) {
            updates.is_active = deriveIsActiveFromArchived(existingSchema.archived);
          }
          if (updates.json_schema != null) {
            const rawString = ensureSchemaString(updates.json_schema);
            updates.json_schema = overrideSchemaIdInString(rawString, existingSchema.id);
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
          const insertPayload = { ...schemaPayload };
          if ("archived" in insertPayload) {
            insertPayload.archived = normalizeArchivedValue(insertPayload.archived);
          } else {
            insertPayload.archived = null;
          }
          insertPayload.is_active = deriveIsActiveFromArchived(insertPayload.archived);
          const rawSchemaString = insertPayload.json_schema != null ? ensureSchemaString(insertPayload.json_schema) : "";
          insertPayload.json_schema = rawSchemaString || "{}";
          const [inserted] = await trx("credential_schemas")
            .insert(insertPayload)
            .returning("*");
          finalRecord = inserted;
          if (rawSchemaString && inserted.id != null) {
            const withOverriddenId = overrideSchemaIdInString(rawSchemaString, inserted.id);
            const [updated] = await trx("credential_schemas")
              .where({ id: inserted.id })
              .update({ json_schema: withOverriddenId })
              .returning("*");
            if (updated) finalRecord = updated;
          }
        }
        try {
          const { title: titleFromSchema, description: descriptionFromSchema } = extractTitleDescriptionFromJsonSchema(finalRecord?.json_schema);
          const metaUpdates: Record<string, string> = {};
          if (titleFromSchema !== null) metaUpdates.title = titleFromSchema;
          if (descriptionFromSchema !== null) metaUpdates.description = descriptionFromSchema;
          if (Object.keys(metaUpdates).length > 0) {
            const [updatedWithMeta] = await trx("credential_schemas").where({ id: finalRecord.id }).update(metaUpdates).returning("*");
            if (updatedWithMeta) finalRecord = updatedWithMeta;
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

      const existing = await knex("credential_schemas").where({ id: payload.id }).first();
      
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

      if ("archived" in updatesWithoutHeight) {
        updatesWithoutHeight.archived = normalizeArchivedValue(updatesWithoutHeight.archived);
        updatesWithoutHeight.is_active = deriveIsActiveFromArchived(updatesWithoutHeight.archived);
      } else if ("is_active" in updatesWithoutHeight) {
        // Keep invariant: is_active is derived from archived, not accepted as independent state.
        updatesWithoutHeight.is_active = deriveIsActiveFromArchived(existing.archived);
      }

      if (updatesWithoutHeight.json_schema != null) {
        const rawString = ensureSchemaString(updatesWithoutHeight.json_schema);
        updatesWithoutHeight.json_schema = overrideSchemaIdInString(rawString, existing.id);
      }

      let [updated] = await knex("credential_schemas")
        .where({ id: existing.id })
        .update(updatesWithoutHeight)
        .returning("*");

      try {
        const { title: titleFromSchema, description: descriptionFromSchema } = extractTitleDescriptionFromJsonSchema(updated?.json_schema);
        const metaUpdates: Record<string, string> = {};
        if (titleFromSchema !== null && titleFromSchema !== updated.title) metaUpdates.title = titleFromSchema;
        if (descriptionFromSchema !== null && descriptionFromSchema !== updated.description) metaUpdates.description = descriptionFromSchema;
        if (Object.keys(metaUpdates).length > 0) {
          const [updatedWithMeta] = await knex("credential_schemas").where({ id: existing.id }).update(metaUpdates).returning("*");
          if (updatedWithMeta) updated = updatedWithMeta;
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
      return ApiResponder.error(ctx, "Internal Server Error", 500);
    }
  }

  @Action({ name: "archive" })
  async archive(ctx: Context<{ payload: any }>) {
    try {
      const { id, archive: archiveRaw, modified } = ctx.params.payload;
      if (!id || archiveRaw === undefined) {
        return ApiResponder.error(ctx, "Missing required parameters: id and archive", 400);
      }
      let archiveFlag: boolean;
      if (typeof archiveRaw === "boolean") {
        archiveFlag = archiveRaw;
      } else if (typeof archiveRaw === "string") {
        const normalizedArchive = archiveRaw.trim().toLowerCase();
        if (normalizedArchive === "true") archiveFlag = true;
        else if (normalizedArchive === "false") archiveFlag = false;
        else return ApiResponder.error(ctx, "Invalid archive value: expected boolean", 400);
      } else {
        return ApiResponder.error(ctx, "Invalid archive value: expected boolean", 400);
      }

      const schemaRecord = await knex("credential_schemas").where({ id }).first();
      if (!schemaRecord) {
        return ApiResponder.error(ctx, `Credential schema with id=${id} not found`, 404);
      }

      if (archiveFlag && schemaRecord.archived !== null) {
        return ApiResponder.error(ctx, `Credential schema id=${id} is already archived`, 400);
      }
      if (!archiveFlag && schemaRecord.archived === null) {
        return ApiResponder.error(ctx, `Credential schema id=${id} is already unarchived`, 400);
      }

      const updates: Record<string, any> = {
        archived: archiveFlag ? modified : null,
        is_active: archiveFlag === false,
        modified,
      };

      const { height } = ctx.params.payload;
      const blockHeight = Number(height) || 0;

      const [updated] = await knex("credential_schemas")
        .where({ id })
        .update(updates)
        .returning("*");

      this.logger.info(
        `[CS] ${archiveFlag ? "Archived" : "Unarchived"} schema id=${id} at height=${blockHeight} (is_active=${updated?.is_active}, archived=${updated?.archived ?? "null"})`
      );

      const hasHeightColumn = await checkHeightColumnExists();
      const historyRow = mapToHistoryRow(updated, {
        changes: JSON.stringify({
          archived: updated.archived,
        }),
        action: archiveFlag ? "archive" : "unarchive",
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
      return ApiResponder.error(ctx, "Internal Server Error", 500);
    }
  }


  @Action({ name: "syncFromLedger" })
  async syncFromLedger(
    ctx: Context<{ ledgerResponse: { schema?: Record<string, unknown> }; blockHeight: number }>
  ) {
    try {
      const { ledgerResponse, blockHeight } = ctx.params;
      const schema = ledgerResponse?.schema;
      if (!schema || typeof schema !== "object") {
        return ApiResponder.error(ctx, "Missing or invalid ledger schema", 400);
      }
      const id = Number(schema.id ?? schema.credential_schema_id);
      if (!Number.isInteger(id) || id <= 0) {
        return ApiResponder.error(ctx, "Invalid schema id from ledger", 400);
      }
      const blockHeightNum = Number(blockHeight) || 0;
      const jsonSchemaRaw = schema.json_schema ?? schema.jsonSchema;
      const jsonSchemaStr = ensureSchemaString(jsonSchemaRaw ?? "{}");
      const normalizedArchived = normalizeArchivedValue(schema.archived);
      const derivedIsActive = deriveIsActiveFromArchived(normalizedArchived);

      const payload: Record<string, unknown> = {
        id,
        tr_id: schema.tr_id ?? schema.trId ?? null,
        json_schema: jsonSchemaStr,
        deposit: Number(schema.deposit ?? 0),
        issuer_grantor_validation_validity_period: Number(schema.issuer_grantor_validation_validity_period ?? 0),
        verifier_grantor_validation_validity_period: Number(schema.verifier_grantor_validation_validity_period ?? 0),
        issuer_validation_validity_period: Number(schema.issuer_validation_validity_period ?? 0),
        verifier_validation_validity_period: Number(schema.verifier_validation_validity_period ?? 0),
        holder_validation_validity_period: Number(schema.holder_validation_validity_period ?? 0),
        issuer_perm_management_mode: String(schema.issuer_perm_management_mode ?? schema.issuerPermManagementMode ?? "MODE_UNSPECIFIED"),
        verifier_perm_management_mode: String(schema.verifier_perm_management_mode ?? schema.verifierPermManagementMode ?? "MODE_UNSPECIFIED"),
        archived: normalizedArchived,
        created: schema.created ?? null,
        modified: schema.modified ?? null,
        is_active: derivedIsActive,
      };
      if (typeof schema.title === "string") payload.title = schema.title;
      if (typeof schema.description === "string") payload.description = schema.description;
      const existing = await knex("credential_schemas").where({ id }).first();
      const updates: Record<string, unknown> = { ...payload };
      delete (updates as Record<string, unknown>).id;
      if (updates.json_schema != null) {
        (updates as Record<string, unknown>).json_schema = overrideSchemaIdInString(
          String(updates.json_schema),
          id
        );
      }
      let finalRecord: Record<string, unknown>;
      let historyChangesForUpdate: Record<string, unknown> = {};
      if (existing) {
        const previousIsActive = Boolean((existing as Record<string, unknown>).is_active);
        const changedUpdates = buildChangedFields(existing as Record<string, unknown>, updates);
        historyChangesForUpdate = { ...changedUpdates };
        if (Object.keys(changedUpdates).length === 0) {
          finalRecord = existing as Record<string, unknown>;
        } else {
        const [updated] = await knex("credential_schemas")
          .where({ id })
          .update(changedUpdates)
          .returning("*");
        if (!updated) {
          return ApiResponder.error(ctx, "Update failed", 500);
        }
        finalRecord = updated as Record<string, unknown>;
        const nextIsActive = Boolean(finalRecord.is_active);
        if (previousIsActive !== nextIsActive) {
          this.logger.info(
            `[CS] syncFromLedger activation transition schema id=${id} at height=${blockHeightNum}: ${previousIsActive} -> ${nextIsActive} (archived=${String(finalRecord.archived ?? "null")})`
          );
        }
        }
      } else {
        const insertPayload = { ...payload };
        (insertPayload as Record<string, unknown>).json_schema = (insertPayload as Record<string, unknown>).json_schema ?? "{}";
        const [inserted] = await knex("credential_schemas")
          .insert(insertPayload)
          .returning("*");
        if (!inserted) {
          return ApiResponder.error(ctx, "Insert failed", 500);
        }
        finalRecord = inserted as Record<string, unknown>;
        const rawStr = String(finalRecord.json_schema ?? "{}");
        if (rawStr && finalRecord.id != null) {
          const withOverride = overrideSchemaIdInString(rawStr, Number(finalRecord.id));
          const [updated] = await knex("credential_schemas")
            .where({ id: finalRecord.id })
            .update({ json_schema: withOverride })
            .returning("*");
          if (updated) finalRecord = updated as Record<string, unknown>;
        }
      }
      try {
        const { title: titleFromSchema, description: descriptionFromSchema } = extractTitleDescriptionFromJsonSchema(finalRecord.json_schema);
        const metaUpdates: Record<string, string> = {};
        if (titleFromSchema !== null) metaUpdates.title = titleFromSchema;
        if (descriptionFromSchema !== null) metaUpdates.description = descriptionFromSchema;
        if (Object.keys(metaUpdates).length > 0) {
          const metaDiffs = buildChangedFields(finalRecord, metaUpdates);
          if (existing) Object.assign(historyChangesForUpdate, metaDiffs);
          if (Object.keys(metaDiffs).length === 0) {
            // Skip no-op metadata write.
          } else {
          const [updatedWithMeta] = await knex("credential_schemas")
            .where({ id: finalRecord.id })
            .update(metaDiffs)
            .returning("*");
          if (updatedWithMeta) finalRecord = updatedWithMeta as Record<string, unknown>;
          }
        }
      } catch (err: any) {
        this.logger.warn(`Failed to persist title/description for CS ${finalRecord.id} in syncFromLedger: ${err?.message || err}`);
      }
      const hasHeightColumn = await checkHeightColumnExists();
      const historyAction = existing ? "update" : "create";
      if (!existing || Object.keys(historyChangesForUpdate).length > 0) {
        const creationChanges: Record<string, unknown> = {};
        for (const [key, value] of Object.entries(finalRecord)) {
          if (value !== null && value !== undefined && key !== "id" && key !== "is_active") {
            creationChanges[key] = value;
          }
        }
        const changesForHistory = existing ? historyChangesForUpdate : creationChanges;
        const historyRow = mapToHistoryRow(finalRecord as any, {
          changes: Object.keys(changesForHistory).length > 0 ? JSON.stringify(changesForHistory) : null,
          action: historyAction,
          height: blockHeightNum,
        }, hasHeightColumn);
        await knex("credential_schema_history").insert(historyRow);
      }
      return ApiResponder.success(ctx, { success: true, result: finalRecord }, 200);
    } catch (err: any) {
      this.logger.error("Error in CredentialSchema syncFromLedger:", err);
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

        const storedSchemaString = getStoredSchemaString(historyRecord.json_schema);
        const historicalSchema = {
          id: historyRecord.credential_schema_id,
          tr_id: historyRecord.tr_id,
          json_schema: storedSchemaString,
          title: historyRecord.title ?? undefined,
          description: historyRecord.description ?? undefined,
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
            participants_ecosystem: 0,
            participants_issuer_grantor: 0,
            participants_issuer: 0,
            participants_verifier_grantor: 0,
            participants_verifier: 0,
            participants_holder: 0,
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
            participants_ecosystem: stats.participants_ecosystem,
            participants_issuer_grantor: stats.participants_issuer_grantor,
            participants_issuer: stats.participants_issuer,
            participants_verifier_grantor: stats.participants_verifier_grantor,
            participants_verifier: stats.participants_verifier,
            participants_holder: stats.participants_holder,
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
      delete schemaRecord?.is_active;
      const storedSchemaString = getStoredSchemaString(schemaRecord.json_schema);

      let stats;
      try {
        stats = await calculateCredentialSchemaStats(id);
      } catch (statsError: any) {
        this.logger.warn(` Failed to calculate statistics for CS ${id}: ${statsError?.message || String(statsError)}`);
        stats = {
          participants: 0,
          participants_ecosystem: 0,
          participants_issuer_grantor: 0,
          participants_issuer: 0,
          participants_verifier_grantor: 0,
          participants_verifier: 0,
          participants_holder: 0,
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
          json_schema: storedSchemaString,
          title: schemaRecord.title ?? undefined,
          description: schemaRecord.description ?? undefined,
          participants: stats.participants,
          participants_ecosystem: stats.participants_ecosystem,
          participants_issuer_grantor: stats.participants_issuer_grantor,
          participants_issuer: stats.participants_issuer,
          participants_verifier_grantor: stats.participants_verifier_grantor,
          participants_verifier: stats.participants_verifier,
          participants_holder: stats.participants_holder,
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
      min_participants_ecosystem: { type: "number", optional: true },
      max_participants_ecosystem: { type: "number", optional: true },
      min_participants_issuer_grantor: { type: "number", optional: true },
      max_participants_issuer_grantor: { type: "number", optional: true },
      min_participants_issuer: { type: "number", optional: true },
      max_participants_issuer: { type: "number", optional: true },
      min_participants_verifier_grantor: { type: "number", optional: true },
      max_participants_verifier_grantor: { type: "number", optional: true },
      min_participants_verifier: { type: "number", optional: true },
      max_participants_verifier: { type: "number", optional: true },
      min_participants_holder: { type: "number", optional: true },
      max_participants_holder: { type: "number", optional: true },
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
    min_participants_ecosystem?: number;
    max_participants_ecosystem?: number;
    min_participants_issuer_grantor?: number;
    max_participants_issuer_grantor?: number;
    min_participants_issuer?: number;
    max_participants_issuer?: number;
    min_participants_verifier_grantor?: number;
    max_participants_verifier_grantor?: number;
    min_participants_verifier?: number;
    max_participants_verifier?: number;
    min_participants_holder?: number;
    max_participants_holder?: number;
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
        min_participants_ecosystem: minParticipantsEcosystem,
        max_participants_ecosystem: maxParticipantsEcosystem,
        min_participants_issuer_grantor: minParticipantsIssuerGrantor,
        max_participants_issuer_grantor: maxParticipantsIssuerGrantor,
        min_participants_issuer: minParticipantsIssuer,
        max_participants_issuer: maxParticipantsIssuer,
        min_participants_verifier_grantor: minParticipantsVerifierGrantor,
        max_participants_verifier_grantor: maxParticipantsVerifierGrantor,
        min_participants_verifier: minParticipantsVerifier,
        max_participants_verifier: maxParticipantsVerifier,
        min_participants_holder: minParticipantsHolder,
        max_participants_holder: maxParticipantsHolder,
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
      let modifiedAfterIso: string | undefined;
      if (modifiedAfter) {
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
        modifiedAfterIso = ts.toISOString();
      }
      let onlyActiveBool: boolean | undefined;
      if (typeof onlyActive === "string") {
        onlyActiveBool = onlyActive.toLowerCase() === "true";
      } else if (typeof onlyActive === "boolean") {
        onlyActiveBool = onlyActive;
      }

      if (typeof blockHeight === "number") {
        const hasHeightColumn = await checkHeightColumnExists();
        const hasHistoryMetricColumns = await checkHistoryMetricColumnsExist();
        let schemaIdsAtHeight: number[];
        if (participantAccount) {
          schemaIdsAtHeight = await this.getCredentialSchemaIdsForParticipantAtHeight(participantAccount, blockHeight);
          if (schemaIdsAtHeight.length === 0) {
            return ApiResponder.success(ctx, { schemas: [] }, 200);
          }
        } else {
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

        const applyMetricRangeFilters = (qb: any) => {
          if (!hasHistoryMetricColumns) return;
          applyHalfOpenRangeToQuery(qb, "participants", minParticipants, maxParticipants);
          applyHalfOpenRangeToQuery(qb, "participants_ecosystem", minParticipantsEcosystem, maxParticipantsEcosystem);
          applyHalfOpenRangeToQuery(qb, "participants_issuer_grantor", minParticipantsIssuerGrantor, maxParticipantsIssuerGrantor);
          applyHalfOpenRangeToQuery(qb, "participants_issuer", minParticipantsIssuer, maxParticipantsIssuer);
          applyHalfOpenRangeToQuery(qb, "participants_verifier_grantor", minParticipantsVerifierGrantor, maxParticipantsVerifierGrantor);
          applyHalfOpenRangeToQuery(qb, "participants_verifier", minParticipantsVerifier, maxParticipantsVerifier);
          applyHalfOpenRangeToQuery(qb, "participants_holder", minParticipantsHolder, maxParticipantsHolder);
          applyHalfOpenRangeToQuery(qb, "weight", minWeight, maxWeight);
          applyHalfOpenRangeToQuery(qb, "issued", minIssued, maxIssued);
          applyHalfOpenRangeToQuery(qb, "verified", minVerified, maxVerified);
          applyHalfOpenRangeToQuery(qb, "ecosystem_slash_events", minEcosystemSlashEvents, maxEcosystemSlashEvents);
          applyHalfOpenRangeToQuery(qb, "network_slash_events", minNetworkSlashEvents, maxNetworkSlashEvents);
        };

        let items: any[] = [];
        if (String((knex as any)?.client?.config?.client || "").includes("pg")) {
          const latestSub = knex("credential_schema_history as csh")
            .distinctOn("csh.credential_schema_id")
            .select("csh.*")
            .whereIn("csh.credential_schema_id", schemaIdsAtHeight)
            .modify((qb) => {
              if (hasHeightColumn) qb.where("csh.height", "<=", blockHeight);
              if (trId) qb.where("csh.tr_id", trId);
              if (modifiedAfterIso) qb.where("csh.modified", ">", modifiedAfterIso);
              if (onlyActiveBool === true) qb.whereNull("csh.archived");
              if (issuerPerm !== undefined) qb.where("csh.issuer_perm_management_mode", issuerPerm);
              if (verifierPerm !== undefined) qb.where("csh.verifier_perm_management_mode", verifierPerm);
              applyMetricRangeFilters(qb);
            })
            .orderBy("csh.credential_schema_id", "asc")
            .modify((qb) => {
              if (hasHeightColumn) qb.orderBy("csh.height", "desc");
            })
            .orderBy("csh.created_at", "desc")
            .orderBy("csh.id", "desc")
            .as("latest");
          const orderedLatest = applyOrdering(knex.from(latestSub).select("*"), sort);
          items = await orderedLatest.limit(limit);
        } else {
          const ranked = knex("credential_schema_history as csh")
            .select(
              "csh.*",
              knex.raw(
                `ROW_NUMBER() OVER (PARTITION BY credential_schema_id ORDER BY ${hasHeightColumn ? "height DESC," : ""} created_at DESC, id DESC) as rn`
              )
            )
            .whereIn("csh.credential_schema_id", schemaIdsAtHeight)
            .modify((qb) => {
              if (hasHeightColumn) qb.where("csh.height", "<=", blockHeight);
              if (trId) qb.where("csh.tr_id", trId);
              if (modifiedAfterIso) qb.where("csh.modified", ">", modifiedAfterIso);
              if (onlyActiveBool === true) qb.whereNull("csh.archived");
              if (issuerPerm !== undefined) qb.where("csh.issuer_perm_management_mode", issuerPerm);
              if (verifierPerm !== undefined) qb.where("csh.verifier_perm_management_mode", verifierPerm);
              applyMetricRangeFilters(qb);
            })
            .as("ranked");
          const orderedLatest = applyOrdering(knex.from(ranked).select("*").where("rn", 1), sort);
          items = await orderedLatest.limit(limit);
        }

        let filteredItems = items
          .filter((item): item is NonNullable<typeof items[0]> => item !== null)
          .map((historyRecord) => {
            const storedSchemaString = getStoredSchemaString(historyRecord.json_schema);
            return {
              id: historyRecord.credential_schema_id,
              tr_id: historyRecord.tr_id,
              json_schema: storedSchemaString,
              title: historyRecord.title ?? undefined,
              description: historyRecord.description ?? undefined,
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

        if (items.length >= limit) {
          filteredItems = filteredItems.slice(0, limit);
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
          if (hasHistoryMetricColumns) {
            const statsMap = new Map<number, any>();
            for (const historyRecord of items) {
              statsMap.set(Number(historyRecord.credential_schema_id), {
                participants: Number(historyRecord.participants || 0),
                participants_ecosystem: Number(historyRecord.participants_ecosystem || 0),
                participants_issuer_grantor: Number(historyRecord.participants_issuer_grantor || 0),
                participants_issuer: Number(historyRecord.participants_issuer || 0),
                participants_verifier_grantor: Number(historyRecord.participants_verifier_grantor || 0),
                participants_verifier: Number(historyRecord.participants_verifier || 0),
                participants_holder: Number(historyRecord.participants_holder || 0),
                weight: Number(historyRecord.weight || 0),
                issued: Number(historyRecord.issued || 0),
                verified: Number(historyRecord.verified || 0),
                ecosystem_slash_events: Number(historyRecord.ecosystem_slash_events || 0),
                ecosystem_slashed_amount: Number(historyRecord.ecosystem_slashed_amount || 0),
                ecosystem_slashed_amount_repaid: Number(historyRecord.ecosystem_slashed_amount_repaid || 0),
                network_slash_events: Number(historyRecord.network_slash_events || 0),
                network_slashed_amount: Number(historyRecord.network_slashed_amount || 0),
                network_slashed_amount_repaid: Number(historyRecord.network_slashed_amount_repaid || 0),
              });
            }
            schemasWithStats = filteredItems.map((item) => ({
              ...item,
              ...(statsMap.get(Number(item.id)) || {
                participants: 0,
                participants_ecosystem: 0,
                participants_issuer_grantor: 0,
                participants_issuer: 0,
                participants_verifier_grantor: 0,
                participants_verifier: 0,
                participants_holder: 0,
                weight: 0,
                issued: 0,
                verified: 0,
                ecosystem_slash_events: 0,
                ecosystem_slashed_amount: 0,
                ecosystem_slashed_amount_repaid: 0,
                network_slash_events: 0,
                network_slashed_amount: 0,
                network_slashed_amount_repaid: 0,
              }),
            }));
          } else {
            const statsMap = await calculateCredentialSchemaStatsBatch(filteredItems.map((item) => Number(item.id)), blockHeight);
            schemasWithStats = filteredItems.map((item) => {
              const stats = statsMap.get(Number(item.id)) || {
                participants: 0,
                participants_ecosystem: 0,
                participants_issuer_grantor: 0,
                participants_issuer: 0,
                participants_verifier_grantor: 0,
                participants_verifier: 0,
                participants_holder: 0,
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
                participants: stats.participants,
                participants_ecosystem: stats.participants_ecosystem,
                participants_issuer_grantor: stats.participants_issuer_grantor,
                participants_issuer: stats.participants_issuer,
                participants_verifier_grantor: stats.participants_verifier_grantor,
                participants_verifier: stats.participants_verifier,
                participants_holder: stats.participants_holder,
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
            });
          }
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
        filteredWithStats = applyHalfOpenRangeToRows(filteredWithStats, minParticipants, maxParticipants, (s) => toFiniteNumber(s.participants));
        filteredWithStats = applyHalfOpenRangeToRows(filteredWithStats, minParticipantsEcosystem, maxParticipantsEcosystem, (s) => toFiniteNumber((s as any).participants_ecosystem));
        filteredWithStats = applyHalfOpenRangeToRows(filteredWithStats, minParticipantsIssuerGrantor, maxParticipantsIssuerGrantor, (s) => toFiniteNumber((s as any).participants_issuer_grantor));
        filteredWithStats = applyHalfOpenRangeToRows(filteredWithStats, minParticipantsIssuer, maxParticipantsIssuer, (s) => toFiniteNumber((s as any).participants_issuer));
        filteredWithStats = applyHalfOpenRangeToRows(filteredWithStats, minParticipantsVerifierGrantor, maxParticipantsVerifierGrantor, (s) => toFiniteNumber((s as any).participants_verifier_grantor));
        filteredWithStats = applyHalfOpenRangeToRows(filteredWithStats, minParticipantsVerifier, maxParticipantsVerifier, (s) => toFiniteNumber((s as any).participants_verifier));
        filteredWithStats = applyHalfOpenRangeToRows(filteredWithStats, minParticipantsHolder, maxParticipantsHolder, (s) => toFiniteNumber((s as any).participants_holder));
        filteredWithStats = applyHalfOpenRangeToRows(filteredWithStats, minWeight, maxWeight, (s) => toFiniteNumber(s.weight));
        filteredWithStats = applyHalfOpenRangeToRows(filteredWithStats, minIssued, maxIssued, (s) => toFiniteNumber(s.issued));
        filteredWithStats = applyHalfOpenRangeToRows(filteredWithStats, minVerified, maxVerified, (s) => toFiniteNumber(s.verified));
        filteredWithStats = applyHalfOpenRangeToRows(filteredWithStats, minEcosystemSlashEvents, maxEcosystemSlashEvents, (s) => toFiniteNumber(s.ecosystem_slash_events));
        filteredWithStats = applyHalfOpenRangeToRows(filteredWithStats, minNetworkSlashEvents, maxNetworkSlashEvents, (s) => toFiniteNumber(s.network_slash_events));

        type FilteredItemWithStats = FilteredItem & {
          participants: number;
          participants_ecosystem: number;
          participants_issuer_grantor: number;
          participants_issuer: number;
          participants_verifier_grantor: number;
          participants_verifier: number;
          participants_holder: number;
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
        const sortedItems = sortCredentialSchemaRows(typedFilteredItems, sort, limit);

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
      applyHalfOpenRangeToQuery(query, "participants", minParticipants, maxParticipants);
      applyHalfOpenRangeToQuery(query, "participants_ecosystem", minParticipantsEcosystem, maxParticipantsEcosystem);
      applyHalfOpenRangeToQuery(query, "participants_issuer_grantor", minParticipantsIssuerGrantor, maxParticipantsIssuerGrantor);
      applyHalfOpenRangeToQuery(query, "participants_issuer", minParticipantsIssuer, maxParticipantsIssuer);
      applyHalfOpenRangeToQuery(query, "participants_verifier_grantor", minParticipantsVerifierGrantor, maxParticipantsVerifierGrantor);
      applyHalfOpenRangeToQuery(query, "participants_verifier", minParticipantsVerifier, maxParticipantsVerifier);
      applyHalfOpenRangeToQuery(query, "participants_holder", minParticipantsHolder, maxParticipantsHolder);
      applyHalfOpenRangeToQuery(query, "weight", minWeight, maxWeight);
      applyHalfOpenRangeToQuery(query, "issued", minIssued, maxIssued);
      applyHalfOpenRangeToQuery(query, "verified", minVerified, maxVerified);
      applyHalfOpenRangeToQuery(query, "ecosystem_slash_events", minEcosystemSlashEvents, maxEcosystemSlashEvents);
      applyHalfOpenRangeToQuery(query, "network_slash_events", minNetworkSlashEvents, maxNetworkSlashEvents);

      if (modifiedAfterIso) {
        query.where("modified", ">", modifiedAfterIso);
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
      const { fullyApplied: liveSortFullyApplied } = applyCredentialSchemaSqlSort(query, sort);
      const liveFetchLimit = liveSortFullyApplied ? limit : Math.max(limit * 2, 256);
      const items = await query.limit(liveFetchLimit);

      const schemasWithStats = items.map((item) => {
        const storedSchemaString = getStoredSchemaString(item.json_schema);
        return {
          ...item,
          json_schema: storedSchemaString,
          title: item.title ?? undefined,
          description: item.description ?? undefined,
          participants: typeof item.participants === "number" ? item.participants : Number(item.participants || 0),
          participants_ecosystem: typeof (item as any).participants_ecosystem === "number" ? (item as any).participants_ecosystem : Number((item as any).participants_ecosystem || 0),
          participants_issuer_grantor: typeof (item as any).participants_issuer_grantor === "number" ? (item as any).participants_issuer_grantor : Number((item as any).participants_issuer_grantor || 0),
          participants_issuer: typeof (item as any).participants_issuer === "number" ? (item as any).participants_issuer : Number((item as any).participants_issuer || 0),
          participants_verifier_grantor: typeof (item as any).participants_verifier_grantor === "number" ? (item as any).participants_verifier_grantor : Number((item as any).participants_verifier_grantor || 0),
          participants_verifier: typeof (item as any).participants_verifier === "number" ? (item as any).participants_verifier : Number((item as any).participants_verifier || 0),
          participants_holder: typeof (item as any).participants_holder === "number" ? (item as any).participants_holder : Number((item as any).participants_holder || 0),
          weight: typeof item.weight === "number" ? item.weight : Number(item.weight || 0),
          issued: typeof item.issued === "number" ? item.issued : Number(item.issued || 0),
          verified: typeof item.verified === "number" ? item.verified : Number(item.verified || 0),
          ecosystem_slash_events: typeof item.ecosystem_slash_events === "number" ? item.ecosystem_slash_events : Number(item.ecosystem_slash_events || 0),
          ecosystem_slashed_amount: typeof item.ecosystem_slashed_amount === "number" ? item.ecosystem_slashed_amount : Number(item.ecosystem_slashed_amount || 0),
          ecosystem_slashed_amount_repaid: typeof item.ecosystem_slashed_amount_repaid === "number" ? item.ecosystem_slashed_amount_repaid : Number(item.ecosystem_slashed_amount_repaid || 0),
          network_slash_events: typeof item.network_slash_events === "number" ? item.network_slash_events : Number(item.network_slash_events || 0),
          network_slashed_amount: typeof item.network_slashed_amount === "number" ? item.network_slashed_amount : Number(item.network_slashed_amount || 0),
          network_slashed_amount_repaid: typeof item.network_slashed_amount_repaid === "number" ? item.network_slashed_amount_repaid : Number(item.network_slashed_amount_repaid || 0),
        };
      });

      const cleanItems = schemasWithStats.map(({ is_active, ...rest }) => rest);

      const filteredItems = cleanItems;

      type SchemaWithStats = typeof filteredItems[0];
      const sortedItems = liveSortFullyApplied
        ? (filteredItems as SchemaWithStats[]).slice(0, limit)
        : sortCredentialSchemaRows(filteredItems as SchemaWithStats[], sort, limit);

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

        const stored = getStoredSchemaString(historyRecord.json_schema);
        if (!stored) {
          return ApiResponder.error(ctx, `Credential schema with id=${id} has no valid JSON schema`, 404);
        }
        (ctx.meta as any).$rawJsonResponse = true;
        return stored;
      }

      const schemaRecord = await knex("credential_schemas")
        .select("json_schema")
        .where({ id })
        .first();

      if (!schemaRecord) {
        return ApiResponder.error(ctx, `Credential schema with id=${id} not found`, 404);
      }

      const stored = getStoredSchemaString(schemaRecord.json_schema);
      if (!stored) {
        return ApiResponder.error(ctx, `Credential schema with id=${id} has no valid JSON schema`, 404);
      }
      (ctx.meta as any).$rawJsonResponse = true;
      return stored;
    } catch (err: any) {
      this.logger.error("Error in renderJsonSchema:", err);
      return ApiResponder.error(ctx, "Internal Server Error", 500);
    }
  }

  @Action()
  public async getParams(ctx: Context) {
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
