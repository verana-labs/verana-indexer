import { Service, ServiceBroker } from "moleculer";
import type { Knex } from "knex";
import { formatTimestamp } from "../../common/utils/date_utils";
import { getBlockChainTimeAsOf } from "../../common/utils/block_time";
import knex from "../../common/utils/db_connection";
import { SERVICE, ModulesParamsNamesTypes } from "../../common";
import getGlobalVariables from "../../common/utils/global_variables";
import { mapPermissionType } from "../../common/utils/utils";
import { extractController, requireController } from "../../common/utils/extract_controller";
import { calculatePermState } from "./perm_state_utils";
import { CS_STATS_FIELDS, statsToUpdateObject } from "../../common/utils/stats_fields";
import { calculateCredentialSchemaStats } from "../crawl-cs/cs_stats";
import { calculateTrustRegistryStats } from "../crawl-tr/tr_stats";
import { syncTrustRegistryStatsAndHistoryFromSchemaChange, insertCredentialSchemaHistoryStatsRow } from "../crawl-cs/cs_database.service";
import { getModuleParams } from "../../common/utils/params_service";
import {
  getPermissionTypeString,
  MsgCancelPermissionVPLastRequest,
  MsgCreateOrUpdatePermissionSession,
  MsgCreatePermission,
  MsgCreateRootPermission,
  MsgAdjustPermission,
  MsgRenewPermissionVP,
  MsgRepayPermissionSlashedTrustDeposit,
  MsgRevokePermission,
  MsgSetPermissionVPToValidated,
  MsgSlashPermissionTrustDeposit,
  MsgStartPermissionVP,
} from "./perm_types";

const PERMISSION_HISTORY_FIELDS = [
  "schema_id",
  "type",
  "did",
  "corporation",
  "created",
  "modified",
  "slashed",
  "repaid",
  "effective_from",
  "effective_until",
  "revoked",
  "validation_fees",
  "issuance_fees",
  "verification_fees",
  "deposit",
  "slashed_deposit",
  "repaid_deposit",
  "validator_perm_id",
  "vp_state",
  "vp_last_state_change",
  "participants",
  "participants_ecosystem",
  "participants_issuer_grantor",
  "participants_issuer",
  "participants_verifier_grantor",
  "participants_verifier",
  "participants_holder",
  "weight",
  "ecosystem_slash_events",
  "ecosystem_slashed_amount",
  "ecosystem_slashed_amount_repaid",
  "network_slash_events",
  "network_slashed_amount",
  "network_slashed_amount_repaid",
  "vp_current_fees",
  "vp_current_deposit",
  "vp_summary_digest",
  "vp_exp",
  "vp_validator_deposit",
  "issued",
  "verified",
  "issuance_fee_discount",
  "verification_fee_discount",
  "expire_soon",
  "vs_operator",
  "adjusted",
  "adjusted_by",
  "vs_operator_authz_enabled",
  "vs_operator_authz_spend_limit",
  "vs_operator_authz_with_feegrant",
  "vs_operator_authz_fee_spend_limit",
  "vs_operator_authz_spend_period",
];

const PARTICIPANT_ROLE_HISTORY_FIELDS = [
  "participants_ecosystem",
  "participants_issuer_grantor",
  "participants_issuer",
  "participants_verifier_grantor",
  "participants_verifier",
  "participants_holder",
] as const;

const PERMISSION_SESSION_HISTORY_FIELDS = [
  "corporation",
  "vs_operator",
  "agent_perm_id",
  "wallet_agent_perm_id",
  "session_records",
  "created",
  "modified",
];

const PERMISSION_HISTORY_V4_FIELDS = [
  "vs_operator",
  "adjusted",
  "adjusted_by",
  "vs_operator_authz_enabled",
  "vs_operator_authz_spend_limit",
  "vs_operator_authz_with_feegrant",
  "vs_operator_authz_fee_spend_limit",
  "vs_operator_authz_spend_period",
] as const;

function normalizeValue(value: any) {
  if (value === undefined) return null;
  return value;
}

function computeChanges(
  oldRecord: any,
  newRecord: any,
  fields: string[]
): Record<string, any> | null {
  const changes: Record<string, any> = {};

  if (!oldRecord) {
    for (const field of fields) {
      const newValue = normalizeValue(newRecord?.[field]);
      if (newValue !== null && newValue !== undefined) {
        changes[field] = newValue;
      }
    }
  } else {
    for (const field of fields) {
      const oldValue = normalizeValue(oldRecord?.[field]);
      const newValue = normalizeValue(newRecord?.[field]);
      if (JSON.stringify(oldValue) !== JSON.stringify(newValue)) {
        changes[field] = newValue;
      }
    }
  }
  return Object.keys(changes).length ? changes : null;
}

function parseJson<T = any>(value: any): T | any {
  if (typeof value !== "string") return value;
  try {
    return JSON.parse(value);
  } catch {
    return value as any;
  }
}

function normalizePermissionType(value: unknown): string {
  if (typeof value === "string") {
    const normalized = value.toUpperCase();
    if (
      normalized === "UNSPECIFIED" ||
      normalized === "ISSUER" ||
      normalized === "VERIFIER" ||
      normalized === "ISSUER_GRANTOR" ||
      normalized === "VERIFIER_GRANTOR" ||
      normalized === "ECOSYSTEM" ||
      normalized === "HOLDER"
    ) {
      return normalized;
    }
  }

  const numeric = Number(value);
  switch (numeric) {
    case 0: return "UNSPECIFIED";
    case 1: return "ISSUER";
    case 2: return "VERIFIER";
    case 3: return "ISSUER_GRANTOR";
    case 4: return "VERIFIER_GRANTOR";
    case 5: return "ECOSYSTEM";
    case 6: return "HOLDER";
    default: return "UNSPECIFIED";
  }
}

function pickMessageValue(msg: Record<string, any>, snake: string, camel: string) {
  return msg[snake] ?? msg[camel];
}

function extractPermissionType(msg: Record<string, any>, fallback: string | number = "UNSPECIFIED") {
  return mapPermissionType(
    msg.permission_type ?? msg.permissionType ?? msg.type ?? fallback
  );
}

function normalizeValidationState(value: unknown): string {
  if (typeof value === "string") {
    const normalized = value.toUpperCase();
    if (
      normalized === "VALIDATION_STATE_UNSPECIFIED" ||
      normalized === "PENDING" ||
      normalized === "VALIDATED"
    ) {
      return normalized;
    }
    if (normalized === "TERMINATED" || normalized === "TERMINATION_REQUESTED") {
      return "VALIDATED";
    }
  }

  const numeric = Number(value);
  switch (numeric) {
    case 1: return "PENDING";
    case 2: return "VALIDATED";
    case 3:
    case 4:
      return "VALIDATED";
    case 0:
    default:
      return "VALIDATION_STATE_UNSPECIFIED";
  }
}

function toIsoOrNull(value: unknown): string | null {
  if (value === null || value === undefined || value === "") return null;
  const d = new Date(String(value));
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString();
}

function jsonbColumnValue(value: unknown): string | object | null {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") {
    try {
      return JSON.parse(value);
    } catch {
      return value;
    }
  }
  return value as object;
}

const permissionHistoryColumnExistsCache: Record<string, boolean> = {};

async function checkPermissionHistoryColumnExists(columnName: string): Promise<boolean> {
  if (permissionHistoryColumnExistsCache[columnName] !== undefined) {
    return permissionHistoryColumnExistsCache[columnName];
  }

  try {
    const result = await knex.schema.hasColumn('permission_history', columnName);
    permissionHistoryColumnExistsCache[columnName] = result;
    return result;
  } catch (error) {
    permissionHistoryColumnExistsCache[columnName] = false;
    return false;
  }
}

async function pickPermissionSnapshot(record: any) {
  const snapshot: Record<string, any> = {
    permission_id: String(record.permission_id ?? record.id ?? ""),
  };

  const hasIssuedColumn = await checkPermissionHistoryColumnExists("issued");
  const hasVerifiedColumn = await checkPermissionHistoryColumnExists("verified");
  const hasParticipantsColumn = await checkPermissionHistoryColumnExists("participants");
  const hasParticipantRoleColumns =
    (await Promise.all(PARTICIPANT_ROLE_HISTORY_FIELDS.map((field) => checkPermissionHistoryColumnExists(field)))).every(Boolean);
  const hasWeightColumn = await checkPermissionHistoryColumnExists("weight");
  const hasEcosystemSlashEventsColumn = await checkPermissionHistoryColumnExists("ecosystem_slash_events");
  const hasExpireSoonColumn = await checkPermissionHistoryColumnExists("expire_soon");
   const hasIssuanceDiscountColumn = await checkPermissionHistoryColumnExists("issuance_fee_discount");
   const hasVerificationDiscountColumn = await checkPermissionHistoryColumnExists("verification_fee_discount");
  const hasV4PermColumns = await checkPermissionHistoryColumnExists("vs_operator");

  for (const field of PERMISSION_HISTORY_FIELDS) {
    if (field === "expire_soon" && !hasExpireSoonColumn) continue;
    if (field === "issued" && !hasIssuedColumn) {
      continue;
    }
    if (field === "verified" && !hasVerifiedColumn) {
      continue;
    }
    if (field === "participants" && !hasParticipantsColumn) {
      continue;
    }
    if ((PARTICIPANT_ROLE_HISTORY_FIELDS as readonly string[]).includes(field) && !hasParticipantRoleColumns) {
      continue;
    }
    if (field === "weight" && !hasWeightColumn) {
      continue;
    }
    if ((field === "ecosystem_slash_events" || field === "ecosystem_slashed_amount" ||
      field === "ecosystem_slashed_amount_repaid" || field === "network_slash_events" ||
      field === "network_slashed_amount" || field === "network_slashed_amount_repaid") && !hasEcosystemSlashEventsColumn) {
      continue;
    }
    if (field === "issuance_fee_discount" && !hasIssuanceDiscountColumn) {
      continue;
    }
    if (field === "verification_fee_discount" && !hasVerificationDiscountColumn) {
      continue;
    }
    if (!hasV4PermColumns && (PERMISSION_HISTORY_V4_FIELDS as readonly string[]).includes(field)) {
      continue;
    }
    if (field === "schema_id") {
      const schemaIdValue = record[field];
      snapshot[field] = schemaIdValue !== null && schemaIdValue !== undefined ? Number(schemaIdValue) : null;
    } else if (
      field === "issued" || field === "verified" || field === "participants" ||
      field === "ecosystem_slash_events" || field === "network_slash_events" ||
      (PARTICIPANT_ROLE_HISTORY_FIELDS as readonly string[]).includes(field)
    ) {
      const v = record[field];
      snapshot[field] = v !== null && v !== undefined ? Number(v) : 0;
    } else if (
      field === "weight" || field === "ecosystem_slashed_amount" || field === "ecosystem_slashed_amount_repaid" ||
      field === "network_slashed_amount" || field === "network_slashed_amount_repaid"
    ) {
      const v = record[field];
      snapshot[field] = v !== null && v !== undefined ? Number(v) : 0;
    } else if (field === "issuance_fee_discount" || field === "verification_fee_discount") {
      const v = record[field];
      snapshot[field] = v !== null && v !== undefined ? Number(v) : 0;
    } else if (field === "vs_operator_authz_spend_limit" || field === "vs_operator_authz_fee_spend_limit") {
      snapshot[field] = jsonbColumnValue(record[field]);
    } else if (field === "vs_operator_authz_enabled" || field === "vs_operator_authz_with_feegrant") {
      snapshot[field] = Boolean(record[field]);
    } else if (field === "adjusted") {
      snapshot[field] = toIsoOrNull(record[field]);
    } else {
      snapshot[field] = normalizeValue(record[field]);
    }
  }
  return snapshot;
}

async function recordPermissionHistory(
  db: any,
  permissionRecord: any,
  eventType: string,
  height: number,
  previousRecord?: any
) {
  if (!permissionRecord) return;

  let permissionRecordForHistory = permissionRecord;
  const permId = permissionRecord.id ?? permissionRecord.permission_id;
  if (permId != null && db && typeof db === "function") {
    const fresh = await db("permissions").where({ id: Number(permId) }).first();
    if (fresh) {
      permissionRecordForHistory = { ...fresh, id: fresh.id ?? permId, permission_id: permId };
    }
  }

  const hasIssuedColumn = await checkPermissionHistoryColumnExists("issued");
  const hasVerifiedColumn = await checkPermissionHistoryColumnExists("verified");
  const hasParticipantsColumn = await checkPermissionHistoryColumnExists("participants");
  const hasParticipantRoleColumns =
    (await Promise.all(PARTICIPANT_ROLE_HISTORY_FIELDS.map((field) => checkPermissionHistoryColumnExists(field)))).every(Boolean);
  const hasWeightColumn = await checkPermissionHistoryColumnExists("weight");
  const hasEcosystemSlashEventsColumn = await checkPermissionHistoryColumnExists("ecosystem_slash_events");

  const hasExpireSoonColumn = await checkPermissionHistoryColumnExists("expire_soon");
  const hasIssuanceDiscountColumn = await checkPermissionHistoryColumnExists("issuance_fee_discount");
  const hasVerificationDiscountColumn = await checkPermissionHistoryColumnExists("verification_fee_discount");
  const hasV4PermColumns = await checkPermissionHistoryColumnExists("vs_operator");

  const fieldsToUse = PERMISSION_HISTORY_FIELDS.filter(field => {
    if (field === "expire_soon" && !hasExpireSoonColumn) return false;
    if (field === "issued" && !hasIssuedColumn) return false;
    if (field === "verified" && !hasVerifiedColumn) return false;
    if (field === "participants" && !hasParticipantsColumn) return false;
    if ((PARTICIPANT_ROLE_HISTORY_FIELDS as readonly string[]).includes(field) && !hasParticipantRoleColumns) return false;
    if (field === "weight" && !hasWeightColumn) return false;
    if ((field === "ecosystem_slash_events" || field === "ecosystem_slashed_amount" ||
      field === "ecosystem_slashed_amount_repaid" || field === "network_slash_events" ||
      field === "network_slashed_amount" || field === "network_slashed_amount_repaid") && !hasEcosystemSlashEventsColumn) {
      return false;
    }
    if (field === "issuance_fee_discount" && !hasIssuanceDiscountColumn) return false;
    if (field === "verification_fee_discount" && !hasVerificationDiscountColumn) return false;
    if (!hasV4PermColumns && (PERMISSION_HISTORY_V4_FIELDS as readonly string[]).includes(field)) return false;
    return true;
  });

  const snapshot = await pickPermissionSnapshot(permissionRecordForHistory);
  const changes = computeChanges(previousRecord, permissionRecordForHistory, fieldsToUse);

  if (previousRecord && !changes) {
    return;
  }

  await db("permission_history").insert({
    ...snapshot,
    event_type: eventType,
    height,
    changes: changes ? JSON.stringify(changes) : null,
    created_at: permissionRecordForHistory?.modified ?? permissionRecordForHistory?.created ?? new Date(),
  });

}

function pickPermissionSessionSnapshot(record: any) {
  const snapshot: Record<string, any> = {
    session_id: String(record.session_id ?? record.id ?? ""),
  };
  for (const field of PERMISSION_SESSION_HISTORY_FIELDS) {
    if (field === "session_records") {
      const sr = record[field];
      if (sr === null || sr === undefined) {
        snapshot[field] = null;
      } else if (typeof sr === "string") {
        snapshot[field] = sr;
      } else {
        snapshot[field] = JSON.stringify(sr);
      }
    } else {
      snapshot[field] = normalizeValue(record[field]);
    }
  }
  return snapshot;
}

async function recordPermissionSessionHistory(
  db: any,
  sessionRecord: any,
  eventType: string,
  height: number,
  previousRecord?: any
) {
  if (!sessionRecord) return;
  const snapshot = pickPermissionSessionSnapshot(sessionRecord);
  const changes = computeChanges(
    previousRecord,
    sessionRecord,
    PERMISSION_SESSION_HISTORY_FIELDS
  );

  if (previousRecord && !changes) {
    return;
  }

  await db("permission_session_history").insert({
    ...snapshot,
    event_type: eventType,
    height,
    changes: changes ? JSON.stringify(changes) : null,
    created_at: sessionRecord?.modified ?? sessionRecord?.created ?? new Date(),
  });
}

interface QueuedPermission {
  message: any;
  reason: string;
  retryCount: number;
  queuedAt: Date;
  nextRetryAt: Date;
}

export default class PermIngestService extends Service {
  private retryQueue: Map<string, QueuedPermission> = new Map();
  private retryInterval: NodeJS.Timeout | null = null;
  private readonly MAX_RETRY_ATTEMPTS = 10;
  private readonly RETRY_INTERVAL_MS = 30000; // 30 seconds
  private readonly RETRY_BACKOFF_MULTIPLIER = 2;

  /**
   * Calculate expire_soon for a permission based on its state and effective_until
   * Returns: null if not active, false if no expiration or not soon, true if expiring soon
   */
  private async calculateExpireSoon(
    perm: any,
    now: Date = new Date(),
    blockHeight?: number
  ): Promise<boolean | null> {
    // Check if permission is active
    const effectiveFrom = perm.effective_from ? new Date(perm.effective_from) : null;
    const effectiveUntil = perm.effective_until ? new Date(perm.effective_until) : null;

    if (effectiveFrom && now < effectiveFrom) return null;
    if (effectiveUntil && now > effectiveUntil) return null;
    if (perm.revoked) return null;
    if (perm.slashed && !perm.repaid) return null;
    if (perm.vp_state !== 'VALIDATED' && perm.type !== 'ECOSYSTEM') return null;
    if (perm.type === "UNSPECIFIED") return null;

    if (!effectiveUntil) {
      return false;
    }

    let nDaysBefore = 0;
    try {
      const moduleParams = await getModuleParams(ModulesParamsNamesTypes.PERM, blockHeight);
      if (moduleParams?.params) {
        nDaysBefore = moduleParams.params.PERMISSION_SET_EXPIRE_SOON_N_DAYS_BEFORE || 0;
      }
    } catch (error) {
      this.logger.warn(`Failed to get PERMISSION module params for expire_soon calculation:`, error);
      nDaysBefore = 0;
    }

    // Calculate expiration check date (now + nDaysBefore)
    const expirationCheckDate = new Date(now);
    expirationCheckDate.setDate(expirationCheckDate.getDate() + nDaysBefore);

    // If expiration check date is greater than effective_until, it's expiring soon
    return expirationCheckDate > effectiveUntil;
  }

  public constructor(broker: ServiceBroker) {
    super(broker);
    this.parseServiceSchema({
      name: "permIngest",
      actions: {
        handleMsgCreateRootPermission: {
          params: { data: "object" },
          handler: async (ctx) =>
            this.handleCreateRootPermission(ctx.params.data),
        },
        handleMsgSelfCreatePermission: {
          params: { data: "object" },
          handler: async (ctx) => this.handleCreatePermission(ctx.params.data),
        },
        handleMsgAdjustPermission: {
          params: { data: "object" },
          handler: async (ctx) => this.handleAdjustPermission(ctx.params.data),
        },
        handleMsgRevokePermission: {
          params: { data: "object" },
          handler: async (ctx) => this.handleRevokePermission(ctx.params.data),
        },
        handleMsgStartPermissionVP: {
          params: { data: "object" },
          handler: async (ctx) => this.handleStartPermissionVP(ctx.params.data),
        },
        handleMsgSetPermissionVPToValidated: {
          params: { data: "object" },
          handler: async (ctx) =>
            this.handleSetPermissionVPToValidated(ctx.params.data),
        },
        handleMsgRenewPermissionVP: {
          params: { data: "object" },
          handler: async (ctx) => this.handleRenewPermissionVP(ctx.params.data),
        },
        handleMsgCancelPermissionVPLastRequest: {
          params: { data: "object" },
          handler: async (ctx) =>
            this.handleCancelPermissionVPLastRequest(ctx.params.data),
        },
        handleMsgCreateOrUpdatePermissionSession: {
          params: { data: "object" },
          handler: async (ctx) =>
            this.handleCreateOrUpdatePermissionSession(ctx.params.data),
        },
        handleMsgSlashPermissionTrustDeposit: {
          params: { data: "object" },
          handler: async (ctx) =>
            this.handleSlashPermissionTrustDeposit(ctx.params.data),
        },
        handleMsgRepayPermissionSlashedTrustDeposit: {
          params: { data: "object" },
          handler: async (ctx) =>
            this.handleRepayPermissionSlashedTrustDeposit(ctx.params.data),
        },
        syncPermissionFromLedger: {
          params: {
            ledgerPermission: "object",
            blockHeight: "number",
            txHash: { type: "string", optional: true },
            msgType: { type: "string", optional: true },
          },
          handler: async (ctx) => this.syncPermissionFromLedger(
            ctx.params.ledgerPermission,
            Number(ctx.params.blockHeight) || 0,
            ctx.params.txHash,
            ctx.params.msgType
          ),
        },
        syncPermissionSessionFromLedger: {
          params: {
            ledgerSession: "object",
            blockHeight: "number",
            txHash: { type: "string", optional: true },
            msgType: { type: "string", optional: true },
          },
          handler: async (ctx) => this.syncPermissionSessionFromLedger(
            ctx.params.ledgerSession,
            Number(ctx.params.blockHeight) || 0,
            ctx.params.txHash,
            ctx.params.msgType
          ),
        },
        getPermissionById: {
          params: { id: "number" },
          handler: async (ctx) => knex("permissions").where({ id: Number(ctx.params.id) }).first(),
        },
        getPermissionSessionById: {
          params: { id: "string" },
          handler: async (ctx) => knex("permission_sessions").where({ id: String(ctx.params.id) }).first(),
        },
        comparePermissionWithLedger: {
          params: {
            permissionId: "number",
            ledgerPermission: "object",
            blockHeight: "number",
          },
          handler: async (ctx) => this.comparePermissionWithLedger(
            Number(ctx.params.permissionId),
            ctx.params.ledgerPermission,
            Number(ctx.params.blockHeight) || 0
          ),
        },
        comparePermissionSessionWithLedger: {
          params: {
            sessionId: "string",
            ledgerSession: "object",
            blockHeight: "number",
          },
          handler: async (ctx) => this.comparePermissionSessionWithLedger(
            String(ctx.params.sessionId),
            ctx.params.ledgerSession,
            Number(ctx.params.blockHeight) || 0
          ),
        },
        rebuildPermissionStats: {
          params: {
            schema_id: { type: "number", optional: true },
          },
          handler: async (ctx) => this.rebuildPermissionStats(ctx.params.schema_id),
        },
        getPermission: {
          params: { schema_id: "number", corporation: "string", type: "string" },
          handler: async (ctx) => {
            const { schema_id: schemaId, corporation, type } = ctx.params;
            return await knex("permissions")
              .where({ schema_id: schemaId, corporation, type })
              .first();
          },
        },
        listPermissions: {
          params: {
            schema_id: { type: "number", optional: true },
            corporation: { type: "string", optional: true },
            type: { type: "string", optional: true },
          },
          handler: async (ctx) => {
            let query = knex("permissions");
            if (ctx.params.schema_id)
              query = query.where("schema_id", ctx.params.schema_id);
            if (ctx.params.corporation)
              query = query.where("corporation", ctx.params.corporation);
            if (ctx.params.type) query = query.where("type", ctx.params.type);
            return await query;
          },
        },
      },
    });
  }

  private async handleAdjustPermission(msg: MsgAdjustPermission & { height?: number }) {
    const height = Number((msg as any)?.height) || 0;
    const permId = Number((msg as any)?.id ?? (msg as any)?.permission_id ?? (msg as any)?.permissionId ?? 0) || 0;
    if (!Number.isInteger(permId) || permId <= 0) {
      this.logger.warn(`[handleAdjustPermission] Invalid permission id: ${String((msg as any)?.id)}`);
      return;
    }

    const ts = (msg as any)?.timestamp ? formatTimestamp((msg as any).timestamp) : null;
    const effectiveUntilRaw = (msg as any)?.effective_until ?? (msg as any)?.effectiveUntil ?? null;
    const effectiveUntil = effectiveUntilRaw ? formatTimestamp(effectiveUntilRaw) : null;

    try {
      await this.ensurePermV4Columns(knex);

      const previous = await knex("permissions").where({ id: permId }).first();
      if (!previous) {
        this.logger.warn(`[handleAdjustPermission] Permission id=${permId} not found; skipping`);
        return;
      }

      const adjustedBy = extractController(msg as unknown as Record<string, unknown>) ?? null;

      await knex("permissions")
        .where({ id: permId })
        .update({
          effective_until: effectiveUntil ?? previous.effective_until ?? null,
          adjusted: ts,
          adjusted_by: adjustedBy ?? previous.adjusted_by ?? null,
          modified: ts ?? previous.modified ?? null,
        });

      const updated = await knex("permissions").where({ id: permId }).first();
      try {
        await recordPermissionHistory(knex, updated ?? { id: permId }, "ADJUST_PERMISSION", height, previous);
      } catch (historyErr: any) {
        this.logger.warn(
          `[handleAdjustPermission] Failed to record permission history for id=${permId}: ${historyErr?.message || historyErr}`
        );
      }

      await this.refreshSchemaAndTrustRegistryStats(previous.schema_id ?? updated?.schema_id, height);
    } catch (err: any) {
      this.logger.error("CRITICAL: Error in handleAdjustPermission:", err);
      throw err;
    }
  }

  private async refreshTrustRegistryStatsBySchemaId(
    schemaId: number | null | undefined,
    blockHeightRaw?: number
  ): Promise<void> {
    if (schemaId == null || schemaId <= 0) return;
    const blockHeight = Number(blockHeightRaw) || 0;
    try {
      const cs = await knex("credential_schemas")
        .where({ id: schemaId })
        .select("tr_id")
        .first();
      const trId = cs?.tr_id != null ? Number(cs.tr_id) : null;
      if (!trId || !Number.isInteger(trId) || trId <= 0) return;

      await syncTrustRegistryStatsAndHistoryFromSchemaChange(knex, trId, blockHeight);
      this.logger.info(
        `[TR Stats] Synced trust_registry stats and history from schema_id=${schemaId}, tr_id=${trId}, height=${blockHeight}`
      );
    } catch (err: any) {
      this.logger.warn(
        `Failed to refresh trust_registry stats/history for schema_id=${schemaId}: ${err?.message || err}`
      );
    }
  }

  private mapLedgerPermissionToDbRow(ledgerPermission: Record<string, any>) {
    const id = Number(ledgerPermission.id ?? ledgerPermission.permission_id);
    const schemaId = Number(ledgerPermission.schema_id ?? ledgerPermission.schemaId);
    const nowIso = new Date().toISOString();

    const corporation = String(ledgerPermission.corporation ?? "").trim();

    return {
      id,
      schema_id: schemaId,
      type: normalizePermissionType(ledgerPermission.type),
      did: ledgerPermission.did ?? null,
      corporation,
      created: toIsoOrNull(ledgerPermission.created) ?? nowIso,
      modified: toIsoOrNull(ledgerPermission.modified) ?? nowIso,
      slashed: toIsoOrNull(ledgerPermission.slashed),
      repaid: toIsoOrNull(ledgerPermission.repaid),
      effective_from: toIsoOrNull(ledgerPermission.effective_from ?? ledgerPermission.effectiveFrom),
      effective_until: toIsoOrNull(ledgerPermission.effective_until ?? ledgerPermission.effectiveUntil),
      revoked: toIsoOrNull(ledgerPermission.revoked),
      validation_fees: Number(ledgerPermission.validation_fees ?? ledgerPermission.validationFees ?? 0),
      issuance_fees: Number(ledgerPermission.issuance_fees ?? ledgerPermission.issuanceFees ?? 0),
      verification_fees: Number(ledgerPermission.verification_fees ?? ledgerPermission.verificationFees ?? 0),
      deposit: Number(ledgerPermission.deposit ?? 0),
      slashed_deposit: Number(ledgerPermission.slashed_deposit ?? ledgerPermission.slashedDeposit ?? 0),
      repaid_deposit: Number(ledgerPermission.repaid_deposit ?? ledgerPermission.repaidDeposit ?? 0),
      validator_perm_id: Number(ledgerPermission.validator_perm_id ?? ledgerPermission.validatorPermId ?? 0) || null,
      vp_state: normalizeValidationState(ledgerPermission.vp_state ?? ledgerPermission.vpState),
      vp_exp: toIsoOrNull(ledgerPermission.vp_exp ?? ledgerPermission.vpExp),
      vp_last_state_change: toIsoOrNull(ledgerPermission.vp_last_state_change ?? ledgerPermission.vpLastStateChange),
      vp_validator_deposit: Number(ledgerPermission.vp_validator_deposit ?? ledgerPermission.vpValidatorDeposit ?? 0),
      vp_current_fees: Number(ledgerPermission.vp_current_fees ?? ledgerPermission.vpCurrentFees ?? 0),
      vp_current_deposit: Number(ledgerPermission.vp_current_deposit ?? ledgerPermission.vpCurrentDeposit ?? 0),
      vp_summary_digest:
        ledgerPermission.vp_summary_digest ?? ledgerPermission.vpSummaryDigest ?? null,
      issuance_fee_discount: Number(
        ledgerPermission.issuance_fee_discount ?? ledgerPermission.issuanceFeeDiscount ?? 0
      ),
      verification_fee_discount: Number(
        ledgerPermission.verification_fee_discount ?? ledgerPermission.verificationFeeDiscount ?? 0
      ),
      vs_operator: ledgerPermission.vs_operator ?? ledgerPermission.vsOperator ?? null,
      adjusted: toIsoOrNull(ledgerPermission.adjusted ?? ledgerPermission.adjustedAt),
      adjusted_by: ledgerPermission.adjusted_by ?? ledgerPermission.adjustedBy ?? null,
      vs_operator_authz_enabled: Boolean(
        ledgerPermission.vs_operator_authz_enabled ?? ledgerPermission.vsOperatorAuthzEnabled ?? false
      ),
      vs_operator_authz_spend_limit: jsonbColumnValue(
        ledgerPermission.vs_operator_authz_spend_limit ?? ledgerPermission.vsOperatorAuthzSpendLimit
      ),
      vs_operator_authz_with_feegrant: Boolean(
        ledgerPermission.vs_operator_authz_with_feegrant ?? ledgerPermission.vsOperatorAuthzWithFeegrant ?? false
      ),
      vs_operator_authz_fee_spend_limit: jsonbColumnValue(
        ledgerPermission.vs_operator_authz_fee_spend_limit ?? ledgerPermission.vsOperatorAuthzFeeSpendLimit
      ),
      vs_operator_authz_spend_period:
        ledgerPermission.vs_operator_authz_spend_period ??
        ledgerPermission.vsOperatorAuthzSpendPeriod ??
        null,
    };
  }

  private didEnsurePermV4Columns = false;
  private async ensurePermV4Columns(db: Knex): Promise<void> {
    if (this.didEnsurePermV4Columns) return;

    await db.raw(`
      ALTER TABLE IF EXISTS permissions
        ADD COLUMN IF NOT EXISTS vs_operator text,
        ADD COLUMN IF NOT EXISTS adjusted timestamptz,
        ADD COLUMN IF NOT EXISTS adjusted_by text,
        ADD COLUMN IF NOT EXISTS vs_operator_authz_enabled boolean NOT NULL DEFAULT false,
        ADD COLUMN IF NOT EXISTS vs_operator_authz_spend_limit jsonb,
        ADD COLUMN IF NOT EXISTS vs_operator_authz_with_feegrant boolean NOT NULL DEFAULT false,
        ADD COLUMN IF NOT EXISTS vs_operator_authz_fee_spend_limit jsonb,
        ADD COLUMN IF NOT EXISTS vs_operator_authz_spend_period text;
    `);

    await db.raw(`
      ALTER TABLE IF EXISTS permission_history
        ADD COLUMN IF NOT EXISTS vs_operator text,
        ADD COLUMN IF NOT EXISTS adjusted timestamptz,
        ADD COLUMN IF NOT EXISTS adjusted_by text,
        ADD COLUMN IF NOT EXISTS vs_operator_authz_enabled boolean NOT NULL DEFAULT false,
        ADD COLUMN IF NOT EXISTS vs_operator_authz_spend_limit jsonb,
        ADD COLUMN IF NOT EXISTS vs_operator_authz_with_feegrant boolean NOT NULL DEFAULT false,
        ADD COLUMN IF NOT EXISTS vs_operator_authz_fee_spend_limit jsonb,
        ADD COLUMN IF NOT EXISTS vs_operator_authz_spend_period text;
    `);

    this.permissionsColumnExistsCache = null;
    this.didEnsurePermV4Columns = true;
  }

  private async refreshSchemaAndTrustRegistryStats(
    schemaId: number | null | undefined,
    blockHeightRaw?: number
  ): Promise<void> {
    if (!schemaId || schemaId <= 0) return;
    const blockHeight = Number(blockHeightRaw) || 0;
    try {
      const stats = await calculateCredentialSchemaStats(schemaId, blockHeight > 0 ? blockHeight : undefined);
      const slashFromPerms = await this.sumSlashStatsFromPermissionsForSchema(schemaId);
      const mergedStats = { ...stats, ...slashFromPerms };
      await knex("credential_schemas")
        .where("id", schemaId)
        .update(statsToUpdateObject(mergedStats as unknown as Record<string, unknown>, CS_STATS_FIELDS));
      const hasParticipantsColumn = await this.checkPermissionsColumnExists("participants");
      if (hasParticipantsColumn) {
        await knex("permissions").where("schema_id", schemaId).update({ participants: mergedStats.participants });
      }
      try {
        await insertCredentialSchemaHistoryStatsRow(knex, schemaId, blockHeight, mergedStats);
      } catch (historyErr: any) {
        this.logger.warn(
          `Failed to insert CS stats history for schema_id=${schemaId}: ${historyErr?.message || historyErr}`
        );
      }
    } catch (error: any) {
      this.logger.warn(
        `Failed to refresh credential schema stats for schema_id=${schemaId}: ${error?.message || error}`
      );
    }

    await this.refreshTrustRegistryStatsBySchemaId(schemaId, blockHeight);

    try {
      await this.broker.call(`${SERVICE.V1.MetricsSnapshotService.path}.computeAndStore`, {});
    } catch (error: any) {
      this.logger.warn(`Failed to refresh global metrics after permission sync: ${error?.message || error}`);
    }
  }

  private async syncPermissionFromLedger(
    ledgerPermission: Record<string, any>,
    blockHeight: number,
    txHash?: string,
    msgType?: string
  ) {
    const mapped = this.mapLedgerPermissionToDbRow(ledgerPermission || {});
    if (!Number.isInteger(mapped.id) || mapped.id <= 0) {
      return { success: false, reason: "Invalid permission id from ledger" };
    }
    if (!Number.isInteger(mapped.schema_id) || mapped.schema_id <= 0) {
      return { success: false, reason: "Invalid schema_id from ledger" };
    }

    const effectiveHeight = Number(blockHeight) || 0;
    let finalPermission: any = null;
    let previousPermission: any = null;

    await this.ensurePermV4Columns(knex);

    await knex.transaction(async (trx) => {
      previousPermission = await trx("permissions").where({ id: mapped.id }).first();

      const payload: any = { ...mapped };
      const hasExpireSoonColumn = await this.checkPermissionsColumnExists("expire_soon");
      if (hasExpireSoonColumn) {
        payload.expire_soon = await this.calculateExpireSoon(
          payload,
          new Date(payload.modified || new Date()),
          effectiveHeight
        );
      }

      if (previousPermission) {
        await trx("permissions")
          .where({ id: mapped.id })
          .update(payload);
        finalPermission = await trx("permissions").where({ id: mapped.id }).first();
      } else {
        await trx("permissions").insert(payload);
        finalPermission = await trx("permissions").where({ id: mapped.id }).first();
      }

      try {
        const hasFlipMetaColumns =
          await this.checkPermissionsColumnExists("last_valid_flip_version");
        const hasIsActiveNowColumn =
          await this.checkPermissionsColumnExists("is_active_now");

        if (hasFlipMetaColumns && finalPermission) {
          const fallbackTime = new Date(mapped.modified || finalPermission.modified || new Date().toISOString());
          const currentBlockTime = await getBlockChainTimeAsOf(effectiveHeight, {
            db: trx,
            logContext: "[perm_database]",
            fallback: fallbackTime,
            logger: this.logger,
          });

          const permState = calculatePermState(
            {
              repaid: finalPermission.repaid,
              slashed: finalPermission.slashed,
              revoked: finalPermission.revoked,
              effective_from: finalPermission.effective_from,
              effective_until: finalPermission.effective_until,
              type: finalPermission.type,
              vp_state: finalPermission.vp_state,
              vp_exp: finalPermission.vp_exp,
              validator_perm_id: finalPermission.validator_perm_id,
            },
            currentBlockTime
          );

          const stateInputsChanged =
            !previousPermission
            || previousPermission.repaid !== finalPermission.repaid
            || previousPermission.slashed !== finalPermission.slashed
            || previousPermission.revoked !== finalPermission.revoked
            || previousPermission.effective_from !== finalPermission.effective_from
            || previousPermission.effective_until !== finalPermission.effective_until;

          if (stateInputsChanged) {
            const prevVersion: number =
              typeof previousPermission?.last_valid_flip_version === "number"
                ? previousPermission.last_valid_flip_version
                : 0;
            const newVersion = prevVersion + 1;

            const prevIsActiveNow =
              hasIsActiveNowColumn && previousPermission
                ? Boolean((previousPermission as any).is_active_now)
                : false;

            const permUpdate: Record<string, any> = {
              last_valid_flip_version: newVersion,
            };
       
            await trx("permissions")
              .where({ id: finalPermission.id })
              .update(permUpdate);

            const enterFlips: Array<{ flip_at_time: string; flip_kind: number }> = [];
            const exitFlips: Array<{ flip_at_time: string; flip_kind: number }> = [];

          const effectiveFrom = finalPermission.effective_from
            ? new Date(finalPermission.effective_from)
            : null;
          const effectiveUntil = finalPermission.effective_until
            ? new Date(finalPermission.effective_until)
            : null;
          const revoked = finalPermission.revoked ? new Date(finalPermission.revoked) : null;
          const slashed = finalPermission.slashed ? new Date(finalPermission.slashed) : null;
          const repaid = finalPermission.repaid ? new Date(finalPermission.repaid) : null;

          if (permState === "FUTURE") {
            if (effectiveFrom && !Number.isNaN(effectiveFrom.getTime())) {
              enterFlips.push({
                flip_at_time: effectiveFrom.toISOString(),
                flip_kind: 1, // ENTER_ACTIVE
              });
            }
            if (effectiveUntil && !Number.isNaN(effectiveUntil.getTime())) {
              exitFlips.push({
                flip_at_time: effectiveUntil.toISOString(),
                flip_kind: 2, // EXIT_ACTIVE
              });
            }
          } else if (permState === "ACTIVE") {
            // Spec: insert ENTER_ACTIVE at effective_from.
            if (effectiveFrom && !Number.isNaN(effectiveFrom.getTime())) {
              enterFlips.push({
                flip_at_time: effectiveFrom.toISOString(),
                flip_kind: 1, // ENTER_ACTIVE
              });
            }
            if (effectiveUntil && !Number.isNaN(effectiveUntil.getTime())) {
              exitFlips.push({
                flip_at_time: effectiveUntil.toISOString(),
                flip_kind: 2, // EXIT_ACTIVE
              });
            }
          } else if (permState === "EXPIRED") {
            if (
              prevIsActiveNow &&
              effectiveUntil &&
              !Number.isNaN(effectiveUntil.getTime()) &&
              effectiveUntil < currentBlockTime
            ) {
              exitFlips.push({
                flip_at_time: effectiveUntil.toISOString(),
                flip_kind: 2, // EXIT_ACTIVE
              });
            }
          } else if (permState === "SLASHED" || permState === "REVOKED" || permState === "REPAID") {
            let exitTime: Date | null = null;
            if (permState === "SLASHED") exitTime = slashed;
            if (permState === "REVOKED") exitTime = revoked;
            if (permState === "REPAID") exitTime = repaid;

            if (
              effectiveFrom &&
              !Number.isNaN(effectiveFrom.getTime()) &&
              effectiveFrom < currentBlockTime &&
              exitTime &&
              !Number.isNaN(exitTime.getTime()) &&
              prevIsActiveNow
            ) {
              exitFlips.push({
                flip_at_time: exitTime.toISOString(),
                flip_kind: 2, // EXIT_ACTIVE
              });
            }
          }

            const flipsToInsert = [...enterFlips, ...exitFlips];
            for (const flip of flipsToInsert) {
              try {
                await trx("permission_scheduled_flips")
                  .insert({
                    perm_id: finalPermission.id,
                    flip_at_time: flip.flip_at_time,
                    flip_kind: flip.flip_kind,
                    status: 0, // PENDING
                    version: newVersion,
                    created_at: currentBlockTime.toISOString(),
                  })
                  .onConflict(["perm_id", "version", "flip_at_time", "flip_kind"])
                  .ignore();
              } catch (err: any) {
                this.logger.warn(
                  `[syncPermissionFromLedger] Failed to insert scheduled flip for permission ${finalPermission.id}:`,
                  err?.message || err
                );
              }
            }
          }
        }
      } catch (err: any) {
        this.logger.warn(
          `[syncPermissionFromLedger] Failed to update scheduled flips metadata for permission ${mapped.id}:`,
          err?.message || err
        );
      }
  try {
        const prevSlashed = BigInt(previousPermission?.slashed_deposit ?? 0);
        const newSlashed = BigInt(mapped.slashed_deposit ?? 0);
        const prevRepaid = BigInt(previousPermission?.repaid_deposit ?? 0);
        const newRepaid = BigInt(mapped.repaid_deposit ?? 0);

        const slashDelta = newSlashed > prevSlashed ? newSlashed - prevSlashed : BigInt(0);
        const repayDelta = newRepaid > prevRepaid ? newRepaid - prevRepaid : BigInt(0);

        if (slashDelta > BigInt(0) || repayDelta > BigInt(0)) {
          const permRow = finalPermission || mapped;
          const isEcosystemPermission = permRow.type === "ECOSYSTEM";
          let isEcosystemSlash = false;
          let isNetworkSlash = false;

          if (isEcosystemPermission) {
            isEcosystemSlash = true;
          } else if (permRow.schema_id && ledgerPermission.slashed_by) {
            const schema = await trx("credential_schemas")
              .where({ id: permRow.schema_id })
              .first();
            if (schema?.tr_id) {
              const tr = await trx("trust_registry").where({ id: schema.tr_id }).first();
              const slashedBy = ledgerPermission.slashed_by;
              if (tr?.corporation && slashedBy === tr.corporation) {
                isEcosystemSlash = true;
              } else {
                isNetworkSlash = true;
              }
            } else if (slashDelta > BigInt(0) || repayDelta > BigInt(0)) {
              isNetworkSlash = true;
            }
          } else if (slashDelta > BigInt(0) || repayDelta > BigInt(0)) {
            isNetworkSlash = true;
          }

          if (isEcosystemSlash || isNetworkSlash) {
            await this.updateSlashStatistics(
              trx,
              Number(mapped.id),
              isEcosystemSlash,
              isNetworkSlash,
              Number(slashDelta),
              Number(repayDelta)
            );
          }
        }
      } catch (err: any) {
        this.logger.warn(
          `[syncPermissionFromLedger] Failed to infer slash statistics for permission ${mapped.id}:`,
          err?.message || err
        );
      }

      await this.updateWeight(trx, mapped.id);
      await this.updateParticipants(trx, mapped.id);

      const refreshed = await trx("permissions").where({ id: mapped.id }).first();
      if (refreshed) {
        finalPermission = refreshed;
      }

      await recordPermissionHistory(
        trx,
        finalPermission,
        `SYNC_LEDGER${msgType ? `:${msgType}` : ""}${txHash ? `:${txHash}` : ""}`,
        effectiveHeight,
        previousPermission ? await pickPermissionSnapshot(previousPermission) : undefined
      );
    });

    await this.refreshSchemaAndTrustRegistryStats(mapped.schema_id, effectiveHeight);

    return {
      success: true,
      permissionId: mapped.id,
      schemaId: mapped.schema_id,
      changed: !!previousPermission,
    };
  }

  private async syncPermissionSessionFromLedger(
    ledgerSession: Record<string, any>,
    blockHeight: number,
    txHash?: string,
    msgType?: string
  ) {
    const id = String(ledgerSession?.id || "").trim();
    if (!id) return { success: false, reason: "Invalid permission session id from ledger" };

    const effectiveHeight = Number(blockHeight) || 0;
    const recordsRaw = Array.isArray(ledgerSession?.session_records)
      ? ledgerSession.session_records
      : Array.isArray(ledgerSession?.sessionRecords)
        ? ledgerSession.sessionRecords
        : Array.isArray(ledgerSession?.authz)
          ? ledgerSession.authz
          : [];

    const enrichedAuthz = await Promise.all(recordsRaw.map(async (entry: any) => {
      const directIssuer = entry?.issuer_perm_id ?? entry?.issuerPermId;
      const directVerifier = entry?.verifier_perm_id ?? entry?.verifierPermId;
      if (directIssuer != null || directVerifier != null) {
        const walletAgentPermId =
          Number(entry?.wallet_agent_perm_id ?? entry?.walletAgentPermId ?? ledgerSession?.wallet_agent_perm_id ?? 0) || 0;
        return {
          issuer_perm_id: directIssuer != null ? Number(directIssuer) : null,
          verifier_perm_id: directVerifier != null ? Number(directVerifier) : null,
          wallet_agent_perm_id: walletAgentPermId || null,
        };
      }

      const walletAgentPermId = Number(entry?.wallet_agent_perm_id ?? ledgerSession?.wallet_agent_perm_id ?? 0) || 0;
      const beneficiaryPermId = Number(entry?.beneficiary_perm_id ?? 0) || 0;

      let issuerPermId: number | null = null;
      let verifierPermId: number | null = null;
      if (beneficiaryPermId > 0) {
        const beneficiaryPerm = await knex("permissions").where({ id: beneficiaryPermId }).select("type").first();
        if (beneficiaryPerm?.type === "ISSUER") issuerPermId = beneficiaryPermId;
        if (beneficiaryPerm?.type === "VERIFIER") verifierPermId = beneficiaryPermId;
      }

      return {
        issuer_perm_id: issuerPermId,
        verifier_perm_id: verifierPermId,
        wallet_agent_perm_id: walletAgentPermId || null,
      };
    }));

      const mappedSession: any = {
        id,
        corporation: ledgerSession?.corporation ?? null,
        vs_operator: ledgerSession?.vs_operator ?? ledgerSession?.vsOperator ?? null,
        agent_perm_id: Number(ledgerSession?.agent_perm_id ?? ledgerSession?.agentPermId ?? 0) || 0,
        wallet_agent_perm_id: Number(ledgerSession?.wallet_agent_perm_id ?? ledgerSession?.walletAgentPermId ?? 0) || 0,
        session_records: JSON.stringify(
          enrichedAuthz.map((e: any, i: number) => ({
            created:
              toIsoOrNull(recordsRaw[i]?.created) ??
              toIsoOrNull(ledgerSession?.modified) ??
              new Date().toISOString(),
            issuer_perm_id: e.issuer_perm_id ?? null,
            verifier_perm_id: e.verifier_perm_id ?? null,
            wallet_agent_perm_id: e.wallet_agent_perm_id ?? null,
          }))
        ),
        created: toIsoOrNull(ledgerSession?.created) ?? new Date().toISOString(),
        modified: toIsoOrNull(ledgerSession?.modified) ?? new Date().toISOString(),
      };

    await knex.transaction(async (trx) => {
      const previous = await trx("permission_sessions").where({ id }).first();
      let finalSession: any = null;
      if (previous) {
        await trx("permission_sessions")
          .where({ id })
          .update(mappedSession);
        finalSession = await trx("permission_sessions").where({ id }).first();
      } else {
        await trx("permission_sessions").insert(mappedSession);
        finalSession = await trx("permission_sessions").where({ id }).first();
      }

      await recordPermissionSessionHistory(
        trx,
        finalSession,
        `SYNC_LEDGER${msgType ? `:${msgType}` : ""}${txHash ? `:${txHash}` : ""}`,
        effectiveHeight,
        previous ? pickPermissionSessionSnapshot(previous) : undefined
      );

      const previousAuthzRaw = previous?.session_records ?? previous?.authz;
      let previousAuthz: any[] = [];
      if (previousAuthzRaw) {
        try {
          previousAuthz = typeof previousAuthzRaw === "string" ? JSON.parse(previousAuthzRaw) : previousAuthzRaw;
        } catch {
          previousAuthz = [];
        }
      }

      const previousIssuerPermIds = new Set(
        previousAuthz
          .map((entry: { issuer_perm_id?: string | number }) => entry.issuer_perm_id)
          .filter((v: any) => v !== null && v !== undefined)
      );
      const previousVerifierPermIds = new Set(
        previousAuthz
          .map((entry: { verifier_perm_id?: string | number }) => entry.verifier_perm_id)
          .filter((v: any) => v !== null && v !== undefined)
      );

      const newIssuerPermIds = new Set(
        enrichedAuthz
          .map((entry: { issuer_perm_id?: number | null }) => entry.issuer_perm_id)
          .filter((v): v is number => v !== null && v !== undefined)
      );
      const newVerifierPermIds = new Set(
        enrichedAuthz
          .map((entry: { verifier_perm_id?: number | null }) => entry.verifier_perm_id)
          .filter((v): v is number => v !== null && v !== undefined)
      );

      for (const issuerId of newIssuerPermIds) {
        if (!previousIssuerPermIds.has(issuerId)) {
          try {
            await this.incrementPermissionStatistics(trx, Number(issuerId), true, false);
            try {
              await recordPermissionHistory(
                trx,
                { id: Number(issuerId) },
                "CREDENTIAL_ISSUED",
                effectiveHeight
              );
            } catch (historyErr: any) {
              this.logger.warn(
                `[Session Height-Sync] Failed to record permission history for issued perm ${issuerId}:`,
                historyErr?.message || historyErr
              );
            }
          } catch (issuedErr: any) {
            this.logger.error(
              `[Session Height-Sync] Failed to increment issued for permission ${issuerId}:`,
              issuedErr?.message || issuedErr
            );
          }
        }
      }

      for (const verifierId of newVerifierPermIds) {
        if (!previousVerifierPermIds.has(verifierId)) {
          try {
            await this.incrementPermissionStatistics(trx, Number(verifierId), false, true);
            try {
              await recordPermissionHistory(
                trx,
                { id: Number(verifierId) },
                "CREDENTIAL_VERIFIED",
                effectiveHeight
              );
            } catch (historyErr: any) {
              this.logger.warn(
                `[Session Height-Sync] Failed to record permission history for verified perm ${verifierId}:`,
                historyErr?.message || historyErr
              );
            }
          } catch (verifiedErr: any) {
            this.logger.error(
              `[Session Height-Sync] Failed to increment verified for permission ${verifierId}:`,
              verifiedErr?.message || verifiedErr
            );
          }
        }
      }
    });

    return { success: true, sessionId: id };
  }


  private async rebuildPermissionStats(schemaId?: number) {
    try {
      this.logger.info(`[rebuildPermissionStats] Starting rebuild${schemaId ? ` for schema_id=${schemaId}` : ""}...`);

      await knex.transaction(async (trx) => {
        let permQuery = trx("permissions").select("id", "schema_id");
        if (schemaId && Number.isInteger(schemaId) && schemaId > 0) {
          permQuery = permQuery.where("schema_id", schemaId);
        }
        const perms = await permQuery;
        if (!perms || perms.length === 0) {
          this.logger.info("[rebuildPermissionStats] No permissions found in scope, nothing to rebuild.");
          return;
        }

        const permIds = perms.map((p: any) => Number(p.id));
        const permIdSet = new Set<number>(permIds);

        for (const { id } of perms) {
          const pid = Number(id);
          if (!Number.isInteger(pid) || pid <= 0) continue;
          try {
            await this.updateWeight(trx, pid);
          } catch (err: any) {
            this.logger.warn(`[rebuildPermissionStats] Failed to update weight for permission ${pid}:`, err?.message || err);
          }
          try {
            await this.updateParticipants(trx, pid);
          } catch (err: any) {
            this.logger.warn(`[rebuildPermissionStats] Failed to update participants for permission ${pid}:`, err?.message || err);
          }
        }

        const issuedCounts = new Map<number, number>();
        const verifiedCounts = new Map<number, number>();
        const sessionRows = await trx("permission_sessions").select("session_records");
        for (const row of sessionRows || []) {
          let authz: any[] = [];
          try {
            const raw = row.session_records ?? (row as any).authz;
            if (typeof raw === "string") {
              authz = JSON.parse(raw || "[]");
            } else if (Array.isArray(raw)) {
              authz = raw;
            }
          } catch {
            authz = [];
          }
          for (const entry of authz) {
            const issuerId = Number(entry?.issuer_perm_id ?? 0) || 0;
            const verifierId = Number(entry?.verifier_perm_id ?? 0) || 0;
            if (issuerId > 0 && permIdSet.has(issuerId)) {
              issuedCounts.set(issuerId, (issuedCounts.get(issuerId) || 0) + 1);
            }
            if (verifierId > 0 && permIdSet.has(verifierId)) {
              verifiedCounts.set(verifierId, (verifiedCounts.get(verifierId) || 0) + 1);
            }
          }
        }

        const historyStats = new Map<number, any>();
        const historyRows = await trx("permission_history")
          .whereIn("permission_id", Array.from(permIdSet))
          .select(
            "permission_id",
            "ecosystem_slash_events",
            "ecosystem_slashed_amount",
            "ecosystem_slashed_amount_repaid",
            "network_slash_events",
            "network_slashed_amount",
            "network_slashed_amount_repaid"
          )
          .orderBy("permission_id")
          .orderBy("height", "desc")
          .orderBy("created_at", "desc")
          .orderBy("id", "desc");

        for (const row of historyRows || []) {
          const pid = Number(row.permission_id);
          if (!permIdSet.has(pid)) continue;
          if (!historyStats.has(pid)) {
            historyStats.set(pid, {
              ecosystem_slash_events: Number(row.ecosystem_slash_events ?? 0),
              ecosystem_slashed_amount: Number(row.ecosystem_slashed_amount ?? 0),
              ecosystem_slashed_amount_repaid: Number(row.ecosystem_slashed_amount_repaid ?? 0),
              network_slash_events: Number(row.network_slash_events ?? 0),
              network_slashed_amount: Number(row.network_slashed_amount ?? 0),
              network_slashed_amount_repaid: Number(row.network_slashed_amount_repaid ?? 0),
            });
          }
        }

        for (const { id } of perms) {
          const pid = Number(id);
          if (!Number.isInteger(pid) || pid <= 0) continue;

          const issued = issuedCounts.get(pid) || 0;
          const verified = verifiedCounts.get(pid) || 0;
          const slash = historyStats.get(pid) || {
            ecosystem_slash_events: 0,
            ecosystem_slashed_amount: 0,
            ecosystem_slashed_amount_repaid: 0,
            network_slash_events: 0,
            network_slashed_amount: 0,
            network_slashed_amount_repaid: 0,
          };

          try {
            await trx("permissions")
              .where({ id: pid })
              .update({
                issued,
                verified,
                ecosystem_slash_events: slash.ecosystem_slash_events,
                ecosystem_slashed_amount: slash.ecosystem_slashed_amount,
                ecosystem_slashed_amount_repaid: slash.ecosystem_slashed_amount_repaid,
                network_slash_events: slash.network_slash_events,
                network_slashed_amount: slash.network_slashed_amount,
                network_slashed_amount_repaid: slash.network_slashed_amount_repaid,
              });
          } catch (err: any) {
            this.logger.warn(
              `[rebuildPermissionStats] Failed to update rebuilt stats for permission ${pid}:`,
              err?.message || err
            );
          }
        }
      });

      this.logger.info(`[rebuildPermissionStats] Completed rebuild${schemaId ? ` for schema_id=${schemaId}` : ""}.`);
      return { success: true };
    } catch (err: any) {
      this.logger.error("[rebuildPermissionStats] Failed to rebuild permission stats:", err);
      return { success: false, reason: err?.message || String(err) };
    }
  }

  private normalizeComparableTimestamp(value: unknown): string | null {
    if (value === null || value === undefined || value === "") return null;
    const d = new Date(String(value));
    if (Number.isNaN(d.getTime())) return null;
    return d.toISOString();
  }

  private normalizeComparablePermissionRecord(record: Record<string, any> | null | undefined): Record<string, any> | null {
    if (!record) return null;
    return {
      id: Number(record.id ?? record.permission_id ?? 0) || 0,
      schema_id: Number(record.schema_id ?? record.schemaId ?? 0) || 0,
      type: normalizePermissionType(record.type),
      did: record.did ?? null,
      corporation: record.corporation ?? null,
      created: this.normalizeComparableTimestamp(record.created),
      modified: this.normalizeComparableTimestamp(record.modified),
      adjusted: this.normalizeComparableTimestamp(record.adjusted),
      adjusted_by: record.adjusted_by ?? record.adjustedBy ?? null,
      slashed: this.normalizeComparableTimestamp(record.slashed),
      repaid: this.normalizeComparableTimestamp(record.repaid),
      effective_from: this.normalizeComparableTimestamp(record.effective_from ?? record.effectiveFrom),
      effective_until: this.normalizeComparableTimestamp(record.effective_until ?? record.effectiveUntil),
      validation_fees: Number(record.validation_fees ?? record.validationFees ?? 0),
      issuance_fees: Number(record.issuance_fees ?? record.issuanceFees ?? 0),
      verification_fees: Number(record.verification_fees ?? record.verificationFees ?? 0),
      deposit: Number(record.deposit ?? 0),
      slashed_deposit: Number(record.slashed_deposit ?? record.slashedDeposit ?? 0),
      repaid_deposit: Number(record.repaid_deposit ?? record.repaidDeposit ?? 0),
      revoked: this.normalizeComparableTimestamp(record.revoked),
      validator_perm_id: Number(record.validator_perm_id ?? record.validatorPermId ?? 0) || null,
      vp_state: normalizeValidationState(record.vp_state ?? record.vpState),
      vp_exp: this.normalizeComparableTimestamp(record.vp_exp ?? record.vpExp),
      vp_last_state_change: this.normalizeComparableTimestamp(record.vp_last_state_change ?? record.vpLastStateChange),
      vp_validator_deposit: Number(record.vp_validator_deposit ?? record.vpValidatorDeposit ?? 0),
      vp_current_fees: Number(record.vp_current_fees ?? record.vpCurrentFees ?? 0),
      vp_current_deposit: Number(record.vp_current_deposit ?? record.vpCurrentDeposit ?? 0),
      vp_summary_digest:
        record.vp_summary_digest ??
        record.vpSummaryDigest ??
        null,
    };
  }

  private normalizeComparablePermissionSessionRecord(record: Record<string, any> | null | undefined): Record<string, any> | null {
    if (!record) return null;
    const srRaw = Array.isArray(record.session_records)
      ? record.session_records
      : parseJson(record.session_records) ?? parseJson(record.authz) ?? [];
    const sessionRecords = Array.isArray(srRaw)
      ? srRaw
          .map((entry: any) => ({
            created: String(entry?.created ?? ""),
            issuer_perm_id: Number(entry?.issuer_perm_id ?? 0) || null,
            verifier_perm_id: Number(entry?.verifier_perm_id ?? 0) || null,
            wallet_agent_perm_id: Number(entry?.wallet_agent_perm_id ?? 0) || null,
          }))
          .sort((a: any, b: any) => JSON.stringify(a).localeCompare(JSON.stringify(b)))
      : [];

    return {
      id: String(record.id ?? record.session_id ?? ""),
      corporation: record.corporation ?? null,
      vs_operator: record.vs_operator ?? record.vsOperator ?? null,
      agent_perm_id: Number(record.agent_perm_id ?? record.agentPermId ?? 0) || 0,
      wallet_agent_perm_id: Number(record.wallet_agent_perm_id ?? record.walletAgentPermId ?? 0) || 0,
      session_records: sessionRecords,
      created: this.normalizeComparableTimestamp(record.created),
      modified: this.normalizeComparableTimestamp(record.modified),
    };
  }

  private async getPermissionSnapshotAtHeight(permissionId: number, blockHeight: number): Promise<any | null> {
    if (!(Number.isInteger(blockHeight) && blockHeight > 0)) {
      return knex("permissions").where({ id: permissionId }).first();
    }
    const history = await knex("permission_history")
      .where({ permission_id: permissionId })
      .andWhere("height", "<=", blockHeight)
      .orderBy("height", "desc")
      .orderBy("created_at", "desc")
      .orderBy("id", "desc")
      .first();
    if (!history) return null;
    return {
      ...history,
      id: Number(history.permission_id),
    };
  }

  private async getPermissionSessionSnapshotAtHeight(sessionId: string, blockHeight: number): Promise<any | null> {
    if (!(Number.isInteger(blockHeight) && blockHeight > 0)) {
      return knex("permission_sessions").where({ id: sessionId }).first();
    }
    const history = await knex("permission_session_history")
      .where({ session_id: sessionId })
      .andWhere("height", "<=", blockHeight)
      .orderBy("height", "desc")
      .orderBy("created_at", "desc")
      .orderBy("id", "desc")
      .first();
    if (!history) return null;
    return {
      ...history,
      id: history.session_id,
    };
  }

  private compareObjects(lhs: Record<string, any> | null, rhs: Record<string, any> | null) {
    if (!lhs || !rhs) {
      return {
        matches: false,
        diffs: [{ field: "record", lhs: lhs ?? null, rhs: rhs ?? null }],
      };
    }
    const keys = new Set([...Object.keys(lhs), ...Object.keys(rhs)]);
    const diffs: Array<{ field: string; lhs: any; rhs: any }> = [];
    for (const key of keys) {
      if (JSON.stringify(lhs[key]) !== JSON.stringify(rhs[key])) {
        diffs.push({ field: key, lhs: lhs[key], rhs: rhs[key] });
      }
    }
    return { matches: diffs.length === 0, diffs };
  }

  private async comparePermissionWithLedger(
    permissionId: number,
    ledgerPermission: Record<string, any>,
    blockHeight: number
  ) {
    const dbSnapshot = await this.getPermissionSnapshotAtHeight(permissionId, blockHeight);
    const normalizedDb = this.normalizeComparablePermissionRecord(dbSnapshot);
    const normalizedLedger = this.normalizeComparablePermissionRecord(ledgerPermission);
    const result = this.compareObjects(normalizedDb, normalizedLedger);
    return {
      success: true,
      matches: result.matches,
      diffs: result.diffs,
      permissionId,
      blockHeight,
    };
  }

  private async comparePermissionSessionWithLedger(
    sessionId: string,
    ledgerSession: Record<string, any>,
    blockHeight: number
  ) {
    const dbSnapshot = await this.getPermissionSessionSnapshotAtHeight(sessionId, blockHeight);
    const normalizedDb = this.normalizeComparablePermissionSessionRecord(dbSnapshot);
    const normalizedLedger = this.normalizeComparablePermissionSessionRecord(ledgerSession);
    const result = this.compareObjects(normalizedDb, normalizedLedger);
    return {
      success: true,
      matches: result.matches,
      diffs: result.diffs,
      sessionId,
      blockHeight,
    };
  }

  private async handleCreateRootPermission(msg: MsgCreateRootPermission & { height?: number }) {
    let permission: any = null;
    try {
      this.logger.info(`🔐 handleCreateRootPermission called with msg:`, JSON.stringify(msg, null, 2));
      const schemaId = (msg as any).schemaId ?? (msg as any).schema_id ?? null;
      this.logger.info(`🔐 Extracted schemaId: ${schemaId}`);
      if (!schemaId) {
        this.logger.error(
          "CRITICAL: Missing schema_id in MsgCreateRootPermission, cannot create root permission. Msg keys:", Object.keys(msg)
        );
        return;

      }

      const creator = requireController(msg, `PERM CREATE_ROOT ${schemaId}`);
      const timestamp = msg?.timestamp ? formatTimestamp(msg.timestamp) : null;
      const effectiveFromRaw = pickMessageValue(msg as any, "effective_from", "effectiveFrom");
      const effectiveUntilRaw = pickMessageValue(msg as any, "effective_until", "effectiveUntil");
      const effectiveFrom = effectiveFromRaw ? formatTimestamp(effectiveFromRaw) : null;
      const effectiveUntil = effectiveUntilRaw ? formatTimestamp(effectiveUntilRaw) : null;
      const permissionType = extractPermissionType(msg as any, "ECOSYSTEM");

      // Calculate expire_soon for the new permission
      const newPermData = {
        type: permissionType,
        vp_state: "VALIDATION_STATE_UNSPECIFIED",
        effective_from: effectiveFrom,
        effective_until: effectiveUntil,
        revoked: null,
        slashed: null,
        repaid: null,
      };
      const height = Number((msg as any)?.height) || 0;
      const expireSoon = await this.calculateExpireSoon(newPermData, new Date(timestamp || new Date()), height);
      const hasExpireSoonColumn = await this.checkPermissionsColumnExists("expire_soon");

      this.logger.info(`[handleCreateRootPermission] expire_soon calculated: ${expireSoon}, column exists: ${hasExpireSoonColumn}`);

      const msgIdNum = Number((msg as any)?.id);
      const hasValidMsgId = Number.isInteger(msgIdNum) && msgIdNum > 0;

      const insertData: any = {
        schema_id: schemaId,
        type: permissionType,
        vp_state: "VALIDATION_STATE_UNSPECIFIED",
        did: msg.did,
        corporation: creator,
        effective_from: effectiveFrom,
        effective_until: effectiveUntil,
        validation_fees: Number(pickMessageValue(msg as any, "validation_fees", "validationFees") ?? 0),
        issuance_fees: Number(pickMessageValue(msg as any, "issuance_fees", "issuanceFees") ?? 0),
        verification_fees: Number(pickMessageValue(msg as any, "verification_fees", "verificationFees") ?? 0),
        vs_operator: pickMessageValue(msg as any, "vs_operator", "vsOperator") ?? null,
        adjusted: timestamp,
        adjusted_by: creator,
        deposit: 0,
        modified: timestamp,
        created: timestamp,
      };

      if (hasValidMsgId) {
        insertData.id = msgIdNum;
      }

      if (hasExpireSoonColumn) {
        insertData.expire_soon = expireSoon;
        this.logger.info(`[handleCreateRootPermission] Adding expire_soon=${expireSoon} to insert data`);
      } else {
        this.logger.warn(`[handleCreateRootPermission] expire_soon column does not exist, skipping. Run migration 20260128000000_add_permission_expire_soon.ts`);
      }

      let insertedPermission: any = null;
      await this.ensurePermV4Columns(knex);
      await knex.transaction(async (trx) => {
        const existing = hasValidMsgId
          ? await trx("permissions").where({ id: msgIdNum }).first()
          : null;

        if (existing) {
          await trx("permissions").where({ id: msgIdNum }).update(insertData);
          insertedPermission = await trx("permissions").where({ id: msgIdNum }).first();
        } else {
          try {
            [insertedPermission] = await trx("permissions")
              .insert(insertData)
              .returning("*");
          } catch (insertError: any) {
            if (insertError?.code === "42703" && insertError?.message?.includes("expire_soon")) {
              this.logger.warn(
                `[handleCreateRootPermission] expire_soon column error detected, clearing cache and retrying without expire_soon`
              );
              this.permissionsColumnExistsCache = null;
              delete insertData.expire_soon;
              [insertedPermission] = await trx("permissions")
                .insert(insertData)
                .returning("*");
            } else {
              throw insertError;
            }
          }
        }
      });

      if (!insertedPermission) {
        this.logger.error(
          "CRITICAL: Failed to create root permission - insert returned no record"
        );
        return;

      }

      permission = insertedPermission;

      try {
        await knex.transaction(async (trx) => {
          await this.updateParticipants(trx, Number(permission.id));
        });
      } catch (participantsErr: any) {
        this.logger.warn(`Failed to update participants for root permission ${permission.id}:`, participantsErr?.message || participantsErr);
      }

      try {
        await recordPermissionHistory(
          knex,
          permission,
          "CREATE_ROOT_PERMISSION",
          height
        );
      } catch (historyErr: any) {
        this.logger.error(
          "CRITICAL: Failed to record permission history for root permission:",
          historyErr
        );

      }

      await this.refreshTrustRegistryStatsBySchemaId(schemaId, height);
    } catch (err: any) {
      this.logger.error("CRITICAL: Error in handleCreateRootPermission:", err);
      console.error("FATAL PERM CREATE ROOT ERROR:", err);

    }
  }

  private async handleCreatePermission(msg: MsgCreatePermission & { height?: number }) {
    try {
      this.logger.info(`🔐 handleCreatePermission called with msg:`, JSON.stringify(msg, null, 2));
      let schemaId = (msg as any).schemaId ?? (msg as any).schema_id ?? null;
      const explicitValidatorPermId = (msg as any).validatorPermId ?? (msg as any).validator_perm_id ?? null;
      let validatorPermFromMessage: any = null;
      if (!schemaId && explicitValidatorPermId) {
        validatorPermFromMessage = await knex("permissions")
          .where({ id: explicitValidatorPermId })
          .first();
        schemaId = validatorPermFromMessage?.schema_id ?? null;
      }
      this.logger.info(`🔐 Extracted schemaId: ${schemaId}`);
      if (!schemaId) {
        this.logger.warn(
          "Missing schema_id and could not infer it from validator_perm_id in MsgCreatePermission, skipping insert. Msg keys:", Object.keys(msg)
        );
        return;

      }

      const type = extractPermissionType(msg as any, (msg as any).type);

      const ecosystemPerm = await knex("permissions")
        .where({ schema_id: schemaId, type: "ECOSYSTEM" })
        .first();

      if (!ecosystemPerm) {
        this.logger.warn(
          `No root ECOSYSTEM permission found for schema_id=${schemaId}, cannot create ${type}`
        );
      }

      const creator = requireController(msg, `PERM CREATE ${schemaId}`);
      const timestamp = msg?.timestamp ? formatTimestamp(msg.timestamp) : null;
      const effectiveFromRaw = pickMessageValue(msg as any, "effective_from", "effectiveFrom");
      const effectiveUntilRaw = pickMessageValue(msg as any, "effective_until", "effectiveUntil");
      const effectiveFrom = effectiveFromRaw ? formatTimestamp(effectiveFromRaw) : null;
      const effectiveUntil = effectiveUntilRaw ? formatTimestamp(effectiveUntilRaw) : null;
      const height = Number((msg as any)?.height) || 0;

      // Calculate expire_soon for the new permission
      const newPermData = {
        type,
        vp_state: "VALIDATION_STATE_UNSPECIFIED",
        effective_from: effectiveFrom,
        effective_until: effectiveUntil,
        revoked: null,
        slashed: null,
        repaid: null,
      };
      const expireSoon = await this.calculateExpireSoon(newPermData, new Date(timestamp || new Date()), height);
      const hasExpireSoonColumn = await this.checkPermissionsColumnExists("expire_soon");

      this.logger.info(`[handleCreatePermission] expire_soon calculated: ${expireSoon}, column exists: ${hasExpireSoonColumn}`);

      const msgIdNum = Number((msg as any)?.id);
      const hasValidMsgId = Number.isInteger(msgIdNum) && msgIdNum > 0;

      const insertData: any = {
        schema_id: schemaId,
        type,
        vp_state: "VALIDATION_STATE_UNSPECIFIED",
        did: msg.did,
        corporation: creator,
        effective_from: effectiveFrom,
        effective_until: effectiveUntil,
        verification_fees: Number(pickMessageValue(msg as any, "verification_fees", "verificationFees") ?? 0),
        validation_fees: Number(pickMessageValue(msg as any, "validation_fees", "validationFees") ?? 0),
        issuance_fees: Number(pickMessageValue(msg as any, "issuance_fees", "issuanceFees") ?? 0),
        deposit: 0,
        validator_perm_id: explicitValidatorPermId ?? ecosystemPerm?.id ?? null,
        vs_operator: pickMessageValue(msg as any, "vs_operator", "vsOperator") ?? null,
        vs_operator_authz_enabled: pickMessageValue(msg as any, "vs_operator_authz_enabled", "vsOperatorAuthzEnabled") ?? null,
        vs_operator_authz_spend_limit: pickMessageValue(msg as any, "vs_operator_authz_spend_limit", "vsOperatorAuthzSpendLimit") ?? null,
        vs_operator_authz_with_feegrant: pickMessageValue(msg as any, "vs_operator_authz_with_feegrant", "vsOperatorAuthzWithFeegrant") ?? null,
        vs_operator_authz_fee_spend_limit: pickMessageValue(msg as any, "vs_operator_authz_fee_spend_limit", "vsOperatorAuthzFeeSpendLimit") ?? null,
        vs_operator_authz_spend_period: pickMessageValue(msg as any, "vs_operator_authz_spend_period", "vsOperatorAuthzSpendPeriod") ?? null,
        modified: timestamp,
        created: timestamp,
      };

      if (hasValidMsgId) {
        insertData.id = msgIdNum;
      }

      if (hasExpireSoonColumn) {
        insertData.expire_soon = expireSoon;
        this.logger.info(`[handleCreatePermission] Adding expire_soon=${expireSoon} to insert data`);
      } else {
        this.logger.warn(`[handleCreatePermission] expire_soon column does not exist, skipping. Run migration 20260128000000_add_permission_expire_soon.ts`);
      }

      let permission: any = null;
      await this.ensurePermV4Columns(knex);
      await knex.transaction(async (trx) => {
        const existing = hasValidMsgId
          ? await trx("permissions").where({ id: msgIdNum }).first()
          : null;

        if (existing) {
          await trx("permissions").where({ id: msgIdNum }).update(insertData);
          permission = await trx("permissions").where({ id: msgIdNum }).first();
        } else {
          try {
            [permission] = await trx("permissions")
              .insert(insertData)
              .returning("*");
          } catch (insertError: any) {
            // If error is about missing column, clear cache and retry without expire_soon
            if (insertError?.code === "42703" && insertError?.message?.includes("expire_soon")) {
              this.logger.warn(
                `[handleCreatePermission] expire_soon column error detected, clearing cache and retrying without expire_soon`
              );
              this.permissionsColumnExistsCache = null;
              delete insertData.expire_soon;
              [permission] = await trx("permissions")
                .insert(insertData)
                .returning("*");
            } else {
              throw insertError;
            }
          }
        }
      });

      if (!permission) {
        this.logger.error(
          "CRITICAL: Failed to create permission - insert returned no record"
        );
        return;


      }

      try {
        await knex.transaction(async (trx) => {
          await this.updateParticipants(trx, Number(permission.id));
        });
      } catch (participantsErr: any) {
        this.logger.warn(`Failed to update participants for permission ${permission.id}:`, participantsErr?.message || participantsErr);
      }

      try {
        await recordPermissionHistory(
          knex,
          permission,
          "CREATE_PERMISSION",
          height
        );
      } catch (historyErr: any) {
        this.logger.error(
          "CRITICAL: Failed to record permission history:",
          historyErr
        );

      }

      await this.refreshTrustRegistryStatsBySchemaId(permission.schema_id, height);
    } catch (err: any) {
      this.logger.error("CRITICAL: Error in handleCreatePermission:", err);
      console.error("FATAL PERM CREATE ERROR:", err);

    }
  }

  private async handleRevokePermission(msg: MsgRevokePermission & { height?: number }) {
    try {
      if (!msg.id) {
        this.logger.warn("Missing mandatory parameter: id");
        return { success: false, reason: "Missing mandatory parameter" };
      }

      const now = formatTimestamp(msg.timestamp);
      const applicantPerm = await knex("permissions")
        .where({ id: msg.id })
        .first();
      if (!applicantPerm) {
        this.logger.warn(`Permission ${msg.id} not found`);
        return { success: false, reason: "Permission not found" };
      }

      const caller = extractController(msg as unknown as Record<string, unknown>);

      let authorized = false;
      if (applicantPerm.validator_perm_id) {
        let validatorPermId = applicantPerm.validator_perm_id;
        while (validatorPermId) {
          const validatorPerm = await knex("permissions")
            .where({ id: validatorPermId })
            .first();
          if (!validatorPerm) break;
          if (validatorPerm.corporation === caller) {
            authorized = true;
            break;
          }
          validatorPermId = validatorPerm.validator_perm_id;
        }
      }

      if (!authorized) {
        const cs = await knex("credential_schemas")
          .where({ id: applicantPerm.schema_id })
          .first();
        if (cs) {
          const tr = await knex("trust_registry")
            .where({ id: cs.tr_id })
            .first();
          if (tr?.corporation === caller) {
            authorized = true;
          }
        }
      }

      if (!authorized) {
        if (applicantPerm.corporation === caller) {
          authorized = true;
        }
      }

      if (!authorized) {
        this.logger.warn("Caller is not authorized to revoke this permission");
        return { success: false, reason: "Unauthorized caller" };
      }

      const height = Number((msg as any)?.height) || 0;
      await knex.transaction(async (trx) => {
        // Revoked permissions are not active, so expire_soon should be null
        const hasExpireSoonColumn = await this.checkPermissionsColumnExists("expire_soon");

        const updateData: any = {
          revoked: now,
          modified: now,
        };

        if (hasExpireSoonColumn) {
          updateData.expire_soon = null;
        }

        const [updated] = await trx("permissions")
          .where({ id: msg.id })
          .update(updateData)
          .returning("*");

        if (!updated) {
          this.logger.error(
            `CRITICAL: Failed to revoke permission ${msg.id} - update returned no record`
          );

        }

        try {
          await this.updateParticipants(trx, Number(msg.id));
        } catch (participantsErr: any) {
          this.logger.warn(`Failed to update participants for permission ${msg.id}:`, participantsErr?.message || participantsErr);
        }

        try {
          await recordPermissionHistory(
            trx,
            updated,
            "REVOKE_PERMISSION",
            height,
            applicantPerm
          );
        } catch (historyErr: any) {
          this.logger.error(
            "CRITICAL: Failed to record permission history for revoke:",
            historyErr
          );

        }
      });

      await this.refreshTrustRegistryStatsBySchemaId(applicantPerm?.schema_id, height);

      this.logger.info(
        `Permission ${msg.id} successfully revoked by ${caller}`
      );
      return { success: true };
    } catch (err: any) {
      this.logger.error("CRITICAL: Error in handleRevokePermission:", err);
      console.error("FATAL PERM REVOKE ERROR:", err);
      return { success: false, reason: "Internal error revoking permission" };
    }
  }

  private async handleStartPermissionVP(msg: MsgStartPermissionVP & { height?: number }) {
    try {
      const typeStr = extractPermissionType(msg as any, getPermissionTypeString(msg));
      const now = formatTimestamp(msg.timestamp);
      const validatorPermId = (msg as any).validatorPermId ?? (msg as any).validator_perm_id;

      const perm = await knex("permissions")
        .where({ id: validatorPermId })
        .first();

      if (!perm) {
        this.logger.warn(
          `Permission ${validatorPermId} not found, skipping VP start`
        );
        return;
      }

      const globalVariables = await getGlobalVariables();
      if (!globalVariables) {
        this.logger.info(
          `Global variables: ${JSON.stringify(globalVariables)}`
        );
      }

      let validationFeesDenom = 0;
      let validationTDDenom = 0;

      if (typeStr !== "HOLDER") {
        const trustUnitPrice = Number(
          globalVariables?.tr?.trust_unit_price ?? 0
        );
        const trustDepositRate = Number(
          globalVariables?.td?.trust_deposit_rate ?? 0
        );

        validationFeesDenom =
          perm?.validation_fees && trustUnitPrice
            ? Number(perm.validation_fees) * trustUnitPrice
            : 0;

        validationTDDenom =
          validationFeesDenom && trustDepositRate
            ? validationFeesDenom * trustDepositRate
            : 0;
      }

      const creator = requireController(msg, `PERM START_VP ${validatorPermId}`);
      const effectiveFromRaw = pickMessageValue(msg as any, "effective_from", "effectiveFrom");
      const effectiveUntilRaw = pickMessageValue(msg as any, "effective_until", "effectiveUntil");
      const effectiveFrom = effectiveFromRaw ? formatTimestamp(effectiveFromRaw) : null;
      const effectiveUntil = effectiveUntilRaw ? formatTimestamp(effectiveUntilRaw) : null;
      const height = Number((msg as any)?.height) || 0;

      // Calculate expire_soon for the new permission (PENDING state means not active, so null)
      const newPermData = {
        type: typeStr,
        vp_state: "PENDING",
        effective_from: effectiveFrom,
        effective_until: effectiveUntil,
        revoked: null,
        slashed: null,
        repaid: null,
      };
      const expireSoon = await this.calculateExpireSoon(newPermData, new Date(now), height);
      const hasExpireSoonColumn = await this.checkPermissionsColumnExists("expire_soon");

      const msgIdNum = Number((msg as any)?.id ?? (msg as any)?.permission_id ?? (msg as any)?.permissionId);
      const hasValidMsgId = Number.isInteger(msgIdNum) && msgIdNum > 0;

      const Entry: any = {
        schema_id: perm?.schema_id,
        type: typeStr,
        did: msg.did,
        corporation: creator,
        effective_from: effectiveFrom,
        effective_until: effectiveUntil,
        verification_fees: Number(pickMessageValue(msg as any, "verification_fees", "verificationFees") ?? 0),
        validation_fees: Number(pickMessageValue(msg as any, "validation_fees", "validationFees") ?? 0),
        issuance_fees: Number(pickMessageValue(msg as any, "issuance_fees", "issuanceFees") ?? 0),
        deposit: Number(validationTDDenom),
        vp_current_deposit: Number(validationTDDenom),
        vp_current_fees: Number(validationFeesDenom), 
        validator_perm_id: validatorPermId,
        vp_state: "PENDING",
        vp_last_state_change: now,
        vp_validator_deposit: 0, 
        vp_summary_digest: null,
        vs_operator: pickMessageValue(msg as any, "vs_operator", "vsOperator") ?? null,
        vs_operator_authz_enabled: pickMessageValue(msg as any, "vs_operator_authz_enabled", "vsOperatorAuthzEnabled") ?? null,
        vs_operator_authz_spend_limit: pickMessageValue(msg as any, "vs_operator_authz_spend_limit", "vsOperatorAuthzSpendLimit") ?? null,
        vs_operator_authz_with_feegrant: pickMessageValue(msg as any, "vs_operator_authz_with_feegrant", "vsOperatorAuthzWithFeegrant") ?? null,
        vs_operator_authz_fee_spend_limit: pickMessageValue(msg as any, "vs_operator_authz_fee_spend_limit", "vsOperatorAuthzFeeSpendLimit") ?? null,
        vs_operator_authz_spend_period: pickMessageValue(msg as any, "vs_operator_authz_spend_period", "vsOperatorAuthzSpendPeriod") ?? null,
        modified: now,
        created: now, 
      };

      if (hasValidMsgId) {
        Entry.id = msgIdNum;
      }

      if (hasExpireSoonColumn) {
        Entry.expire_soon = expireSoon;
        this.logger.info(`[handleStartPermissionVP] Adding expire_soon=${expireSoon} to insert data`);
      } else {
        this.logger.warn(`[handleStartPermissionVP] expire_soon column does not exist, skipping. Run migration 20260128000000_add_permission_expire_soon.ts`);
      }

      await this.ensurePermV4Columns(knex);

      let newPermission: any = null;
      await knex.transaction(async (trx) => {
        const existing = hasValidMsgId ? await trx("permissions").where({ id: msgIdNum }).first() : null;

        if (existing) {
          await trx("permissions").where({ id: msgIdNum }).update(Entry);
          newPermission = await trx("permissions").where({ id: msgIdNum }).first();
          return;
        }

        try {
          [newPermission] = await trx("permissions").insert(Entry).returning("*");
        } catch (insertError: any) {
          if (insertError?.code === "42703" && insertError?.message?.includes("expire_soon")) {
            this.logger.warn(
              `[handleStartPermissionVP] expire_soon column error detected, clearing cache and retrying without expire_soon`
            );
            this.permissionsColumnExistsCache = null;
            delete Entry.expire_soon;
            [newPermission] = await trx("permissions").insert(Entry).returning("*");
          } else {
            throw insertError;
          }
        }
      });

      if (!newPermission) {
        this.logger.error(
          "CRITICAL: Failed to create permission via VP start - insert returned no record"
        );

      }

      this.logger.info(
        `Inserted new VP entry handleStartPermissionVP: ${JSON.stringify(
          Entry
        )}`
      );

      try {
        await knex.transaction(async (trx) => {
          await this.updateWeight(trx, Number(newPermission.id));
          await this.updateParticipants(trx, Number(newPermission.id));
        });
      } catch (updateErr: any) {
        this.logger.warn(`Failed to update weight/participants for new permission ${newPermission.id}:`, updateErr?.message || updateErr);
      }

      try {
        await recordPermissionHistory(
          knex,
          newPermission,
          "START_PERMISSION_VP",
          height
        );
      } catch (historyErr: any) {
        this.logger.error(
          "CRITICAL: Failed to record permission history for VP start:",
          historyErr
        );

      }

      await this.refreshTrustRegistryStatsBySchemaId(newPermission.schema_id, height);
    } catch (err: any) {
      this.logger.error("CRITICAL: Error in handleStartPermissionVP:", err);
      console.error("FATAL PERM START VP ERROR:", err);
      // No structured response expected; handler returns void
    }
  }

  public async computeVpExp(perm: any, knex: any): Promise<string | null> {
    const cs = await knex("credential_schemas")
      .where({ id: perm.schema_id })
      .first();

    if (!cs) {
      const schemaExists = await knex("credential_schemas")
        .where({ id: perm.schema_id })
        .first();

      if (!schemaExists) {
        this.logger.warn(`CredentialSchema ${perm.schema_id} not found for permission ${perm.id} - schema may not exist yet, queuing for retry`);
      } else {
        this.logger.warn(`CredentialSchema ${perm.schema_id} exists but not visible in current transaction for permission ${perm.id} - queuing for retry`);
      }
      return null;
    }

    let validityPeriodField: number | null = null;
    let validityPeriodFieldName = "";

    switch (perm.type) {
      case "ISSUER_GRANTOR":
        validityPeriodField = cs.issuer_grantor_validation_validity_period;
        validityPeriodFieldName = "issuer_grantor_validation_validity_period";
        break;
      case "VERIFIER_GRANTOR":
        validityPeriodField = cs.verifier_grantor_validation_validity_period;
        validityPeriodFieldName = "verifier_grantor_validation_validity_period";
        break;
      case "ISSUER":
        validityPeriodField = cs.issuer_validation_validity_period;
        validityPeriodFieldName = "issuer_validation_validity_period";
        break;
      case "VERIFIER":
        validityPeriodField = cs.verifier_validation_validity_period;
        validityPeriodFieldName = "verifier_validation_validity_period";
        break;
      case "HOLDER":
        validityPeriodField = cs.holder_validation_validity_period;
        validityPeriodFieldName = "holder_validation_validity_period";
        break;
      default:
        this.logger.warn(`Unknown permission type '${perm.type}' for permission ${perm.id} - cannot compute vp_exp`);
        return null;
    }

    if (validityPeriodField === null || validityPeriodField === undefined || validityPeriodField === 0) {
      this.logger.info(
        `CredentialSchema ${perm.schema_id} validity period field '${validityPeriodFieldName}' is null/undefined/zero ` +
        `for permission ${perm.id} (type: ${perm.type}) - returning null vp_exp per spec`
      );
      return null;
    }

    const validitySeconds = Number(validityPeriodField);
    if (Number.isNaN(validitySeconds)) {
      this.logger.warn(
        `CredentialSchema ${perm.schema_id} has invalid validity period value '${validityPeriodField}' ` +
        `for field '${validityPeriodFieldName}' (permission ${perm.id}, type: ${perm.type})`
      );
      return null;
    }

    const now = new Date();

    let vpExp: Date;

    if (!perm.vp_exp) {
      vpExp = new Date(now.getTime() + validitySeconds * 1000);
    } else {
      vpExp = new Date(
        new Date(perm.vp_exp).getTime() + validitySeconds * 1000
      );
    }

    return vpExp.toISOString();
  }

  private async handleSetPermissionVPToValidated(
    msg: MsgSetPermissionVPToValidated & { height?: number }
  ) {
    try {
      const now = formatTimestamp(msg.timestamp);
      const height = Number((msg as any)?.height) || 0;

      this.logger.info(`[SetVPToValidated] Processing permission id=${msg.id}, height=${height}`);

      const perm = await knex("permissions").where({ id: msg.id }).first();
      if (!perm) {
        this.logger.error(`[SetVPToValidated] Permission ${msg.id} not found in database`);
        return { success: false, reason: `Permission ${msg.id} not found` };
      }

      if (perm.vp_state !== "PENDING") {
        this.logger.warn(`[SetVPToValidated] Permission ${msg.id} is not PENDING (current state: ${perm.vp_state})`);
        return { success: false, reason: `Permission not pending, current state: ${perm.vp_state}` };
      }

      const isFirstValidation = !perm.effective_from;

      if (
        msg.validation_fees < 0 ||
        msg.issuance_fees < 0 ||
        msg.verification_fees < 0
      ) {
        this.logger.warn(`Fees must be >= 0`);
        return { success: false, reason: "Invalid fees" };
      }

      const schemaExists = await knex("credential_schemas")
        .where({ id: perm.schema_id })
        .first();

      if (!schemaExists) {
        await this.queuePermissionForRetry(msg, "SCHEMA_NOT_FOUND");
        this.logger.info(`Permission ${msg.id} queued for retry - waiting for CredentialSchema ${perm.schema_id}`);
        return { success: true, message: "Permission queued for retry - schema not ready" };
      }

      const vpExp = await this.computeVpExp(perm, knex);
      if (vpExp === null) {
        const validityPeriodFieldName =
          perm.type === "ISSUER_GRANTOR" ? "issuer_grantor_validation_validity_period" :
            perm.type === "VERIFIER_GRANTOR" ? "verifier_grantor_validation_validity_period" :
              perm.type === "ISSUER" ? "issuer_validation_validity_period" :
                perm.type === "VERIFIER" ? "verifier_validation_validity_period" :
                  perm.type === "HOLDER" ? "holder_validation_validity_period" : "unknown";
        
        const validityPeriodValue = schemaExists[validityPeriodFieldName];
        if (validityPeriodValue !== undefined && validityPeriodValue !== null) {
          this.logger.info(
            `Permission ${msg.id}: CredentialSchema ${perm.schema_id} has validity period field ` +
            `'${validityPeriodFieldName}' = ${validityPeriodValue} (0 means no expiration per spec). ` +
            `Proceeding with vp_exp = null.`
          );
        } else {
          await this.queuePermissionForRetry(msg, "VALIDITY_PERIOD_MISSING");
          this.logger.error(
            `Permission ${msg.id} queued for retry - CredentialSchema ${perm.schema_id} exists but ` +
            `validity period field '${validityPeriodFieldName}' is missing/null for permission type '${perm.type}'. ` +
            `This indicates a data integrity issue. Schema data: ${JSON.stringify({
              id: schemaExists.id,
              issuer_grantor_validation_validity_period: schemaExists.issuer_grantor_validation_validity_period,
              verifier_grantor_validation_validity_period: schemaExists.verifier_grantor_validation_validity_period,
              issuer_validation_validity_period: schemaExists.issuer_validation_validity_period,
              verifier_validation_validity_period: schemaExists.verifier_validation_validity_period,
              holder_validation_validity_period: schemaExists.holder_validation_validity_period,
            })}`
          );
          return { success: true, message: "Permission queued for retry - validity period missing" };
        }
      }

      const effectiveUntil =
        msg.effective_until ?? perm.effective_until ?? vpExp ?? null;

      if (
        effectiveUntil &&
        vpExp &&
        new Date(effectiveUntil) > new Date(vpExp)
      ) {
        this.logger.warn(
          `effective_until ${effectiveUntil} exceeds vp_exp ${vpExp}`
        );
        return { success: false, reason: "effective_until exceeds vp_exp" };
      }

      const currentVpValidatorDeposit = Number(perm.vp_validator_deposit || 0);
      const vpCurrentDeposit = Number(perm.vp_current_deposit || 0);
      const newVpValidatorDeposit = currentVpValidatorDeposit + vpCurrentDeposit;

      const updatedPermData = {
        ...perm,
        vp_state: "VALIDATED",
        effective_until: effectiveUntil,
        effective_from: isFirstValidation ? now : perm.effective_from,
      };
      const expireSoon = await this.calculateExpireSoon(updatedPermData, new Date(now), height);
      const hasExpireSoonColumn = await this.checkPermissionsColumnExists("expire_soon");

      const entry: any = {
        vp_state: "VALIDATED",
        vp_last_state_change: now,
        vp_current_fees: 0,
        vp_current_deposit: 0,
        vp_validator_deposit: Number(newVpValidatorDeposit),
        vp_summary_digest: msg.vp_summary_digest ?? perm.vp_summary_digest ?? null,
        vp_exp: vpExp,
        effective_until: effectiveUntil,
        modified: now,
      };

      if (hasExpireSoonColumn) {
        entry.expire_soon = expireSoon;
      }

      if (isFirstValidation) {
        entry.validation_fees = Number(msg.validation_fees ?? 0);
        entry.issuance_fees = Number(msg.issuance_fees ?? 0);
        entry.verification_fees = Number(msg.verification_fees ?? 0);
        entry.effective_from = now;
        if ((msg as any).issuance_fee_discount !== undefined) {
          entry.issuance_fee_discount = Number((msg as any).issuance_fee_discount ?? 0);
        } else if (perm.issuance_fee_discount != null) {
          entry.issuance_fee_discount = Number(perm.issuance_fee_discount);
        }
        if ((msg as any).verification_fee_discount !== undefined) {
          entry.verification_fee_discount = Number((msg as any).verification_fee_discount ?? 0);
        } else if (perm.verification_fee_discount != null) {
          entry.verification_fee_discount = Number(perm.verification_fee_discount);
        }

        this.logger.info(
          `[SetVPToValidated] First validation: adding vp_current_deposit ${vpCurrentDeposit} to vp_validator_deposit (was ${currentVpValidatorDeposit}, now ${newVpValidatorDeposit}) for permission ${msg.id}`
        );
      } else {
        const feesChanged =
          (msg.validation_fees &&
            msg.validation_fees !== perm.validation_fees) ||
          (msg.issuance_fees && msg.issuance_fees !== perm.issuance_fees) ||
          (msg.verification_fees &&
            msg.verification_fees !== perm.verification_fees);

        if (feesChanged) {
          this.logger.warn("Cannot change fees during renewal");
          return {
            success: false,
            reason: "Cannot change fees on renewal",
          };
        }

        this.logger.info(
          `[SetVPToValidated] Renewal: adding vp_current_deposit ${vpCurrentDeposit} to vp_validator_deposit (was ${currentVpValidatorDeposit}, now ${newVpValidatorDeposit}) for permission ${msg.id}`
        );
      }

      this.logger.info(`[SetVPToValidated] Updating permission ${msg.id} to VALIDATED`);
      const [updated] = await knex("permissions")
        .where({ id: msg.id })
        .update(entry)
        .returning("*");

      if (!updated) {
        this.logger.error(
          `[SetVPToValidated] CRITICAL: Failed to update permission ${msg.id} - update returned no record`
        );
        throw new Error(`Failed to update permission ${msg.id}`);
      }

      this.logger.info(`[SetVPToValidated] Permission ${msg.id} updated successfully, vp_state=${updated.vp_state}`);
      try {
        await knex.transaction(async (trx) => {
          await this.updateParticipants(trx, Number(msg.id));
        });
      } catch (participantsErr: any) {
        this.logger.warn(`Failed to update participants for permission ${msg.id}:`, participantsErr?.message || participantsErr);
      }

      try {
        await recordPermissionHistory(
          knex,
          updated,
          "SET_VALIDATE_PERMISSION_VP",
          height,
          perm
        );
      } catch (historyErr: any) {
        this.logger.error(
          "CRITICAL: Failed to record permission history for VP validation:",
          historyErr
        );

      }

      await this.refreshTrustRegistryStatsBySchemaId(perm.schema_id, height);

      this.logger.info(`Permission ${msg.id} successfully validated`);
      return { success: true };
    } catch (err: any) {
      this.logger.error("CRITICAL: Error in handleSetPermissionVPToValidated:", err);
      console.error("FATAL PERM VP VALIDATED ERROR:", err);
      return { success: false, reason: "Internal error validating permission VP" };
    }
  }

  private async handleRenewPermissionVP(msg: MsgRenewPermissionVP) {
    try {
      const now = formatTimestamp(msg.timestamp);
      const applicantPerm = await knex("permissions")
        .where({ id: msg.id })
        .first();
      if (!applicantPerm) {
        this.logger.warn(`Permission ${msg.id} not found`);
        return { success: false, reason: "Permission not found" };
      }

      const caller = extractController(msg as unknown as Record<string, unknown>);
      if (
        caller &&
        applicantPerm.corporation &&
        caller !== applicantPerm.corporation
      ) {
        this.logger.warn(`Caller ${caller} is not the permission corporation`);
        return { success: false, reason: "Caller is not corporation" };
      }

      const validatorPerm = await knex("permissions")
        .where({ id: applicantPerm.validator_perm_id })
        .first();
      if (!validatorPerm) {
        this.logger.warn(
          `Validator permission ${applicantPerm.validator_perm_id} not found`
        );
        return { success: false, reason: "Validator permission not found" };
      }

      const globalVariables = await getGlobalVariables();
      if (!globalVariables) {
        this.logger.info(
          `Global variables: ${JSON.stringify(globalVariables)}`
        );
      }

      const trustUnitPrice = globalVariables?.tr?.trust_unit_price;
      const trustDepositRate = globalVariables?.td?.trust_deposit_rate;

      if (trustUnitPrice === undefined || trustDepositRate === undefined) {
        this.logger.warn("Global variables not set for fee calculation");
        return { success: false, reason: "Invalid global variables" };
      }

      const validationFeesInDenom =
        Number(validatorPerm.validation_fees) * trustUnitPrice;
      const validationTrustDepositInDenom =
        validationFeesInDenom * trustDepositRate;
      if (
        Number.isNaN(validationFeesInDenom) ||
        Number.isNaN(validationTrustDepositInDenom)
      ) {
        this.logger.warn("Error calculating fees/deposit");
        return { success: false, reason: "Error calculating fees/deposit" };
      }
      const height = Number((msg as any)?.height) || 0;
      await knex.transaction(async (trx) => {
        // Calculate expire_soon for renewed permission (PENDING state means not active)
        const renewedPermData = {
          ...applicantPerm,
          vp_state: "PENDING",
        };
        const expireSoon = await this.calculateExpireSoon(renewedPermData, new Date(now), height);
        const hasExpireSoonColumn = await this.checkPermissionsColumnExists("expire_soon");

        const updateData: any = {
          vp_state: "PENDING",
          vp_last_state_change: now,
          vp_current_fees: Number(validationFeesInDenom),
          vp_current_deposit: Number(validationTrustDepositInDenom),
          deposit: Number(applicantPerm.deposit || 0) + Number(validationTrustDepositInDenom),
          modified: now, 
        };

        if (hasExpireSoonColumn) {
          updateData.expire_soon = expireSoon;
        }

        const [updated] = await trx("permissions")
          .where({ id: msg.id })
          .update(updateData)
          .returning("*");

        if (!updated) {
          this.logger.error(
            `CRITICAL: Failed to update permission ${msg.id} for VP renewal - update returned no record`
          );

        }

        try {
          await this.updateWeight(trx, Number(msg.id));
        } catch (weightErr: any) {
          this.logger.warn(`Failed to update weight for permission ${msg.id} after VP renewal:`, weightErr?.message || weightErr);
        }

        try {
          await this.updateParticipants(trx, Number(msg.id));
        } catch (participantsErr: any) {
          this.logger.warn(`Failed to update participants for permission ${msg.id}:`, participantsErr?.message || participantsErr);
        }

        try {
          await recordPermissionHistory(
            trx,
            updated,
            "RENEW_PERMISSION_VP",
            height,
            applicantPerm
          );
        } catch (historyErr: any) {
          this.logger.error(
            "CRITICAL: Failed to record permission history for VP renewal:",
            historyErr
          );

        }
      });

      this.logger.info(`Permission ${msg.id} successfully renewed`);
      return { success: true };
    } catch (err: any) {
      this.logger.error("CRITICAL: Error in handleRenewPermissionVP:", err);
      console.error("FATAL PERM RENEW VP ERROR:", err);
      return { success: false, reason: "Internal error renewing permission VP" };
    }
  }

  private async handleCancelPermissionVPLastRequest(
    msg: MsgCancelPermissionVPLastRequest & { height?: number }
  ) {
    try {
      const now = formatTimestamp(msg.timestamp);
      const height = Number((msg as any)?.height) || 0;

      const perm = await knex("permissions").where({ id: msg.id }).first();
      if (!perm) {
        this.logger.warn(`Permission ${msg.id} not found`);
        return { success: false, reason: "Permission not found" };
      }

      if (perm.vp_state !== "PENDING") {
        this.logger.warn(`Permission ${msg.id} is not PENDING`);
        return { success: false, reason: "Permission not pending" };
      }

      const caller = extractController(msg as unknown as Record<string, unknown>);
      if (caller && perm.corporation && caller !== perm.corporation) {
        this.logger.warn(`Creator ${caller} is not permission corporation`);
        return { success: false, reason: "Creator is not corporation" };
      }

      // v4-draft13 removed TERMINATED; treat "no vp_exp" as validated-without-exp for legacy rows.
      const newVpState = "VALIDATED";

      const vpValidatorDeposit =
        perm.vp_validator_deposit;

      // Calculate expire_soon for the updated permission
      const updatedPermData = {
        ...perm,
        vp_state: newVpState,
      };
      const expireSoon = await this.calculateExpireSoon(updatedPermData, new Date(now), height);
      const hasExpireSoonColumn = await this.checkPermissionsColumnExists("expire_soon");

      const updateData: any = {
        vp_state: newVpState,
        vp_last_state_change: now,
        vp_current_fees: 0,
        vp_current_deposit: 0,
        vp_validator_deposit: Number(vpValidatorDeposit),
        modified: now,
      };

      if (hasExpireSoonColumn) {
        updateData.expire_soon = expireSoon;
      }

      const [updated] = await knex("permissions")
        .where({ id: msg.id })
        .update(updateData)
        .returning("*");

      if (!updated) {
        this.logger.error(
          `CRITICAL: Failed to update permission ${msg.id} - update returned no record`
        );

      }

      try {
        await knex.transaction(async (trx) => {
          await this.updateParticipants(trx, Number(msg.id));
        });
      } catch (participantsErr: any) {
        this.logger.warn(`Failed to update participants for permission ${msg.id}:`, participantsErr?.message || participantsErr);
      }

      // Record history for the permission update
      try {
        await recordPermissionHistory(
          knex,
          updated,
          "CANCEL_PERMISSION_VP",
          height,
          perm
        );
      } catch (historyErr: any) {
        this.logger.error(
          "CRITICAL: Failed to record permission history for VP cancellation:",
          historyErr
        );

      }

      this.logger.info(
        `Permission ${msg.id} validation cancelled. New state: ${newVpState}`
      );

      return { success: true };
    } catch (err: any) {
      this.logger.error("CRITICAL: Error in handleCancelPermissionVPLastRequest:", err);
      console.error("FATAL PERM CANCEL VP ERROR:", err);
      return { success: false, reason: "Internal error cancelling permission VP request" };
    }
  }

  private async handleSlashPermissionTrustDeposit(
    msg: MsgSlashPermissionTrustDeposit & { height?: number }
  ) {
    try {
      const slashAmount = (msg as any).amount ?? (msg as any).deposit;
      if (!msg.id || slashAmount == null) {
        this.logger.warn("Missing mandatory parameter: id or amount");
        return { success: false, reason: "Missing mandatory parameter" };
      }

      const idNum = Number(msg.id);
      if (!Number.isInteger(idNum) || idNum <= 0) {
        this.logger.warn(`Invalid id: ${msg.id}. Must be uint64`);
        return { success: false, reason: "Invalid permission ID" };
      }

      const amountNum = Number(slashAmount);
      if (Number.isNaN(amountNum) || amountNum <= 0) {
        this.logger.warn(`Invalid amount: ${slashAmount}`);
        return { success: false, reason: "Invalid amount" };
      }

      const perm = await knex("permissions").where({ id: msg.id }).first();
      if (!perm) {
        this.logger.warn(`Permission ${msg.id} not found`);
        return { success: false, reason: "Permission not found" };
      }

      const deposit = Number(perm.deposit || 0);
      if (amountNum > deposit) {
        this.logger.warn(
          `Slash amount ${amountNum} exceeds deposit ${deposit}`
        );
        return { success: false, reason: "Amount exceeds deposit" };
      }

      let isAuthorized = false;
      const caller = extractController(msg as unknown as Record<string, unknown>);

      let validatorPerm = perm;
      while (validatorPerm && validatorPerm.validator_perm_id) {
        validatorPerm = await knex("permissions")
          .where({ id: validatorPerm.validator_perm_id })
          .first();
        if (validatorPerm && validatorPerm.corporation === caller) {
          isAuthorized = true;
          break;
        }
      }

      if (!isAuthorized && perm.schema_id) {
        const schema = await knex("credential_schemas")
          .where({ id: perm.schema_id })
          .first();
        if (schema && schema.tr_id) {
          const tr = await knex("trust_registry")
            .where({ id: schema.tr_id })
            .first();
          if (tr && tr.corporation === caller) {
            isAuthorized = true;
          }
        }
      }

      if (!isAuthorized) {
        this.logger.warn("Unauthorized caller for slash operation");
        return { success: false, reason: "Unauthorized caller" };
      }

      const now = formatTimestamp(msg.timestamp);
      const height = Number((msg as any)?.height) || 0;
      const prevSlashed = Number(perm.slashed_deposit || 0);

      // Determine if this is ecosystem or network slash
      const isEcosystemPermission = perm.type === "ECOSYSTEM";
      let isEcosystemSlash = false;
      const isNetworkSlash = false;
      let trController: string | null = null;
      let classificationReason = '';

      if (isEcosystemPermission) {
        isEcosystemSlash = true;
        classificationReason = 'ECOSYSTEM permission type';
        this.logger.info(`[Slash] Permission ${msg.id} is ECOSYSTEM type - marking as ecosystem slash`);
      } else if (perm.schema_id) {
        const schema = await knex("credential_schemas")
          .where({ id: perm.schema_id })
          .first();

        if (!schema) {
          this.logger.warn(`[Slash] Permission ${msg.id} has schema_id ${perm.schema_id} but schema not found in database`);
          classificationReason = `Schema ${perm.schema_id} not found`;
        } else if (!schema.tr_id) {
          this.logger.warn(`[Slash] Permission ${msg.id} schema ${perm.schema_id} has no tr_id`);
          classificationReason = `Schema ${perm.schema_id} has no tr_id`;
        } else {
          const tr = await knex("trust_registry")
            .where({ id: schema.tr_id })
            .first();

          if (!tr) {
            this.logger.warn(`[Slash] Permission ${msg.id} schema ${perm.schema_id} references TR ${schema.tr_id} but TR not found in database`);
            classificationReason = `TR ${schema.tr_id} not found`;
            trController = null;
          } else {
            trController = tr.corporation || null;
            if (!trController) {
              this.logger.warn(`[Slash] Permission ${msg.id} TR ${schema.tr_id} exists but has no corporation field`);
              classificationReason = `TR ${schema.tr_id} has no corporation`;
            } else if (tr.corporation === caller) {
              isEcosystemSlash = true;
              classificationReason = `Slashed by TR corporation ${caller}`;
              this.logger.info(`[Slash] Permission ${msg.id} slashed by TR corporation ${caller} - marking as ecosystem slash`);
            } else {
              this.logger.warn(`[Slash] Permission ${msg.id} slashed by ${caller} but TR corporation is ${tr.corporation} - no slash type determined`);
              classificationReason = `Caller ${caller} != TR corporation ${tr.corporation}`;
            }
          }
        }
      } else {
        this.logger.warn(`[Slash] Permission ${msg.id} has no schema_id - no slash type determined`);
        classificationReason = 'No schema_id';
      }

      if (!isEcosystemSlash && !isNetworkSlash) {
        this.logger.error(`[Slash] CRITICAL: Could not classify slash for permission ${msg.id}. Classification reason: ${classificationReason}. Schema ID: ${perm.schema_id || 'N/A'}, Type: ${perm.type}, Caller: ${caller}, TR Controller: ${trController || 'N/A'}`);
      }

      await knex.transaction(async (trx) => {
        const currentDeposit = BigInt(perm.deposit || "0");
        const newDeposit = currentDeposit > BigInt(amountNum)
          ? currentDeposit - BigInt(amountNum)
          : BigInt(0);

        // Calculate expire_soon for slashed permission (slashed without repaid = inactive)
        const slashedPermData = {
          ...perm,
          slashed: now,
          repaid: null, // Not repaid yet
        };
        const expireSoon = await this.calculateExpireSoon(slashedPermData, new Date(now), height);
        const hasExpireSoonColumn = await this.checkPermissionsColumnExists("expire_soon");

        const updateData: any = {
          slashed: now,
          slashed_deposit: Number(prevSlashed + amountNum),
          deposit: Number(newDeposit),
          modified: now,
        };

        if (hasExpireSoonColumn) {
          updateData.expire_soon = expireSoon;
        }

        const [updated] = await trx("permissions")
          .where({ id: msg.id })
          .update(updateData)
          .returning("*");

        if (!updated) {
          this.logger.error(
            `CRITICAL: Failed to slash permission ${msg.id} - update returned no record`
          );

        }

        // Update slash statistics for this permission and ancestors
        try {
          if (isEcosystemSlash || isNetworkSlash) {
            await this.updateSlashStatistics(
              trx,
              Number(msg.id),
              isEcosystemSlash,
              isNetworkSlash,
              Number(amountNum),
              null
            );
            this.logger.info(`[Slash] Updated slash statistics for permission ${msg.id} - ecosystem: ${isEcosystemSlash}, network: ${isNetworkSlash}, amount: ${amountNum}`);
          } else {
            this.logger.warn(`[Slash] Skipping slash statistics update for permission ${msg.id} - neither ecosystem nor network slash detected`);
          }
        } catch (statsErr: any) {
          this.logger.warn(`Failed to update slash statistics: ${statsErr?.message || statsErr}`);
        }

        try {
          await this.updateWeight(trx, Number(msg.id));
        } catch (weightErr: any) {
          this.logger.warn(`Failed to update weight for permission ${Number(msg.id)}:`, weightErr?.message || weightErr);
        }

        try {
          await this.updateParticipants(trx, Number(msg.id));
        } catch (participantsErr: any) {
          this.logger.warn(`Failed to update participants for permission ${msg.id}:`, participantsErr?.message || participantsErr);
        }

        try {
          await recordPermissionHistory(
            trx,
            updated,
            "SLASH_PERMISSION_TRUST_DEPOSIT",
            height,
            perm
          );
        } catch (historyErr: any) {
          this.logger.error(
            "CRITICAL: Failed to record permission history for slash:",
            historyErr
          );
          process.exit(1);
        }
      });

      try {
        const schema = await knex("permissions").where({ id: msg.id }).first();
        const corporation = schema?.corporation;
        if (corporation) {
          await (this as any).broker.call(
            `${SERVICE.V1.TrustDepositDatabaseService.path}.slash_perm_trust_deposit`,
            {
              corporation,
              amount: String(amountNum),
              ts: now,
            }
          );
        }
      } catch (err) {
        this.logger.warn("TD processor slash call failed, continuing: ", err);
      }


      try {
        const slashedPerm = await knex("permissions").where({ id: msg.id }).first();
        if (slashedPerm?.schema_id) {
          const schemaId = Number(slashedPerm.schema_id);
          const csStats = await calculateCredentialSchemaStats(schemaId);
          const slashFromPerms = await this.sumSlashStatsFromPermissionsForSchema(schemaId);
          const mergedStats = { ...csStats, ...slashFromPerms };
          const csUpdate = statsToUpdateObject(mergedStats as unknown as Record<string, unknown>, CS_STATS_FIELDS);
          const updatedCount = await knex("credential_schemas").where("id", schemaId).update(csUpdate);
          if (updatedCount === 0) {
            this.logger.warn(`[Slash] credential_schemas update affected 0 rows for schema_id=${schemaId}`);
          }
          const height = Number((msg as any)?.height) || 0;
          await this.refreshTrustRegistryStatsBySchemaId(schemaId, height);
          const csStatsForHistory = { ...csStats, ...slashFromPerms };
          try {
            await insertCredentialSchemaHistoryStatsRow(knex, schemaId, height, csStatsForHistory);
          } catch (historyErr: any) {
            this.logger.warn(
              `Failed to insert CS stats history after slash for schema_id=${schemaId}: ${historyErr?.message || historyErr}`
            );
          }
        }
      } catch (statsErr: any) {
        const code = statsErr?.nativeError?.code ?? statsErr?.code;
        this.logger.warn(
          `Failed to update CS/TR statistics after slash: ${statsErr?.message ?? String(statsErr)}${code ? ` [code=${code}]` : ""}`
        );
      }

      this.logger.info(
        `✅ Permission ${msg.id} slashed by ${caller} amount ${amountNum}`
      );

      return { success: true };
    } catch (err: any) {
      this.logger.error("CRITICAL: Error in handleSlashPermissionTrustDeposit:", err);
      console.error("FATAL PERM SLASH ERROR:", err);
      return { success: false, reason: "Internal error slashing permission trust deposit" };
    }
  }

  private async handleRepayPermissionSlashedTrustDeposit(
    msg: MsgRepayPermissionSlashedTrustDeposit & { height?: number }
  ) {
    try {
      if (!msg.id) {
        this.logger.warn("Missing mandatory parameter: id");
        return { success: false, reason: "Missing mandatory parameter" };
      }

      const idNum = Number(msg.id);
      if (!Number.isInteger(idNum) || idNum <= 0) {
        this.logger.warn(`Invalid id: ${msg.id}. Must be a valid uint64.`);
        return { success: false, reason: "Invalid permission ID" };
      }

      const perm = await knex("permissions").where({ id: msg.id }).first();
      if (!perm) {
        this.logger.warn(`Permission ${msg.id} not found`);
        return { success: false, reason: "Permission not found" };
      }

      const slashedDeposit = Number(perm.slashed_deposit || 0);
      if (slashedDeposit <= 0) {
        this.logger.warn(
          `Permission ${msg.id} has no slashed deposit to repay`
        );
        return { success: false, reason: "No slashed deposit to repay" };
      }

      const now = formatTimestamp(msg.timestamp);
      const height = Number((msg as any)?.height) || 0;

      // Determine if this was ecosystem or network slash based on previous slash
      const isEcosystemPermission = perm.type === "ECOSYSTEM";
      let isEcosystemSlash = false;
      const isNetworkSlash = false;
      let trController: string | null = null;
      let classificationReason = '';

      const repayer = extractController(msg as unknown as Record<string, unknown>);

      if (isEcosystemPermission) {
        isEcosystemSlash = true;
        classificationReason = "ECOSYSTEM permission type";
      } else if (perm.schema_id) {
        const schema = await knex("credential_schemas")
          .where({ id: perm.schema_id })
          .first();

        if (!schema) {
          this.logger.warn(`[Repay] Permission ${msg.id} has schema_id ${perm.schema_id} but schema not found`);
          classificationReason = `Schema ${perm.schema_id} not found`;
        } else if (!schema.tr_id) {
          this.logger.warn(`[Repay] Permission ${msg.id} schema ${perm.schema_id} has no tr_id`);
          classificationReason = `Schema ${perm.schema_id} has no tr_id`;
        } else {
          const tr = await knex("trust_registry")
            .where({ id: schema.tr_id })
            .first();

          if (!tr) {
            this.logger.warn(`[Repay] Permission ${msg.id} TR ${schema.tr_id} not found`);
            classificationReason = `TR ${schema.tr_id} not found`;
            trController = null;
          } else {
            trController = tr.corporation || null;
            if (!trController) {
              this.logger.warn(`[Repay] Permission ${msg.id} TR ${schema.tr_id} has no corporation`);
              classificationReason = `TR ${schema.tr_id} has no corporation`;
            } else if (repayer && tr.corporation === repayer) {
              isEcosystemSlash = true;
              classificationReason = `Repay by TR corporation ${repayer}`;
            } else {
              classificationReason = `Repayer ${repayer || "unknown"} != TR corporation ${tr.corporation}`;
            }
          }
        }
      } else {
        classificationReason = "No schema_id";
      }

      if (!isEcosystemSlash && !isNetworkSlash) {
        this.logger.error(
          `[Repay] CRITICAL: Could not classify repay for permission ${msg.id}. Classification reason: ${classificationReason}. Schema ID: ${perm.schema_id || "N/A"}, Type: ${perm.type}, TR corporation: ${trController || "N/A"}`
        );
      }

      await knex.transaction(async (trx) => {
        requireController(msg, `PERM REPAY_SLASHED ${msg.id}`);
        const currentDeposit = BigInt(perm.deposit || "0");
        const requestedRepayAmount = (msg as any).amount ?? (msg as any).deposit;
        const repaidAmount = requestedRepayAmount == null
          ? BigInt(slashedDeposit)
          : BigInt(String(requestedRepayAmount));
        const newDeposit = currentDeposit + repaidAmount;

        // Calculate expire_soon for repaid permission (may become active again)
        const repaidPermData = {
          ...perm,
          repaid: now,
          slashed: perm.slashed, // Keep original slashed timestamp
        };
        const expireSoon = await this.calculateExpireSoon(repaidPermData, new Date(now), height);
        const hasExpireSoonColumn = await this.checkPermissionsColumnExists("expire_soon");

        const updateData: any = {
          repaid: now,
          repaid_deposit: Number(repaidAmount),
          deposit: Number(newDeposit),
          modified: now,
        };

        if (hasExpireSoonColumn) {
          updateData.expire_soon = expireSoon;
        }

        const [updated] = await trx("permissions")
          .where({ id: msg.id })
          .update(updateData)
          .returning("*");

        if (!updated) {
          this.logger.error(
            `CRITICAL: Failed to repay permission ${msg.id} - update returned no record`
          );

        }

        // Update repaid amount in slash statistics
        try {
          await this.updateSlashStatistics(
            trx,
            Number(msg.id),
            isEcosystemSlash,
            isNetworkSlash,
            0, // No new slash, just repayment
            Number(repaidAmount)
          );
        } catch (statsErr: any) {
          this.logger.warn(`Failed to update slash statistics for repay: ${statsErr?.message || statsErr}`);
        }

        try {
          await this.updateWeight(trx, Number(msg.id));
        } catch (weightErr: any) {
          this.logger.warn(`Failed to update weight for permission ${msg.id} after repay:`, weightErr?.message || weightErr);
        }

        try {
          await this.updateParticipants(trx, Number(msg.id));
        } catch (participantsErr: any) {
          this.logger.warn(`Failed to update participants for permission ${msg.id}:`, participantsErr?.message || participantsErr);
        }

        try {
          await recordPermissionHistory(
            trx,
            updated,
            "REPAY_PERMISSION_SLASHED_TRUST_DEPOSIT",
            height,
            perm
          );
        } catch (historyErr: any) {
          this.logger.error(
            "CRITICAL: Failed to record permission history for repay:",
            historyErr
          );

        }
      });

      try {
        const repaidPerm = await knex("permissions").where({ id: msg.id }).first();
        if (repaidPerm?.schema_id) {
          const schemaId = Number(repaidPerm.schema_id);
          const csStats = await calculateCredentialSchemaStats(schemaId);
          const slashFromPerms = await this.sumSlashStatsFromPermissionsForSchema(schemaId);
          const mergedStats = { ...csStats, ...slashFromPerms };
          const csUpdate = statsToUpdateObject(mergedStats as unknown as Record<string, unknown>, CS_STATS_FIELDS);
          const updatedCount = await knex("credential_schemas").where("id", schemaId).update(csUpdate);
          if (updatedCount === 0) {
            this.logger.warn(`[Repay] credential_schemas update affected 0 rows for schema_id=${schemaId}`);
          }
          const height = Number((msg as any)?.height) || 0;
          await this.refreshTrustRegistryStatsBySchemaId(schemaId, height);
          const csStatsForHistory = { ...csStats, ...slashFromPerms };
          try {
            await insertCredentialSchemaHistoryStatsRow(knex, schemaId, height, csStatsForHistory);
          } catch (historyErr: any) {
            this.logger.warn(
              `Failed to insert CS stats history after repay for schema_id=${schemaId}: ${historyErr?.message || historyErr}`
            );
          }
        }
      } catch (statsErr: any) {
        const code = statsErr?.nativeError?.code ?? statsErr?.code;
        this.logger.warn(
          `Failed to update CS/TR statistics after repay: ${statsErr?.message ?? String(statsErr)}${code ? ` [code=${code}]` : ""}`
        );
      }

      this.logger.info(
        `✅ Permission ${msg.id} slashed deposit (${slashedDeposit}) repaid by ${repayer}`
      );

      return { success: true };
    } catch (err: any) {
      this.logger.error(
        "CRITICAL: Error in handleRepayPermissionSlashedTrustDeposit:",
        err
      );
      console.error("FATAL PERM REPAY ERROR:", err);
      return { success: false, reason: "Internal error repaying permission slashed trust deposit" };
    }
  }

  private permissionsColumnExistsCache: Record<string, boolean> | null = null;

  private async checkPermissionsColumnExists(columnName: string): Promise<boolean> {
    if (this.permissionsColumnExistsCache === null) {
      this.permissionsColumnExistsCache = {};
    }

    if (this.permissionsColumnExistsCache[columnName] !== undefined) {
      return this.permissionsColumnExistsCache[columnName];
    }

    try {
      const exists = await knex.schema.hasColumn("permissions", columnName);
      this.permissionsColumnExistsCache[columnName] = exists;
      if (columnName === "expire_soon") {
        this.logger.info(`[checkPermissionsColumnExists] expire_soon column exists: ${exists}`);
      }
      return exists;
    } catch (error: any) {
      this.logger.warn(`[checkPermissionsColumnExists] Error checking column ${columnName}:`, error?.message || error);
      this.permissionsColumnExistsCache[columnName] = false;
      return false;
    }
  }

  private async incrementPermissionStatistics(
    trx: any,
    permId: number,
    incrementIssued: boolean,
    incrementVerified: boolean
  ): Promise<void> {
    const hasIssuedColumn = await this.checkPermissionsColumnExists("issued");
    const hasVerifiedColumn = await this.checkPermissionsColumnExists("verified");

    if (!hasIssuedColumn && !hasVerifiedColumn) {
      this.logger.warn(`[incrementPermissionStatistics] Neither issued nor verified column exists for permission ${permId}`);
      return;
    }

    if (incrementIssued && !hasIssuedColumn) {
      this.logger.warn(`[incrementPermissionStatistics] Attempted to increment issued for permission ${permId} but issued column does not exist`);
    }

    if (incrementVerified && !hasVerifiedColumn) {
      this.logger.warn(`[incrementPermissionStatistics] Attempted to increment verified for permission ${permId} but verified column does not exist`);
    }

    const initialPerm: { schema_id: number; validator_perm_id: number | null } | undefined = await trx("permissions").where({ id: permId }).select('schema_id', 'validator_perm_id').first();
    if (!initialPerm) return;

    const schemaId = initialPerm.schema_id;
    let currentPermId: number | null = permId;

    while (currentPermId) {
      const perm: { schema_id: number; validator_perm_id: number | null } | undefined = await trx("permissions").where({ id: currentPermId }).select('schema_id', 'validator_perm_id').first();
      if (!perm) break;
      if (perm.schema_id !== schemaId) {
        this.logger.warn(`Permission tree traversal crossed schema boundary. permId=${currentPermId}, expected schema=${schemaId}, found schema=${perm.schema_id}. Stopping traversal.`);
        break;
      }

      const updates: any = {};
      if (incrementIssued && hasIssuedColumn) {
        updates.issued = knex.raw("COALESCE(issued, 0) + 1");
      }
      if (incrementVerified && hasVerifiedColumn) {
        updates.verified = knex.raw("COALESCE(verified, 0) + 1");
      }

      if (Object.keys(updates).length > 0) {
        try {
          await trx("permissions")
            .where({ id: currentPermId })
            .update(updates);
        } catch (error: any) {
          if (error?.nativeError?.code === '42703') {
            this.permissionsColumnExistsCache = null;
            return;
          }
          throw error;
        }
      }

      currentPermId = perm.validator_perm_id;
    }
  }


  private async sumSlashStatsFromPermissionsForSchema(schemaId: number): Promise<{
    ecosystem_slash_events: number;
    ecosystem_slashed_amount: number;
    ecosystem_slashed_amount_repaid: number;
    network_slash_events: number;
    network_slashed_amount: number;
    network_slashed_amount_repaid: number;
  }> {
    const hasCol = await this.checkPermissionsColumnExists("ecosystem_slash_events");
    if (!hasCol) {
      return {
        ecosystem_slash_events: 0,
        ecosystem_slashed_amount: 0,
        ecosystem_slashed_amount_repaid: 0,
        network_slash_events: 0,
        network_slashed_amount: 0,
        network_slashed_amount_repaid: 0,
      };
    }
    const rows = await knex("permissions")
      .where("schema_id", schemaId)
      .select(
        "ecosystem_slash_events",
        "ecosystem_slashed_amount",
        "ecosystem_slashed_amount_repaid",
        "network_slash_events",
        "network_slashed_amount",
        "network_slashed_amount_repaid"
      );
    let ecosystemSlashEvents = 0;
    let ecosystemSlashedAmount = 0;
    let ecosystemSlashedAmountRepaid = 0;
    let networkSlashEvents = 0;
    let networkSlashedAmount = 0;
    let networkSlashedAmountRepaid = 0;
    for (const r of rows || []) {
      ecosystemSlashEvents += Number(r.ecosystem_slash_events ?? 0);
      ecosystemSlashedAmount += Number(r.ecosystem_slashed_amount ?? 0);
      ecosystemSlashedAmountRepaid += Number(r.ecosystem_slashed_amount_repaid ?? 0);
      networkSlashEvents += Number(r.network_slash_events ?? 0);
      networkSlashedAmount += Number(r.network_slashed_amount ?? 0);
      networkSlashedAmountRepaid += Number(r.network_slashed_amount_repaid ?? 0);
    }
    return {
      ecosystem_slash_events: ecosystemSlashEvents,
      ecosystem_slashed_amount: ecosystemSlashedAmount,
      ecosystem_slashed_amount_repaid: ecosystemSlashedAmountRepaid,
      network_slash_events: networkSlashEvents,
      network_slashed_amount: networkSlashedAmount,
      network_slashed_amount_repaid: networkSlashedAmountRepaid,
    };
  }

  private async updateSlashStatistics(
    trx: any,
    permId: number,
    isEcosystemSlash: boolean,
    isNetworkSlash: boolean,
    slashAmount: number,
    repayAmount: number | null
  ): Promise<void> {
    const hasEcosystemSlashEventsColumn = await this.checkPermissionsColumnExists("ecosystem_slash_events");
    if (!hasEcosystemSlashEventsColumn) {
      this.logger.warn(`[updateSlashStatistics] Column ecosystem_slash_events does not exist, skipping update for permission ${permId}`);
      return;
    }

    if (!isEcosystemSlash && !isNetworkSlash) {
      this.logger.warn(`[updateSlashStatistics] Neither ecosystem nor network slash flag is set for permission ${permId}, skipping update`);
      return;
    }

    const permExists = await trx("permissions").where({ id: permId }).first();
    if (!permExists) {
      this.logger.warn(`[updateSlashStatistics] Permission ${permId} not found, skipping update`);
      return;
    }

    const updates: any = {};

    if (isEcosystemSlash) {
      if (slashAmount !== 0) {
        updates.ecosystem_slash_events = knex.raw("COALESCE(ecosystem_slash_events, 0) + 1");
        updates.ecosystem_slashed_amount = knex.raw(
          "COALESCE(ecosystem_slashed_amount, 0::numeric) + ?::numeric",
          [slashAmount]
        );
      }
      if (repayAmount) {
        updates.ecosystem_slashed_amount_repaid = knex.raw(
          "COALESCE(ecosystem_slashed_amount_repaid, 0::numeric) + ?::numeric",
          [repayAmount]
        );
      }
    }

    if (isNetworkSlash) {
      if (slashAmount !== 0) {
        updates.network_slash_events = knex.raw("COALESCE(network_slash_events, 0) + 1");
        updates.network_slashed_amount = knex.raw(
          "COALESCE(network_slashed_amount, 0::numeric) + ?::numeric",
          [slashAmount]
        );
      }
      if (repayAmount) {
        updates.network_slashed_amount_repaid = knex.raw(
          "COALESCE(network_slashed_amount_repaid, 0::numeric) + ?::numeric",
          [repayAmount]
        );
      }
    }

    if (Object.keys(updates).length > 0) {
      try {
        const result = await trx("permissions")
          .where({ id: permId })
          .update(updates);
        this.logger.debug(
          `[updateSlashStatistics] Updated permission ${permId} with ${Object.keys(updates).length} fields, rows affected: ${result}`
        );
      } catch (error: any) {
        if (error?.nativeError?.code === '42703') {
          this.logger.warn(
            `[updateSlashStatistics] Column does not exist, clearing cache for permission ${permId}`
          );
          this.permissionsColumnExistsCache = null;
          return;
        }
        throw error;
      }
    } else {
      this.logger.warn(
        `[updateSlashStatistics] No updates to apply for permission ${permId} - isEcosystemSlash: ${isEcosystemSlash}, isNetworkSlash: ${isNetworkSlash}, slashAmount: ${slashAmount}, repayAmount: ${repayAmount}`
      );
    }
  }


  private async updateWeight(trx: any, permId: number): Promise<void> {
    const hasWeightColumn = await this.checkPermissionsColumnExists("weight");
    if (!hasWeightColumn) {
      return;
    }

    const initialPerm: { schema_id: number; validator_perm_id: number | null; deposit: number } | undefined =
      await trx("permissions").where({ id: permId }).select('schema_id', 'validator_perm_id', 'deposit').first();
    if (!initialPerm) return;

    const schemaId = initialPerm.schema_id;
    let currentPermId: number | null = permId;

    const permStack: number[] = [];
    while (currentPermId) {
      permStack.push(currentPermId);
      const perm: { schema_id: number; validator_perm_id: number | null } | undefined =
        await trx("permissions").where({ id: currentPermId }).select('schema_id', 'validator_perm_id').first();
      if (!perm) break;
      if (perm.schema_id !== schemaId) {
        this.logger.warn(`Permission tree traversal crossed schema boundary. permId=${currentPermId}, expected schema=${schemaId}, found schema=${perm.schema_id}. Stopping traversal.`);
        break;
      }
      currentPermId = perm.validator_perm_id;
    }

    for (let i = permStack.length - 1; i >= 0; i--) {
      const pid = permStack[i];
      const perm = await trx("permissions").where({ id: pid }).select('deposit', 'schema_id').first();
      if (!perm) continue;

      const children = await trx("permissions")
        .where("validator_perm_id", pid)
        .where("schema_id", perm.schema_id)
        .select("weight");

      let childWeightSum = BigInt(0);
      for (const child of children) {
        const childWeight = child.weight ? BigInt(child.weight) : BigInt(0);
        childWeightSum += childWeight;
      }

      const ownDeposit = BigInt(perm.deposit || "0");
      const totalWeight = ownDeposit + childWeightSum;

      try {
        await trx("permissions")
          .where({ id: pid })
          .update({ weight: String(totalWeight) });
      } catch (error: any) {
        if (error?.nativeError?.code === '42703') {
          this.permissionsColumnExistsCache = null;
          return;
        }
        throw error;
      }
    }
  }

  private async updateParticipants(trx: any, permId: number): Promise<void> {
    const hasParticipantsColumn = await this.checkPermissionsColumnExists("participants");
    if (!hasParticipantsColumn) {
      return;
    }
    const roleColumns = [
      "participants_ecosystem",
      "participants_issuer_grantor",
      "participants_issuer",
      "participants_verifier_grantor",
      "participants_verifier",
      "participants_holder",
    ] as const;
    const roleColumnsAvailabilityEntries = await Promise.all(
      roleColumns.map(async (col) => [col, await this.checkPermissionsColumnExists(col)] as const)
    );
    const roleColumnsAvailability = new Map<string, boolean>(roleColumnsAvailabilityEntries);
    const availableRoleColumns = roleColumns.filter((col) => roleColumnsAvailability.get(col));

    const initialPerm: { schema_id: number; validator_perm_id: number | null } | undefined =
      await trx("permissions").where({ id: permId }).select('schema_id', 'validator_perm_id').first();
    if (!initialPerm) return;

    const schemaId = initialPerm.schema_id;
    let currentPermId: number | null = permId;
    const permStack: number[] = [];

    while (currentPermId) {
      permStack.push(currentPermId);
      const perm: { schema_id: number; validator_perm_id: number | null } | undefined =
        await trx("permissions").where({ id: currentPermId }).select('schema_id', 'validator_perm_id').first();
      if (!perm) break;
      if (perm.schema_id !== schemaId) {
        this.logger.warn(`Permission tree traversal crossed schema boundary. permId=${currentPermId}, expected schema=${schemaId}, found schema=${perm.schema_id}. Stopping traversal.`);
        break;
      }
      currentPermId = perm.validator_perm_id;
    }

    const now = new Date();

    for (let i = permStack.length - 1; i >= 0; i--) {
      const pid = permStack[i];
      const perm = await trx("permissions").where({ id: pid }).select(
        "repaid",
        "slashed",
        "revoked",
        "effective_from",
        "effective_until",
        "type",
        "vp_state",
        "vp_exp",
        "validator_perm_id",
        "schema_id"
      ).first();
      if (!perm) continue;

      const permState = calculatePermState(
        {
          repaid: perm.repaid,
          slashed: perm.slashed,
          revoked: perm.revoked,
          effective_from: perm.effective_from,
          effective_until: perm.effective_until,
          type: perm.type,
          vp_state: perm.vp_state,
          vp_exp: perm.vp_exp,
          validator_perm_id: perm.validator_perm_id,
        },
        now
      );

      let count = permState === "ACTIVE" ? 1 : 0;
      const roleTotals: Record<string, number> = {
        participants_ecosystem: 0,
        participants_issuer_grantor: 0,
        participants_issuer: 0,
        participants_verifier_grantor: 0,
        participants_verifier: 0,
        participants_holder: 0,
      };
      const permType = normalizePermissionType(perm.type);
      if (permState === "ACTIVE") {
        if (permType === "ECOSYSTEM") roleTotals.participants_ecosystem += 1;
        if (permType === "ISSUER_GRANTOR") roleTotals.participants_issuer_grantor += 1;
        if (permType === "ISSUER") roleTotals.participants_issuer += 1;
        if (permType === "VERIFIER_GRANTOR") roleTotals.participants_verifier_grantor += 1;
        if (permType === "VERIFIER") roleTotals.participants_verifier += 1;
        if (permType === "HOLDER") roleTotals.participants_holder += 1;
      }

      const childSelectColumns: string[] = [
        "repaid",
        "slashed",
        "revoked",
        "effective_from",
        "effective_until",
        "type",
        "vp_state",
        "vp_exp",
        "validator_perm_id",
        "participants",
      ];
      for (const roleCol of availableRoleColumns) {
        childSelectColumns.push(roleCol);
      }
      const children = await trx("permissions")
        .where("validator_perm_id", pid)
        .where("schema_id", perm.schema_id)
        .select(childSelectColumns);

      for (const child of children) {
        const childState = calculatePermState(
          {
            repaid: child.repaid,
            slashed: child.slashed,
            revoked: child.revoked,
            effective_from: child.effective_from,
            effective_until: child.effective_until,
            type: child.type,
            vp_state: child.vp_state,
            vp_exp: child.vp_exp,
            validator_perm_id: child.validator_perm_id,
          },
          now
        );

        if (childState === "ACTIVE") {
          count++;
        }

        const childParticipants = child.participants ? Number(child.participants) : 0;
        count += childParticipants;
        for (const roleCol of availableRoleColumns) {
          roleTotals[roleCol] += Number(child?.[roleCol] || 0);
        }
      }

      try {
        const updates: Record<string, any> = { participants: count };
        for (const roleCol of availableRoleColumns) {
          updates[roleCol] = roleTotals[roleCol];
        }
        await trx("permissions")
          .where({ id: pid })
          .update(updates);
      } catch (error: any) {
        if (error?.nativeError?.code === '42703') {
          this.permissionsColumnExistsCache = null;
          return;
        }
        throw error;
      }
    }
  }

  private async handleCreateOrUpdatePermissionSession(
    msg: MsgCreateOrUpdatePermissionSession & { height?: number }
  ) {
    const trx = await knex.transaction();
    try {
      const now = formatTimestamp(msg.timestamp);
      const height = Number((msg as any)?.height) || 0;

      const agentPermId = (msg as any).agentPermId ?? (msg as any).agent_perm_id;
      const walletAgentPermId = (msg as any).walletAgentPermId ?? (msg as any).wallet_agent_perm_id;
      const issuerPermId = (msg as any).issuerPermId ?? (msg as any).issuer_perm_id;
      const verifierPermId = (msg as any).verifierPermId ?? (msg as any).verifier_perm_id;

      if (!msg.id || !agentPermId || !walletAgentPermId) {
        throw new Error("Missing mandatory parameters");
      }
      if (!issuerPermId && !verifierPermId) {
        throw new Error(
          "At least one of issuer_perm_id or verifier_perm_id must be provided"
        );
      }

      const [agentPerm, walletAgentPerm, issuerPerm, verifierPerm] =
        await Promise.all([
          trx("permissions").where({ id: agentPermId }).first(),
          trx("permissions").where({ id: walletAgentPermId }).first(),
          issuerPermId
            ? trx("permissions").where({ id: issuerPermId }).first()
            : null,
          verifierPermId
            ? trx("permissions").where({ id: verifierPermId }).first()
            : null,
        ]);

      if (!agentPerm) {
        this.logger.warn(
          `Agent permission not found for session ${msg.id}. agentPermId=${agentPermId}. Session will be saved but statistics may be incomplete.`
        );
      }
      if (!walletAgentPerm) {
        this.logger.warn(
          `Wallet Agent permission not found for session ${msg.id}. walletAgentPermId=${walletAgentPermId}. Session will be saved but statistics may be incomplete.`
        );
      }
      if (issuerPermId && issuerPerm && issuerPerm.type !== "ISSUER") {
        this.logger.warn(
          `Invalid issuer permission type for session ${msg.id}. Expected ISSUER, got ${issuerPerm.type}. issuerPermId=${issuerPermId}. Session will be saved.`
        );
      }
      if (verifierPermId && verifierPerm && verifierPerm.type !== "VERIFIER") {
        this.logger.warn(
          `Invalid verifier permission type for session ${msg.id}. Expected VERIFIER, got ${verifierPerm.type}. verifierPermId=${verifierPermId}. Session will be saved.`
        );
      }

      const parseSessionRecordsLocal = (row: any): any[] => {
        const raw = row?.session_records ?? row?.authz;
        try {
          if (typeof raw === "string") return JSON.parse(raw || "[]");
          if (Array.isArray(raw)) return raw;
        } catch {
          /* ignore */
        }
        return [];
      };

      const existing = await trx("permission_sessions")
        .where({ id: msg.id })
        .first();
      const previousSession = existing
        ? {
            ...existing,
            session_records: parseSessionRecordsLocal(existing),
          }
        : undefined;

      const recordEntry = {
        created: now,
        issuer_perm_id: issuerPermId || null,
        verifier_perm_id: verifierPermId || null,
        wallet_agent_perm_id: walletAgentPermId,
      };

      const vsOp =
        (msg as any).vs_operator ?? (msg as any).vsOperator ?? (msg as any).operator ?? existing?.vs_operator ?? null;

      if (!existing) {
        const creator = requireController(msg, `PERM SESSION ${msg.id}`);
        const [session] = await trx("permission_sessions")
          .insert({
            id: msg.id,
            corporation: creator,
            vs_operator: vsOp,
            agent_perm_id: agentPermId,
            wallet_agent_perm_id: walletAgentPermId,
            session_records: JSON.stringify([recordEntry]),
            created: now,
            modified: now,
          })
          .returning("*");

        const normalizedSession =
          typeof session.session_records === "string"
            ? { ...session, session_records: parseJson(session.session_records) }
            : session;

        await recordPermissionSessionHistory(
          trx,
          normalizedSession,
          "CREATE_PERMISSION_SESSION",
          height
        );

        if (issuerPermId) {
          try {
            await this.incrementPermissionStatistics(trx, Number(issuerPermId), true, false);
            try {
              await recordPermissionHistory(trx, { id: Number(issuerPermId) }, "CREDENTIAL_ISSUED", height);
            } catch (historyErr: any) {
              this.logger.warn(`[Session] Failed to record permission history for issued perm ${issuerPermId}:`, historyErr?.message || historyErr);
            }
          } catch (issuedErr: any) {
            this.logger.error(`[Session] Failed to increment issued for permission ${issuerPermId}:`, issuedErr?.message || issuedErr);
          }
        }
        if (verifierPermId) {
          try {
            await this.incrementPermissionStatistics(trx, Number(verifierPermId), false, true);
            try {
              await recordPermissionHistory(trx, { id: Number(verifierPermId) }, "CREDENTIAL_VERIFIED", height);
            } catch (historyErr: any) {
              this.logger.warn(`[Session] Failed to record permission history for verified perm ${verifierPermId}:`, historyErr?.message || historyErr);
            }
          } catch (verifiedErr: any) {
            this.logger.error(`[Session] Failed to increment verified for permission ${verifierPermId}:`, verifiedErr?.message || verifiedErr);
          }
        }
      } else {
        const existingRecords = parseSessionRecordsLocal(existing);
        existingRecords.push(recordEntry);

        const [session] = await trx("permission_sessions")
          .where({ id: msg.id })
          .update({
            session_records: JSON.stringify(existingRecords),
            vs_operator: vsOp,
            modified: now,
          })
          .returning("*");

        const normalizedSession =
          typeof session.session_records === "string"
            ? { ...session, session_records: parseJson(session.session_records) }
            : session;

        await recordPermissionSessionHistory(
          trx,
          normalizedSession,
          "UPDATE_PERMISSION_SESSION",
          height,
          previousSession
        );

        const previousRecords = previousSession?.session_records || [];
        const previousIssuerPermIds = new Set(
          previousRecords.map((entry: { issuer_perm_id?: string | number }) => entry.issuer_perm_id).filter(Boolean)
        );
        const previousVerifierPermIds = new Set(
          previousRecords.map((entry: { verifier_perm_id?: string | number }) => entry.verifier_perm_id).filter(Boolean)
        );

        const newIssuerPermIds = new Set(
          existingRecords.map((entry: { issuer_perm_id?: string | number }) => entry.issuer_perm_id).filter(Boolean)
        );
        const newVerifierPermIds = new Set(
          existingRecords.map((entry: { verifier_perm_id?: string | number }) => entry.verifier_perm_id).filter(Boolean)
        );

        for (const issuerId of newIssuerPermIds) {
          if (!previousIssuerPermIds.has(issuerId)) {
            try {
              await this.incrementPermissionStatistics(trx, Number(issuerId), true, false);
              try {
                await recordPermissionHistory(trx, { id: Number(issuerId) }, "CREDENTIAL_ISSUED", height);
              } catch (historyErr: any) {
                this.logger.warn(`[Session] Failed to record permission history for issued perm ${issuerId}:`, historyErr?.message || historyErr);
              }
            } catch (issuedErr: any) {
              this.logger.error(`[Session] Failed to increment issued for permission ${issuerId}:`, issuedErr?.message || issuedErr);
            }
          }
        }
        for (const verifierId of newVerifierPermIds) {
          if (!previousVerifierPermIds.has(verifierId)) {
            try {
              await this.incrementPermissionStatistics(trx, Number(verifierId), false, true);
              try {
                await recordPermissionHistory(trx, { id: Number(verifierId) }, "CREDENTIAL_VERIFIED", height);
              } catch (historyErr: any) {
                this.logger.warn(`[Session] Failed to record permission history for verified perm ${verifierId}:`, historyErr?.message || historyErr);
              }
            } catch (verifiedErr: any) {
              this.logger.error(`[Session] Failed to increment verified for permission ${verifierId}:`, verifiedErr?.message || verifiedErr);
            }
          }
        }
      }

      await trx.commit();
      return { success: true };
    } catch (err) {
      await trx.rollback();
      this.logger.error("Error in handleCreateOrUpdatePermissionSession:", err);
      return { success: false, reason: String(err) };

    }
  }

  private async queuePermissionForRetry(message: unknown, reason: string): Promise<void> {
    const msg = message as { id: string | number; type?: string };
    const key = `${msg.id}_${msg.type || 'permission'}`;
    const queuedPermission: QueuedPermission = {
      message: msg,
      reason,
      retryCount: 0,
      queuedAt: new Date(),
      nextRetryAt: new Date(Date.now() + this.RETRY_INTERVAL_MS)
    };

    this.retryQueue.set(key, queuedPermission);
    this.logger.info(`Queued permission ${msg.id} for retry: ${reason}`);
  }

  private startRetryProcessor(): void {
    this.retryInterval = setInterval(async () => {
      await this.processRetryQueue();
    }, this.RETRY_INTERVAL_MS);

    this.logger.info("Started permission retry processor");
  }

  private async processRetryQueue(): Promise<void> {
    const now = new Date();
    const keysToRetry: string[] = [];

    for (const [key, queuedPermission] of this.retryQueue.entries()) {
      if (queuedPermission.nextRetryAt <= now && queuedPermission.retryCount < this.MAX_RETRY_ATTEMPTS) {
        keysToRetry.push(key);
      }
    }

    for (const key of keysToRetry) {
      const queuedPermission = this.retryQueue.get(key)!;
      try {
        this.logger.info(`Retrying permission ${queuedPermission.message.id} (attempt ${queuedPermission.retryCount + 1}/${this.MAX_RETRY_ATTEMPTS})`);

        const result = await this.handleSetPermissionVPToValidated(queuedPermission.message);

        if (result && result.success !== false) {
          this.retryQueue.delete(key);
          this.logger.info(`Successfully processed queued permission ${queuedPermission.message.id}`);
        } else {
          queuedPermission.retryCount++;
          const delay = this.RETRY_INTERVAL_MS * (this.RETRY_BACKOFF_MULTIPLIER ** (queuedPermission.retryCount - 1));
          queuedPermission.nextRetryAt = new Date(Date.now() + delay);
          this.logger.warn(`Permission ${queuedPermission.message.id} still failed, scheduled next retry in ${delay}ms`);
        }
      } catch (error) {
        queuedPermission.retryCount++;
        const delay = this.RETRY_INTERVAL_MS * (this.RETRY_BACKOFF_MULTIPLIER ** (queuedPermission.retryCount - 1));
        queuedPermission.nextRetryAt = new Date(Date.now() + delay);
        this.logger.error(`Error retrying permission ${queuedPermission.message.id}:`, error);
      }
    }

    for (const [key, queuedPermission] of this.retryQueue.entries()) {
      if (queuedPermission.retryCount >= this.MAX_RETRY_ATTEMPTS) {
        this.retryQueue.delete(key);
        this.logger.error(`Permission ${queuedPermission.message.id} exceeded max retries (${this.MAX_RETRY_ATTEMPTS}), giving up`);
      }
    }

    if (this.retryQueue.size > 0) {
      this.logger.info(`Retry queue status: ${this.retryQueue.size} permissions waiting`);
    }
  }

  public async started(): Promise<void> {
    this.startRetryProcessor();
  }

  public async stopped(): Promise<void> {
    if (this.retryInterval) {
      clearInterval(this.retryInterval);
      this.retryInterval = null;
    }
    this.logger.info("Stopped permission retry processor");
  }
}
