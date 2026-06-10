import { Service, ServiceBroker } from "moleculer";
import type { Knex } from "knex";
import { formatTimestamp } from "../../common/utils/date_utils";
import { getBlockChainTimeAsOf } from "../../common/utils/block_time";
import knex from "../../common/utils/db_connection";
import { SERVICE, ModulesParamsNamesTypes } from "../../common";
import getGlobalVariables from "../../common/utils/global_variables";
import { mapParticipantType } from "../../common/utils/utils";
import { extractController, requireController } from "../../common/utils/extract_controller";
import {
  resolveCorporationIdByAddress,
  resolveCorporationIdForMessage,
  resolveAddressByCorporationId,
} from "../crawl-co/corporation_resolve";
import { calculateParticipantState } from "./pp_state_utils";
import { CS_STATS_FIELDS, statsToUpdateObject } from "../../common/utils/stats_fields";
import { calculateCredentialSchemaStats } from "../crawl-cs/cs_stats";
import { calculateEcosystemStats } from "../crawl-ec/ec_stats";
import { syncEcosystemStatsAndHistoryFromSchemaChange, insertCredentialSchemaHistoryStatsRow } from "../crawl-cs/cs_database.service";
import { getModuleParams } from "../../common/utils/params_service";
import {
  getParticipantTypeString,
  MsgCancelParticipantOPLastRequest,
  MsgCreateOrUpdateParticipantSession,
  MsgSelfCreateParticipant,
  MsgCreateRootParticipant,
  MsgSetParticipantEffectiveUntil,
  MsgRenewParticipantOP,
  MsgRepayParticipantSlashedTrustDeposit,
  MsgRevokeParticipant,
  MsgSetParticipantOPToValidated,
  MsgSlashParticipantTrustDeposit,
  MsgStartParticipantOP,
} from "./pp_types";

const PARTICIPANT_HISTORY_FIELDS = [
  "schema_id",
  "role",
  "did",
  "corporation_id",
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
  "validator_participant_id",
  "op_state",
  "op_last_state_change",
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
  "op_current_fees",
  "op_current_deposit",
  "op_summary_digest",
  "op_exp",
  "op_validator_deposit",
  "issued",
  "verified",
  "issuance_fee_discount",
  "verification_fee_discount",
  "expire_soon",
  "vs_operator",
  "adjusted",
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

const PARTICIPANT_SESSION_HISTORY_FIELDS = [
  "corporation_id",
  "vs_operator",
  "agent_participant_id",
  "wallet_agent_participant_id",
  "session_records",
  "created",
  "modified",
];

const PARTICIPANT_HISTORY_V4_FIELDS = [
  "vs_operator",
  "adjusted",
  "vs_operator_authz_enabled",
  "vs_operator_authz_spend_limit",
  "vs_operator_authz_with_feegrant",
  "vs_operator_authz_fee_spend_limit",
  "vs_operator_authz_spend_period",
] as const;

async function callerOwnsCorporation(
  corporationId: number | null | undefined,
  callerAddress: string | null | undefined,
  db: Knex | Knex.Transaction = knex
): Promise<boolean> {
  const corpId = Number(corporationId ?? 0) || 0;
  if (corpId <= 0 || !callerAddress) return false;
  const callerCorpId = await resolveCorporationIdByAddress(callerAddress, db);
  return callerCorpId !== null && callerCorpId === corpId;
}

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

function normalizeParticipantType(value: unknown): string {
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

function pickMessageBool(msg: Record<string, any>, snake: string, camel: string, fallback = false): boolean {
  const raw = pickMessageValue(msg, snake, camel);
  if (raw === true) return true;
  if (raw === false) return false;
  if (typeof raw === "string") {
    const v = raw.trim().toLowerCase();
    if (v === "true") return true;
    if (v === "false") return false;
    if (v === "1") return true;
    if (v === "0") return false;
  }
  if (typeof raw === "number") {
    if (raw === 1) return true;
    if (raw === 0) return false;
  }
  return fallback;
}

function extractParticipantType(msg: Record<string, any>, fallback: string | number = "UNSPECIFIED") {
  return mapParticipantType(
    msg.participant_type ?? msg.participantType ?? msg.role ?? fallback
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
      return "TERMINATED";
    }
  }

  const numeric = Number(value);
  switch (numeric) {
    case 1: return "PENDING";
    case 2: return "VALIDATED";
    case 3: return "TERMINATED";
    case 4: return "VALIDATED";
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

function normalizeDenomAmountArrayForDb(value: unknown): unknown {
  if (value === null) return null;
  if (value === undefined) return [];
  if (typeof value === "string") {
    try {
      return normalizeDenomAmountArrayForDb(JSON.parse(value));
    } catch {
      return [];
    }
  }
  if (Array.isArray(value)) return value;
  return [];
}

let participantsInsertSavepointSeq = 0;
function nextParticipantsInsertSavepoint(): string {
  participantsInsertSavepointSeq += 1;
  return `participant_insert_sp_${participantsInsertSavepointSeq}`;
}

async function healParticipantsIdSequence(trx: any): Promise<void> {
  await trx.raw(`
    SELECT setval(pg_get_serial_sequence('participants','id'),
      GREATEST((SELECT COALESCE(MAX(id), 1) FROM participants), 1)
    )
  `);
}

async function insertParticipantsWithSequenceHeal<T = any>(
  trx: any,
  insertData: any
): Promise<T> {
  const sp = nextParticipantsInsertSavepoint();
  await trx.raw(`SAVEPOINT ${sp}`);
  try {
    const [row] = await trx("participants").insert(insertData).returning("*");
    await trx.raw(`RELEASE SAVEPOINT ${sp}`);
    return row as T;
  } catch (err: any) {
    const code = err?.code ?? err?.nativeError?.code;
    if (code !== "23505") {
      throw err;
    }

    // Clear the failed statement so we can continue in the same transaction.
    await trx.raw(`ROLLBACK TO SAVEPOINT ${sp}`);
    await healParticipantsIdSequence(trx);
    const [row] = await trx("participants").insert(insertData).returning("*");
    await trx.raw(`RELEASE SAVEPOINT ${sp}`);
    return row as T;
  }
}

const participantHistoryColumnExistsCache: Record<string, boolean> = {};

async function checkParticipantHistoryColumnExists(columnName: string): Promise<boolean> {
  if (participantHistoryColumnExistsCache[columnName] !== undefined) {
    return participantHistoryColumnExistsCache[columnName];
  }

  try {
    const result = await knex.schema.hasColumn('participant_history', columnName);
    participantHistoryColumnExistsCache[columnName] = result;
    return result;
  } catch (error) {
    participantHistoryColumnExistsCache[columnName] = false;
    return false;
  }
}

async function pickParticipantSnapshot(record: any) {
  const snapshot: Record<string, any> = {
    participant_id: String(record.participant_id ?? record.id ?? ""),
  };

  const hasIssuedColumn = await checkParticipantHistoryColumnExists("issued");
  const hasVerifiedColumn = await checkParticipantHistoryColumnExists("verified");
  const hasParticipantsColumn = await checkParticipantHistoryColumnExists("participants");
  const hasParticipantRoleColumns =
    (await Promise.all(PARTICIPANT_ROLE_HISTORY_FIELDS.map((field) => checkParticipantHistoryColumnExists(field)))).every(Boolean);
  const hasWeightColumn = await checkParticipantHistoryColumnExists("weight");
  const hasEcosystemSlashEventsColumn = await checkParticipantHistoryColumnExists("ecosystem_slash_events");
  const hasExpireSoonColumn = await checkParticipantHistoryColumnExists("expire_soon");
   const hasIssuanceDiscountColumn = await checkParticipantHistoryColumnExists("issuance_fee_discount");
   const hasVerificationDiscountColumn = await checkParticipantHistoryColumnExists("verification_fee_discount");
  const hasV4ParticipantColumns = await checkParticipantHistoryColumnExists("vs_operator");

  for (const field of PARTICIPANT_HISTORY_FIELDS) {
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
    if (!hasV4ParticipantColumns && (PARTICIPANT_HISTORY_V4_FIELDS as readonly string[]).includes(field)) {
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
      snapshot[field] = normalizeDenomAmountArrayForDb(record[field]);
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

async function recordParticipantHistory(
  db: any,
  participantRecord: any,
  eventType: string,
  height: number,
  previousRecord?: any
) {
  if (!participantRecord) return;

  let participantRecordForHistory = participantRecord;
  const participantId = participantRecord.id ?? participantRecord.participant_id;
  if (participantId != null && db && typeof db === "function") {
    const fresh = await db("participants").where({ id: Number(participantId) }).first();
    if (fresh) {
      participantRecordForHistory = { ...fresh, id: fresh.id ?? participantId, participant_id: participantId };
    }
  }

  const hasIssuedColumn = await checkParticipantHistoryColumnExists("issued");
  const hasVerifiedColumn = await checkParticipantHistoryColumnExists("verified");
  const hasParticipantsColumn = await checkParticipantHistoryColumnExists("participants");
  const hasParticipantRoleColumns =
    (await Promise.all(PARTICIPANT_ROLE_HISTORY_FIELDS.map((field) => checkParticipantHistoryColumnExists(field)))).every(Boolean);
  const hasWeightColumn = await checkParticipantHistoryColumnExists("weight");
  const hasEcosystemSlashEventsColumn = await checkParticipantHistoryColumnExists("ecosystem_slash_events");

  const hasExpireSoonColumn = await checkParticipantHistoryColumnExists("expire_soon");
  const hasIssuanceDiscountColumn = await checkParticipantHistoryColumnExists("issuance_fee_discount");
  const hasVerificationDiscountColumn = await checkParticipantHistoryColumnExists("verification_fee_discount");
  const hasV4ParticipantColumns = await checkParticipantHistoryColumnExists("vs_operator");

  const fieldsToUse = PARTICIPANT_HISTORY_FIELDS.filter(field => {
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
    if (!hasV4ParticipantColumns && (PARTICIPANT_HISTORY_V4_FIELDS as readonly string[]).includes(field)) return false;
    return true;
  });

  const snapshot = await pickParticipantSnapshot(participantRecordForHistory);
  const changes = computeChanges(previousRecord, participantRecordForHistory, fieldsToUse);

  if (previousRecord && !changes) {
    return;
  }

  await db("participant_history").insert({
    ...snapshot,
    event_type: eventType,
    height,
    changes: changes ? JSON.stringify(changes) : null,
    created_at: participantRecordForHistory?.modified ?? participantRecordForHistory?.created ?? new Date(),
  });

}

function pickParticipantSessionSnapshot(record: any) {
  const snapshot: Record<string, any> = {
    session_id: String(record.session_id ?? record.id ?? ""),
  };
  for (const field of PARTICIPANT_SESSION_HISTORY_FIELDS) {
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

async function recordParticipantSessionHistory(
  db: any,
  sessionRecord: any,
  eventType: string,
  height: number,
  previousRecord?: any
) {
  if (!sessionRecord) return;
  const snapshot = pickParticipantSessionSnapshot(sessionRecord);
  const changes = computeChanges(
    previousRecord,
    sessionRecord,
    PARTICIPANT_SESSION_HISTORY_FIELDS
  );

  if (previousRecord && !changes) {
    return;
  }

  await db("participant_session_history").insert({
    ...snapshot,
    event_type: eventType,
    height,
    changes: changes ? JSON.stringify(changes) : null,
    created_at: sessionRecord?.modified ?? sessionRecord?.created ?? new Date(),
  });
}

interface QueuedParticipant {
  message: any;
  reason: string;
  retryCount: number;
  queuedAt: Date;
  nextRetryAt: Date;
}

export default class ParticipantIngestService extends Service {
  private retryQueue: Map<string, QueuedParticipant> = new Map();
  private retryInterval: NodeJS.Timeout | null = null;
  private readonly MAX_RETRY_ATTEMPTS = 10;
  private readonly RETRY_INTERVAL_MS = 30000; // 30 seconds
  private readonly RETRY_BACKOFF_MULTIPLIER = 2;

  /**
   * Calculate expire_soon for a participant based on its state and effective_until
   * Returns: null if not active, false if no expiration or not soon, true if expiring soon
   */
  private async calculateExpireSoon(
    participant: any,
    now: Date = new Date(),
    blockHeight?: number
  ): Promise<boolean | null> {
    // Check if participant is active
    const effectiveFrom = participant.effective_from ? new Date(participant.effective_from) : null;
    const effectiveUntil = participant.effective_until ? new Date(participant.effective_until) : null;

    if (effectiveFrom && now < effectiveFrom) return null;
    if (effectiveUntil && now > effectiveUntil) return null;
    if (participant.revoked) return null;
    if (participant.slashed && !participant.repaid) return null;
    if (participant.op_state !== 'VALIDATED' && participant.role !== 'ECOSYSTEM') return null;
    if (participant.role === "UNSPECIFIED") return null;

    if (!effectiveUntil) {
      return false;
    }

    let nDaysBefore = 0;
    try {
      const moduleParams = await getModuleParams(ModulesParamsNamesTypes.PP, blockHeight);
      if (moduleParams?.params) {
        nDaysBefore = moduleParams.params.PARTICIPANT_SET_EXPIRE_SOON_N_DAYS_BEFORE || 0;
      }
    } catch (error) {
      this.logger.warn(`Failed to get PARTICIPANT module params for expire_soon calculation:`, error);
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
      name: "participantIngest",
      actions: {
        handleMsgCreateRootParticipant: {
          params: { data: "object" },
          handler: async (ctx) =>
            this.handleCreateRootParticipant(ctx.params.data),
        },
        handleMsgSelfCreateParticipant: {
          params: { data: "object" },
          handler: async (ctx) => this.handleCreateParticipant(ctx.params.data),
        },
        handleMsgSetParticipantEffectiveUntil: {
          params: { data: "object" },
          handler: async (ctx) => this.handleSetParticipantEffectiveUntil(ctx.params.data),
        },
        handleMsgRevokeParticipant: {
          params: { data: "object" },
          handler: async (ctx) => this.handleRevokeParticipant(ctx.params.data),
        },
        handleMsgStartParticipantOP: {
          params: { data: "object" },
          handler: async (ctx) => this.handleStartParticipantOP(ctx.params.data),
        },
        handleMsgSetParticipantOPToValidated: {
          params: { data: "object" },
          handler: async (ctx) =>
            this.handleSetParticipantOPToValidated(ctx.params.data),
        },
        handleMsgRenewParticipantOP: {
          params: { data: "object" },
          handler: async (ctx) => this.handleRenewParticipantOP(ctx.params.data),
        },
        handleMsgCancelParticipantOPLastRequest: {
          params: { data: "object" },
          handler: async (ctx) =>
            this.handleCancelParticipantOPLastRequest(ctx.params.data),
        },
        handleMsgCreateOrUpdateParticipantSession: {
          params: { data: "object" },
          handler: async (ctx) =>
            this.handleCreateOrUpdateParticipantSession(ctx.params.data),
        },
        handleMsgSlashParticipantTrustDeposit: {
          params: { data: "object" },
          handler: async (ctx) =>
            this.handleSlashParticipantTrustDeposit(ctx.params.data),
        },
        handleMsgRepayParticipantSlashedTrustDeposit: {
          params: { data: "object" },
          handler: async (ctx) =>
            this.handleRepayParticipantSlashedTrustDeposit(ctx.params.data),
        },
        syncParticipantFromLedger: {
          params: {
            ledgerParticipant: "object",
            blockHeight: "number",
            txHash: { type: "string", optional: true },
            msgType: { type: "string", optional: true },
          },
          handler: async (ctx) => this.syncParticipantFromLedger(
            ctx.params.ledgerParticipant,
            Number(ctx.params.blockHeight) || 0,
            ctx.params.txHash,
            ctx.params.msgType
          ),
        },
        syncParticipantSessionFromLedger: {
          params: {
            ledgerSession: "object",
            blockHeight: "number",
            txHash: { type: "string", optional: true },
            msgType: { type: "string", optional: true },
          },
          handler: async (ctx) => this.syncParticipantSessionFromLedger(
            ctx.params.ledgerSession,
            Number(ctx.params.blockHeight) || 0,
            ctx.params.txHash,
            ctx.params.msgType
          ),
        },
        getParticipantById: {
          params: { id: "number" },
          handler: async (ctx) => knex("participants").where({ id: Number(ctx.params.id) }).first(),
        },
        getParticipantSessionById: {
          params: { id: "string" },
          handler: async (ctx) => knex("participant_sessions").where({ id: String(ctx.params.id) }).first(),
        },
        compareParticipantWithLedger: {
          params: {
            participantId: "number",
            ledgerParticipant: "object",
            blockHeight: "number",
          },
          handler: async (ctx) => this.compareParticipantWithLedger(
            Number(ctx.params.participantId),
            ctx.params.ledgerParticipant,
            Number(ctx.params.blockHeight) || 0
          ),
        },
        compareParticipantSessionWithLedger: {
          params: {
            sessionId: "string",
            ledgerSession: "object",
            blockHeight: "number",
          },
          handler: async (ctx) => this.compareParticipantSessionWithLedger(
            String(ctx.params.sessionId),
            ctx.params.ledgerSession,
            Number(ctx.params.blockHeight) || 0
          ),
        },
        rebuildParticipantStats: {
          params: {
            schema_id: { type: "number", optional: true },
          },
          handler: async (ctx) => this.rebuildParticipantStats(ctx.params.schema_id),
        },
        getParticipant: {
          params: { schema_id: "number", corporation: "string", role: "string" },
          handler: async (ctx) => {
            const { schema_id: schemaId, corporation, role } = ctx.params;
            const corporationId = await resolveCorporationIdByAddress(corporation);
            if (corporationId === null) return undefined;
            return await knex("participants")
              .where({ schema_id: schemaId, corporation_id: corporationId, role })
              .first();
          },
        },
        listParticipants: {
          params: {
            schema_id: { type: "number", optional: true },
            corporation: { type: "string", optional: true },
            role: { type: "string", optional: true },
          },
          handler: async (ctx) => {
            let query = knex("participants");
            if (ctx.params.schema_id)
              query = query.where("schema_id", ctx.params.schema_id);
            if (ctx.params.corporation) {
              const corporationId = await resolveCorporationIdByAddress(ctx.params.corporation);
              if (corporationId === null) return [];
              query = query.where("corporation_id", corporationId);
            }
            if (ctx.params.role) query = query.where("role", ctx.params.role);
            return await query;
          },
        },
      },
    });
  }

  private async handleSetParticipantEffectiveUntil(msg: MsgSetParticipantEffectiveUntil & { height?: number }) {
    const height = Number((msg as any)?.height) || 0;
    const participantId = Number((msg as any)?.id ?? (msg as any)?.participant_id ?? (msg as any)?.participantId ?? 0) || 0;
    if (!Number.isInteger(participantId) || participantId <= 0) {
      this.logger.warn(`[handleSetParticipantEffectiveUntil] Invalid participant id: ${String((msg as any)?.id)}`);
      return;
    }

    const ts = (msg as any)?.timestamp ? formatTimestamp((msg as any).timestamp) : null;
    const effectiveUntilRaw = (msg as any)?.effective_until ?? (msg as any)?.effectiveUntil ?? null;
    const effectiveUntil = effectiveUntilRaw ? formatTimestamp(effectiveUntilRaw) : null;

    try {
      await this.ensureParticipantV4Columns(knex);

      const previous = await knex("participants").where({ id: participantId }).first();
      if (!previous) {
        this.logger.warn(`[handleSetParticipantEffectiveUntil] Participant id=${participantId} not found; skipping`);
        return;
      }

      const adjustedBy = extractController(msg as unknown as Record<string, unknown>) ?? null;

      await knex("participants")
        .where({ id: participantId })
        .update({
          effective_until: effectiveUntil ?? previous.effective_until ?? null,
          adjusted: ts,
          modified: ts ?? previous.modified ?? null,
        });

      const updated = await knex("participants").where({ id: participantId }).first();
      try {
        await recordParticipantHistory(knex, updated ?? { id: participantId }, "ADJUST_PARTICIPANT", height, previous);
      } catch (historyErr: any) {
        this.logger.warn(
          `[handleSetParticipantEffectiveUntil] Failed to record participant history for id=${participantId}: ${historyErr?.message || historyErr}`
        );
      }

      await this.refreshSchemaAndEcosystemStats(previous.schema_id ?? updated?.schema_id, height);
    } catch (err: any) {
      this.logger.error("CRITICAL: Error in handleSetParticipantEffectiveUntil:", err);
      throw err;
    }
  }

  private async refreshEcosystemStatsBySchemaId(
    schemaId: number | null | undefined,
    blockHeightRaw?: number
  ): Promise<void> {
    if (schemaId == null || schemaId <= 0) return;
    const blockHeight = Number(blockHeightRaw) || 0;
    try {
      const cs = await knex("credential_schemas")
        .where({ id: schemaId })
        .select("ecosystem_id")
        .first();
      const ecosystemId = cs?.ecosystem_id != null ? Number(cs.ecosystem_id) : null;
      if (!ecosystemId || !Number.isInteger(ecosystemId) || ecosystemId <= 0) return;

      await syncEcosystemStatsAndHistoryFromSchemaChange(knex, ecosystemId, blockHeight);
      this.logger.info(
        `[EC Stats] Synced ecosystem stats and history from schema_id=${schemaId}, ecosystem_id=${ecosystemId}, height=${blockHeight}`
      );
    } catch (err: any) {
      this.logger.warn(
        `Failed to refresh ecosystem stats/history for schema_id=${schemaId}: ${err?.message || err}`
      );
    }
  }

  private mapLedgerParticipantToDbRow(ledgerParticipant: Record<string, any>) {
    const id = Number(ledgerParticipant.id ?? ledgerParticipant.participant_id);
    const schemaId = Number(ledgerParticipant.schema_id ?? ledgerParticipant.schemaId);
    const nowIso = new Date().toISOString();

    const corporationId =
      Number(ledgerParticipant.corporation_id ?? ledgerParticipant.corporationId ?? 0) || 0;

    return {
      id,
      schema_id: schemaId,
      role: normalizeParticipantType(ledgerParticipant.role),
      did: ledgerParticipant.did ?? null,
      corporation_id: corporationId,
      created: toIsoOrNull(ledgerParticipant.created) ?? nowIso,
      modified: toIsoOrNull(ledgerParticipant.modified) ?? nowIso,
      slashed: toIsoOrNull(ledgerParticipant.slashed),
      repaid: toIsoOrNull(ledgerParticipant.repaid),
      effective_from: toIsoOrNull(ledgerParticipant.effective_from ?? ledgerParticipant.effectiveFrom),
      effective_until: toIsoOrNull(ledgerParticipant.effective_until ?? ledgerParticipant.effectiveUntil),
      revoked: toIsoOrNull(ledgerParticipant.revoked),
      validation_fees: Number(ledgerParticipant.validation_fees ?? ledgerParticipant.validationFees ?? 0),
      issuance_fees: Number(ledgerParticipant.issuance_fees ?? ledgerParticipant.issuanceFees ?? 0),
      verification_fees: Number(ledgerParticipant.verification_fees ?? ledgerParticipant.verificationFees ?? 0),
      deposit: Number(ledgerParticipant.deposit ?? 0),
      slashed_deposit: Number(ledgerParticipant.slashed_deposit ?? ledgerParticipant.slashedDeposit ?? 0),
      repaid_deposit: Number(ledgerParticipant.repaid_deposit ?? ledgerParticipant.repaidDeposit ?? 0),
      validator_participant_id: Number(ledgerParticipant.validator_participant_id ?? ledgerParticipant.validatorParticipantId ?? 0) || null,
      op_state: normalizeValidationState(ledgerParticipant.op_state ?? ledgerParticipant.opState),
      op_exp: toIsoOrNull(ledgerParticipant.op_exp ?? ledgerParticipant.opExp),
      op_last_state_change: toIsoOrNull(ledgerParticipant.op_last_state_change ?? ledgerParticipant.opLastStateChange),
      op_validator_deposit: Number(ledgerParticipant.op_validator_deposit ?? ledgerParticipant.opValidatorDeposit ?? 0),
      op_current_fees: Number(ledgerParticipant.op_current_fees ?? ledgerParticipant.opCurrentFees ?? 0),
      op_current_deposit: Number(ledgerParticipant.op_current_deposit ?? ledgerParticipant.opCurrentDeposit ?? 0),
      op_summary_digest:
        ledgerParticipant.op_summary_digest ?? ledgerParticipant.opSummaryDigest ?? null,
      issuance_fee_discount: Number(
        ledgerParticipant.issuance_fee_discount ?? ledgerParticipant.issuanceFeeDiscount ?? 0
      ),
      verification_fee_discount: Number(
        ledgerParticipant.verification_fee_discount ?? ledgerParticipant.verificationFeeDiscount ?? 0
      ),
      vs_operator: ledgerParticipant.vs_operator ?? ledgerParticipant.vsOperator ?? null,
      adjusted: toIsoOrNull(ledgerParticipant.adjusted ?? ledgerParticipant.adjustedAt),
      vs_operator_authz_enabled: Boolean(
        ledgerParticipant.vs_operator_authz_enabled ?? ledgerParticipant.vsOperatorAuthzEnabled ?? false
      ),
      vs_operator_authz_spend_limit: normalizeDenomAmountArrayForDb(
        ledgerParticipant.vs_operator_authz_spend_limit ?? ledgerParticipant.vsOperatorAuthzSpendLimit
      ),
      vs_operator_authz_with_feegrant: Boolean(
        ledgerParticipant.vs_operator_authz_with_feegrant ?? ledgerParticipant.vsOperatorAuthzWithFeegrant ?? false
      ),
      vs_operator_authz_fee_spend_limit: normalizeDenomAmountArrayForDb(
        ledgerParticipant.vs_operator_authz_fee_spend_limit ?? ledgerParticipant.vsOperatorAuthzFeeSpendLimit
      ),
      vs_operator_authz_spend_period:
        ledgerParticipant.vs_operator_authz_spend_period ??
        ledgerParticipant.vsOperatorAuthzSpendPeriod ??
        null,
    };
  }

  private didEnsureParticipantV4Columns = false;
  private async ensureParticipantV4Columns(db: Knex): Promise<void> {
    if (this.didEnsureParticipantV4Columns) return;

    await db.raw(`
      ALTER TABLE IF EXISTS participants
        ADD COLUMN IF NOT EXISTS corporation_id bigint NOT NULL DEFAULT 0,
        ADD COLUMN IF NOT EXISTS vs_operator text,
        ADD COLUMN IF NOT EXISTS adjusted timestamptz,
        ADD COLUMN IF NOT EXISTS vs_operator_authz_enabled boolean NOT NULL DEFAULT false,
        ADD COLUMN IF NOT EXISTS vs_operator_authz_spend_limit jsonb,
        ADD COLUMN IF NOT EXISTS vs_operator_authz_with_feegrant boolean NOT NULL DEFAULT false,
        ADD COLUMN IF NOT EXISTS vs_operator_authz_fee_spend_limit jsonb,
        ADD COLUMN IF NOT EXISTS vs_operator_authz_spend_period text;
    `);

    await db.raw(`
      ALTER TABLE IF EXISTS participant_history
        ADD COLUMN IF NOT EXISTS vs_operator text,
        ADD COLUMN IF NOT EXISTS adjusted timestamptz,
        ADD COLUMN IF NOT EXISTS vs_operator_authz_enabled boolean NOT NULL DEFAULT false,
        ADD COLUMN IF NOT EXISTS vs_operator_authz_spend_limit jsonb,
        ADD COLUMN IF NOT EXISTS vs_operator_authz_with_feegrant boolean NOT NULL DEFAULT false,
        ADD COLUMN IF NOT EXISTS vs_operator_authz_fee_spend_limit jsonb,
        ADD COLUMN IF NOT EXISTS vs_operator_authz_spend_period text;
    `);

    this.participantsColumnExistsCache = null;
    this.didEnsureParticipantV4Columns = true;
  }

  private async refreshSchemaAndEcosystemStats(
    schemaId: number | null | undefined,
    blockHeightRaw?: number
  ): Promise<void> {
    if (!schemaId || schemaId <= 0) return;
    const blockHeight = Number(blockHeightRaw) || 0;
    try {
      const stats = await calculateCredentialSchemaStats(schemaId, blockHeight > 0 ? blockHeight : undefined);
      const slashFromParticipants = await this.sumSlashStatsFromParticipantsForSchema(schemaId);
      const mergedStats = { ...stats, ...slashFromParticipants };
      await knex("credential_schemas")
        .where("id", schemaId)
        .update(statsToUpdateObject(mergedStats as unknown as Record<string, unknown>, CS_STATS_FIELDS));
      const hasParticipantsColumn = await this.checkParticipantsColumnExists("participants");
      if (hasParticipantsColumn) {
        await knex("participants").where("schema_id", schemaId).update({ participants: mergedStats.participants });
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

    await this.refreshEcosystemStatsBySchemaId(schemaId, blockHeight);

    try {
      await this.broker.call(`${SERVICE.V1.MetricsSnapshotService.path}.computeAndStore`, {});
    } catch (error: any) {
      this.logger.warn(`Failed to refresh global metrics after participant sync: ${error?.message || error}`);
    }
  }

  private async syncParticipantFromLedger(
    ledgerParticipant: Record<string, any>,
    blockHeight: number,
    txHash?: string,
    msgType?: string
  ) {
    const mapped = this.mapLedgerParticipantToDbRow(ledgerParticipant || {});
    if (!Number.isInteger(mapped.id) || mapped.id <= 0) {
      return { success: false, reason: "Invalid participant id from ledger" };
    }
    if (!Number.isInteger(mapped.schema_id) || mapped.schema_id <= 0) {
      return { success: false, reason: "Invalid schema_id from ledger" };
    }

    const effectiveHeight = Number(blockHeight) || 0;
    let finalParticipant: any = null;
    let previousParticipant: any = null;

    await this.ensureParticipantV4Columns(knex);

    await knex.transaction(async (trx) => {
      previousParticipant = await trx("participants").where({ id: mapped.id }).first();

      const payload: any = { ...mapped };
      const hasExpireSoonColumn = await this.checkParticipantsColumnExists("expire_soon");
      if (hasExpireSoonColumn) {
        payload.expire_soon = await this.calculateExpireSoon(
          payload,
          new Date(payload.modified || new Date()),
          effectiveHeight
        );
      }

      if (previousParticipant) {
        await trx("participants")
          .where({ id: mapped.id })
          .update(payload);
        finalParticipant = await trx("participants").where({ id: mapped.id }).first();
      } else {
        await trx("participants").insert(payload);
        finalParticipant = await trx("participants").where({ id: mapped.id }).first();
      }

      try {
        const hasFlipMetaColumns =
          await this.checkParticipantsColumnExists("last_valid_flip_version");
        const hasIsActiveNowColumn =
          await this.checkParticipantsColumnExists("is_active_now");

        if (hasFlipMetaColumns && finalParticipant) {
          const fallbackTime = new Date(mapped.modified || finalParticipant.modified || new Date().toISOString());
          const currentBlockTime = await getBlockChainTimeAsOf(effectiveHeight, {
            db: trx,
            logContext: "[pp_database]",
            fallback: fallbackTime,
            logger: this.logger,
          });

          const participantState = calculateParticipantState(
            {
              repaid: finalParticipant.repaid,
              slashed: finalParticipant.slashed,
              revoked: finalParticipant.revoked,
              effective_from: finalParticipant.effective_from,
              effective_until: finalParticipant.effective_until,
              role: finalParticipant.role,
              op_state: finalParticipant.op_state,
              op_exp: finalParticipant.op_exp,
              validator_participant_id: finalParticipant.validator_participant_id,
            },
            currentBlockTime
          );

          const stateInputsChanged =
            !previousParticipant
            || previousParticipant.repaid !== finalParticipant.repaid
            || previousParticipant.slashed !== finalParticipant.slashed
            || previousParticipant.revoked !== finalParticipant.revoked
            || previousParticipant.effective_from !== finalParticipant.effective_from
            || previousParticipant.effective_until !== finalParticipant.effective_until;

          if (stateInputsChanged) {
            const prevVersion: number =
              typeof previousParticipant?.last_valid_flip_version === "number"
                ? previousParticipant.last_valid_flip_version
                : 0;
            const newVersion = prevVersion + 1;

            const prevIsActiveNow =
              hasIsActiveNowColumn && previousParticipant
                ? Boolean((previousParticipant as any).is_active_now)
                : false;

            const participantUpdate: Record<string, any> = {
              last_valid_flip_version: newVersion,
            };
       
            await trx("participants")
              .where({ id: finalParticipant.id })
              .update(participantUpdate);

            const enterFlips: Array<{ flip_at_time: string; flip_kind: number }> = [];
            const exitFlips: Array<{ flip_at_time: string; flip_kind: number }> = [];

          const effectiveFrom = finalParticipant.effective_from
            ? new Date(finalParticipant.effective_from)
            : null;
          const effectiveUntil = finalParticipant.effective_until
            ? new Date(finalParticipant.effective_until)
            : null;
          const revoked = finalParticipant.revoked ? new Date(finalParticipant.revoked) : null;
          const slashed = finalParticipant.slashed ? new Date(finalParticipant.slashed) : null;
          const repaid = finalParticipant.repaid ? new Date(finalParticipant.repaid) : null;

          if (participantState === "FUTURE") {
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
          } else if (participantState === "ACTIVE") {
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
          } else if (participantState === "EXPIRED") {
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
          } else if (participantState === "SLASHED" || participantState === "REVOKED" || participantState === "REPAID") {
            let exitTime: Date | null = null;
            if (participantState === "SLASHED") exitTime = slashed;
            if (participantState === "REVOKED") exitTime = revoked;
            if (participantState === "REPAID") exitTime = repaid;

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
                await trx("participant_scheduled_flips")
                  .insert({
                    participant_id: finalParticipant.id,
                    flip_at_time: flip.flip_at_time,
                    flip_kind: flip.flip_kind,
                    status: 0, // PENDING
                    version: newVersion,
                    created_at: currentBlockTime.toISOString(),
                  })
                  .onConflict(["participant_id", "version", "flip_at_time", "flip_kind"])
                  .ignore();
              } catch (err: any) {
                this.logger.warn(
                  `[syncParticipantFromLedger] Failed to insert scheduled flip for participant ${finalParticipant.id}:`,
                  err?.message || err
                );
              }
            }
          }
        }
      } catch (err: any) {
        this.logger.warn(
          `[syncParticipantFromLedger] Failed to update scheduled flips metadata for participant ${mapped.id}:`,
          err?.message || err
        );
      }
  try {
        const prevSlashed = BigInt(previousParticipant?.slashed_deposit ?? 0);
        const newSlashed = BigInt(mapped.slashed_deposit ?? 0);
        const prevRepaid = BigInt(previousParticipant?.repaid_deposit ?? 0);
        const newRepaid = BigInt(mapped.repaid_deposit ?? 0);

        const slashDelta = newSlashed > prevSlashed ? newSlashed - prevSlashed : BigInt(0);
        const repayDelta = newRepaid > prevRepaid ? newRepaid - prevRepaid : BigInt(0);

        if (slashDelta > BigInt(0) || repayDelta > BigInt(0)) {
          const participantRow = finalParticipant || mapped;
          const isEcosystemParticipant = participantRow.role === "ECOSYSTEM";
          let isEcosystemSlash = false;
          let isNetworkSlash = false;

          if (isEcosystemParticipant) {
            isEcosystemSlash = true;
          } else if (participantRow.schema_id && ledgerParticipant.slashed_by) {
            const schema = await trx("credential_schemas")
              .where({ id: participantRow.schema_id })
              .first();
            if (schema?.ecosystem_id) {
              const ec = await trx("ecosystem").where({ id: schema.ecosystem_id }).first();
              const slashedBy = ledgerParticipant.slashed_by;
              if (await callerOwnsCorporation(ec?.corporation_id, slashedBy, trx)) {
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
          `[syncParticipantFromLedger] Failed to infer slash statistics for participant ${mapped.id}:`,
          err?.message || err
        );
      }

      await this.updateWeight(trx, mapped.id);
      await this.updateParticipants(trx, mapped.id);

      const refreshed = await trx("participants").where({ id: mapped.id }).first();
      if (refreshed) {
        finalParticipant = refreshed;
      }

      await recordParticipantHistory(
        trx,
        finalParticipant,
        `SYNC_LEDGER${msgType ? `:${msgType}` : ""}${txHash ? `:${txHash}` : ""}`,
        effectiveHeight,
        previousParticipant ? await pickParticipantSnapshot(previousParticipant) : undefined
      );
    });

    await this.refreshSchemaAndEcosystemStats(mapped.schema_id, effectiveHeight);

    return {
      success: true,
      participantId: mapped.id,
      schemaId: mapped.schema_id,
      changed: !!previousParticipant,
    };
  }

  private async syncParticipantSessionFromLedger(
    ledgerSession: Record<string, any>,
    blockHeight: number,
    txHash?: string,
    msgType?: string
  ) {
    const id = String(ledgerSession?.id || "").trim();
    if (!id) return { success: false, reason: "Invalid participant session id from ledger" };

    const effectiveHeight = Number(blockHeight) || 0;
    const recordsRaw = Array.isArray(ledgerSession?.session_records)
      ? ledgerSession.session_records
      : Array.isArray(ledgerSession?.sessionRecords)
        ? ledgerSession.sessionRecords
        : Array.isArray(ledgerSession?.authz)
          ? ledgerSession.authz
          : [];

    const enrichedAuthz = await Promise.all(recordsRaw.map(async (entry: any) => {
      const directIssuer = entry?.issuer_participant_id ?? entry?.issuerParticipantId;
      const directVerifier = entry?.verifier_participant_id ?? entry?.verifierParticipantId;
      if (directIssuer != null || directVerifier != null) {
        const walletAgentParticipantId =
          Number(entry?.wallet_agent_participant_id ?? entry?.walletAgentParticipantId ?? ledgerSession?.wallet_agent_participant_id ?? 0) || 0;
        return {
          issuer_participant_id: directIssuer != null ? Number(directIssuer) : null,
          verifier_participant_id: directVerifier != null ? Number(directVerifier) : null,
          wallet_agent_participant_id: walletAgentParticipantId || null,
        };
      }

      const walletAgentParticipantId = Number(entry?.wallet_agent_participant_id ?? ledgerSession?.wallet_agent_participant_id ?? 0) || 0;
      const beneficiaryParticipantId = Number(entry?.beneficiary_participant_id ?? 0) || 0;

      let issuerParticipantId: number | null = null;
      let verifierParticipantId: number | null = null;
      if (beneficiaryParticipantId > 0) {
        const beneficiaryParticipant = await knex("participants").where({ id: beneficiaryParticipantId }).select("role").first();
        if (beneficiaryParticipant?.role === "ISSUER") issuerParticipantId = beneficiaryParticipantId;
        if (beneficiaryParticipant?.role === "VERIFIER") verifierParticipantId = beneficiaryParticipantId;
      }

      return {
        issuer_participant_id: issuerParticipantId,
        verifier_participant_id: verifierParticipantId,
        wallet_agent_participant_id: walletAgentParticipantId || null,
      };
    }));

      const mappedSession: any = {
        id,
        corporation_id:
          Number(ledgerSession?.corporation_id ?? ledgerSession?.corporationId ?? 0) || 0,
        vs_operator: ledgerSession?.vs_operator ?? ledgerSession?.vsOperator ?? null,
        agent_participant_id: Number(ledgerSession?.agent_participant_id ?? ledgerSession?.agentParticipantId ?? 0) || 0,
        wallet_agent_participant_id: Number(ledgerSession?.wallet_agent_participant_id ?? ledgerSession?.walletAgentParticipantId ?? 0) || 0,
        session_records: JSON.stringify(
          enrichedAuthz.map((e: any, i: number) => ({
            created:
              toIsoOrNull(recordsRaw[i]?.created) ??
              toIsoOrNull(ledgerSession?.modified) ??
              new Date().toISOString(),
            issuer_participant_id: e.issuer_participant_id ?? null,
            verifier_participant_id: e.verifier_participant_id ?? null,
            wallet_agent_participant_id: e.wallet_agent_participant_id ?? null,
          }))
        ),
        created: toIsoOrNull(ledgerSession?.created) ?? new Date().toISOString(),
        modified: toIsoOrNull(ledgerSession?.modified) ?? new Date().toISOString(),
      };

    await knex.transaction(async (trx) => {
      const previous = await trx("participant_sessions").where({ id }).first();
      let finalSession: any = null;
      if (previous) {
        await trx("participant_sessions")
          .where({ id })
          .update(mappedSession);
        finalSession = await trx("participant_sessions").where({ id }).first();
      } else {
        await trx("participant_sessions").insert(mappedSession);
        finalSession = await trx("participant_sessions").where({ id }).first();
      }

      await recordParticipantSessionHistory(
        trx,
        finalSession,
        `SYNC_LEDGER${msgType ? `:${msgType}` : ""}${txHash ? `:${txHash}` : ""}`,
        effectiveHeight,
        previous ? pickParticipantSessionSnapshot(previous) : undefined
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

      const previousIssuerParticipantIds = new Set(
        previousAuthz
          .map((entry: { issuer_participant_id?: string | number }) => entry.issuer_participant_id)
          .filter((v: any) => v !== null && v !== undefined)
      );
      const previousVerifierParticipantIds = new Set(
        previousAuthz
          .map((entry: { verifier_participant_id?: string | number }) => entry.verifier_participant_id)
          .filter((v: any) => v !== null && v !== undefined)
      );

      const newIssuerParticipantIds = new Set(
        enrichedAuthz
          .map((entry: { issuer_participant_id?: number | null }) => entry.issuer_participant_id)
          .filter((v): v is number => v !== null && v !== undefined)
      );
      const newVerifierParticipantIds = new Set(
        enrichedAuthz
          .map((entry: { verifier_participant_id?: number | null }) => entry.verifier_participant_id)
          .filter((v): v is number => v !== null && v !== undefined)
      );

      for (const issuerId of newIssuerParticipantIds) {
        if (!previousIssuerParticipantIds.has(issuerId)) {
          try {
            await this.incrementParticipantStatistics(trx, Number(issuerId), true, false);
            try {
              await recordParticipantHistory(
                trx,
                { id: Number(issuerId) },
                "CREDENTIAL_ISSUED",
                effectiveHeight
              );
            } catch (historyErr: any) {
              this.logger.warn(
                `[Session Height-Sync] Failed to record participant history for issued participant ${issuerId}:`,
                historyErr?.message || historyErr
              );
            }
          } catch (issuedErr: any) {
            this.logger.error(
              `[Session Height-Sync] Failed to increment issued for participant ${issuerId}:`,
              issuedErr?.message || issuedErr
            );
          }
        }
      }

      for (const verifierId of newVerifierParticipantIds) {
        if (!previousVerifierParticipantIds.has(verifierId)) {
          try {
            await this.incrementParticipantStatistics(trx, Number(verifierId), false, true);
            try {
              await recordParticipantHistory(
                trx,
                { id: Number(verifierId) },
                "CREDENTIAL_VERIFIED",
                effectiveHeight
              );
            } catch (historyErr: any) {
              this.logger.warn(
                `[Session Height-Sync] Failed to record participant history for verified participant ${verifierId}:`,
                historyErr?.message || historyErr
              );
            }
          } catch (verifiedErr: any) {
            this.logger.error(
              `[Session Height-Sync] Failed to increment verified for participant ${verifierId}:`,
              verifiedErr?.message || verifiedErr
            );
          }
        }
      }
    });

    return { success: true, sessionId: id };
  }


  private async rebuildParticipantStats(schemaId?: number) {
    try {
      this.logger.info(`[rebuildParticipantStats] Starting rebuild${schemaId ? ` for schema_id=${schemaId}` : ""}...`);

      await knex.transaction(async (trx) => {
        let participantQuery = trx("participants").select("id", "schema_id");
        if (schemaId && Number.isInteger(schemaId) && schemaId > 0) {
          participantQuery = participantQuery.where("schema_id", schemaId);
        }
        const participants = await participantQuery;
        if (!participants || participants.length === 0) {
          this.logger.info("[rebuildParticipantStats] No participants found in scope, nothing to rebuild.");
          return;
        }

        const participantIds = participants.map((p: any) => Number(p.id));
        const participantIdSet = new Set<number>(participantIds);

        for (const { id } of participants) {
          const pid = Number(id);
          if (!Number.isInteger(pid) || pid <= 0) continue;
          try {
            await this.updateWeight(trx, pid);
          } catch (err: any) {
            this.logger.warn(`[rebuildParticipantStats] Failed to update weight for participant ${pid}:`, err?.message || err);
          }
          try {
            await this.updateParticipants(trx, pid);
          } catch (err: any) {
            this.logger.warn(`[rebuildParticipantStats] Failed to update participants for participant ${pid}:`, err?.message || err);
          }
        }

        const issuedCounts = new Map<number, number>();
        const verifiedCounts = new Map<number, number>();
        const sessionRows = await trx("participant_sessions").select("session_records");
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
            const issuerId = Number(entry?.issuer_participant_id ?? 0) || 0;
            const verifierId = Number(entry?.verifier_participant_id ?? 0) || 0;
            if (issuerId > 0 && participantIdSet.has(issuerId)) {
              issuedCounts.set(issuerId, (issuedCounts.get(issuerId) || 0) + 1);
            }
            if (verifierId > 0 && participantIdSet.has(verifierId)) {
              verifiedCounts.set(verifierId, (verifiedCounts.get(verifierId) || 0) + 1);
            }
          }
        }

        const historyStats = new Map<number, any>();
        const historyRows = await trx("participant_history")
          .whereIn("participant_id", Array.from(participantIdSet))
          .select(
            "participant_id",
            "ecosystem_slash_events",
            "ecosystem_slashed_amount",
            "ecosystem_slashed_amount_repaid",
            "network_slash_events",
            "network_slashed_amount",
            "network_slashed_amount_repaid"
          )
          .orderBy("participant_id")
          .orderBy("height", "desc")
          .orderBy("created_at", "desc")
          .orderBy("id", "desc");

        for (const row of historyRows || []) {
          const pid = Number(row.participant_id);
          if (!participantIdSet.has(pid)) continue;
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

        for (const { id } of participants) {
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
            await trx("participants")
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
              `[rebuildParticipantStats] Failed to update rebuilt stats for participant ${pid}:`,
              err?.message || err
            );
          }
        }
      });

      this.logger.info(`[rebuildParticipantStats] Completed rebuild${schemaId ? ` for schema_id=${schemaId}` : ""}.`);
      return { success: true };
    } catch (err: any) {
      this.logger.error("[rebuildParticipantStats] Failed to rebuild participant stats:", err);
      return { success: false, reason: err?.message || String(err) };
    }
  }

  private normalizeComparableTimestamp(value: unknown): string | null {
    if (value === null || value === undefined || value === "") return null;
    const d = new Date(String(value));
    if (Number.isNaN(d.getTime())) return null;
    return d.toISOString();
  }

  private normalizeComparableParticipantRecord(record: Record<string, any> | null | undefined): Record<string, any> | null {
    if (!record) return null;
    return {
      id: Number(record.id ?? record.participant_id ?? 0) || 0,
      schema_id: Number(record.schema_id ?? record.schemaId ?? 0) || 0,
      role: normalizeParticipantType(record.role),
      did: record.did ?? null,
      corporation_id: Number(record.corporation_id ?? record.corporationId ?? 0) || 0,
      created: this.normalizeComparableTimestamp(record.created),
      modified: this.normalizeComparableTimestamp(record.modified),
      adjusted: this.normalizeComparableTimestamp(record.adjusted),
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
      validator_participant_id: Number(record.validator_participant_id ?? record.validatorParticipantId ?? 0) || null,
      op_state: normalizeValidationState(record.op_state ?? record.opState),
      op_exp: this.normalizeComparableTimestamp(record.op_exp ?? record.opExp),
      op_last_state_change: this.normalizeComparableTimestamp(record.op_last_state_change ?? record.opLastStateChange),
      op_validator_deposit: Number(record.op_validator_deposit ?? record.opValidatorDeposit ?? 0),
      op_current_fees: Number(record.op_current_fees ?? record.opCurrentFees ?? 0),
      op_current_deposit: Number(record.op_current_deposit ?? record.opCurrentDeposit ?? 0),
      op_summary_digest:
        record.op_summary_digest ??
        record.opSummaryDigest ??
        null,
    };
  }

  private normalizeComparableParticipantSessionRecord(record: Record<string, any> | null | undefined): Record<string, any> | null {
    if (!record) return null;
    const srRaw = Array.isArray(record.session_records)
      ? record.session_records
      : parseJson(record.session_records) ?? parseJson(record.authz) ?? [];
    const sessionRecords = Array.isArray(srRaw)
      ? srRaw
          .map((entry: any) => ({
            created: String(entry?.created ?? ""),
            issuer_participant_id: Number(entry?.issuer_participant_id ?? 0) || null,
            verifier_participant_id: Number(entry?.verifier_participant_id ?? 0) || null,
            wallet_agent_participant_id: Number(entry?.wallet_agent_participant_id ?? 0) || null,
          }))
          .sort((a: any, b: any) => JSON.stringify(a).localeCompare(JSON.stringify(b)))
      : [];

    return {
      id: String(record.id ?? record.session_id ?? ""),
      corporation_id: Number(record.corporation_id ?? record.corporationId ?? 0) || 0,
      vs_operator: record.vs_operator ?? record.vsOperator ?? null,
      agent_participant_id: Number(record.agent_participant_id ?? record.agentParticipantId ?? 0) || 0,
      wallet_agent_participant_id: Number(record.wallet_agent_participant_id ?? record.walletAgentParticipantId ?? 0) || 0,
      session_records: sessionRecords,
      created: this.normalizeComparableTimestamp(record.created),
      modified: this.normalizeComparableTimestamp(record.modified),
    };
  }

  private async getParticipantSnapshotAtHeight(participantId: number, blockHeight: number): Promise<any | null> {
    if (!(Number.isInteger(blockHeight) && blockHeight > 0)) {
      return knex("participants").where({ id: participantId }).first();
    }
    const history = await knex("participant_history")
      .where({ participant_id: participantId })
      .andWhere("height", "<=", blockHeight)
      .orderBy("height", "desc")
      .orderBy("created_at", "desc")
      .orderBy("id", "desc")
      .first();
    if (!history) return null;
    return {
      ...history,
      id: Number(history.participant_id),
    };
  }

  private async getParticipantSessionSnapshotAtHeight(sessionId: string, blockHeight: number): Promise<any | null> {
    if (!(Number.isInteger(blockHeight) && blockHeight > 0)) {
      return knex("participant_sessions").where({ id: sessionId }).first();
    }
    const history = await knex("participant_session_history")
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

  private async compareParticipantWithLedger(
    participantId: number,
    ledgerParticipant: Record<string, any>,
    blockHeight: number
  ) {
    const dbSnapshot = await this.getParticipantSnapshotAtHeight(participantId, blockHeight);
    const normalizedDb = this.normalizeComparableParticipantRecord(dbSnapshot);
    const normalizedLedger = this.normalizeComparableParticipantRecord(ledgerParticipant);
    const result = this.compareObjects(normalizedDb, normalizedLedger);
    return {
      success: true,
      matches: result.matches,
      diffs: result.diffs,
      participantId,
      blockHeight,
    };
  }

  private async compareParticipantSessionWithLedger(
    sessionId: string,
    ledgerSession: Record<string, any>,
    blockHeight: number
  ) {
    const dbSnapshot = await this.getParticipantSessionSnapshotAtHeight(sessionId, blockHeight);
    const normalizedDb = this.normalizeComparableParticipantSessionRecord(dbSnapshot);
    const normalizedLedger = this.normalizeComparableParticipantSessionRecord(ledgerSession);
    const result = this.compareObjects(normalizedDb, normalizedLedger);
    return {
      success: true,
      matches: result.matches,
      diffs: result.diffs,
      sessionId,
      blockHeight,
    };
  }

  private async handleCreateRootParticipant(msg: MsgCreateRootParticipant & { height?: number }) {
    let participant: any = null;
    try {
      this.logger.info(`🔐 handleCreateRootParticipant called with msg:`, JSON.stringify(msg, null, 2));
      const schemaId = (msg as any).schemaId ?? (msg as any).schema_id ?? null;
      this.logger.info(`🔐 Extracted schemaId: ${schemaId}`);
      if (!schemaId) {
        this.logger.error(
          "CRITICAL: Missing schema_id in MsgCreateRootParticipant, cannot create root participant. Msg keys:", Object.keys(msg)
        );
        return;

      }

      const corporationId = await resolveCorporationIdForMessage(msg);
      const timestamp = msg?.timestamp ? formatTimestamp(msg.timestamp) : null;
      const effectiveFromRaw = pickMessageValue(msg as any, "effective_from", "effectiveFrom");
      const effectiveUntilRaw = pickMessageValue(msg as any, "effective_until", "effectiveUntil");
      const effectiveFrom = effectiveFromRaw ? formatTimestamp(effectiveFromRaw) : null;
      const effectiveUntil = effectiveUntilRaw ? formatTimestamp(effectiveUntilRaw) : null;
      const participantType = extractParticipantType(msg as any, "ECOSYSTEM");

      // Calculate expire_soon for the new participant
      const newParticipantData = {
        role: participantType,
        op_state: "VALIDATION_STATE_UNSPECIFIED",
        effective_from: effectiveFrom,
        effective_until: effectiveUntil,
        revoked: null,
        slashed: null,
        repaid: null,
      };
      const height = Number((msg as any)?.height) || 0;
      const expireSoon = await this.calculateExpireSoon(newParticipantData, new Date(timestamp || new Date()), height);
      const hasExpireSoonColumn = await this.checkParticipantsColumnExists("expire_soon");

      this.logger.info(`[handleCreateRootParticipant] expire_soon calculated: ${expireSoon}, column exists: ${hasExpireSoonColumn}`);

      const msgIdNum = Number((msg as any)?.id ?? (msg as any)?.participant_id ?? (msg as any)?.participantId);
      const hasValidMsgId = Number.isInteger(msgIdNum) && msgIdNum > 0;

      const insertData: any = {
        schema_id: schemaId,
        role: participantType,
        op_state: "VALIDATION_STATE_UNSPECIFIED",
        did: msg.did,
        corporation_id: corporationId,
        effective_from: effectiveFrom,
        effective_until: effectiveUntil,
        validation_fees: Number(pickMessageValue(msg as any, "validation_fees", "validationFees") ?? 0),
        issuance_fees: Number(pickMessageValue(msg as any, "issuance_fees", "issuanceFees") ?? 0),
        verification_fees: Number(pickMessageValue(msg as any, "verification_fees", "verificationFees") ?? 0),
        vs_operator: pickMessageValue(msg as any, "vs_operator", "vsOperator") ?? null,
        adjusted: timestamp,
        deposit: 0,
        modified: timestamp,
        created: timestamp,
      };

      if (hasValidMsgId) {
        insertData.id = msgIdNum;
      }

      if (hasExpireSoonColumn) {
        insertData.expire_soon = expireSoon;
        this.logger.info(`[handleCreateRootParticipant] Adding expire_soon=${expireSoon} to insert data`);
      } else {
        this.logger.warn(`[handleCreateRootParticipant] expire_soon column does not exist, skipping. Please run database migrations.`);
      }

      let insertedParticipant: any = null;
      await this.ensureParticipantV4Columns(knex);
      await knex.transaction(async (trx) => {
        const existing = hasValidMsgId
          ? await trx("participants").where({ id: msgIdNum }).first()
          : null;

        if (existing) {
          await trx("participants").where({ id: msgIdNum }).update(insertData);
          insertedParticipant = await trx("participants").where({ id: msgIdNum }).first();
        } else {
          try {
            const q = trx("participants").insert(insertData);
            if (hasValidMsgId) {
              [insertedParticipant] = await q.onConflict("id").merge(insertData).returning("*");
            } else {
              insertedParticipant = await insertParticipantsWithSequenceHeal(trx, insertData);
            }
          } catch (insertError: any) {
            if (insertError?.code === "42703" && insertError?.message?.includes("expire_soon")) {
              this.logger.warn(
                `[handleCreateRootParticipant] expire_soon column error detected, clearing cache and retrying without expire_soon`
              );
              this.participantsColumnExistsCache = null;
              delete insertData.expire_soon;
              const q = trx("participants").insert(insertData);
              if (hasValidMsgId) {
                [insertedParticipant] = await q.onConflict("id").merge(insertData).returning("*");
              } else {
                insertedParticipant = await insertParticipantsWithSequenceHeal(trx, insertData);
              }
            } else {
              throw insertError;
            }
          }
        }
      });

      if (!insertedParticipant) {
        this.logger.error(
          "CRITICAL: Failed to create root participant - insert returned no record"
        );
        return;

      }

      participant = insertedParticipant;

      try {
        await knex.transaction(async (trx) => {
          await this.updateParticipants(trx, Number(participant.id));
        });
      } catch (participantsErr: any) {
        this.logger.warn(`Failed to update participants for root participant ${participant.id}:`, participantsErr?.message || participantsErr);
      }

      try {
        await recordParticipantHistory(
          knex,
          participant,
          "CREATE_ROOT_PARTICIPANT",
          height
        );
      } catch (historyErr: any) {
        this.logger.error(
          "CRITICAL: Failed to record participant history for root participant:",
          historyErr
        );

      }

      await this.refreshEcosystemStatsBySchemaId(schemaId, height);
    } catch (err: any) {
      this.logger.error("CRITICAL: Error in handleCreateRootParticipant:", err);
      console.error("FATAL PP CREATE ROOT ERROR:", err);

    }
  }

  private async handleCreateParticipant(msg: MsgSelfCreateParticipant & { height?: number }) {
    try {
      this.logger.info(`🔐 handleCreateParticipant called with msg:`, JSON.stringify(msg, null, 2));
      let schemaId = (msg as any).schemaId ?? (msg as any).schema_id ?? null;
      const explicitValidatorParticipantId =
        (msg as any).validatorParticipantId ?? (msg as any).validator_participant_id ??
        (msg as any).validatorParticipantId ?? (msg as any).validator_participant_id ?? null;
      let validatorParticipantFromMessage: any = null;
      if (!schemaId && explicitValidatorParticipantId) {
        validatorParticipantFromMessage = await knex("participants")
          .where({ id: explicitValidatorParticipantId })
          .first();
        schemaId = validatorParticipantFromMessage?.schema_id ?? null;
      }
      this.logger.info(`🔐 Extracted schemaId: ${schemaId}`);
      if (!schemaId) {
        this.logger.warn(
          "Missing schema_id and could not infer it from validator_participant_id in MsgSelfCreateParticipant, skipping insert. Msg keys:", Object.keys(msg)
        );
        return;

      }

      const role = extractParticipantType(msg as any, (msg as any).type);

      const ecosystemParticipant = await knex("participants")
        .where({ schema_id: schemaId, role: "ECOSYSTEM" })
        .first();

      if (!ecosystemParticipant) {
        this.logger.warn(
          `No root ECOSYSTEM participant found for schema_id=${schemaId}, cannot create ${role}`
        );
      }

      const corporationId = await resolveCorporationIdForMessage(msg);
      const timestamp = msg?.timestamp ? formatTimestamp(msg.timestamp) : null;
      const effectiveFromRaw = pickMessageValue(msg as any, "effective_from", "effectiveFrom");
      const effectiveUntilRaw = pickMessageValue(msg as any, "effective_until", "effectiveUntil");
      const effectiveFrom = effectiveFromRaw ? formatTimestamp(effectiveFromRaw) : null;
      const effectiveUntil = effectiveUntilRaw ? formatTimestamp(effectiveUntilRaw) : null;
      const height = Number((msg as any)?.height) || 0;

      // Calculate expire_soon for the new participant
      const newParticipantData = {
        role,
        op_state: "VALIDATION_STATE_UNSPECIFIED",
        effective_from: effectiveFrom,
        effective_until: effectiveUntil,
        revoked: null,
        slashed: null,
        repaid: null,
      };
      const expireSoon = await this.calculateExpireSoon(newParticipantData, new Date(timestamp || new Date()), height);
      const hasExpireSoonColumn = await this.checkParticipantsColumnExists("expire_soon");

      this.logger.info(`[handleCreateParticipant] expire_soon calculated: ${expireSoon}, column exists: ${hasExpireSoonColumn}`);

      const msgIdNum = Number((msg as any)?.id ?? (msg as any)?.participant_id ?? (msg as any)?.participantId);
      const hasValidMsgId = Number.isInteger(msgIdNum) && msgIdNum > 0;

      const insertData: any = {
        schema_id: schemaId,
        role,
        op_state: "VALIDATION_STATE_UNSPECIFIED",
        did: msg.did,
        corporation_id: corporationId,
        effective_from: effectiveFrom,
        effective_until: effectiveUntil,
        verification_fees: Number(pickMessageValue(msg as any, "verification_fees", "verificationFees") ?? 0),
        validation_fees: Number(pickMessageValue(msg as any, "validation_fees", "validationFees") ?? 0),
        issuance_fees: Number(pickMessageValue(msg as any, "issuance_fees", "issuanceFees") ?? 0),
        deposit: 0,
        validator_participant_id: explicitValidatorParticipantId ?? ecosystemParticipant?.id ?? null,
        vs_operator: pickMessageValue(msg as any, "vs_operator", "vsOperator") ?? null,
        vs_operator_authz_enabled: pickMessageBool(msg as any, "vs_operator_authz_enabled", "vsOperatorAuthzEnabled", false),
        vs_operator_authz_spend_limit: normalizeDenomAmountArrayForDb(
          pickMessageValue(msg as any, "vs_operator_authz_spend_limit", "vsOperatorAuthzSpendLimit")
        ),
        vs_operator_authz_with_feegrant: pickMessageBool(
          msg as any,
          "vs_operator_authz_with_feegrant",
          "vsOperatorAuthzWithFeegrant",
          false
        ),
        vs_operator_authz_fee_spend_limit: normalizeDenomAmountArrayForDb(
          pickMessageValue(msg as any, "vs_operator_authz_fee_spend_limit", "vsOperatorAuthzFeeSpendLimit")
        ),
        vs_operator_authz_spend_period: pickMessageValue(msg as any, "vs_operator_authz_spend_period", "vsOperatorAuthzSpendPeriod") ?? null,
        modified: timestamp,
        created: timestamp,
      };

      if (hasValidMsgId) {
        insertData.id = msgIdNum;
      }

      if (hasExpireSoonColumn) {
        insertData.expire_soon = expireSoon;
        this.logger.info(`[handleCreateParticipant] Adding expire_soon=${expireSoon} to insert data`);
      } else {
        this.logger.warn(`[handleCreateParticipant] expire_soon column does not exist, skipping. Please run database migrations.`);
      }

      let participant: any = null;
      await this.ensureParticipantV4Columns(knex);
      await knex.transaction(async (trx) => {
        const existing = hasValidMsgId
          ? await trx("participants").where({ id: msgIdNum }).first()
          : null;

        if (existing) {
          await trx("participants").where({ id: msgIdNum }).update(insertData);
          participant = await trx("participants").where({ id: msgIdNum }).first();
        } else {
          try {
            const q = trx("participants").insert(insertData);
            if (hasValidMsgId) {
              [participant] = await q.onConflict("id").merge(insertData).returning("*");
            } else {
              participant = await insertParticipantsWithSequenceHeal(trx, insertData);
            }
          } catch (insertError: any) {
            if (insertError?.code === "42703" && insertError?.message?.includes("expire_soon")) {
              this.logger.warn(
                `[handleCreateParticipant] expire_soon column error detected, clearing cache and retrying without expire_soon`
              );
              this.participantsColumnExistsCache = null;
              delete insertData.expire_soon;
              const q = trx("participants").insert(insertData);
              if (hasValidMsgId) {
                [participant] = await q.onConflict("id").merge(insertData).returning("*");
              } else {
                participant = await insertParticipantsWithSequenceHeal(trx, insertData);
              }
            } else {
              throw insertError;
            }
          }
        }
      });

      if (!participant) {
        this.logger.error(
          "CRITICAL: Failed to create participant - insert returned no record"
        );
        return;


      }

      try {
        await knex.transaction(async (trx) => {
          await this.updateParticipants(trx, Number(participant.id));
        });
      } catch (participantsErr: any) {
        this.logger.warn(`Failed to update participants for participant ${participant.id}:`, participantsErr?.message || participantsErr);
      }

      try {
        await recordParticipantHistory(
          knex,
          participant,
          "CREATE_PARTICIPANT",
          height
        );
      } catch (historyErr: any) {
        this.logger.error(
          "CRITICAL: Failed to record participant history:",
          historyErr
        );

      }

      await this.refreshEcosystemStatsBySchemaId(participant.schema_id, height);
    } catch (err: any) {
      this.logger.error("CRITICAL: Error in handleCreateParticipant:", err);
      console.error("FATAL PP CREATE ERROR:", err);

    }
  }

  private async handleRevokeParticipant(msg: MsgRevokeParticipant & { height?: number }) {
    try {
      if (!msg.id) {
        this.logger.warn("Missing mandatory parameter: id");
        return { success: false, reason: "Missing mandatory parameter" };
      }

      const now = formatTimestamp(msg.timestamp);
      const applicantParticipant = await knex("participants")
        .where({ id: msg.id })
        .first();
      if (!applicantParticipant) {
        this.logger.warn(`Participant ${msg.id} not found`);
        return { success: false, reason: "Participant not found" };
      }

      const caller = extractController(msg as unknown as Record<string, unknown>);
      // VPR v4: ownership compares by corporation_id; resolve the caller account once.
      const callerCorpId = await resolveCorporationIdByAddress(caller ?? null);

      let authorized = false;
      if (callerCorpId !== null && applicantParticipant.validator_participant_id) {
        let validatorParticipantId = applicantParticipant.validator_participant_id;
        while (validatorParticipantId) {
          const validatorParticipant = await knex("participants")
            .where({ id: validatorParticipantId })
            .first();
          if (!validatorParticipant) break;
          if (Number(validatorParticipant.corporation_id ?? 0) === callerCorpId) {
            authorized = true;
            break;
          }
          validatorParticipantId = validatorParticipant.validator_participant_id;
        }
      }

      if (!authorized && callerCorpId !== null) {
        const cs = await knex("credential_schemas")
          .where({ id: applicantParticipant.schema_id })
          .first();
        if (cs) {
          const ec = await knex("ecosystem")
            .where({ id: cs.ecosystem_id })
            .first();
          if (Number(ec?.corporation_id ?? 0) === callerCorpId) {
            authorized = true;
          }
        }
      }

      if (!authorized && callerCorpId !== null) {
        if (Number(applicantParticipant.corporation_id ?? 0) === callerCorpId) {
          authorized = true;
        }
      }

      if (!authorized) {
        this.logger.warn("Caller is not authorized to revoke this participant");
        return { success: false, reason: "Unauthorized caller" };
      }

      const height = Number((msg as any)?.height) || 0;
      await knex.transaction(async (trx) => {
        // Revoked participants are not active, so expire_soon should be null
        const hasExpireSoonColumn = await this.checkParticipantsColumnExists("expire_soon");

        const updateData: any = {
          revoked: now,
          modified: now,
        };

        if (hasExpireSoonColumn) {
          updateData.expire_soon = null;
        }

        const [updated] = await trx("participants")
          .where({ id: msg.id })
          .update(updateData)
          .returning("*");

        if (!updated) {
          this.logger.error(
            `CRITICAL: Failed to revoke participant ${msg.id} - update returned no record`
          );

        }

        try {
          await this.updateParticipants(trx, Number(msg.id));
        } catch (participantsErr: any) {
          this.logger.warn(`Failed to update participants for participant ${msg.id}:`, participantsErr?.message || participantsErr);
        }

        try {
          await recordParticipantHistory(
            trx,
            updated,
            "REVOKE_PARTICIPANT",
            height,
            applicantParticipant
          );
        } catch (historyErr: any) {
          this.logger.error(
            "CRITICAL: Failed to record participant history for revoke:",
            historyErr
          );

        }
      });

      await this.refreshEcosystemStatsBySchemaId(applicantParticipant?.schema_id, height);

      this.logger.info(
        `Participant ${msg.id} successfully revoked by ${caller}`
      );
      return { success: true };
    } catch (err: any) {
      this.logger.error("CRITICAL: Error in handleRevokeParticipant:", err);
      console.error("FATAL PP REVOKE ERROR:", err);
      return { success: false, reason: "Internal error revoking participant" };
    }
  }

  private async handleStartParticipantOP(msg: MsgStartParticipantOP & { height?: number }) {
    try {
      const typeStr = extractParticipantType(msg as any, getParticipantTypeString(msg));
      const now = formatTimestamp(msg.timestamp);
      const validatorParticipantId = (msg as any).validatorParticipantId ?? (msg as any).validator_participant_id;

      const participant = await knex("participants")
        .where({ id: validatorParticipantId })
        .first();

      if (!participant) {
        this.logger.warn(
          `Participant ${validatorParticipantId} not found, skipping OP start`
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
          globalVariables?.ec?.trust_unit_price ?? 0
        );
        const trustDepositRate = Number(
          globalVariables?.td?.trust_deposit_rate ?? 0
        );

        validationFeesDenom =
          participant?.validation_fees && trustUnitPrice
            ? Number(participant.validation_fees) * trustUnitPrice
            : 0;

        validationTDDenom =
          validationFeesDenom && trustDepositRate
            ? validationFeesDenom * trustDepositRate
            : 0;
      }

      const corporationId = await resolveCorporationIdForMessage(msg);
      const effectiveFromRaw = pickMessageValue(msg as any, "effective_from", "effectiveFrom");
      const effectiveUntilRaw = pickMessageValue(msg as any, "effective_until", "effectiveUntil");
      const effectiveFrom = effectiveFromRaw ? formatTimestamp(effectiveFromRaw) : null;
      const effectiveUntil = effectiveUntilRaw ? formatTimestamp(effectiveUntilRaw) : null;
      const height = Number((msg as any)?.height) || 0;

      // Calculate expire_soon for the new participant (PENDING state means not active, so null)
      const newParticipantData = {
        role: typeStr,
        op_state: "PENDING",
        effective_from: effectiveFrom,
        effective_until: effectiveUntil,
        revoked: null,
        slashed: null,
        repaid: null,
      };
      const expireSoon = await this.calculateExpireSoon(newParticipantData, new Date(now), height);
      const hasExpireSoonColumn = await this.checkParticipantsColumnExists("expire_soon");

      const msgIdNum = Number((msg as any)?.id ?? (msg as any)?.participant_id ?? (msg as any)?.participantId);
      const hasValidMsgId = Number.isInteger(msgIdNum) && msgIdNum > 0;

      const Entry: any = {
        schema_id: participant?.schema_id,
        role: typeStr,
        did: msg.did,
        corporation_id: corporationId,
        effective_from: effectiveFrom,
        effective_until: effectiveUntil,
        verification_fees: Number(pickMessageValue(msg as any, "verification_fees", "verificationFees") ?? 0),
        validation_fees: Number(pickMessageValue(msg as any, "validation_fees", "validationFees") ?? 0),
        issuance_fees: Number(pickMessageValue(msg as any, "issuance_fees", "issuanceFees") ?? 0),
        deposit: Number(validationTDDenom),
        op_current_deposit: Number(validationTDDenom),
        op_current_fees: Number(validationFeesDenom), 
        validator_participant_id: validatorParticipantId,
        op_state: "PENDING",
        op_last_state_change: now,
        op_validator_deposit: 0, 
        op_summary_digest: null,
        vs_operator: pickMessageValue(msg as any, "vs_operator", "vsOperator") ?? null,
        vs_operator_authz_enabled: pickMessageBool(msg as any, "vs_operator_authz_enabled", "vsOperatorAuthzEnabled", false),
        vs_operator_authz_spend_limit: normalizeDenomAmountArrayForDb(
          pickMessageValue(msg as any, "vs_operator_authz_spend_limit", "vsOperatorAuthzSpendLimit")
        ),
        vs_operator_authz_with_feegrant: pickMessageBool(
          msg as any,
          "vs_operator_authz_with_feegrant",
          "vsOperatorAuthzWithFeegrant",
          false
        ),
        vs_operator_authz_fee_spend_limit: normalizeDenomAmountArrayForDb(
          pickMessageValue(msg as any, "vs_operator_authz_fee_spend_limit", "vsOperatorAuthzFeeSpendLimit")
        ),
        vs_operator_authz_spend_period: pickMessageValue(msg as any, "vs_operator_authz_spend_period", "vsOperatorAuthzSpendPeriod") ?? null,
        modified: now,
        created: now, 
      };

      if (hasValidMsgId) {
        Entry.id = msgIdNum;
      }

      if (hasExpireSoonColumn) {
        Entry.expire_soon = expireSoon;
        this.logger.info(`[handleStartParticipantOP] Adding expire_soon=${expireSoon} to insert data`);
      } else {
        this.logger.warn(`[handleStartParticipantOP] expire_soon column does not exist, skipping. Please run database migrations.`);
      }

      await this.ensureParticipantV4Columns(knex);

      let newParticipant: any = null;
      await knex.transaction(async (trx) => {
        const existing = hasValidMsgId ? await trx("participants").where({ id: msgIdNum }).first() : null;

        if (existing) {
          await trx("participants").where({ id: msgIdNum }).update(Entry);
          newParticipant = await trx("participants").where({ id: msgIdNum }).first();
          return;
        }

        try {
          [newParticipant] = await trx("participants").insert(Entry).returning("*");
        } catch (insertError: any) {
          if (insertError?.code === "42703" && insertError?.message?.includes("expire_soon")) {
            this.logger.warn(
              `[handleStartParticipantOP] expire_soon column error detected, clearing cache and retrying without expire_soon`
            );
            this.participantsColumnExistsCache = null;
            delete Entry.expire_soon;
            [newParticipant] = await trx("participants").insert(Entry).returning("*");
          } else {
            throw insertError;
          }
        }
      });

      if (!newParticipant) {
        this.logger.error(
          "CRITICAL: Failed to create participant via OP start - insert returned no record"
        );

      }

      this.logger.info(
        `Inserted new OP entry handleStartParticipantOP: ${JSON.stringify(
          Entry
        )}`
      );

      try {
        await knex.transaction(async (trx) => {
          await this.updateWeight(trx, Number(newParticipant.id));
          await this.updateParticipants(trx, Number(newParticipant.id));
        });
      } catch (updateErr: any) {
        this.logger.warn(`Failed to update weight/participants for new participant ${newParticipant.id}:`, updateErr?.message || updateErr);
      }

      try {
        await recordParticipantHistory(
          knex,
          newParticipant,
          "START_PARTICIPANT_OP",
          height
        );
      } catch (historyErr: any) {
        this.logger.error(
          "CRITICAL: Failed to record participant history for OP start:",
          historyErr
        );

      }

      await this.refreshEcosystemStatsBySchemaId(newParticipant.schema_id, height);
    } catch (err: any) {
      this.logger.error("CRITICAL: Error in handleStartParticipantOP:", err);
      console.error("FATAL PP START OP ERROR:", err);
      // No structured response expected; handler returns void
    }
  }

  public async computeOpExp(participant: any, knex: any): Promise<string | null> {
    const cs = await knex("credential_schemas")
      .where({ id: participant.schema_id })
      .first();

    if (!cs) {
      const schemaExists = await knex("credential_schemas")
        .where({ id: participant.schema_id })
        .first();

      if (!schemaExists) {
        this.logger.warn(`CredentialSchema ${participant.schema_id} not found for participant ${participant.id} - schema may not exist yet, queuing for retry`);
      } else {
        this.logger.warn(`CredentialSchema ${participant.schema_id} exists but not visible in current transaction for participant ${participant.id} - queuing for retry`);
      }
      return null;
    }

    let validityPeriodField: number | null = null;
    let validityPeriodFieldName = "";

    switch (participant.role) {
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
        this.logger.warn(`Unknown participant type '${participant.role}' for participant ${participant.id} - cannot compute op_exp`);
        return null;
    }

    if (validityPeriodField === null || validityPeriodField === undefined || validityPeriodField === 0) {
      this.logger.info(
        `CredentialSchema ${participant.schema_id} validity period field '${validityPeriodFieldName}' is null/undefined/zero ` +
        `for participant ${participant.id} (type: ${participant.role}) - returning null op_exp per spec`
      );
      return null;
    }

    const validitySeconds = Number(validityPeriodField);
    if (Number.isNaN(validitySeconds)) {
      this.logger.warn(
        `CredentialSchema ${participant.schema_id} has invalid validity period value '${validityPeriodField}' ` +
        `for field '${validityPeriodFieldName}' (participant ${participant.id}, type: ${participant.role})`
      );
      return null;
    }

    const now = new Date();

    let opExp: Date;

    if (!participant.op_exp) {
      opExp = new Date(now.getTime() + validitySeconds * 1000);
    } else {
      opExp = new Date(
        new Date(participant.op_exp).getTime() + validitySeconds * 1000
      );
    }

    return opExp.toISOString();
  }

  private async handleSetParticipantOPToValidated(
    msg: MsgSetParticipantOPToValidated & { height?: number }
  ) {
    try {
      const now = formatTimestamp(msg.timestamp);
      const height = Number((msg as any)?.height) || 0;

      this.logger.info(`[SetOPToValidated] Processing participant id=${msg.id}, height=${height}`);

      const participant = await knex("participants").where({ id: msg.id }).first();
      if (!participant) {
        this.logger.error(`[SetOPToValidated] Participant ${msg.id} not found in database`);
        return { success: false, reason: `Participant ${msg.id} not found` };
      }

      if (participant.op_state !== "PENDING") {
        this.logger.warn(`[SetOPToValidated] Participant ${msg.id} is not PENDING (current state: ${participant.op_state})`);
        return { success: false, reason: `Participant not pending, current state: ${participant.op_state}` };
      }

      const isFirstValidation = !participant.effective_from;

      if (
        msg.validation_fees < 0 ||
        msg.issuance_fees < 0 ||
        msg.verification_fees < 0
      ) {
        this.logger.warn(`Fees must be >= 0`);
        return { success: false, reason: "Invalid fees" };
      }

      const schemaExists = await knex("credential_schemas")
        .where({ id: participant.schema_id })
        .first();

      if (!schemaExists) {
        await this.queueParticipantForRetry(msg, "SCHEMA_NOT_FOUND");
        this.logger.info(`Participant ${msg.id} queued for retry - waiting for CredentialSchema ${participant.schema_id}`);
        return { success: true, message: "Participant queued for retry - schema not ready" };
      }

      const opExp = await this.computeOpExp(participant, knex);
      if (opExp === null) {
        const validityPeriodFieldName =
          participant.role === "ISSUER_GRANTOR" ? "issuer_grantor_validation_validity_period" :
            participant.role === "VERIFIER_GRANTOR" ? "verifier_grantor_validation_validity_period" :
              participant.role === "ISSUER" ? "issuer_validation_validity_period" :
                participant.role === "VERIFIER" ? "verifier_validation_validity_period" :
                  participant.role === "HOLDER" ? "holder_validation_validity_period" : "unknown";
        
        const validityPeriodValue = schemaExists[validityPeriodFieldName];
        if (validityPeriodValue !== undefined && validityPeriodValue !== null) {
          this.logger.info(
            `Participant ${msg.id}: CredentialSchema ${participant.schema_id} has validity period field ` +
            `'${validityPeriodFieldName}' = ${validityPeriodValue} (0 means no expiration per spec). ` +
            `Proceeding with op_exp = null.`
          );
        } else {
          await this.queueParticipantForRetry(msg, "VALIDITY_PERIOD_MISSING");
          this.logger.error(
            `Participant ${msg.id} queued for retry - CredentialSchema ${participant.schema_id} exists but ` +
            `validity period field '${validityPeriodFieldName}' is missing/null for participant type '${participant.role}'. ` +
            `This indicates a data integrity issue. Schema data: ${JSON.stringify({
              id: schemaExists.id,
              issuer_grantor_validation_validity_period: schemaExists.issuer_grantor_validation_validity_period,
              verifier_grantor_validation_validity_period: schemaExists.verifier_grantor_validation_validity_period,
              issuer_validation_validity_period: schemaExists.issuer_validation_validity_period,
              verifier_validation_validity_period: schemaExists.verifier_validation_validity_period,
              holder_validation_validity_period: schemaExists.holder_validation_validity_period,
            })}`
          );
          return { success: true, message: "Participant queued for retry - validity period missing" };
        }
      }

      const effectiveUntil =
        msg.effective_until ?? participant.effective_until ?? opExp ?? null;

      if (
        effectiveUntil &&
        opExp &&
        new Date(effectiveUntil) > new Date(opExp)
      ) {
        this.logger.warn(
          `effective_until ${effectiveUntil} exceeds op_exp ${opExp}`
        );
        return { success: false, reason: "effective_until exceeds op_exp" };
      }

      const currentOpValidatorDeposit = Number(participant.op_validator_deposit || 0);
      const opCurrentDeposit = Number(participant.op_current_deposit || 0);
      const newOpValidatorDeposit = currentOpValidatorDeposit + opCurrentDeposit;

      const updatedParticipantData = {
        ...participant,
        op_state: "VALIDATED",
        effective_until: effectiveUntil,
        effective_from: isFirstValidation ? now : participant.effective_from,
      };
      const expireSoon = await this.calculateExpireSoon(updatedParticipantData, new Date(now), height);
      const hasExpireSoonColumn = await this.checkParticipantsColumnExists("expire_soon");

      const entry: any = {
        op_state: "VALIDATED",
        op_last_state_change: now,
        op_current_fees: 0,
        op_current_deposit: 0,
        op_validator_deposit: Number(newOpValidatorDeposit),
        op_summary_digest: msg.op_summary_digest ?? participant.op_summary_digest ?? null,
        op_exp: opExp,
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
        } else if (participant.issuance_fee_discount != null) {
          entry.issuance_fee_discount = Number(participant.issuance_fee_discount);
        }
        if ((msg as any).verification_fee_discount !== undefined) {
          entry.verification_fee_discount = Number((msg as any).verification_fee_discount ?? 0);
        } else if (participant.verification_fee_discount != null) {
          entry.verification_fee_discount = Number(participant.verification_fee_discount);
        }

        this.logger.info(
          `[SetOPToValidated] First validation: adding op_current_deposit ${opCurrentDeposit} to op_validator_deposit (was ${currentOpValidatorDeposit}, now ${newOpValidatorDeposit}) for participant ${msg.id}`
        );
      } else {
        const feesChanged =
          (msg.validation_fees &&
            msg.validation_fees !== participant.validation_fees) ||
          (msg.issuance_fees && msg.issuance_fees !== participant.issuance_fees) ||
          (msg.verification_fees &&
            msg.verification_fees !== participant.verification_fees);

        if (feesChanged) {
          this.logger.warn("Cannot change fees during renewal");
          return {
            success: false,
            reason: "Cannot change fees on renewal",
          };
        }

        this.logger.info(
          `[SetOPToValidated] Renewal: adding op_current_deposit ${opCurrentDeposit} to op_validator_deposit (was ${currentOpValidatorDeposit}, now ${newOpValidatorDeposit}) for participant ${msg.id}`
        );
      }

      this.logger.info(`[SetOPToValidated] Updating participant ${msg.id} to VALIDATED`);
      const [updated] = await knex("participants")
        .where({ id: msg.id })
        .update(entry)
        .returning("*");

      if (!updated) {
        this.logger.error(
          `[SetOPToValidated] CRITICAL: Failed to update participant ${msg.id} - update returned no record`
        );
        throw new Error(`Failed to update participant ${msg.id}`);
      }

      this.logger.info(`[SetOPToValidated] Participant ${msg.id} updated successfully, op_state=${updated.op_state}`);
      try {
        await knex.transaction(async (trx) => {
          await this.updateParticipants(trx, Number(msg.id));
        });
      } catch (participantsErr: any) {
        this.logger.warn(`Failed to update participants for participant ${msg.id}:`, participantsErr?.message || participantsErr);
      }

      try {
        await recordParticipantHistory(
          knex,
          updated,
          "SET_VALIDATE_PARTICIPANT_OP",
          height,
          participant
        );
      } catch (historyErr: any) {
        this.logger.error(
          "CRITICAL: Failed to record participant history for OP validation:",
          historyErr
        );

      }

      await this.refreshEcosystemStatsBySchemaId(participant.schema_id, height);

      this.logger.info(`Participant ${msg.id} successfully validated`);
      return { success: true };
    } catch (err: any) {
      this.logger.error("CRITICAL: Error in handleSetParticipantOPToValidated:", err);
      console.error("FATAL PP OP VALIDATED ERROR:", err);
      return { success: false, reason: "Internal error validating participant OP" };
    }
  }

  private async handleRenewParticipantOP(msg: MsgRenewParticipantOP) {
    try {
      const now = formatTimestamp(msg.timestamp);
      const applicantParticipant = await knex("participants")
        .where({ id: msg.id })
        .first();
      if (!applicantParticipant) {
        this.logger.warn(`Participant ${msg.id} not found`);
        return { success: false, reason: "Participant not found" };
      }

      const caller = extractController(msg as unknown as Record<string, unknown>);
      const callerCorpId = await resolveCorporationIdByAddress(caller ?? null);
      const applicantCorpId = Number(applicantParticipant.corporation_id ?? 0) || 0;
      if (caller && applicantCorpId > 0 && callerCorpId !== applicantCorpId) {
        this.logger.warn(`Caller ${caller} is not the participant corporation`);
        return { success: false, reason: "Caller is not corporation" };
      }

      const validatorParticipant = await knex("participants")
        .where({ id: applicantParticipant.validator_participant_id })
        .first();
      if (!validatorParticipant) {
        this.logger.warn(
          `Validator participant ${applicantParticipant.validator_participant_id} not found`
        );
        return { success: false, reason: "Validator participant not found" };
      }

      const globalVariables = await getGlobalVariables();
      if (!globalVariables) {
        this.logger.info(
          `Global variables: ${JSON.stringify(globalVariables)}`
        );
      }

      const trustUnitPrice = globalVariables?.ec?.trust_unit_price;
      const trustDepositRate = globalVariables?.td?.trust_deposit_rate;

      if (trustUnitPrice === undefined || trustDepositRate === undefined) {
        this.logger.warn("Global variables not set for fee calculation");
        return { success: false, reason: "Invalid global variables" };
      }

      const validationFeesInDenom =
        Number(validatorParticipant.validation_fees) * trustUnitPrice;
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
        // Calculate expire_soon for renewed participant (PENDING state means not active)
        const renewedParticipantData = {
          ...applicantParticipant,
          op_state: "PENDING",
        };
        const expireSoon = await this.calculateExpireSoon(renewedParticipantData, new Date(now), height);
        const hasExpireSoonColumn = await this.checkParticipantsColumnExists("expire_soon");

        const updateData: any = {
          op_state: "PENDING",
          op_last_state_change: now,
          op_current_fees: Number(validationFeesInDenom),
          op_current_deposit: Number(validationTrustDepositInDenom),
          deposit: Number(applicantParticipant.deposit || 0) + Number(validationTrustDepositInDenom),
          modified: now, 
        };

        if (hasExpireSoonColumn) {
          updateData.expire_soon = expireSoon;
        }

        const [updated] = await trx("participants")
          .where({ id: msg.id })
          .update(updateData)
          .returning("*");

        if (!updated) {
          this.logger.error(
            `CRITICAL: Failed to update participant ${msg.id} for OP renewal - update returned no record`
          );

        }

        try {
          await this.updateWeight(trx, Number(msg.id));
        } catch (weightErr: any) {
          this.logger.warn(`Failed to update weight for participant ${msg.id} after OP renewal:`, weightErr?.message || weightErr);
        }

        try {
          await this.updateParticipants(trx, Number(msg.id));
        } catch (participantsErr: any) {
          this.logger.warn(`Failed to update participants for participant ${msg.id}:`, participantsErr?.message || participantsErr);
        }

        try {
          await recordParticipantHistory(
            trx,
            updated,
            "RENEW_PARTICIPANT_OP",
            height,
            applicantParticipant
          );
        } catch (historyErr: any) {
          this.logger.error(
            "CRITICAL: Failed to record participant history for OP renewal:",
            historyErr
          );

        }
      });

      this.logger.info(`Participant ${msg.id} successfully renewed`);
      return { success: true };
    } catch (err: any) {
      this.logger.error("CRITICAL: Error in handleRenewParticipantOP:", err);
      console.error("FATAL PP RENEW OP ERROR:", err);
      return { success: false, reason: "Internal error renewing participant OP" };
    }
  }

  private async handleCancelParticipantOPLastRequest(
    msg: MsgCancelParticipantOPLastRequest & { height?: number }
  ) {
    try {
      const now = formatTimestamp(msg.timestamp);
      const height = Number((msg as any)?.height) || 0;

      const participant = await knex("participants").where({ id: msg.id }).first();
      if (!participant) {
        this.logger.warn(`Participant ${msg.id} not found`);
        return { success: false, reason: "Participant not found" };
      }

      if (participant.op_state !== "PENDING") {
        this.logger.warn(`Participant ${msg.id} is not PENDING`);
        return { success: false, reason: "Participant not pending" };
      }

      const caller = extractController(msg as unknown as Record<string, unknown>);
      const callerCorpId = await resolveCorporationIdByAddress(caller ?? null);
      const participantCorpId = Number(participant.corporation_id ?? 0) || 0;
      if (caller && participantCorpId > 0 && callerCorpId !== participantCorpId) {
        this.logger.warn(`Creator ${caller} is not participant corporation`);
        return { success: false, reason: "Creator is not corporation" };
      }

      // v4-draft13 removed TERMINATED; treat "no op_exp" as validated-without-exp for legacy rows.
      const newOpState = "VALIDATED";

      const opValidatorDeposit =
        participant.op_validator_deposit;

      // Calculate expire_soon for the updated participant
      const updatedParticipantData = {
        ...participant,
        op_state: newOpState,
      };
      const expireSoon = await this.calculateExpireSoon(updatedParticipantData, new Date(now), height);
      const hasExpireSoonColumn = await this.checkParticipantsColumnExists("expire_soon");

      const updateData: any = {
        op_state: newOpState,
        op_last_state_change: now,
        op_current_fees: 0,
        op_current_deposit: 0,
        op_validator_deposit: Number(opValidatorDeposit),
        modified: now,
      };

      if (hasExpireSoonColumn) {
        updateData.expire_soon = expireSoon;
      }

      const [updated] = await knex("participants")
        .where({ id: msg.id })
        .update(updateData)
        .returning("*");

      if (!updated) {
        this.logger.error(
          `CRITICAL: Failed to update participant ${msg.id} - update returned no record`
        );

      }

      try {
        await knex.transaction(async (trx) => {
          await this.updateParticipants(trx, Number(msg.id));
        });
      } catch (participantsErr: any) {
        this.logger.warn(`Failed to update participants for participant ${msg.id}:`, participantsErr?.message || participantsErr);
      }

      // Record history for the participant update
      try {
        await recordParticipantHistory(
          knex,
          updated,
          "CANCEL_PARTICIPANT_OP",
          height,
          participant
        );
      } catch (historyErr: any) {
        this.logger.error(
          "CRITICAL: Failed to record participant history for OP cancellation:",
          historyErr
        );

      }

      this.logger.info(
        `Participant ${msg.id} validation cancelled. New state: ${newOpState}`
      );

      return { success: true };
    } catch (err: any) {
      this.logger.error("CRITICAL: Error in handleCancelParticipantOPLastRequest:", err);
      console.error("FATAL PP CANCEL OP ERROR:", err);
      return { success: false, reason: "Internal error cancelling participant OP request" };
    }
  }

  private async handleSlashParticipantTrustDeposit(
    msg: MsgSlashParticipantTrustDeposit & { height?: number }
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
        return { success: false, reason: "Invalid participant ID" };
      }

      const amountNum = Number(slashAmount);
      if (Number.isNaN(amountNum) || amountNum <= 0) {
        this.logger.warn(`Invalid amount: ${slashAmount}`);
        return { success: false, reason: "Invalid amount" };
      }

      const participant = await knex("participants").where({ id: msg.id }).first();
      if (!participant) {
        this.logger.warn(`Participant ${msg.id} not found`);
        return { success: false, reason: "Participant not found" };
      }

      const deposit = Number(participant.deposit || 0);
      if (amountNum > deposit) {
        this.logger.warn(
          `Slash amount ${amountNum} exceeds deposit ${deposit}`
        );
        return { success: false, reason: "Amount exceeds deposit" };
      }

      let isAuthorized = false;
      const caller = extractController(msg as unknown as Record<string, unknown>);
      const callerCorpId = await resolveCorporationIdByAddress(caller ?? null);

      let validatorParticipant = participant;
      while (callerCorpId !== null && validatorParticipant && validatorParticipant.validator_participant_id) {
        validatorParticipant = await knex("participants")
          .where({ id: validatorParticipant.validator_participant_id })
          .first();
        if (validatorParticipant && Number(validatorParticipant.corporation_id ?? 0) === callerCorpId) {
          isAuthorized = true;
          break;
        }
      }

      if (!isAuthorized && callerCorpId !== null && participant.schema_id) {
        const schema = await knex("credential_schemas")
          .where({ id: participant.schema_id })
          .first();
        if (schema && schema.ecosystem_id) {
          const ec = await knex("ecosystem")
            .where({ id: schema.ecosystem_id })
            .first();
          if (ec && Number(ec.corporation_id ?? 0) === callerCorpId) {
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
      const prevSlashed = Number(participant.slashed_deposit || 0);

      // Determine if this is ecosystem or network slash
      const isEcosystemParticipant = participant.role === "ECOSYSTEM";
      let isEcosystemSlash = false;
      const isNetworkSlash = false;
      let trController: string | null = null;
      let classificationReason = '';

      if (isEcosystemParticipant) {
        isEcosystemSlash = true;
        classificationReason = 'ECOSYSTEM participant type';
        this.logger.info(`[Slash] Participant ${msg.id} is ECOSYSTEM type - marking as ecosystem slash`);
      } else if (participant.schema_id) {
        const schema = await knex("credential_schemas")
          .where({ id: participant.schema_id })
          .first();

        if (!schema) {
          this.logger.warn(`[Slash] Participant ${msg.id} has schema_id ${participant.schema_id} but schema not found in database`);
          classificationReason = `Schema ${participant.schema_id} not found`;
        } else if (!schema.ecosystem_id) {
          this.logger.warn(`[Slash] Participant ${msg.id} schema ${participant.schema_id} has no ecosystem_id`);
          classificationReason = `Schema ${participant.schema_id} has no ecosystem_id`;
        } else {
          const ec = await knex("ecosystem")
            .where({ id: schema.ecosystem_id })
            .first();

          if (!ec) {
            this.logger.warn(`[Slash] Participant ${msg.id} schema ${participant.schema_id} references EC ${schema.ecosystem_id} but EC not found in database`);
            classificationReason = `EC ${schema.ecosystem_id} not found`;
            trController = null;
          } else {
            const ecCorpId = Number(ec.corporation_id ?? 0) || 0;
            trController = ecCorpId > 0 ? String(ecCorpId) : null;
            if (!ecCorpId) {
              this.logger.warn(`[Slash] Participant ${msg.id} EC ${schema.ecosystem_id} exists but has no corporation_id`);
              classificationReason = `EC ${schema.ecosystem_id} has no corporation_id`;
            } else if (callerCorpId !== null && ecCorpId === callerCorpId) {
              isEcosystemSlash = true;
              classificationReason = `Slashed by EC corporation_id ${ecCorpId}`;
              this.logger.info(`[Slash] Participant ${msg.id} slashed by EC corporation_id ${ecCorpId} - marking as ecosystem slash`);
            } else {
              this.logger.warn(`[Slash] Participant ${msg.id} slashed by ${caller} (corporation_id ${callerCorpId ?? "none"}) but EC corporation_id is ${ecCorpId} - no slash type determined`);
              classificationReason = `Caller corporation_id ${callerCorpId ?? "none"} != EC corporation_id ${ecCorpId}`;
            }
          }
        }
      } else {
        this.logger.warn(`[Slash] Participant ${msg.id} has no schema_id - no slash type determined`);
        classificationReason = 'No schema_id';
      }

      if (!isEcosystemSlash && !isNetworkSlash) {
        this.logger.error(`[Slash] CRITICAL: Could not classify slash for participant ${msg.id}. Classification reason: ${classificationReason}. Schema ID: ${participant.schema_id || 'N/A'}, Type: ${participant.role}, Caller: ${caller}, EC Controller: ${trController || 'N/A'}`);
      }

      await knex.transaction(async (trx) => {
        const currentDeposit = BigInt(participant.deposit || "0");
        const newDeposit = currentDeposit > BigInt(amountNum)
          ? currentDeposit - BigInt(amountNum)
          : BigInt(0);

        // Calculate expire_soon for slashed participant (slashed without repaid = inactive)
        const slashedParticipantData = {
          ...participant,
          slashed: now,
          repaid: null, // Not repaid yet
        };
        const expireSoon = await this.calculateExpireSoon(slashedParticipantData, new Date(now), height);
        const hasExpireSoonColumn = await this.checkParticipantsColumnExists("expire_soon");

        const updateData: any = {
          slashed: now,
          slashed_deposit: Number(prevSlashed + amountNum),
          deposit: Number(newDeposit),
          modified: now,
        };

        if (hasExpireSoonColumn) {
          updateData.expire_soon = expireSoon;
        }

        const [updated] = await trx("participants")
          .where({ id: msg.id })
          .update(updateData)
          .returning("*");

        if (!updated) {
          this.logger.error(
            `CRITICAL: Failed to slash participant ${msg.id} - update returned no record`
          );

        }

        // Update slash statistics for this participant and ancestors
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
            this.logger.info(`[Slash] Updated slash statistics for participant ${msg.id} - ecosystem: ${isEcosystemSlash}, network: ${isNetworkSlash}, amount: ${amountNum}`);
          } else {
            this.logger.warn(`[Slash] Skipping slash statistics update for participant ${msg.id} - neither ecosystem nor network slash detected`);
          }
        } catch (statsErr: any) {
          this.logger.warn(`Failed to update slash statistics: ${statsErr?.message || statsErr}`);
        }

        try {
          await this.updateWeight(trx, Number(msg.id));
        } catch (weightErr: any) {
          this.logger.warn(`Failed to update weight for participant ${Number(msg.id)}:`, weightErr?.message || weightErr);
        }

        try {
          await this.updateParticipants(trx, Number(msg.id));
        } catch (participantsErr: any) {
          this.logger.warn(`Failed to update participants for participant ${msg.id}:`, participantsErr?.message || participantsErr);
        }

        try {
          await recordParticipantHistory(
            trx,
            updated,
            "SLASH_PARTICIPANT_TRUST_DEPOSIT",
            height,
            participant
          );
        } catch (historyErr: any) {
          this.logger.error(
            "CRITICAL: Failed to record participant history for slash:",
            historyErr
          );
          process.exit(1);
        }
      });

      try {
        const slashed = await knex("participants").where({ id: msg.id }).first();
        // TrustDeposit is still address-based: resolve the owner's policy address.
        const corporation = await resolveAddressByCorporationId(
          Number(slashed?.corporation_id ?? 0) || 0
        );
        if (corporation) {
          await (this as any).broker.call(
            `${SERVICE.V1.TrustDepositDatabaseService.path}.slash_participant_trust_deposit`,
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
        const slashedParticipant = await knex("participants").where({ id: msg.id }).first();
        if (slashedParticipant?.schema_id) {
          const schemaId = Number(slashedParticipant.schema_id);
          const csStats = await calculateCredentialSchemaStats(schemaId);
          const slashFromParticipants = await this.sumSlashStatsFromParticipantsForSchema(schemaId);
          const mergedStats = { ...csStats, ...slashFromParticipants };
          const csUpdate = statsToUpdateObject(mergedStats as unknown as Record<string, unknown>, CS_STATS_FIELDS);
          const updatedCount = await knex("credential_schemas").where("id", schemaId).update(csUpdate);
          if (updatedCount === 0) {
            this.logger.warn(`[Slash] credential_schemas update affected 0 rows for schema_id=${schemaId}`);
          }
          const height = Number((msg as any)?.height) || 0;
          await this.refreshEcosystemStatsBySchemaId(schemaId, height);
          const csStatsForHistory = { ...csStats, ...slashFromParticipants };
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
          `Failed to update CS/EC statistics after slash: ${statsErr?.message ?? String(statsErr)}${code ? ` [code=${code}]` : ""}`
        );
      }

      this.logger.info(
        `✅ Participant ${msg.id} slashed by ${caller} amount ${amountNum}`
      );

      return { success: true };
    } catch (err: any) {
      this.logger.error("CRITICAL: Error in handleSlashParticipantTrustDeposit:", err);
      console.error("FATAL PP SLASH ERROR:", err);
      return { success: false, reason: "Internal error slashing participant trust deposit" };
    }
  }

  private async handleRepayParticipantSlashedTrustDeposit(
    msg: MsgRepayParticipantSlashedTrustDeposit & { height?: number }
  ) {
    try {
      if (!msg.id) {
        this.logger.warn("Missing mandatory parameter: id");
        return { success: false, reason: "Missing mandatory parameter" };
      }

      const idNum = Number(msg.id);
      if (!Number.isInteger(idNum) || idNum <= 0) {
        this.logger.warn(`Invalid id: ${msg.id}. Must be a valid uint64.`);
        return { success: false, reason: "Invalid participant ID" };
      }

      const participant = await knex("participants").where({ id: msg.id }).first();
      if (!participant) {
        this.logger.warn(`Participant ${msg.id} not found`);
        return { success: false, reason: "Participant not found" };
      }

      const slashedDeposit = Number(participant.slashed_deposit || 0);
      if (slashedDeposit <= 0) {
        this.logger.warn(
          `Participant ${msg.id} has no slashed deposit to repay`
        );
        return { success: false, reason: "No slashed deposit to repay" };
      }

      const now = formatTimestamp(msg.timestamp);
      const height = Number((msg as any)?.height) || 0;

      // Determine if this was ecosystem or network slash based on previous slash
      const isEcosystemParticipant = participant.role === "ECOSYSTEM";
      let isEcosystemSlash = false;
      const isNetworkSlash = false;
      let trController: string | null = null;
      let classificationReason = '';

      const repayer = extractController(msg as unknown as Record<string, unknown>);
      const repayerCorpId = await resolveCorporationIdByAddress(repayer ?? null);

      if (isEcosystemParticipant) {
        isEcosystemSlash = true;
        classificationReason = "ECOSYSTEM participant type";
      } else if (participant.schema_id) {
        const schema = await knex("credential_schemas")
          .where({ id: participant.schema_id })
          .first();

        if (!schema) {
          this.logger.warn(`[Repay] Participant ${msg.id} has schema_id ${participant.schema_id} but schema not found`);
          classificationReason = `Schema ${participant.schema_id} not found`;
        } else if (!schema.ecosystem_id) {
          this.logger.warn(`[Repay] Participant ${msg.id} schema ${participant.schema_id} has no ecosystem_id`);
          classificationReason = `Schema ${participant.schema_id} has no ecosystem_id`;
        } else {
          const ec = await knex("ecosystem")
            .where({ id: schema.ecosystem_id })
            .first();

          if (!ec) {
            this.logger.warn(`[Repay] Participant ${msg.id} EC ${schema.ecosystem_id} not found`);
            classificationReason = `EC ${schema.ecosystem_id} not found`;
            trController = null;
          } else {
            const ecCorpId = Number(ec.corporation_id ?? 0) || 0;
            trController = ecCorpId > 0 ? String(ecCorpId) : null;
            if (!ecCorpId) {
              this.logger.warn(`[Repay] Participant ${msg.id} EC ${schema.ecosystem_id} has no corporation_id`);
              classificationReason = `EC ${schema.ecosystem_id} has no corporation_id`;
            } else if (repayerCorpId !== null && ecCorpId === repayerCorpId) {
              isEcosystemSlash = true;
              classificationReason = `Repay by EC corporation_id ${ecCorpId}`;
            } else {
              classificationReason = `Repayer corporation_id ${repayerCorpId ?? "none"} != EC corporation_id ${ecCorpId}`;
            }
          }
        }
      } else {
        classificationReason = "No schema_id";
      }

      if (!isEcosystemSlash && !isNetworkSlash) {
        this.logger.error(
          `[Repay] CRITICAL: Could not classify repay for participant ${msg.id}. Classification reason: ${classificationReason}. Schema ID: ${participant.schema_id || "N/A"}, Type: ${participant.role}, EC corporation: ${trController || "N/A"}`
        );
      }

      await knex.transaction(async (trx) => {
        requireController(msg, `PP REPAY_SLASHED ${msg.id}`);
        const currentDeposit = BigInt(participant.deposit || "0");
        const requestedRepayAmount = (msg as any).amount ?? (msg as any).deposit;
        const repaidAmount = requestedRepayAmount == null
          ? BigInt(slashedDeposit)
          : BigInt(String(requestedRepayAmount));
        const newDeposit = currentDeposit + repaidAmount;

        // Calculate expire_soon for repaid participant (may become active again)
        const repaidParticipantData = {
          ...participant,
          repaid: now,
          slashed: participant.slashed, // Keep original slashed timestamp
        };
        const expireSoon = await this.calculateExpireSoon(repaidParticipantData, new Date(now), height);
        const hasExpireSoonColumn = await this.checkParticipantsColumnExists("expire_soon");

        const updateData: any = {
          repaid: now,
          repaid_deposit: Number(repaidAmount),
          deposit: Number(newDeposit),
          modified: now,
        };

        if (hasExpireSoonColumn) {
          updateData.expire_soon = expireSoon;
        }

        const [updated] = await trx("participants")
          .where({ id: msg.id })
          .update(updateData)
          .returning("*");

        if (!updated) {
          this.logger.error(
            `CRITICAL: Failed to repay participant ${msg.id} - update returned no record`
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
          this.logger.warn(`Failed to update weight for participant ${msg.id} after repay:`, weightErr?.message || weightErr);
        }

        try {
          await this.updateParticipants(trx, Number(msg.id));
        } catch (participantsErr: any) {
          this.logger.warn(`Failed to update participants for participant ${msg.id}:`, participantsErr?.message || participantsErr);
        }

        try {
          await recordParticipantHistory(
            trx,
            updated,
            "REPAY_PARTICIPANT_SLASHED_TRUST_DEPOSIT",
            height,
            participant
          );
        } catch (historyErr: any) {
          this.logger.error(
            "CRITICAL: Failed to record participant history for repay:",
            historyErr
          );

        }
      });

      try {
        const repaidParticipant = await knex("participants").where({ id: msg.id }).first();
        if (repaidParticipant?.schema_id) {
          const schemaId = Number(repaidParticipant.schema_id);
          const csStats = await calculateCredentialSchemaStats(schemaId);
          const slashFromParticipants = await this.sumSlashStatsFromParticipantsForSchema(schemaId);
          const mergedStats = { ...csStats, ...slashFromParticipants };
          const csUpdate = statsToUpdateObject(mergedStats as unknown as Record<string, unknown>, CS_STATS_FIELDS);
          const updatedCount = await knex("credential_schemas").where("id", schemaId).update(csUpdate);
          if (updatedCount === 0) {
            this.logger.warn(`[Repay] credential_schemas update affected 0 rows for schema_id=${schemaId}`);
          }
          const height = Number((msg as any)?.height) || 0;
          await this.refreshEcosystemStatsBySchemaId(schemaId, height);
          const csStatsForHistory = { ...csStats, ...slashFromParticipants };
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
          `Failed to update CS/EC statistics after repay: ${statsErr?.message ?? String(statsErr)}${code ? ` [code=${code}]` : ""}`
        );
      }

      this.logger.info(
        `✅ Participant ${msg.id} slashed deposit (${slashedDeposit}) repaid by ${repayer}`
      );

      return { success: true };
    } catch (err: any) {
      this.logger.error(
        "CRITICAL: Error in handleRepayParticipantSlashedTrustDeposit:",
        err
      );
      console.error("FATAL PP REPAY ERROR:", err);
      return { success: false, reason: "Internal error repaying participant slashed trust deposit" };
    }
  }

  private participantsColumnExistsCache: Record<string, boolean> | null = null;

  private async checkParticipantsColumnExists(columnName: string): Promise<boolean> {
    if (this.participantsColumnExistsCache === null) {
      this.participantsColumnExistsCache = {};
    }

    if (this.participantsColumnExistsCache[columnName] !== undefined) {
      return this.participantsColumnExistsCache[columnName];
    }

    try {
      const exists = await knex.schema.hasColumn("participants", columnName);
      this.participantsColumnExistsCache[columnName] = exists;
      if (columnName === "expire_soon") {
        this.logger.info(`[checkParticipantsColumnExists] expire_soon column exists: ${exists}`);
      }
      return exists;
    } catch (error: any) {
      this.logger.warn(`[checkParticipantsColumnExists] Error checking column ${columnName}:`, error?.message || error);
      this.participantsColumnExistsCache[columnName] = false;
      return false;
    }
  }

  private async incrementParticipantStatistics(
    trx: any,
    participantId: number,
    incrementIssued: boolean,
    incrementVerified: boolean
  ): Promise<void> {
    const hasIssuedColumn = await this.checkParticipantsColumnExists("issued");
    const hasVerifiedColumn = await this.checkParticipantsColumnExists("verified");

    if (!hasIssuedColumn && !hasVerifiedColumn) {
      this.logger.warn(`[incrementParticipantStatistics] Neither issued nor verified column exists for participant ${participantId}`);
      return;
    }

    if (incrementIssued && !hasIssuedColumn) {
      this.logger.warn(`[incrementParticipantStatistics] Attempted to increment issued for participant ${participantId} but issued column does not exist`);
    }

    if (incrementVerified && !hasVerifiedColumn) {
      this.logger.warn(`[incrementParticipantStatistics] Attempted to increment verified for participant ${participantId} but verified column does not exist`);
    }

    const initialParticipant: { schema_id: number; validator_participant_id: number | null } | undefined = await trx("participants").where({ id: participantId }).select('schema_id', 'validator_participant_id').first();
    if (!initialParticipant) return;

    const schemaId = initialParticipant.schema_id;
    let currentParticipantId: number | null = participantId;

    while (currentParticipantId) {
      const participant: { schema_id: number; validator_participant_id: number | null } | undefined = await trx("participants").where({ id: currentParticipantId }).select('schema_id', 'validator_participant_id').first();
      if (!participant) break;
      if (participant.schema_id !== schemaId) {
        this.logger.warn(`Participant tree traversal crossed schema boundary. participantId=${currentParticipantId}, expected schema=${schemaId}, found schema=${participant.schema_id}. Stopping traversal.`);
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
          await trx("participants")
            .where({ id: currentParticipantId })
            .update(updates);
        } catch (error: any) {
          if (error?.nativeError?.code === '42703') {
            this.participantsColumnExistsCache = null;
            return;
          }
          throw error;
        }
      }

      currentParticipantId = participant.validator_participant_id;
    }
  }


  private async sumSlashStatsFromParticipantsForSchema(schemaId: number): Promise<{
    ecosystem_slash_events: number;
    ecosystem_slashed_amount: number;
    ecosystem_slashed_amount_repaid: number;
    network_slash_events: number;
    network_slashed_amount: number;
    network_slashed_amount_repaid: number;
  }> {
    const hasCol = await this.checkParticipantsColumnExists("ecosystem_slash_events");
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
    const rows = await knex("participants")
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
    participantId: number,
    isEcosystemSlash: boolean,
    isNetworkSlash: boolean,
    slashAmount: number,
    repayAmount: number | null
  ): Promise<void> {
    const hasEcosystemSlashEventsColumn = await this.checkParticipantsColumnExists("ecosystem_slash_events");
    if (!hasEcosystemSlashEventsColumn) {
      this.logger.warn(`[updateSlashStatistics] Column ecosystem_slash_events does not exist, skipping update for participant ${participantId}`);
      return;
    }

    if (!isEcosystemSlash && !isNetworkSlash) {
      this.logger.warn(`[updateSlashStatistics] Neither ecosystem nor network slash flag is set for participant ${participantId}, skipping update`);
      return;
    }

    const participantExists = await trx("participants").where({ id: participantId }).first();
    if (!participantExists) {
      this.logger.warn(`[updateSlashStatistics] Participant ${participantId} not found, skipping update`);
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
        const result = await trx("participants")
          .where({ id: participantId })
          .update(updates);
        this.logger.debug(
          `[updateSlashStatistics] Updated participant ${participantId} with ${Object.keys(updates).length} fields, rows affected: ${result}`
        );
      } catch (error: any) {
        if (error?.nativeError?.code === '42703') {
          this.logger.warn(
            `[updateSlashStatistics] Column does not exist, clearing cache for participant ${participantId}`
          );
          this.participantsColumnExistsCache = null;
          return;
        }
        throw error;
      }
    } else {
      this.logger.warn(
        `[updateSlashStatistics] No updates to apply for participant ${participantId} - isEcosystemSlash: ${isEcosystemSlash}, isNetworkSlash: ${isNetworkSlash}, slashAmount: ${slashAmount}, repayAmount: ${repayAmount}`
      );
    }
  }


  private async updateWeight(trx: any, participantId: number): Promise<void> {
    const hasWeightColumn = await this.checkParticipantsColumnExists("weight");
    if (!hasWeightColumn) {
      return;
    }

    const initialParticipant: { schema_id: number; validator_participant_id: number | null; deposit: number } | undefined =
      await trx("participants").where({ id: participantId }).select('schema_id', 'validator_participant_id', 'deposit').first();
    if (!initialParticipant) return;

    const schemaId = initialParticipant.schema_id;
    let currentParticipantId: number | null = participantId;

    const participantStack: number[] = [];
    while (currentParticipantId) {
      participantStack.push(currentParticipantId);
      const participant: { schema_id: number; validator_participant_id: number | null } | undefined =
        await trx("participants").where({ id: currentParticipantId }).select('schema_id', 'validator_participant_id').first();
      if (!participant) break;
      if (participant.schema_id !== schemaId) {
        this.logger.warn(`Participant tree traversal crossed schema boundary. participantId=${currentParticipantId}, expected schema=${schemaId}, found schema=${participant.schema_id}. Stopping traversal.`);
        break;
      }
      currentParticipantId = participant.validator_participant_id;
    }

    for (let i = participantStack.length - 1; i >= 0; i--) {
      const pid = participantStack[i];
      const participant = await trx("participants").where({ id: pid }).select('deposit', 'schema_id').first();
      if (!participant) continue;

      const children = await trx("participants")
        .where("validator_participant_id", pid)
        .where("schema_id", participant.schema_id)
        .select("weight");

      let childWeightSum = BigInt(0);
      for (const child of children) {
        const childWeight = child.weight ? BigInt(child.weight) : BigInt(0);
        childWeightSum += childWeight;
      }

      const ownDeposit = BigInt(participant.deposit || "0");
      const totalWeight = ownDeposit + childWeightSum;

      try {
        await trx("participants")
          .where({ id: pid })
          .update({ weight: String(totalWeight) });
      } catch (error: any) {
        if (error?.nativeError?.code === '42703') {
          this.participantsColumnExistsCache = null;
          return;
        }
        throw error;
      }
    }
  }

  private async updateParticipants(trx: any, participantId: number): Promise<void> {
    const hasParticipantsColumn = await this.checkParticipantsColumnExists("participants");
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
      roleColumns.map(async (col) => [col, await this.checkParticipantsColumnExists(col)] as const)
    );
    const roleColumnsAvailability = new Map<string, boolean>(roleColumnsAvailabilityEntries);
    const availableRoleColumns = roleColumns.filter((col) => roleColumnsAvailability.get(col));

    const initialParticipant: { schema_id: number; validator_participant_id: number | null } | undefined =
      await trx("participants").where({ id: participantId }).select('schema_id', 'validator_participant_id').first();
    if (!initialParticipant) return;

    const schemaId = initialParticipant.schema_id;
    let currentParticipantId: number | null = participantId;
    const participantStack: number[] = [];

    while (currentParticipantId) {
      participantStack.push(currentParticipantId);
      const participant: { schema_id: number; validator_participant_id: number | null } | undefined =
        await trx("participants").where({ id: currentParticipantId }).select('schema_id', 'validator_participant_id').first();
      if (!participant) break;
      if (participant.schema_id !== schemaId) {
        this.logger.warn(`Participant tree traversal crossed schema boundary. participantId=${currentParticipantId}, expected schema=${schemaId}, found schema=${participant.schema_id}. Stopping traversal.`);
        break;
      }
      currentParticipantId = participant.validator_participant_id;
    }

    const now = new Date();

    for (let i = participantStack.length - 1; i >= 0; i--) {
      const pid = participantStack[i];
      const participant = await trx("participants").where({ id: pid }).select(
        "repaid",
        "slashed",
        "revoked",
        "effective_from",
        "effective_until",
        "role",
        "op_state",
        "op_exp",
        "validator_participant_id",
        "schema_id"
      ).first();
      if (!participant) continue;

      const participantState = calculateParticipantState(
        {
          repaid: participant.repaid,
          slashed: participant.slashed,
          revoked: participant.revoked,
          effective_from: participant.effective_from,
          effective_until: participant.effective_until,
          role: participant.role,
          op_state: participant.op_state,
          op_exp: participant.op_exp,
          validator_participant_id: participant.validator_participant_id,
        },
        now
      );

      let count = participantState === "ACTIVE" ? 1 : 0;
      const roleTotals: Record<string, number> = {
        participants_ecosystem: 0,
        participants_issuer_grantor: 0,
        participants_issuer: 0,
        participants_verifier_grantor: 0,
        participants_verifier: 0,
        participants_holder: 0,
      };
      const participantType = normalizeParticipantType(participant.role);
      if (participantState === "ACTIVE") {
        if (participantType === "ECOSYSTEM") roleTotals.participants_ecosystem += 1;
        if (participantType === "ISSUER_GRANTOR") roleTotals.participants_issuer_grantor += 1;
        if (participantType === "ISSUER") roleTotals.participants_issuer += 1;
        if (participantType === "VERIFIER_GRANTOR") roleTotals.participants_verifier_grantor += 1;
        if (participantType === "VERIFIER") roleTotals.participants_verifier += 1;
        if (participantType === "HOLDER") roleTotals.participants_holder += 1;
      }

      const childSelectColumns: string[] = [
        "repaid",
        "slashed",
        "revoked",
        "effective_from",
        "effective_until",
        "role",
        "op_state",
        "op_exp",
        "validator_participant_id",
        "participants",
      ];
      for (const roleCol of availableRoleColumns) {
        childSelectColumns.push(roleCol);
      }
      const children = await trx("participants")
        .where("validator_participant_id", pid)
        .where("schema_id", participant.schema_id)
        .select(childSelectColumns);

      for (const child of children) {
        const childState = calculateParticipantState(
          {
            repaid: child.repaid,
            slashed: child.slashed,
            revoked: child.revoked,
            effective_from: child.effective_from,
            effective_until: child.effective_until,
            role: child.role,
            op_state: child.op_state,
            op_exp: child.op_exp,
            validator_participant_id: child.validator_participant_id,
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
        await trx("participants")
          .where({ id: pid })
          .update(updates);
      } catch (error: any) {
        if (error?.nativeError?.code === '42703') {
          this.participantsColumnExistsCache = null;
          return;
        }
        throw error;
      }
    }
  }

  private async handleCreateOrUpdateParticipantSession(
    msg: MsgCreateOrUpdateParticipantSession & { height?: number }
  ) {
    const trx = await knex.transaction();
    try {
      const now = formatTimestamp(msg.timestamp);
      const height = Number((msg as any)?.height) || 0;

      const agentParticipantId = (msg as any).agentParticipantId ?? (msg as any).agent_participant_id;
      const walletAgentParticipantId = (msg as any).walletAgentParticipantId ?? (msg as any).wallet_agent_participant_id;
      const issuerParticipantId = (msg as any).issuerParticipantId ?? (msg as any).issuer_participant_id;
      const verifierParticipantId = (msg as any).verifierParticipantId ?? (msg as any).verifier_participant_id;

      if (!msg.id || !agentParticipantId || !walletAgentParticipantId) {
        throw new Error("Missing mandatory parameters");
      }
      if (!issuerParticipantId && !verifierParticipantId) {
        throw new Error(
          "At least one of issuer_participant_id or verifier_participant_id must be provided"
        );
      }

      const [agentParticipant, walletAgentParticipant, issuerParticipant, verifierParticipant] =
        await Promise.all([
          trx("participants").where({ id: agentParticipantId }).first(),
          trx("participants").where({ id: walletAgentParticipantId }).first(),
          issuerParticipantId
            ? trx("participants").where({ id: issuerParticipantId }).first()
            : null,
          verifierParticipantId
            ? trx("participants").where({ id: verifierParticipantId }).first()
            : null,
        ]);

      if (!agentParticipant) {
        this.logger.warn(
          `Agent participant not found for session ${msg.id}. agentParticipantId=${agentParticipantId}. Session will be saved but statistics may be incomplete.`
        );
      }
      if (!walletAgentParticipant) {
        this.logger.warn(
          `Wallet Agent participant not found for session ${msg.id}. walletAgentParticipantId=${walletAgentParticipantId}. Session will be saved but statistics may be incomplete.`
        );
      }
      if (issuerParticipantId && issuerParticipant && issuerParticipant.role !== "ISSUER") {
        this.logger.warn(
          `Invalid issuer participant type for session ${msg.id}. Expected ISSUER, got ${issuerParticipant.role}. issuerParticipantId=${issuerParticipantId}. Session will be saved.`
        );
      }
      if (verifierParticipantId && verifierParticipant && verifierParticipant.role !== "VERIFIER") {
        this.logger.warn(
          `Invalid verifier participant type for session ${msg.id}. Expected VERIFIER, got ${verifierParticipant.role}. verifierParticipantId=${verifierParticipantId}. Session will be saved.`
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

      const existing = await trx("participant_sessions")
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
        issuer_participant_id: issuerParticipantId || null,
        verifier_participant_id: verifierParticipantId || null,
        wallet_agent_participant_id: walletAgentParticipantId,
      };

      const vsOp =
        (msg as any).vs_operator ?? (msg as any).vsOperator ?? (msg as any).operator ?? existing?.vs_operator ?? null;

      if (!existing) {
        const corporationId = await resolveCorporationIdForMessage(msg);
        const [session] = await trx("participant_sessions")
          .insert({
            id: msg.id,
            corporation_id: corporationId,
            vs_operator: vsOp,
            agent_participant_id: agentParticipantId,
            wallet_agent_participant_id: walletAgentParticipantId,
            session_records: JSON.stringify([recordEntry]),
            created: now,
            modified: now,
          })
          .returning("*");

        const normalizedSession =
          typeof session.session_records === "string"
            ? { ...session, session_records: parseJson(session.session_records) }
            : session;

        await recordParticipantSessionHistory(
          trx,
          normalizedSession,
          "CREATE_PARTICIPANT_SESSION",
          height
        );

        if (issuerParticipantId) {
          try {
            await this.incrementParticipantStatistics(trx, Number(issuerParticipantId), true, false);
            try {
              await recordParticipantHistory(trx, { id: Number(issuerParticipantId) }, "CREDENTIAL_ISSUED", height);
            } catch (historyErr: any) {
              this.logger.warn(`[Session] Failed to record participant history for issued participant ${issuerParticipantId}:`, historyErr?.message || historyErr);
            }
          } catch (issuedErr: any) {
            this.logger.error(`[Session] Failed to increment issued for participant ${issuerParticipantId}:`, issuedErr?.message || issuedErr);
          }
        }
        if (verifierParticipantId) {
          try {
            await this.incrementParticipantStatistics(trx, Number(verifierParticipantId), false, true);
            try {
              await recordParticipantHistory(trx, { id: Number(verifierParticipantId) }, "CREDENTIAL_VERIFIED", height);
            } catch (historyErr: any) {
              this.logger.warn(`[Session] Failed to record participant history for verified participant ${verifierParticipantId}:`, historyErr?.message || historyErr);
            }
          } catch (verifiedErr: any) {
            this.logger.error(`[Session] Failed to increment verified for participant ${verifierParticipantId}:`, verifiedErr?.message || verifiedErr);
          }
        }
      } else {
        const existingRecords = parseSessionRecordsLocal(existing);
        existingRecords.push(recordEntry);

        const [session] = await trx("participant_sessions")
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

        await recordParticipantSessionHistory(
          trx,
          normalizedSession,
          "UPDATE_PARTICIPANT_SESSION",
          height,
          previousSession
        );

        const previousRecords = previousSession?.session_records || [];
        const previousIssuerParticipantIds = new Set(
          previousRecords.map((entry: { issuer_participant_id?: string | number }) => entry.issuer_participant_id).filter(Boolean)
        );
        const previousVerifierParticipantIds = new Set(
          previousRecords.map((entry: { verifier_participant_id?: string | number }) => entry.verifier_participant_id).filter(Boolean)
        );

        const newIssuerParticipantIds = new Set(
          existingRecords.map((entry: { issuer_participant_id?: string | number }) => entry.issuer_participant_id).filter(Boolean)
        );
        const newVerifierParticipantIds = new Set(
          existingRecords.map((entry: { verifier_participant_id?: string | number }) => entry.verifier_participant_id).filter(Boolean)
        );

        for (const issuerId of newIssuerParticipantIds) {
          if (!previousIssuerParticipantIds.has(issuerId)) {
            try {
              await this.incrementParticipantStatistics(trx, Number(issuerId), true, false);
              try {
                await recordParticipantHistory(trx, { id: Number(issuerId) }, "CREDENTIAL_ISSUED", height);
              } catch (historyErr: any) {
                this.logger.warn(`[Session] Failed to record participant history for issued participant ${issuerId}:`, historyErr?.message || historyErr);
              }
            } catch (issuedErr: any) {
              this.logger.error(`[Session] Failed to increment issued for participant ${issuerId}:`, issuedErr?.message || issuedErr);
            }
          }
        }
        for (const verifierId of newVerifierParticipantIds) {
          if (!previousVerifierParticipantIds.has(verifierId)) {
            try {
              await this.incrementParticipantStatistics(trx, Number(verifierId), false, true);
              try {
                await recordParticipantHistory(trx, { id: Number(verifierId) }, "CREDENTIAL_VERIFIED", height);
              } catch (historyErr: any) {
                this.logger.warn(`[Session] Failed to record participant history for verified participant ${verifierId}:`, historyErr?.message || historyErr);
              }
            } catch (verifiedErr: any) {
              this.logger.error(`[Session] Failed to increment verified for participant ${verifierId}:`, verifiedErr?.message || verifiedErr);
            }
          }
        }
      }

      await trx.commit();
      return { success: true };
    } catch (err) {
      await trx.rollback();
      this.logger.error("Error in handleCreateOrUpdateParticipantSession:", err);
      return { success: false, reason: String(err) };

    }
  }

  private async queueParticipantForRetry(message: unknown, reason: string): Promise<void> {
    const msg = message as { id: string | number; type?: string };
    const key = `${msg.id}_${msg.type || 'participant'}`;
    const queuedParticipant: QueuedParticipant = {
      message: msg,
      reason,
      retryCount: 0,
      queuedAt: new Date(),
      nextRetryAt: new Date(Date.now() + this.RETRY_INTERVAL_MS)
    };

    this.retryQueue.set(key, queuedParticipant);
    this.logger.info(`Queued participant ${msg.id} for retry: ${reason}`);
  }

  private startRetryProcessor(): void {
    this.retryInterval = setInterval(async () => {
      await this.processRetryQueue();
    }, this.RETRY_INTERVAL_MS);

    this.logger.info("Started participant retry processor");
  }

  private async processRetryQueue(): Promise<void> {
    const now = new Date();
    const keysToRetry: string[] = [];

    for (const [key, queuedParticipant] of this.retryQueue.entries()) {
      if (queuedParticipant.nextRetryAt <= now && queuedParticipant.retryCount < this.MAX_RETRY_ATTEMPTS) {
        keysToRetry.push(key);
      }
    }

    for (const key of keysToRetry) {
      const queuedParticipant = this.retryQueue.get(key)!;
      try {
        this.logger.info(`Retrying participant ${queuedParticipant.message.id} (attempt ${queuedParticipant.retryCount + 1}/${this.MAX_RETRY_ATTEMPTS})`);

        const result = await this.handleSetParticipantOPToValidated(queuedParticipant.message);

        if (result && result.success !== false) {
          this.retryQueue.delete(key);
          this.logger.info(`Successfully processed queued participant ${queuedParticipant.message.id}`);
        } else {
          queuedParticipant.retryCount++;
          const delay = this.RETRY_INTERVAL_MS * (this.RETRY_BACKOFF_MULTIPLIER ** (queuedParticipant.retryCount - 1));
          queuedParticipant.nextRetryAt = new Date(Date.now() + delay);
          this.logger.warn(`Participant ${queuedParticipant.message.id} still failed, scheduled next retry in ${delay}ms`);
        }
      } catch (error) {
        queuedParticipant.retryCount++;
        const delay = this.RETRY_INTERVAL_MS * (this.RETRY_BACKOFF_MULTIPLIER ** (queuedParticipant.retryCount - 1));
        queuedParticipant.nextRetryAt = new Date(Date.now() + delay);
        this.logger.error(`Error retrying participant ${queuedParticipant.message.id}:`, error);
      }
    }

    for (const [key, queuedParticipant] of this.retryQueue.entries()) {
      if (queuedParticipant.retryCount >= this.MAX_RETRY_ATTEMPTS) {
        this.retryQueue.delete(key);
        this.logger.error(`Participant ${queuedParticipant.message.id} exceeded max retries (${this.MAX_RETRY_ATTEMPTS}), giving up`);
      }
    }

    if (this.retryQueue.size > 0) {
      this.logger.info(`Retry queue status: ${this.retryQueue.size} participants waiting`);
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
    this.logger.info("Stopped participant retry processor");
  }
}
