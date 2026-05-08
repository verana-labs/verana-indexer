import knex from "../../common/utils/db_connection";
import {
  VeranaCredentialSchemaMessageTypes,
  VeranaPermissionMessageTypes,
  VeranaTrustRegistryMessageTypes,
} from "../../common/verana-message-types";
import { applyBlockHeightFilter, isValidDid, toIsoSeconds } from "./api_shared";

export type IndexerTxEvent = {
  type: "transaction-executed";
  module: "trust-registry" | "credential-schema" | "permission";
  action: string;
  messageType: string;
  blockHeight: number;
  txHash: string;
  txIndex: number;
  messageIndex: number;
  sender: string;
  relatedDids: string[];
  entityType?: string;
  entityId?: string;
  timestamp: string;
};

export type IndexerEventRecord = {
  type: "indexer-event";
  event_type: string;
  did: string;
  block_height: number;
  tx_hash: string;
  timestamp: string;
  payload: {
    module: IndexerTxEvent["module"];
    action: string;
    message_type: string;
    tx_index: number;
    message_index: number;
    sender: string;
    related_dids: string[];
    entity_type?: string;
    entity_id?: string;
  };
};

type EventRow = {
  message_id: number;
  tx_id: number;
  message_index: number;
  message_type: string;
  sender: string;
  content: unknown;
  tx_data?: any;
  tx_message_count?: number;
  block_height: number;
  tx_hash: string;
  tx_index: number;
  timestamp: Date | string;
};

type EventMeta = {
  module: IndexerTxEvent["module"];
  action: string;
  entityType?: string;
};

const EVENT_META: Record<string, EventMeta> = {
  [VeranaTrustRegistryMessageTypes.CreateTrustRegistry]: {
    module: "trust-registry",
    action: "CreateNewTrustRegistry",
    entityType: "TrustRegistry",
  },
  [VeranaTrustRegistryMessageTypes.UpdateTrustRegistry]: {
    module: "trust-registry",
    action: "UpdateTrustRegistry",
    entityType: "TrustRegistry",
  },
  [VeranaTrustRegistryMessageTypes.ArchiveTrustRegistry]: {
    module: "trust-registry",
    action: "ArchiveTrustRegistry",
    entityType: "TrustRegistry",
  },
  [VeranaTrustRegistryMessageTypes.AddGovernanceFrameworkDoc]: {
    module: "trust-registry",
    action: "AddGovernanceFrameworkDocument",
    entityType: "GovernanceFrameworkDocument",
  },
  [VeranaTrustRegistryMessageTypes.IncreaseGovernanceFrameworkVersion]: {
    module: "trust-registry",
    action: "IncreaseActiveGFVersion",
    entityType: "GovernanceFrameworkVersion",
  },
  [VeranaCredentialSchemaMessageTypes.CreateCredentialSchema]: {
    module: "credential-schema",
    action: "CreateNewCredentialSchema",
    entityType: "CredentialSchema",
  },
  [VeranaCredentialSchemaMessageTypes.UpdateCredentialSchema]: {
    module: "credential-schema",
    action: "UpdateCredentialSchema",
    entityType: "CredentialSchema",
  },
  [VeranaCredentialSchemaMessageTypes.ArchiveCredentialSchema]: {
    module: "credential-schema",
    action: "ArchiveCredentialSchema",
    entityType: "CredentialSchema",
  },
  [VeranaPermissionMessageTypes.CreateRootPermission]: {
    module: "permission",
    action: "CreateRootPermission",
    entityType: "Permission",
  },
  [VeranaPermissionMessageTypes.SelfCreatePermission]: {
    module: "permission",
    action: "SelfCreatePermission",
    entityType: "Permission",
  },
  [VeranaPermissionMessageTypes.StartPermissionVP]: {
    module: "permission",
    action: "StartPermissionVP",
    entityType: "Permission",
  },
  [VeranaPermissionMessageTypes.RenewPermissionVP]: {
    module: "permission",
    action: "RenewPermissionVP",
    entityType: "Permission",
  },
  [VeranaPermissionMessageTypes.SetPermissionVPToValidated]: {
    module: "permission",
    action: "SetPermissionVPToValidated",
    entityType: "Permission",
  },
  [VeranaPermissionMessageTypes.AdjustPermission]: {
    module: "permission",
    action: "AdjustPermission",
    entityType: "Permission",
  },
  [VeranaPermissionMessageTypes.RevokePermission]: {
    module: "permission",
    action: "RevokePermission",
    entityType: "Permission",
  },
  [VeranaPermissionMessageTypes.SlashPermissionTrustDeposit]: {
    module: "permission",
    action: "SlashPermissionTrustDeposit",
    entityType: "Permission",
  },
  [VeranaPermissionMessageTypes.RepayPermissionSlashedTrustDeposit]: {
    module: "permission",
    action: "RepayPermissionSlashedTrustDeposit",
    entityType: "Permission",
  },
  [VeranaPermissionMessageTypes.CancelPermissionVPLastRequest]: {
    module: "permission",
    action: "CancelPermissionVPLastRequest",
    entityType: "Permission",
  },
};

const WATCHED_MESSAGE_TYPES = Object.keys(EVENT_META);

function addDid(out: Set<string>, value: unknown): void {
  if (isValidDid(value)) out.add(value);
}

function getLogger(): { warn?: (...args: any[]) => void } | undefined {
  return (global as any).logger as { warn?: (...args: any[]) => void } | undefined;
}

function collectDids(value: unknown, out: Set<string>): void {
  if (isValidDid(value)) {
    out.add(value);
    return;
  }
  if (Array.isArray(value)) {
    value.forEach((item) => collectDids(item, out));
    return;
  }
  if (value && typeof value === "object") {
    Object.values(value as Record<string, unknown>).forEach((item) => collectDids(item, out));
  }
}

type TxEventAttribute = { key?: unknown; value?: unknown };
type TxEvent = { type?: unknown; attributes?: unknown };
type TxResponse = { events?: TxEvent[]; logs?: Array<{ events?: TxEvent[] }> };

function toLowerSnake(input: string): string {
  const s = String(input ?? "");
  if (!s) return "";
  return s
    .replace(/\./g, "_")
    .replace(/[\s-]+/g, "_")
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .replace(/__+/g, "_")
    .toLowerCase()
    .trim();
}

function looksLikeBase64(s: string): boolean {
  return /^[A-Za-z0-9+/=]+$/.test(s) && s.length % 4 === 0;
}

function decodeMaybeBase64(v: unknown): string {
  const s = typeof v === "string" ? v : "";
  if (!s) return "";
  try {
    if (looksLikeBase64(s)) return Buffer.from(s, "base64").toString("utf-8");
  } catch {
    //
  }
  return s;
}

function readTxResponse(row: EventRow): TxResponse | null {
  const data = row.tx_data ?? null;
  if (!data || typeof data !== "object") return null;
  const txResponse = (data as any).tx_response ?? (data as any).txResponse ?? null;
  if (!txResponse || typeof txResponse !== "object") return null;
  return txResponse as TxResponse;
}

function normalizePositiveIntStrings(values: string[]): string[] {
  return values
    .map((v) => Number(v))
    .filter((n) => Number.isInteger(n) && n > 0)
    .map((n) => String(n));
}

export function resolveEntityIdFromTxResponseEvents(args: {
  txHash: string;
  blockHeight: number;
  messageIndex: number;
  txMessageCount: number;
  action: string;
  module: EventMeta["module"];
  txResponse: TxResponse | null;
}): { entityId?: string; reason?: string; debug?: any } {
  const action = String(args.action ?? "");
  const msgIndex = Number(args.messageIndex);
  const txMessageCount = Number(args.txMessageCount);
  const events = Array.isArray(args.txResponse?.events) ? (args.txResponse!.events as TxEvent[]) : [];

  const normalizedAction = toLowerSnake(action);

  const ruleByAction: Record<
    string,
    { eventTypes: string[]; idKeys: string[]; preferEventTypes?: string[]; allowEventTypeFallback?: boolean }
  > = {
    create_new_trust_registry: {
      eventTypes: ["create_trust_registry", "create_new_trust_registry"],
      idKeys: ["trust_registry_id", "tr_id", "id"],
    },
    update_trust_registry: {
      eventTypes: ["update_trust_registry"],
      idKeys: ["trust_registry_id", "tr_id", "id"],
    },
    archive_trust_registry: {
      eventTypes: ["archive_trust_registry"],
      idKeys: ["trust_registry_id", "tr_id", "id"],
    },

    // Governance Framework (only valid for GF entity types)
    add_governance_framework_document: {
      eventTypes: ["add_governance_framework_document", "create_governance_framework_document"],
      idKeys: ["gf_document_id", "governance_framework_document_id", "gfd_id", "id"],
    },
    increase_active_gf_version: {
      eventTypes: ["increase_active_gf_version", "create_governance_framework_version"],
      idKeys: ["gf_version_id", "governance_framework_version_id", "gfv_id", "id"],
    },

    // Credential Schema
    create_new_credential_schema: {
      eventTypes: ["create_credential_schema"],
      idKeys: ["credential_schema_id", "schema_id", "cs_id", "id"],
    },
    update_credential_schema: {
      eventTypes: ["update_credential_schema"],
      idKeys: ["credential_schema_id", "schema_id", "cs_id", "id"],
    },
    archive_credential_schema: {
      eventTypes: ["archive_credential_schema"],
      idKeys: ["credential_schema_id", "schema_id", "cs_id", "id"],
    },

    // Permission
    create_root_permission: {
      eventTypes: ["create_root_permission"],
      idKeys: ["root_permission_id", "permission_id", "perm_id", "id"],
    },
    self_create_permission: {
      eventTypes: [
        "self_create_permission",
        "create_permission",
        "create_self_permission",
        "self_create_perm",
        "permission_created",
        "create_permission_vp",
      ],
      preferEventTypes: ["self_create_permission"],
      allowEventTypeFallback: true,
      idKeys: ["permission_id", "self_permission_id", "perm_id", "permission_vp_id", "id"],
    },
    start_permission_vp: {
      eventTypes: ["start_permission_vp"],
      idKeys: ["permission_id", "perm_id", "id"],
    },
    renew_permission_vp: {
      eventTypes: ["renew_permission_vp"],
      idKeys: ["permission_id", "perm_id", "id"],
    },
    set_permission_vp_to_validated: {
      eventTypes: ["set_permission_vp_to_validated"],
      idKeys: ["permission_id", "perm_id", "id"],
    },
    set_permission_vp_to_rejected: {
      eventTypes: ["set_permission_vp_to_rejected"],
      idKeys: ["permission_id", "perm_id", "id"],
    },
    set_permission_vp_to_cancelled: {
      eventTypes: ["set_permission_vp_to_cancelled"],
      idKeys: ["permission_id", "perm_id", "id"],
    },
    set_permission_vp_to_terminated: {
      eventTypes: ["set_permission_vp_to_terminated"],
      idKeys: ["permission_id", "perm_id", "id"],
    },
    cancel_permission_vp_last_request: {
      eventTypes: ["cancel_permission_vp_last_request", "set_permission_vp_to_cancelled"],
      idKeys: ["permission_id", "perm_id", "id"],
    },
    adjust_permission: {
      eventTypes: ["adjust_permission"],
      idKeys: ["permission_id", "perm_id", "id"],
    },
    revoke_permission: {
      eventTypes: ["revoke_permission"],
      idKeys: ["permission_id", "perm_id", "id"],
    },
    slash_permission_trust_deposit: {
      eventTypes: ["slash_permission_trust_deposit"],
      idKeys: ["permission_id", "perm_id", "id"],
    },
    repay_permission_slashed_trust_deposit: {
      eventTypes: ["repay_permission_slashed_trust_deposit"],
      idKeys: ["permission_id", "perm_id", "id"],
    },
    update_permission: {
      eventTypes: ["update_permission"],
      idKeys: ["permission_id", "perm_id", "id"],
    },
    archive_permission: {
      eventTypes: ["archive_permission"],
      idKeys: ["permission_id", "perm_id", "id"],
    },
  };

  const rule =
    ruleByAction[normalizedAction] ??
    (args.module === "permission"
      ? { eventTypes: [], idKeys: ["permission_id", "perm_id", "id"] }
      : null);

  if (!rule) return { reason: "no_rule" };

  const candidates: Array<{ value: string; key: string; eventType: string }> = [];
  const consideredEvents: Array<{ type: string; msgIndex?: string }> = [];
  const idAttributesFound: Array<{ eventType: string; key: string; value: string }> = [];

  const matchesMessageIndex = (attrMap: Record<string, string>): boolean => {
    const eventMsgIndexRaw = attrMap.msg_index ?? attrMap.message_index ?? attrMap.tx_msg_index ?? "";
    if (!eventMsgIndexRaw) {
      return Number.isInteger(txMessageCount) && txMessageCount === 1;
    }
    const n = Number(eventMsgIndexRaw);
    return Number.isInteger(n) && n === msgIndex;
  };

  const scan = (ev: TxEvent, eventType: string, attrMap: Record<string, string>) => {
    for (const idKey of rule.idKeys) {
      if (idKey === "validator_perm_id") continue;
      if (args.module === "permission") {
        if (idKey === "schema_id") continue;
        if (idKey === "trust_registry_id") continue;
        if (idKey === "tr_id") continue;
        if (idKey === "credential_schema_id") continue;
        if (idKey === "cs_id") continue;
      }
      if (args.module === "credential-schema") {
        if (idKey === "trust_registry_id") continue;
        if (idKey === "tr_id") continue;
      }
      const v = attrMap[idKey];
      if (v == null || v === "") continue;
      idAttributesFound.push({ eventType, key: idKey, value: v });
      candidates.push({ value: v, key: idKey, eventType });
    }
  };

  const buildAttrMap = (ev: TxEvent): Record<string, string> => {
    const attrs = Array.isArray(ev?.attributes) ? (ev.attributes as TxEventAttribute[]) : [];
    const attrMap: Record<string, string> = {};
    for (const a of attrs) {
      const k = toLowerSnake(decodeMaybeBase64((a as any)?.key));
      if (!k) continue;
      attrMap[k] = decodeMaybeBase64((a as any)?.value);
    }
    return attrMap;
  };

  const preferred = rule.preferEventTypes?.length ? rule.preferEventTypes : rule.eventTypes;
  for (const ev of events) {
    const eventType = toLowerSnake(String(ev?.type ?? ""));
    if (!eventType) continue;
    if (preferred.length > 0 && !preferred.includes(eventType)) continue;
    const attrMap = buildAttrMap(ev);
    const eventMsgIndexRaw = attrMap.msg_index ?? attrMap.message_index ?? attrMap.tx_msg_index ?? "";
    consideredEvents.push({ type: eventType, msgIndex: eventMsgIndexRaw || undefined });
    if (!matchesMessageIndex(attrMap)) continue;
    scan(ev, eventType, attrMap);
  }

  if (candidates.length === 0 && rule.allowEventTypeFallback === true) {
    for (const ev of events) {
      const eventType = toLowerSnake(String(ev?.type ?? ""));
      if (!eventType) continue;
      const attrMap = buildAttrMap(ev);
      const eventMsgIndexRaw = attrMap.msg_index ?? attrMap.message_index ?? attrMap.tx_msg_index ?? "";
      consideredEvents.push({ type: eventType, msgIndex: eventMsgIndexRaw || undefined });
      if (!matchesMessageIndex(attrMap)) continue;
      scan(ev, eventType, attrMap);
    }
  }

  const unique = Array.from(new Set(normalizePositiveIntStrings(candidates.map((c) => c.value))));
  if (unique.length === 1) return { entityId: unique[0] };
  if (unique.length === 0) {
    return {
      reason: "no_candidate",
      debug: { action: normalizedAction, consideredEvents, idKeys: rule.idKeys, idAttributesFound },
    };
  }

  return {
    reason: "conflict",
    debug: {
      txHash: args.txHash,
      blockHeight: args.blockHeight,
      action: normalizedAction,
      module: args.module,
      candidates,
      consideredEvents,
      idAttributesFound,
      unique,
    },
  };
}

async function debugTxAttributesForMissingEntityId(row: EventRow): Promise<void> {
  if (process.env.INDEXER_EVENTS_DEBUG_ENTITY_ID !== "1") return;
  const logger = getLogger();
  if (!logger?.warn) return;
  const txResponse = readTxResponse(row);
  logger.warn(`[IndexerEvents] Missing entity_id for create message; raw tx attributes dumped`, {
    tx_hash: row.tx_hash,
    block_height: row.block_height,
    message_type: row.message_type,
    message_index: row.message_index,
    tx_id: row.tx_id,
    tx_response: txResponse,
  });
}

async function resolveEntityIdFromTxLocalData(row: EventRow, meta: EventMeta): Promise<string | undefined> {
  const messageType = String(row.message_type ?? "");
  const txResponse = readTxResponse(row);
  const resolved = resolveEntityIdFromTxResponseEvents({
    txHash: row.tx_hash,
    blockHeight: row.block_height,
    messageIndex: row.message_index,
    txMessageCount: Number(row.tx_message_count ?? 0),
    action: meta.action,
    module: meta.module,
    txResponse,
  });
  const entityId = resolved.entityId;
  if (resolved.reason === "conflict") {
    getLogger()?.warn?.(`[IndexerEvents] conflicting entity_id candidates; leaving entity_id empty`, resolved.debug);
  }
  if (!entityId && resolved.reason) {
    getLogger()?.warn?.(`[IndexerEvents] missing entity_id from tx_response events`, {
      block_height: row.block_height,
      tx_hash: row.tx_hash,
      message_index: row.message_index,
      action: meta.action,
      module: meta.module,
      entity_type: meta.entityType,
      reason: resolved.reason,
      debug: resolved.debug,
    });
  }
  if (
    !entityId &&
    [
      VeranaTrustRegistryMessageTypes.CreateTrustRegistry,
      VeranaCredentialSchemaMessageTypes.CreateCredentialSchema,
      VeranaPermissionMessageTypes.CreateRootPermission,
      VeranaPermissionMessageTypes.StartPermissionVP,
    ].includes(messageType as any)
  ) {
    await debugTxAttributesForMissingEntityId(row);
  }
  return entityId;
}

async function resolveEntityId(row: EventRow, meta: EventMeta): Promise<string | undefined> {
  return await resolveEntityIdFromTxLocalData(row, meta);
}

async function toIndexerEvent(row: EventRow): Promise<IndexerTxEvent | null> {
  const meta = EVENT_META[row.message_type];
  if (!meta) return null;

  const relatedDids = new Set<string>();
  addDid(relatedDids, row.sender);
  collectDids(row.content, relatedDids);
  const entityId = await resolveEntityId(row, meta);
  return {
    type: "transaction-executed",
    module: meta.module,
    action: meta.action,
    messageType: row.message_type,
    blockHeight: Number(row.block_height),
    txHash: row.tx_hash,
    txIndex: Number(row.tx_index),
    messageIndex: Number(row.message_index),
    sender: row.sender,
    relatedDids: Array.from(relatedDids).sort(),
    entityType: meta.entityType,
    entityId,
    timestamp: toIsoSeconds(row.timestamp),
  };
}

function toEventRows(event: IndexerTxEvent): Array<Record<string, unknown>> {
  return event.relatedDids.map((did) => ({
    event_type: event.action,
    did,
    block_height: event.blockHeight,
    tx_hash: event.txHash,
    tx_index: event.txIndex,
    message_index: event.messageIndex,
    message_type: event.messageType,
    module: event.module,
    entity_type: event.entityType ?? null,
    entity_id: event.entityId ?? null,
    timestamp: event.timestamp,
    payload: {
      module: event.module,
      action: event.action,
      message_type: event.messageType,
      tx_index: event.txIndex,
      message_index: event.messageIndex,
      sender: event.sender,
      related_dids: event.relatedDids,
      entity_type: event.entityType,
      entity_id: event.entityId,
    },
  }));
}

function fromStoredRow(row: Record<string, any>): IndexerEventRecord {
  return {
    type: "indexer-event",
    event_type: String(row.event_type),
    did: String(row.did),
    block_height: Number(row.block_height),
    tx_hash: String(row.tx_hash),
    timestamp: toIsoSeconds(row.timestamp),
    payload: {
      module: row.payload?.module ?? row.module,
      action: row.payload?.action ?? row.event_type,
      // Backward compatible: accept old camelCase payload keys.
      message_type: row.payload?.message_type ?? row.payload?.messageType ?? row.message_type,
      tx_index: Number(row.payload?.tx_index ?? row.payload?.txIndex ?? row.tx_index ?? 0),
      message_index: Number(row.payload?.message_index ?? row.payload?.messageIndex ?? row.message_index ?? 0),
      sender: String(row.payload?.sender ?? ""),
      related_dids: Array.isArray(row.payload?.related_dids)
        ? row.payload.related_dids
        : Array.isArray(row.payload?.relatedDids)
          ? row.payload.relatedDids
          : [String(row.did)],
      entity_type: row.payload?.entity_type ?? row.payload?.entityType ?? row.entity_type ?? undefined,
      entity_id: row.payload?.entity_id ?? row.payload?.entityId ?? row.entity_id ?? undefined,
    },
  };
}

async function buildIndexerTxEvents(args: {
  afterBlockHeight?: number;
  blockHeight?: number;
  limit?: number;
  offset?: number;
}): Promise<IndexerTxEvent[]> {
  const limit = Math.max(1, Math.min(500, Math.floor(Number(args.limit ?? 100))));
  const query = knex("transaction_message as tm")
    .innerJoin("transaction as tx", "tx.id", "tm.tx_id")
    .whereIn("tm.type", WATCHED_MESSAGE_TYPES)
    .andWhere("tx.code", 0)
    .select(
      "tm.id as message_id",
      "tm.tx_id",
      "tm.index as message_index",
      "tm.type as message_type",
      "tm.sender",
      "tm.content",
      "tx.data as tx_data",
      knex.raw("count(*) over (partition by tx.id)::int as tx_message_count"),
      "tx.height as block_height",
      "tx.hash as tx_hash",
      "tx.index as tx_index",
      "tx.timestamp"
    )
    .orderBy("tx.height", "asc")
    .orderBy("tx.index", "asc")
    .orderBy("tm.index", "asc")
    .limit(limit);

  applyBlockHeightFilter(query, args, "tx.height");
  if (Number.isInteger(args.offset) && Number(args.offset) > 0) {
    query.offset(Number(args.offset));
  }

  const rows = (await query) as EventRow[];
  return (await Promise.all(rows.map((row) => toIndexerEvent(row)))).filter(Boolean) as IndexerTxEvent[];
}

export async function persistIndexerEventsForBlock(blockHeight: number): Promise<IndexerEventRecord[]> {
  const rows: Array<Record<string, unknown>> = [];
  const pageSize = 500;
  let offset = 0;
  while (true) {
    const txEvents = await buildIndexerTxEvents({ blockHeight, limit: pageSize, offset });
    rows.push(...txEvents.flatMap(toEventRows));
    if (txEvents.length < pageSize) break;
    offset += pageSize;
  }
  let insertedIds: number[] = [];
  if (rows.length > 0) {
    const inserted = await knex("indexer_events")
      .insert(rows)
      .onConflict(["did", "tx_hash", "message_index", "event_type"])
      .merge({
        entity_id: knex.raw("COALESCE(indexer_events.entity_id, EXCLUDED.entity_id)"),
        entity_type: knex.raw("COALESCE(indexer_events.entity_type, EXCLUDED.entity_type)"),
        payload: knex.raw(
          `
          CASE
            WHEN (indexer_events.payload->>'entity_id') IS NULL AND EXCLUDED.entity_id IS NOT NULL
              THEN jsonb_set(COALESCE(indexer_events.payload, '{}'::jsonb), '{entity_id}', to_jsonb(EXCLUDED.entity_id::text), true)
            ELSE COALESCE(indexer_events.payload, '{}'::jsonb)
          END
          `
        ),
      })
      .where((builder) =>
        builder.whereNull("indexer_events.entity_id").orWhereRaw("(indexer_events.payload->>'entity_id') IS NULL")
      )
      .returning("id");
    insertedIds = inserted
      .map((row: number | string | { id?: number | string }) => Number(typeof row === "object" ? row.id : row))
      .filter((id): id is number => Number.isInteger(id));
  }

  if (insertedIds.length === 0) return [];

  const results: IndexerEventRecord[] = [];
  const chunkSize = 500;
  for (let i = 0; i < insertedIds.length; i += chunkSize) {
    const chunk = insertedIds.slice(i, i + chunkSize);
    const rows = await listIndexerEvents({ ids: chunk, limit: chunk.length });
    results.push(...rows);
  }
  return results;
}

export async function listIndexerEvents(args: {
  afterBlockHeight?: number;
  blockHeight?: number;
  did?: string;
  ids?: number[];
  limit?: number;
}): Promise<IndexerEventRecord[]> {
  const limit = Math.max(1, Math.min(500, Math.floor(Number(args.limit ?? 100))));
  const query = knex("indexer_events")
    .select(
      "id",
      "event_type",
      "did",
      "block_height",
      "tx_hash",
      "tx_index",
      "message_index",
      "message_type",
      "module",
      "entity_type",
      "entity_id",
      "timestamp",
      "payload"
    )
    .orderBy("block_height", "asc")
    .orderBy("tx_index", "asc")
    .orderBy("message_index", "asc")
    .orderBy("did", "asc")
    .orderBy("id", "asc")
    .limit(limit);

  if (args.ids) query.whereIn("id", args.ids);
  if (args.did) query.where("did", args.did);
  applyBlockHeightFilter(query, args, "block_height");

  const rows = (await query) as Array<Record<string, any>>;
  return rows.map(fromStoredRow);
}
