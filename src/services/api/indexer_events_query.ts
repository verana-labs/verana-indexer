import knex from "../../common/utils/db_connection";
import {
  VeranaCredentialSchemaMessageTypes,
  VeranaDelegationMessageTypes,
  VeranaDiMessageTypes,
  VeranaPermissionMessageTypes,
  VeranaTrustRegistryMessageTypes,
} from "../../common/verana-message-types";
import { applyBlockHeightFilter, createLogger, toIsoSeconds } from "./api_shared";
import {
  collectDidsDeep,
  firstNormalizedDid,
  normalizeDid,
  readFirstPositiveInteger,
  uniqueNormalizedDids,
} from "./indexer_event_utils";

function logger() {
  return createLogger((global as any).logger);
}

export type IndexerTxEvent = {
  type: "transaction-executed";
  module: "trust-registry" | "credential-schema" | "permission" | "digital-identity" | "delegation";
  action: string;
  messageType: string;
  blockHeight: number;
  txHash: string;
  txIndex: number;
  messageIndex: number;
  sender: string;
  did: string;
  relatedDids: string[];
  entityType?: string;
  entityId?: string;
  trId?: string;
  schemaId?: string;
  permissionId?: string;
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
    tr_id?: string;
    schema_id?: string;
    permission_id?: string;
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
  [VeranaPermissionMessageTypes.CreateOrUpdatePermissionSession]: {
    module: "permission",
    action: "CreateOrUpdatePermissionSession",
    entityType: "PermissionSession",
  },
  [VeranaDiMessageTypes.StoreDigest]: {
    module: "digital-identity",
    action: "StoreDigest",
    entityType: "DigitalIdentityDigest",
  },
  [VeranaDelegationMessageTypes.GrantOperatorAuthorization]: {
    module: "delegation",
    action: "GrantOperatorAuthorization",
    entityType: "OperatorAuthorization",
  },
  [VeranaDelegationMessageTypes.RevokeOperatorAuthorization]: {
    module: "delegation",
    action: "RevokeOperatorAuthorization",
    entityType: "OperatorAuthorization",
  },
};

const WATCHED_MESSAGE_TYPES = Object.keys(EVENT_META);

function readNumber(content: unknown, keys: string[]): number | null {
  return readFirstPositiveInteger(content, keys);
}

type TxEventAttribute = { key?: unknown; value?: unknown };
type TxEvent = { type?: unknown; attributes?: unknown };
type TxResponse = { events?: TxEvent[] };

function toLowerSnake(input: string): string {
  const s = String(input ?? "");
  if (!s) return "";
  return s
    .replace(/\./g, "_")
    .replace(/[\s-]+/g, "_")
    .replace(/([A-Z]+)([A-Z][a-z])/g, "$1_$2")
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .replace(/__+/g, "_")
    .toLowerCase()
    .trim();
}

function looksLikeBase64(s: string): boolean {
  return /^[A-Za-z0-9+/=]+$/.test(s) && s.length % 4 === 0;
}

let warnedBase64DecodeFailure = false;

function decodeMaybeBase64(v: unknown): string {
  const s = typeof v === "string" ? v : "";
  if (!s) return "";
  try {
    if (looksLikeBase64(s)) return Buffer.from(s, "base64").toString("utf-8");
  } catch (err) {
    if (!warnedBase64DecodeFailure) {
      warnedBase64DecodeFailure = true;
      logger().warn("[IndexerEvents] failed to decode base64 tx event attribute", {
        sample: s.slice(0, 64),
        error: err instanceof Error ? err.message : String(err),
      });
    }
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
  entityType?: string;
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

    add_governance_framework_document: {
      eventTypes: [
        "add_governance_framework_document",
        "create_governance_framework_document",
        "governance_framework_document_added",
        "add_gf_document",
        "create_gf_document",
      ],
      idKeys: [
        "gf_document_id",
        "governance_framework_document_id",
        "governance_framework_doc_id",
        "gfd_id",
        "document_id",
        "doc_id",
        "id",
      ],
    },
    increase_active_gf_version: {
      eventTypes: [
        "increase_active_gf_version",
        "increase_active_governance_framework_version",
        "create_governance_framework_version",
        "governance_framework_version_created",
      ],
      idKeys: ["gf_version_id", "governance_framework_version_id", "gfv_id", "version_id", "active_version", "id"],
    },

    // Credential Schema
    create_new_credential_schema: {
      eventTypes: ["create_credential_schema", "create_new_credential_schema"],
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
  const gfFallbackCandidates: Array<{ value: string; key: string; eventType: string }> = [];
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

  const isGovernanceFrameworkEntity =
    args.entityType === "GovernanceFrameworkDocument" || args.entityType === "GovernanceFrameworkVersion";

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

    if (isGovernanceFrameworkEntity) {
      for (const fallbackKey of ["tr_id", "trust_registry_id"] as const) {
        const v = attrMap[fallbackKey];
        if (v == null || v === "") continue;
        idAttributesFound.push({ eventType, key: fallbackKey, value: v });
        gfFallbackCandidates.push({ value: v, key: fallbackKey, eventType });
      }
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
    if (isGovernanceFrameworkEntity) {
      const gfUnique = Array.from(new Set(normalizePositiveIntStrings(gfFallbackCandidates.map((c) => c.value))));
      if (gfUnique.length === 1) return { entityId: gfUnique[0] };
      if (gfUnique.length > 1) {
        return {
          reason: "conflict",
          debug: {
            txHash: args.txHash,
            blockHeight: args.blockHeight,
            action: normalizedAction,
            module: args.module,
            candidates: gfFallbackCandidates,
            consideredEvents,
            idAttributesFound,
            unique: gfUnique,
          },
        };
      }
    }
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

async function resolveEntityIdFromTxLocalData(row: EventRow, meta: EventMeta): Promise<string | undefined> {
  const txResponse = readTxResponse(row);
  const resolved = resolveEntityIdFromTxResponseEvents({
    txHash: row.tx_hash,
    blockHeight: row.block_height,
    messageIndex: row.message_index,
    txMessageCount: Number(row.tx_message_count ?? 0),
    action: meta.action,
    module: meta.module,
    entityType: meta.entityType,
    txResponse,
  });
  const entityId = resolved.entityId;
  if (resolved.reason === "conflict") {
    logger().warn(`[IndexerEvents] conflicting entity_id candidates; leaving entity_id empty`, resolved.debug);
  }
  return entityId;
}

async function resolveEntityId(row: EventRow, meta: EventMeta): Promise<string | undefined> {
  return await resolveEntityIdFromTxLocalData(row, meta);
}

function normalizeRequestedDid(value: unknown): string | undefined {
  return normalizeDid(value);
}

async function loadTrustRegistryDid(trId: number | null | undefined): Promise<string | undefined> {
  if (!trId) return undefined;
  const row = await knex("trust_registry").select("did").where({ id: trId }).first();
  return normalizeDid(row?.did);
}

async function loadSchemaRelation(schemaId: number | null | undefined): Promise<{
  schemaId?: string;
  trId?: string;
  trDid?: string;
}> {
  if (!schemaId) return {};
  const schema = await knex("credential_schemas as cs")
    .leftJoin("trust_registry as tr", "tr.id", "cs.tr_id")
    .where("cs.id", schemaId)
    .select("cs.id as schema_id", "cs.tr_id", "tr.did as tr_did")
    .first();
  if (!schema) return { schemaId: String(schemaId) };
  return {
    schemaId: String(schema.schema_id ?? schemaId),
    trId: schema.tr_id != null ? String(schema.tr_id) : undefined,
    trDid: normalizeDid(schema.tr_did),
  };
}

async function loadPermissionRelation(permissionId: number | null | undefined): Promise<{
  permissionId?: string;
  permissionDid?: string;
  schemaId?: string;
  trId?: string;
  trDid?: string;
  validatorPermissionDid?: string;
}> {
  if (!permissionId) return {};
  const perm = await knex("permissions as p")
    .leftJoin("credential_schemas as cs", "cs.id", "p.schema_id")
    .leftJoin("trust_registry as tr", "tr.id", "cs.tr_id")
    .leftJoin("permissions as validator", "validator.id", "p.validator_perm_id")
    .where("p.id", permissionId)
    .select(
      "p.id as permission_id",
      "p.did as permission_did",
      "p.schema_id",
      "cs.tr_id",
      "tr.did as tr_did",
      "validator.did as validator_permission_did"
    )
    .first();
  if (!perm) return { permissionId: String(permissionId) };
  return {
    permissionId: String(perm.permission_id ?? permissionId),
    permissionDid: normalizeDid(perm.permission_did),
    schemaId: perm.schema_id != null ? String(perm.schema_id) : undefined,
    trId: perm.tr_id != null ? String(perm.tr_id) : undefined,
    trDid: normalizeDid(perm.tr_did),
    validatorPermissionDid: normalizeDid(perm.validator_permission_did),
  };
}

async function toIndexerEvent(row: EventRow): Promise<IndexerTxEvent | null> {
  const meta = EVENT_META[row.message_type];
  if (!meta) return null;

  let entityId = await resolveEntityId(row, meta);
  const content = row.content && typeof row.content === "object" ? (row.content as Record<string, unknown>) : {};
  const collected = collectDidsDeep([row.sender, row.content]);
  let trId: string | undefined;
  let schemaId: string | undefined;
  let permissionId: string | undefined;
  const explicitPrimaryDid = firstNormalizedDid([
    content.did,
    content.trust_registry_did,
    content.trustRegistryDid,
    content.permission_did,
    content.permissionDid,
    content.participant_did,
    content.participantDid,
    content.sender,
    row.sender,
  ]);

  if (meta.module === "trust-registry") {
    const isTrustRegistryEntity = meta.entityType === "TrustRegistry";
    const rawTrId = readNumber(
      row.content,
      isTrustRegistryEntity ? ["trust_registry_id", "trustRegistryId", "tr_id", "trId", "id"] : ["trust_registry_id", "trustRegistryId", "tr_id", "trId"]
    );
    trId = rawTrId ? String(rawTrId) : isTrustRegistryEntity ? entityId : undefined;
    const trDid = await loadTrustRegistryDid(rawTrId);
    if (trDid) collected.add(trDid);
  }

  if (meta.module === "credential-schema") {
    const rawSchemaId = readNumber(row.content, ["schema_id", "schemaId", "credential_schema_id", "credentialSchemaId", "id"]);
    const rawTrId = readNumber(row.content, ["trust_registry_id", "trustRegistryId", "tr_id", "trId"]);
    const relation = await loadSchemaRelation(rawSchemaId);
    schemaId = relation.schemaId ?? (rawSchemaId ? String(rawSchemaId) : entityId);
    trId = relation.trId ?? (rawTrId ? String(rawTrId) : undefined);
    const trDid = relation.trDid ?? (await loadTrustRegistryDid(rawTrId));
    if (trDid) collected.add(trDid);
  }

  if (meta.module === "permission") {
    const rawPermissionId = readNumber(row.content, ["permission_id", "permissionId", "perm_id", "permId", "id"]);
    const rawSchemaId = readNumber(row.content, ["schema_id", "schemaId", "credential_schema_id", "credentialSchemaId"]);
    const rawValidatorPermId = readNumber(row.content, ["validator_perm_id", "validatorPermId"]);
    const relation = await loadPermissionRelation(rawPermissionId);
    permissionId = relation.permissionId ?? (rawPermissionId ? String(rawPermissionId) : entityId);
    schemaId = relation.schemaId ?? (rawSchemaId ? String(rawSchemaId) : undefined);
    trId = relation.trId;
    [relation.permissionDid, relation.trDid, relation.validatorPermissionDid].forEach((did) => {
      if (did) collected.add(did);
    });
    if (rawSchemaId && !relation.trDid) {
      const schemaRelation = await loadSchemaRelation(rawSchemaId);
      schemaId = schemaId ?? schemaRelation.schemaId;
      trId = trId ?? schemaRelation.trId;
      if (schemaRelation.trDid) collected.add(schemaRelation.trDid);
    }
    if (rawValidatorPermId) {
      const validatorRelation = await loadPermissionRelation(rawValidatorPermId);
      [validatorRelation.permissionDid, validatorRelation.trDid].forEach((did) => {
        if (did) collected.add(did);
      });
    }
  }

  if (!entityId && (meta.entityType === "GovernanceFrameworkDocument" || meta.entityType === "GovernanceFrameworkVersion")) {
    entityId = trId;
  }

  const relatedDids = uniqueNormalizedDids(collected);
  const primaryDid =
    explicitPrimaryDid ??
    (meta.module === "permission" ? firstNormalizedDid(relatedDids) : undefined) ??
    firstNormalizedDid(relatedDids);
  if (!primaryDid) return null;
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
    did: primaryDid,
    relatedDids,
    entityType: meta.entityType,
    entityId,
    trId,
    schemaId,
    permissionId,
    timestamp: toIsoSeconds(row.timestamp),
  };
}

function toEventRow(event: IndexerTxEvent): Record<string, unknown> {
  return {
    event_type: event.action,
    did: event.did,
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
      tr_id: event.trId,
      schema_id: event.schemaId,
      permission_id: event.permissionId,
    },
  };
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
      tr_id: row.payload?.tr_id ?? row.payload?.trId ?? undefined,
      schema_id: row.payload?.schema_id ?? row.payload?.schemaId ?? undefined,
      permission_id: row.payload?.permission_id ?? row.payload?.permissionId ?? undefined,
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
    rows.push(...txEvents.map(toEventRow));
    if (txEvents.length < pageSize) break;
    offset += pageSize;
  }
  let insertedIds: number[] = [];
  if (rows.length > 0) {
    const updatable = rows
      .map((r: any) => ({
        tx_hash: r.tx_hash,
        tx_index: r.tx_index,
        message_index: r.message_index,
        event_type: r.event_type,
        entity_type: r.entity_type,
        entity_id: r.entity_id,
      }))
      .filter((r) => r.entity_id != null);
    if (updatable.length > 0) {
      await knex.raw(
        `
          UPDATE indexer_events ie
          SET
            entity_id = COALESCE(ie.entity_id, v.entity_id),
            payload = CASE
              WHEN COALESCE(ie.payload->>'entity_id', '') <> '' THEN ie.payload
              ELSE jsonb_set(ie.payload, '{entity_id}', to_jsonb(v.entity_id), true)
            END
          FROM (
            VALUES ${updatable.map(() => "(?::text, ?::int, ?::int, ?::text, ?::text, ?::text)").join(",")}
          ) AS v(tx_hash, tx_index, message_index, event_type, entity_type, entity_id)
          WHERE
            ie.tx_hash = v.tx_hash
            AND ie.tx_index = v.tx_index
            AND ie.message_index = v.message_index
            AND ie.event_type = v.event_type
            AND (
              (ie.entity_type IS NULL AND v.entity_type IS NULL)
              OR ie.entity_type = v.entity_type
            )
            AND ie.entity_id IS NULL
        `,
        updatable.flatMap((r) => [
          r.tx_hash,
          Number(r.tx_index),
          Number(r.message_index),
          r.event_type,
          r.entity_type,
          r.entity_id,
        ])
      );
    }

    const inserted = await knex("indexer_events")
      .insert(rows)
      .onConflict(knex.raw("(tx_hash, tx_index, message_index, event_type, entity_type, COALESCE(entity_id, ''))"))
      .ignore()
      .returning("id");
    insertedIds = inserted
      .map((row: number | string | { id?: number | string }) => Number(typeof row === "object" ? row.id : row))
      .filter((id): id is number => Number.isInteger(id));
    if (insertedIds.length === 0) {
      logger().info(`[IndexerEvents] skipped duplicate event batch for block_height=${blockHeight}, candidates=${rows.length}`);
    } else {
      logger().info(`[IndexerEvents] saved ${insertedIds.length}/${rows.length} event(s) for block_height=${blockHeight}`);
    }
  } else {
    logger().info(`[IndexerEvents] no DID found or no watched messages for block_height=${blockHeight}`);
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
  const normalizedDid = normalizeRequestedDid(args.did);
  const query = knex("indexer_events as ie")
    .select(
      "ie.id",
      "ie.event_type",
      "ie.did",
      "ie.block_height",
      "ie.tx_hash",
      "ie.tx_index",
      "ie.message_index",
      "ie.message_type",
      "ie.module",
      "ie.entity_type",
      "ie.entity_id",
      "ie.timestamp",
      "ie.payload"
    )
    .orderBy("ie.block_height", "asc")
    .orderBy("ie.tx_index", "asc")
    .orderBy("ie.message_index", "asc")
    .orderBy("ie.id", "asc")
    .limit(limit);

  if (args.ids) query.whereIn("ie.id", args.ids);
  if (args.did && !normalizedDid) return [];
  if (normalizedDid && !args.ids) {
    query.andWhere(function () {
      this
        .where("ie.did", normalizedDid)
        .orWhereRaw("(ie.payload -> 'related_dids') \\? ?", [normalizedDid])
        .orWhereRaw("(ie.payload -> 'relatedDids') \\? ?", [normalizedDid]);
    });
  }
  applyBlockHeightFilter(query, args, "ie.block_height");

  const rows = (await query) as Array<Record<string, any>>;
  return rows.map(fromStoredRow);
}
