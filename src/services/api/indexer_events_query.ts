import knex from "../../common/utils/db_connection";
import {
  VeranaCredentialSchemaMessageTypes,
  VeranaDelegationMessageTypes,
  VeranaDiMessageTypes,
  VeranaParticipantMessageTypes,
  VeranaEcosystemMessageTypes,
} from "../../common/verana-message-types";
import { applyBlockHeightFilter, toIsoSeconds } from "./api_shared";
import {
  collectDidsDeep,
  firstNormalizedDid,
  normalizeDid,
  readFirstPositiveInteger,
  uniqueNormalizedDids,
} from "./indexer_event_utils";

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
  [VeranaEcosystemMessageTypes.CreateEcosystem]: {
    module: "trust-registry",
    action: "CreateNewTrustRegistry",
    entityType: "TrustRegistry",
  },
  [VeranaEcosystemMessageTypes.UpdateEcosystem]: {
    module: "trust-registry",
    action: "UpdateEcosystem",
    entityType: "TrustRegistry",
  },
  [VeranaEcosystemMessageTypes.ArchiveEcosystem]: {
    module: "trust-registry",
    action: "ArchiveEcosystem",
    entityType: "TrustRegistry",
  },
  [VeranaEcosystemMessageTypes.AddGovernanceFrameworkDoc]: {
    module: "trust-registry",
    action: "AddGovernanceFrameworkDocument",
    entityType: "GovernanceFrameworkDocument",
  },
  [VeranaEcosystemMessageTypes.IncreaseGovernanceFrameworkVersion]: {
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
  [VeranaParticipantMessageTypes.StartParticipantOP]: {
    module: "permission",
    action: "StartParticipantOP",
    entityType: "Permission",
  },
  [VeranaParticipantMessageTypes.CreateRootParticipant]: {
    module: "permission",
    action: "CreateRootParticipant",
    entityType: "Permission",
  },
  [VeranaParticipantMessageTypes.SelfCreateParticipant]: {
    module: "permission",
    action: "SelfCreateParticipant",
    entityType: "Permission",
  },
  [VeranaParticipantMessageTypes.RenewParticipantOP]: {
    module: "permission",
    action: "RenewParticipantOP",
    entityType: "Permission",
  },
  [VeranaParticipantMessageTypes.SetParticipantOPToValidated]: {
    module: "permission",
    action: "SetParticipantOPToValidated",
    entityType: "Permission",
  },
  [VeranaParticipantMessageTypes.SetParticipantEffectiveUntil]: {
    module: "permission",
    action: "SetParticipantEffectiveUntil",
    entityType: "Permission",
  },
  [VeranaParticipantMessageTypes.RevokeParticipant]: {
    module: "permission",
    action: "RevokeParticipant",
    entityType: "Permission",
  },
  [VeranaParticipantMessageTypes.SlashParticipantTrustDeposit]: {
    module: "permission",
    action: "SlashParticipantTrustDeposit",
    entityType: "Permission",
  },
  [VeranaParticipantMessageTypes.RepayParticipantSlashedTrustDeposit]: {
    module: "permission",
    action: "RepayParticipantSlashedTrustDeposit",
    entityType: "Permission",
  },
  [VeranaParticipantMessageTypes.CancelParticipantOPLastRequest]: {
    module: "permission",
    action: "CancelParticipantOPLastRequest",
    entityType: "Permission",
  },
  [VeranaParticipantMessageTypes.CreateOrUpdateParticipantSession]: {
    module: "permission",
    action: "CreateOrUpdateParticipantSession",
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

function getEntityId(row: EventRow, meta: EventMeta): string | undefined {
  if (meta.module === "permission") {
    const permissionId = readNumber(row.content, ["id", "permission_id", "permissionId", "perm_id", "permId"]);
    return permissionId ? String(permissionId) : undefined;
  }

  if (meta.module === "credential-schema") {
    const schemaId = readNumber(row.content, ["id", "schema_id", "schemaId", "credential_schema_id", "credentialSchemaId"]);
    return schemaId ? String(schemaId) : undefined;
  }

  const trId =
    readNumber(row.content, ["id", "tr_id", "trId", "trust_registry_id", "trustRegistryId"]) ??
    readNumber(row.content, ["gfv_id", "gfvId", "gfd_id", "gfdId"]);
  return trId ? String(trId) : undefined;
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
    .leftJoin("trust_registry as tr", "tr.id", "cs.ecosystem_id")
    .where("cs.id", schemaId)
    .select("cs.id as schema_id", "cs.ecosystem_id", "tr.did as tr_did")
    .first();
  if (!schema) return { schemaId: String(schemaId) };
  return {
    schemaId: String(schema.schema_id ?? schemaId),
    trId: schema.ecosystem_id != null ? String(schema.ecosystem_id) : undefined,
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
    .leftJoin("trust_registry as tr", "tr.id", "cs.ecosystem_id")
    .leftJoin("permissions as validator", "validator.id", "p.validator_participant_id")
    .where("p.id", permissionId)
    .select(
      "p.id as permission_id",
      "p.did as permission_did",
      "p.schema_id",
      "cs.ecosystem_id",
      "tr.did as tr_did",
      "validator.did as validator_permission_did"
    )
    .first();
  if (!perm) return { permissionId: String(permissionId) };
  return {
    permissionId: String(perm.permission_id ?? permissionId),
    permissionDid: normalizeDid(perm.permission_did),
    schemaId: perm.schema_id != null ? String(perm.schema_id) : undefined,
    trId: perm.ecosystem_id != null ? String(perm.ecosystem_id) : undefined,
    trDid: normalizeDid(perm.tr_did),
    validatorPermissionDid: normalizeDid(perm.validator_permission_did),
  };
}

async function toIndexerEvent(row: EventRow): Promise<IndexerTxEvent | null> {
  const meta = EVENT_META[row.message_type];
  if (!meta) return null;

  const entityId = getEntityId(row, meta);
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
    const rawTrId = readNumber(row.content, ["trust_registry_id", "trustRegistryId", "tr_id", "trId", "id"]);
    trId = rawTrId ? String(rawTrId) : entityId;
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
    const rawValidatorPermId = readNumber(row.content, ["validator_participant_id", "validatorParticipantId"]);
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
    const inserted = await knex("indexer_events")
      .insert(rows)
      .onConflict(knex.raw("(tx_hash, tx_index, message_index, event_type, entity_type, COALESCE(entity_id, ''))"))
      .ignore()
      .returning("id");
    insertedIds = inserted
      .map((row: number | string | { id?: number | string }) => Number(typeof row === "object" ? row.id : row))
      .filter((id): id is number => Number.isInteger(id));
    if (insertedIds.length === 0) {
      console.info(`[IndexerEvents] skipped duplicate event batch for block_height=${blockHeight}, candidates=${rows.length}`);
    } else {
      console.info(`[IndexerEvents] saved ${insertedIds.length}/${rows.length} event(s) for block_height=${blockHeight}`);
    }
  } else {
    console.info(`[IndexerEvents] no DID found or no watched messages for block_height=${blockHeight}`);
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
