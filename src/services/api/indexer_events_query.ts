import knex from "../../common/utils/db_connection";
import {
  VeranaCredentialSchemaMessageTypes,
  VeranaDelegationMessageTypes,
  VeranaDiMessageTypes,
  VeranaPermissionMessageTypes,
  VeranaTrustRegistryMessageTypes,
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
  [VeranaPermissionMessageTypes.StartPermissionVP]: {
    module: "permission",
    action: "StartPermissionVP",
    entityType: "Permission",
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

function readNumber(content: unknown, keys: readonly string[]): number | null {
  return readFirstPositiveInteger(content, keys);
}

const ID_ALIASES = {
  trustRegistry: ["tr_id", "trId", "trust_registry_id", "trustRegistryId"],
  credentialSchema: ["schema_id", "schemaId", "credential_schema_id", "credentialSchemaId"],
  permission: ["permission_id", "permissionId", "perm_id", "permId"],
  validatorPermission: ["validator_perm_id", "validatorPermId"],
  governanceFramework: ["gfv_id", "gfvId", "gfd_id", "gfdId"],
} as const;

async function getEntityId(row: EventRow, meta: EventMeta): Promise<string | undefined> {
  if (meta.module === "permission") {
    const permissionId = readNumber(row.content, ["id", ...ID_ALIASES.permission]);
    return permissionId ? String(permissionId) : undefined;
  }

  if (meta.module === "credential-schema") {
    const schemaId = readNumber(row.content, ["id", ...ID_ALIASES.credentialSchema]);
    return schemaId ? String(schemaId) : undefined;
  }

  const trId =
    readNumber(row.content, ["id", ...ID_ALIASES.trustRegistry]) ??
    readNumber(row.content, ID_ALIASES.governanceFramework);
  return trId ? String(trId) : resolveEntityIdFromDomain(row, meta);
}

async function resolveEntityIdFromDomain(row: EventRow, meta: EventMeta): Promise<string | undefined> {
  const content = row.content && typeof row.content === "object" ? (row.content as Record<string, unknown>) : {};
  const height = Number(row.block_height);

  if (meta.module === "trust-registry") {
    const did = normalizeDid(content.did);
    if (!did) return undefined;
    const tr = await knex("trust_registry").select("id").where({ did }).first();
    return tr?.id != null ? String(tr.id) : undefined;
  }

  if (meta.module === "credential-schema") {
    const trId = readNumber(content, ID_ALIASES.trustRegistry);
    const query = knex("credential_schema_history").select("credential_schema_id").where({ height });
    if (trId) query.andWhere({ tr_id: trId });
    const cs = await query.orderBy("credential_schema_id", "desc").first();
    return cs?.credential_schema_id != null ? String(cs.credential_schema_id) : undefined;
  }

  if (meta.module === "permission") {
    const did = normalizeDid(content.did);
    const query = knex("permission_history").select("permission_id").where({ height });
    if (did) query.andWhere({ did });
    const perm = await query.orderBy("permission_id", "desc").first();
    return perm?.permission_id != null ? String(perm.permission_id) : undefined;
  }

  return undefined;
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

  const entityId = await getEntityId(row, meta);
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
    const rawTrId = readNumber(row.content, [...ID_ALIASES.trustRegistry, "id"]);
    trId = rawTrId ? String(rawTrId) : entityId;
    const trDid = await loadTrustRegistryDid(rawTrId);
    if (trDid) collected.add(trDid);
  }

  if (meta.module === "credential-schema") {
    const rawSchemaId = readNumber(row.content, [...ID_ALIASES.credentialSchema, "id"]);
    const rawTrId = readNumber(row.content, ID_ALIASES.trustRegistry);
    const relation = await loadSchemaRelation(rawSchemaId);
    schemaId = relation.schemaId ?? (rawSchemaId ? String(rawSchemaId) : entityId);
    trId = relation.trId ?? (rawTrId ? String(rawTrId) : undefined);
    const trDid = relation.trDid ?? (await loadTrustRegistryDid(rawTrId));
    if (trDid) collected.add(trDid);
  }

  if (meta.module === "permission") {
    const rawPermissionId = readNumber(row.content, [...ID_ALIASES.permission, "id"]);
    const rawSchemaId = readNumber(row.content, ID_ALIASES.credentialSchema);
    const rawValidatorPermId = readNumber(row.content, ID_ALIASES.validatorPermission);
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
