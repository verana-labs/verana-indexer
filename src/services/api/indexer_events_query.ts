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

function readNumber(content: unknown, keys: string[]): number | null {
  if (!content || typeof content !== "object") return null;
  const obj = content as Record<string, unknown>;
  for (const key of keys) {
    const raw = obj[key];
    const n = Number(raw);
    if (Number.isInteger(n) && n > 0) return n;
  }
  return null;
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
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  if (!trimmed) return undefined;
  try {
    return decodeURIComponent(trimmed).trim();
  } catch {
    return trimmed;
  }
}

async function toIndexerEvent(row: EventRow): Promise<IndexerTxEvent | null> {
  const meta = EVENT_META[row.message_type];
  if (!meta) return null;

  const relatedDids = new Set<string>();
  addDid(relatedDids, row.sender);
  collectDids(row.content, relatedDids);
  const entityId = getEntityId(row, meta);

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
      .ignore()
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
    query.andWhere((builder) => {
      builder
        .where("ie.did", normalizedDid)
        .orWhereRaw("(ie.payload -> 'related_dids') \\? ?", [normalizedDid])
        .orWhereRaw("(ie.payload -> 'relatedDids') \\? ?", [normalizedDid]);
    });
  }
  applyBlockHeightFilter(query, args, "ie.block_height");

  const rows = (await query) as Array<Record<string, any>>;
  return rows.map(fromStoredRow);
}
