import knex from "../../common/utils/db_connection";
import {
  VeranaCredentialSchemaMessageTypes,
  VeranaPermissionMessageTypes,
  VeranaTrustRegistryMessageTypes,
} from "../../common/verana-message-types";
import { applyBlockHeightFilter, isValidDid } from "./api_shared";

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
  id?: number;
  type: "indexer-event";
  eventType: string;
  did: string;
  blockHeight: number;
  txHash: string;
  timestamp: string;
  payload: {
    module: IndexerTxEvent["module"];
    action: string;
    messageType: string;
    txIndex: number;
    messageIndex: number;
    sender: string;
    relatedDids: string[];
    entityType?: string;
    entityId?: string;
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
  [VeranaPermissionMessageTypes.ExtendPermission]: {
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

const TABLE_COLUMNS_TTL_MS = 10 * 60 * 1000;
const tableColumnsCache = new Map<string, { expiresAt: number; value: Promise<Set<string>> }>();

const DID_QUERY_CACHE_TTL_MS = 60 * 1000;
const CACHE_MAX_ENTRIES = 5000;

class ExpiringBoundedMap<K, V extends { expiresAt: number }> extends Map<K, V> {
  constructor(private readonly maxEntries: number) {
    super();
  }

  private pruneExpired(now = Date.now()): void {
    for (const [key, entry] of super.entries()) {
      if (entry.expiresAt <= now) {
        super.delete(key);
      }
    }
  }

  override get(key: K): V | undefined {
    const entry = super.get(key);
    if (!entry) return undefined;
    if (entry.expiresAt <= Date.now()) {
      super.delete(key);
      return undefined;
    }
    return entry;
  }

  override set(key: K, value: V): this {
    this.pruneExpired();
    super.set(key, value);
    while (this.size > this.maxEntries) {
      const oldestKey = this.keys().next().value as K | undefined;
      if (oldestKey === undefined) break;
      super.delete(oldestKey);
    }
    return this;
  }
}

const permissionSnapshotCache = new ExpiringBoundedMap<string, { expiresAt: number; value: Promise<any> }>(CACHE_MAX_ENTRIES);
const credentialSchemaSnapshotCache = new ExpiringBoundedMap<string, { expiresAt: number; value: Promise<any> }>(CACHE_MAX_ENTRIES);
const trustRegistrySnapshotCache = new ExpiringBoundedMap<string, { expiresAt: number; value: Promise<any> }>(CACHE_MAX_ENTRIES);

function toIsoSeconds(value: Date | string): string {
  const date = value instanceof Date ? value : new Date(value);
  return date.toISOString().replace(/\.\d{3}Z$/, "Z");
}

function isDid(value: unknown): value is string {
  return isValidDid(value);
}

function addDid(out: Set<string>, value: unknown): void {
  if (isDid(value)) out.add(value);
}

function collectDids(value: unknown, out: Set<string>): void {
  if (isDid(value)) {
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

async function getTableColumns(tableName: string): Promise<Set<string>> {
  const now = Date.now();
  const cached = tableColumnsCache.get(tableName);
  if (cached && cached.expiresAt > now) return cached.value;

  const value = knex(tableName)
    .columnInfo()
    .then((info) => new Set(Object.keys(info)));
  tableColumnsCache.set(tableName, { expiresAt: now + TABLE_COLUMNS_TTL_MS, value });
  return value;
}

async function existingColumns(tableName: string, columns: string[]): Promise<string[]> {
  const available = await getTableColumns(tableName);
  return columns.filter((column) => available.has(column));
}

async function addTrustRegistryDids(trId: number | null, height: number, dids: Set<string>): Promise<void> {
  if (!trId) return;
  const now = Date.now();
  const cacheKey = `${trId}:${height}`;
  const cached = trustRegistrySnapshotCache.get(cacheKey);
  if (cached && cached.expiresAt > now) {
    const row = await cached.value;
    addDid(dids, (row as any)?.did);
    addDid(dids, (row as any)?.controller);
    return;
  }
  const columns = await existingColumns("trust_registry_history", ["did", "controller"]);
  if (columns.length === 0) return;

  const value = knex("trust_registry_history")
    .select(columns)
    .where("tr_id", trId)
    .where("height", "<=", height)
    .orderBy("height", "desc")
    .first();
  trustRegistrySnapshotCache.set(cacheKey, { expiresAt: now + DID_QUERY_CACHE_TTL_MS, value });

  const row = await value;
  addDid(dids, (row as any)?.did);
  addDid(dids, (row as any)?.controller);
}

async function enrichRelatedDids(row: EventRow, meta: EventMeta, dids: Set<string>): Promise<string | undefined> {
  if (meta.module === "permission") {
    const permissionId = readNumber(row.content, ["id", "permission_id", "permissionId", "perm_id", "permId"]);
    if (!permissionId) return undefined;
    const now = Date.now();
    const permCacheKey = `${permissionId}:${row.block_height}`;
    const cachedPerm = permissionSnapshotCache.get(permCacheKey);
    const columns = await existingColumns("permission_history", [
      "permission_id",
      "did",
      "grantee",
      "created_by",
      "extended_by",
      "revoked_by",
      "slashed_by",
      "repaid_by",
      "schema_id",
    ]);
    const permPromise =
      cachedPerm && cachedPerm.expiresAt > now
        ? cachedPerm.value
        : knex("permission_history")
          .select(columns)
          .where("permission_id", permissionId)
          .where("height", "<=", row.block_height)
          .orderBy("height", "desc")
          .first();
    if (!cachedPerm || cachedPerm.expiresAt <= now) {
      permissionSnapshotCache.set(permCacheKey, { expiresAt: now + DID_QUERY_CACHE_TTL_MS, value: permPromise });
    }
    const perm = await permPromise;
    addDid(dids, (perm as any)?.did);
    addDid(dids, (perm as any)?.grantee);
    addDid(dids, (perm as any)?.created_by);
    addDid(dids, (perm as any)?.extended_by);
    addDid(dids, (perm as any)?.revoked_by);
    addDid(dids, (perm as any)?.slashed_by);
    addDid(dids, (perm as any)?.repaid_by);
    const schemaId = Number((perm as any)?.schema_id);
    if (Number.isInteger(schemaId) && schemaId > 0) {
      const csCacheKey = `${schemaId}:${row.block_height}`;
      const cachedCs = credentialSchemaSnapshotCache.get(csCacheKey);
      const csPromise =
        cachedCs && cachedCs.expiresAt > now
          ? cachedCs.value
          : knex("credential_schema_history")
            .select("tr_id")
            .where("credential_schema_id", schemaId)
            .where("height", "<=", row.block_height)
            .orderBy("height", "desc")
            .first();
      if (!cachedCs || cachedCs.expiresAt <= now) {
        credentialSchemaSnapshotCache.set(csCacheKey, { expiresAt: now + DID_QUERY_CACHE_TTL_MS, value: csPromise });
      }
      const cs = await csPromise;
      await addTrustRegistryDids(Number((cs as any)?.tr_id) || null, row.block_height, dids);
    }
    return String(permissionId);
  }

  if (meta.module === "credential-schema") {
    const schemaId = readNumber(row.content, ["id", "schema_id", "schemaId", "credential_schema_id", "credentialSchemaId"]);
    const trIdFromContent = readNumber(row.content, ["tr_id", "trId", "trust_registry_id", "trustRegistryId"]);
    let trId = trIdFromContent;
    if (schemaId) {
      const columns = await existingColumns("credential_schema_history", ["credential_schema_id", "tr_id"]);
      const cs = await knex("credential_schema_history")
        .select(columns)
        .where("credential_schema_id", schemaId)
        .where("height", "<=", row.block_height)
        .orderBy("height", "desc")
        .first();
      trId = Number((cs as any)?.tr_id) || trId;
    }
    await addTrustRegistryDids(trId, row.block_height, dids);
    return schemaId ? String(schemaId) : undefined;
  }

  const trId =
    readNumber(row.content, ["id", "tr_id", "trId", "trust_registry_id", "trustRegistryId"]) ??
    readNumber(row.content, ["gfv_id", "gfvId", "gfd_id", "gfdId"]);
  await addTrustRegistryDids(trId, row.block_height, dids);
  return trId ? String(trId) : undefined;
}

async function toIndexerEvent(row: EventRow): Promise<IndexerTxEvent | null> {
  const meta = EVENT_META[row.message_type];
  if (!meta) return null;

  const relatedDids = new Set<string>();
  addDid(relatedDids, row.sender);
  collectDids(row.content, relatedDids);
  const entityId = await enrichRelatedDids(row, meta, relatedDids);

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
      messageType: event.messageType,
      txIndex: event.txIndex,
      messageIndex: event.messageIndex,
      sender: event.sender,
      relatedDids: event.relatedDids,
      entityType: event.entityType,
      entityId: event.entityId,
    },
  }));
}

function fromStoredRow(row: Record<string, any>): IndexerEventRecord {
  return {
    id: Number(row.id),
    type: "indexer-event",
    eventType: String(row.event_type),
    did: String(row.did),
    blockHeight: Number(row.block_height),
    txHash: String(row.tx_hash),
    timestamp: toIsoSeconds(row.timestamp),
    payload: {
      module: row.payload?.module ?? row.module,
      action: row.payload?.action ?? row.event_type,
      messageType: row.payload?.messageType ?? row.message_type,
      txIndex: Number(row.payload?.txIndex ?? row.tx_index ?? 0),
      messageIndex: Number(row.payload?.messageIndex ?? row.message_index ?? 0),
      sender: String(row.payload?.sender ?? ""),
      relatedDids: Array.isArray(row.payload?.relatedDids) ? row.payload.relatedDids : [String(row.did)],
      entityType: row.payload?.entityType ?? row.entity_type ?? undefined,
      entityId: row.payload?.entityId ?? row.entity_id ?? undefined,
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
