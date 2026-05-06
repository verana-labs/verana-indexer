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
  [VeranaPermissionMessageTypes.CreateRootPermission]: {
    module: "permission",
    action: "CreateRootPermission",
    entityType: "Permission",
  },
  [VeranaPermissionMessageTypes.SelfCreatePermission]: {
    module: "permission",
    action: "CreatePermission",
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
    this.pruneExpired();
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

async function readEventAttributeNumber(args: {
  txId: number;
  messageIndex: number;
  keys: string[];
}): Promise<number | null> {
  const txId = Number(args.txId);
  const messageIndex = Number(args.messageIndex);
  if (!Number.isInteger(txId) || txId <= 0) return null;
  if (!Number.isInteger(messageIndex) || messageIndex < 0) return null;
  if (!args.keys || args.keys.length === 0) return null;

  async function query(whereMsgIndex: boolean): Promise<number | null> {
    const q = knex("event_attribute as ea")
      .innerJoin("event as e", "e.id", "ea.event_id")
      .select("ea.key", "ea.value")
      .where("e.tx_id", txId)
      .whereIn("ea.key", args.keys)
      .orderBy("ea.index", "asc");

    if (whereMsgIndex) {
      q.andWhere("e.tx_msg_index", messageIndex);
    } else {
      q.whereNull("e.tx_msg_index");
    }

    const rows = (await q) as Array<{ key?: unknown; value?: unknown }>;
    for (const row of rows) {
      const n = Number(row?.value);
      if (Number.isInteger(n) && n > 0) return n;
    }
    return null;
  }

  return (await query(true)) ?? (await query(false));
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

async function findTrustRegistryIdByDid(did: string, height: number): Promise<number | null> {
  if (!isValidDid(did)) return null;
  const h = Number(height);
  if (!Number.isInteger(h) || h <= 0) return null;
  const row = await knex("trust_registry_history")
    .select("tr_id")
    .where("did", did)
    .where("height", "<=", h)
    .orderBy("height", "desc")
    .orderBy("id", "desc")
    .first();
  const id = Number((row as any)?.tr_id);
  return Number.isInteger(id) && id > 0 ? id : null;
}

async function findCredentialSchemaIdByTrId(trId: number, height: number): Promise<number | null> {
  const t = Number(trId);
  const h = Number(height);
  if (!Number.isInteger(t) || t <= 0) return null;
  if (!Number.isInteger(h) || h <= 0) return null;
  const row = await knex("credential_schema_history")
    .select("credential_schema_id")
    .where("tr_id", t)
    .where("height", "<=", h)
    .orderBy("height", "desc")
    .orderBy("id", "desc")
    .first();
  const id = Number((row as any)?.credential_schema_id);
  return Number.isInteger(id) && id > 0 ? id : null;
}

async function findPermissionIdByActors(args: {
  did?: unknown;
  grantee?: unknown;
  createdBy?: unknown;
  height: number;
}): Promise<number | null> {
  const h = Number(args.height);
  if (!Number.isInteger(h) || h <= 0) return null;
  const did = typeof args.did === "string" ? args.did : null;
  const grantee = typeof args.grantee === "string" ? args.grantee : null;
  const createdBy = typeof args.createdBy === "string" ? args.createdBy : null;
  if (!isValidDid(did) && !isValidDid(grantee) && !isValidDid(createdBy)) return null;

  const q = knex("permission_history")
    .select("permission_id")
    .where("height", "<=", h)
    .orderBy("height", "desc")
    .orderBy("id", "desc")
    .limit(1);

  q.andWhere(function () {
    if (isValidDid(did)) this.orWhere("did", did);
    if (isValidDid(grantee)) this.orWhere("grantee", grantee);
    if (isValidDid(createdBy)) this.orWhere("created_by", createdBy);
  });

  const row = await q.first();
  const id = Number((row as any)?.permission_id);
  return Number.isInteger(id) && id > 0 ? id : null;
}

async function enrichRelatedDids(row: EventRow, meta: EventMeta, dids: Set<string>): Promise<string | undefined> {
  if (meta.module === "permission") {
    const permissionId =
      readNumber(row.content, ["id", "permission_id", "permissionId", "perm_id", "permId"]) ??
      (await readEventAttributeNumber({
        txId: row.tx_id,
        messageIndex: row.message_index,
        keys: ["permission_id", "permissionId", "perm_id", "permId", "id"],
      }));
    const resolvedPermissionId =
      permissionId ??
      (row.message_type === VeranaPermissionMessageTypes.SelfCreatePermission ||
      row.message_type === VeranaPermissionMessageTypes.CreateRootPermission
        ? await findPermissionIdByActors({
            did: (row.content as any)?.did,
            grantee: (row.content as any)?.grantee,
            createdBy: (row.content as any)?.creator ?? (row.content as any)?.created_by ?? (row.content as any)?.createdBy,
            height: row.block_height,
          })
        : null);
    if (!resolvedPermissionId) return undefined;
    const now = Date.now();
    const permCacheKey = `${resolvedPermissionId}:${row.block_height}`;
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
          .where("permission_id", resolvedPermissionId)
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
    return String(resolvedPermissionId);
  }

  if (meta.module === "credential-schema") {
    const schemaId =
      readNumber(row.content, ["id", "schema_id", "schemaId", "credential_schema_id", "credentialSchemaId"]) ??
      (await readEventAttributeNumber({
        txId: row.tx_id,
        messageIndex: row.message_index,
        keys: ["credential_schema_id", "credentialSchemaId", "schema_id", "schemaId", "id"],
      }));
    const trIdFromContent =
      readNumber(row.content, ["tr_id", "trId", "trust_registry_id", "trustRegistryId"]) ??
      (await readEventAttributeNumber({
        txId: row.tx_id,
        messageIndex: row.message_index,
        keys: ["trust_registry_id", "trustRegistryId", "tr_id", "trId"],
      }));
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
    const resolvedSchemaId =
      schemaId ??
      (row.message_type === VeranaCredentialSchemaMessageTypes.CreateCredentialSchema && trId
        ? await findCredentialSchemaIdByTrId(trId, row.block_height)
        : null);
    await addTrustRegistryDids(trId, row.block_height, dids);
    return resolvedSchemaId ? String(resolvedSchemaId) : undefined;
  }

  const trId =
    readNumber(row.content, ["id", "tr_id", "trId", "trust_registry_id", "trustRegistryId"]) ??
    (await readEventAttributeNumber({
      txId: row.tx_id,
      messageIndex: row.message_index,
      keys: ["trust_registry_id", "trustRegistryId", "tr_id", "trId", "id"],
    })) ??
    readNumber(row.content, ["gfv_id", "gfvId", "gfd_id", "gfdId"]) ??
    (await readEventAttributeNumber({
      txId: row.tx_id,
      messageIndex: row.message_index,
      keys: ["gfv_id", "gfvId", "gfd_id", "gfdId"],
    }));
  const resolvedTrId =
    trId ??
    (row.message_type === VeranaTrustRegistryMessageTypes.CreateTrustRegistry
      ? await findTrustRegistryIdByDid(String((row.content as any)?.did ?? ""), row.block_height)
      : null);
  await addTrustRegistryDids(resolvedTrId, row.block_height, dids);
  return resolvedTrId ? String(resolvedTrId) : undefined;
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

async function deriveMissingEntityIdFromHistory(row: Record<string, any>): Promise<string | null> {
  const messageType = String(row.payload?.message_type ?? row.payload?.messageType ?? row.message_type ?? "");
  const did = String(row.did ?? "");
  const height = Number(row.block_height);
  if (!messageType || !isValidDid(did)) return null;
  if (!Number.isInteger(height) || height <= 0) return null;

  if (messageType === VeranaTrustRegistryMessageTypes.CreateTrustRegistry) {
    const trId = await findTrustRegistryIdByDid(did, height);
    return trId ? String(trId) : null;
  }

  if (messageType === VeranaCredentialSchemaMessageTypes.CreateCredentialSchema) {
    const trId = await findTrustRegistryIdByDid(did, height);
    if (!trId) return null;
    const schemaId = await findCredentialSchemaIdByTrId(trId, height);
    return schemaId ? String(schemaId) : null;
  }

  if (
    messageType === VeranaPermissionMessageTypes.CreateRootPermission ||
    messageType === VeranaPermissionMessageTypes.CreatePermission
  ) {
    const permId = await findPermissionIdByActors({ did, grantee: did, createdBy: did, height });
    return permId ? String(permId) : null;
  }

  return null;
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
  const out = rows.map(fromStoredRow);

  const logger = (global as any).logger as { warn?: (...args: any[]) => void } | undefined;
  let backfillFailures = 0;
  const backfillFailureIds: number[] = [];

  await Promise.all(
    out.map(async (ev, i) => {
      const existing = ev.payload.entity_id;
      if (existing !== undefined && existing !== null && String(existing).length > 0) return;

      const derived = await deriveMissingEntityIdFromHistory(rows[i]);
      if (!derived) return;

      ev.payload.entity_id = derived;

      try {
        const id = Number(rows[i]?.id);
        if (Number.isInteger(id) && id > 0) {
          await knex("indexer_events")
            .where({ id })
            .update({
              entity_id: derived,
              payload: knex.raw(
                "jsonb_set(COALESCE(payload, '{}'::jsonb), '{entity_id}', to_jsonb(?::text), true)",
                [derived]
              ),
            });
        }
      } catch {
        backfillFailures += 1;
        const id = Number(rows[i]?.id);
        if (Number.isInteger(id) && id > 0 && backfillFailureIds.length < 10) {
          backfillFailureIds.push(id);
        }
      }
    })
  );

  if (backfillFailures > 0 && logger?.warn) {
    logger.warn(
      `[IndexerEvents] entity_id backfill failed for ${backfillFailures} row(s)` +
      (backfillFailureIds.length ? ` (sample ids: ${backfillFailureIds.join(", ")})` : "")
    );
  }

  return out;
}
