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
import {
  extractTitleDescriptionFromJsonSchema,
  normalizeCredentialSchemaV4LedgerFields,
} from "../../modules/cs-height-sync/cs_height_sync_helpers";
import { overrideSchemaIdInString } from "../../common/utils/schema_id_normalizer";
import { isValidISO8601UTC } from "../../common/utils/date_utils";
import { getModuleParamsAction } from "../../common/utils/params_service";
import { buildActivityTimeline } from "../../common/utils/activity_timeline_helper";
import {
  mapCredentialSchemaApiFields,
  normalizeIssuerVerifierOnboardingModeForDbFilter,
} from "../../common/vpr-v4-mapping";
import {
  ensureDepositDefaultIfColumnExists,
  finalizeTrustRegistryHistoryInsert,
  resolvePermissionHistoryParticipantColumn,
  resolvePermissionsParticipantColumn,
  resolveTrustRegistryHistoryParticipantColumn,
  resolveTrustRegistryParticipantColumn,
} from "../../common/utils/installed_table_columns";

let heightColumnExistsCache: boolean | null = null;
let historyMetricColumnsExistCache: boolean | null = null;
let trHistoryColumnsCache: Set<string> | null = null;

const CS_V4_OPTIONAL_COLUMNS = [
  "holder_onboarding_mode",
  "pricing_asset_type",
  "pricing_asset",
  "digest_algorithm",
] as const;

type KnexSchemaLike = {
  schema: { hasColumn: (table: string, column: string) => Promise<boolean> };
};

const csV4OptionalColumnsPresentCache = new Map<
  "credential_schemas" | "credential_schema_history",
  Promise<ReadonlySet<string>>
>();

let didEnsureCsV4SchemaColumns = false;

async function ensureCsV4SchemaColumns(): Promise<void> {
  if (didEnsureCsV4SchemaColumns) return;
  await knex.raw(`
    ALTER TABLE IF EXISTS credential_schemas
      ADD COLUMN IF NOT EXISTS holder_onboarding_mode text,
      ADD COLUMN IF NOT EXISTS pricing_asset_type text,
      ADD COLUMN IF NOT EXISTS pricing_asset text,
      ADD COLUMN IF NOT EXISTS digest_algorithm text;
  `);
  await knex.raw(`
    ALTER TABLE IF EXISTS credential_schema_history
      ADD COLUMN IF NOT EXISTS holder_onboarding_mode text,
      ADD COLUMN IF NOT EXISTS pricing_asset_type text,
      ADD COLUMN IF NOT EXISTS pricing_asset text,
      ADD COLUMN IF NOT EXISTS digest_algorithm text;
  `);
  csV4OptionalColumnsPresentCache.clear();
  didEnsureCsV4SchemaColumns = true;
}

async function getCsV4OptionalColumnsPresent(
  db: KnexSchemaLike,
  table: "credential_schemas" | "credential_schema_history"
): Promise<ReadonlySet<string>> {
  let pending = csV4OptionalColumnsPresentCache.get(table);
  if (!pending) {
    pending = (async () => {
      const present = new Set<string>();
      for (const col of CS_V4_OPTIONAL_COLUMNS) {
        if (await db.schema.hasColumn(table, col)) present.add(col);
      }
      return present;
    })();
    csV4OptionalColumnsPresentCache.set(table, pending);
  }
  return pending;
}

async function stripCsV4OptionalFields(
  db: KnexSchemaLike,
  table: "credential_schemas" | "credential_schema_history",
  row: Record<string, unknown>
): Promise<Record<string, unknown>> {
  await ensureCsV4SchemaColumns();
  const present = await getCsV4OptionalColumnsPresent(db, table);
  const out: Record<string, unknown> = { ...row };
  for (const col of CS_V4_OPTIONAL_COLUMNS) {
    if (!present.has(col)) {
      delete out[col];
    }
  }
  return out;
}

async function alignCredentialSchemaRowToInstalledColumns(
  db: KnexSchemaLike,
  table: "credential_schemas" | "credential_schema_history",
  row: Record<string, unknown>
): Promise<Record<string, unknown>> {
  const out: Record<string, unknown> = { ...row };
  const hasIssuerV4 = await db.schema.hasColumn(table, "issuer_onboarding_mode");
  const hasIssuerV3 = await db.schema.hasColumn(table, "issuer_perm_management_mode");
  if (hasIssuerV4 && !hasIssuerV3 && "issuer_perm_management_mode" in out) {
    const r = out as {
      issuer_onboarding_mode?: unknown;
      issuer_perm_management_mode?: unknown;
    };
    if (r.issuer_onboarding_mode === undefined) {
      r.issuer_onboarding_mode = r.issuer_perm_management_mode;
    }
    delete r.issuer_perm_management_mode;
  }
  if (!hasIssuerV4 && hasIssuerV3 && "issuer_onboarding_mode" in out) {
    const r = out as {
      issuer_onboarding_mode?: unknown;
      issuer_perm_management_mode?: unknown;
    };
    r.issuer_perm_management_mode = r.issuer_onboarding_mode;
    delete r.issuer_onboarding_mode;
  }
  if (hasIssuerV4 && hasIssuerV3) {
    const r = out as {
      issuer_onboarding_mode?: unknown;
      issuer_perm_management_mode?: unknown;
    };
    if (r.issuer_onboarding_mode !== undefined && r.issuer_perm_management_mode !== undefined) {
      delete r.issuer_perm_management_mode;
    } else if (r.issuer_onboarding_mode === undefined && r.issuer_perm_management_mode !== undefined) {
      r.issuer_onboarding_mode = r.issuer_perm_management_mode;
      delete r.issuer_perm_management_mode;
    }
  }
  const hasVerifierV4 = await db.schema.hasColumn(table, "verifier_onboarding_mode");
  const hasVerifierV3 = await db.schema.hasColumn(table, "verifier_perm_management_mode");
  if (hasVerifierV4 && !hasVerifierV3 && "verifier_perm_management_mode" in out) {
    const r = out as {
      verifier_onboarding_mode?: unknown;
      verifier_perm_management_mode?: unknown;
    };
    if (r.verifier_onboarding_mode === undefined) {
      r.verifier_onboarding_mode = r.verifier_perm_management_mode;
    }
    delete r.verifier_perm_management_mode;
  }
  if (!hasVerifierV4 && hasVerifierV3 && "verifier_onboarding_mode" in out) {
    const r = out as {
      verifier_onboarding_mode?: unknown;
      verifier_perm_management_mode?: unknown;
    };
    r.verifier_perm_management_mode = r.verifier_onboarding_mode;
    delete r.verifier_onboarding_mode;
  }
  if (hasVerifierV4 && hasVerifierV3) {
    const r = out as {
      verifier_onboarding_mode?: unknown;
      verifier_perm_management_mode?: unknown;
    };
    if (r.verifier_onboarding_mode !== undefined && r.verifier_perm_management_mode !== undefined) {
      delete r.verifier_perm_management_mode;
    } else if (r.verifier_onboarding_mode === undefined && r.verifier_perm_management_mode !== undefined) {
      r.verifier_onboarding_mode = r.verifier_perm_management_mode;
      delete r.verifier_perm_management_mode;
    }
  }
  return out;
}

async function prepareCredentialSchemaHistoryRowForInsert(
  db: KnexSchemaLike,
  historyRow: Record<string, unknown>
): Promise<Record<string, unknown>> {
  let row = await stripCsV4OptionalFields(db, "credential_schema_history", historyRow);
  row = await alignCredentialSchemaRowToInstalledColumns(db, "credential_schema_history", row);
  const kdb = db as unknown as { (t: string): { columnInfo: () => Promise<Record<string, unknown>> } };
  const info = await kdb("credential_schema_history").columnInfo();
  const columns = new Set(Object.keys(info || {}));
  if (columns.has("deposit") && (row.deposit === undefined || row.deposit === null)) {
    row = { ...row, deposit: 0 };
  }
  const filtered: Record<string, unknown> = {};
  for (const k of Object.keys(row)) {
    if (!columns.has(k)) continue;
    const val = row[k];
    if (val === undefined) continue;
    filtered[k] = val;
  }
  return filtered;
}

function getDefaultCSStats(): any {
  return {
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

function addStatsToHistoryRow(historyRow: any, stats: any): void {
  const target = historyRow;
  target.participants = stats.participants;
  target.participants_ecosystem = stats.participants_ecosystem;
  target.participants_issuer_grantor = stats.participants_issuer_grantor;
  target.participants_issuer = stats.participants_issuer;
  target.participants_verifier_grantor = stats.participants_verifier_grantor;
  target.participants_verifier = stats.participants_verifier;
  target.participants_holder = stats.participants_holder;
  target.weight = Number(stats.weight ?? 0);
  target.issued = Number(stats.issued ?? 0);
  target.verified = Number(stats.verified ?? 0);
  target.ecosystem_slash_events = stats.ecosystem_slash_events;
  target.ecosystem_slashed_amount = Number(stats.ecosystem_slashed_amount ?? 0);
  target.ecosystem_slashed_amount_repaid = Number(stats.ecosystem_slashed_amount_repaid ?? 0);
  target.network_slash_events = stats.network_slash_events;
  target.network_slashed_amount = Number(stats.network_slashed_amount ?? 0);
  target.network_slashed_amount_repaid = Number(stats.network_slashed_amount_repaid ?? 0);
}

function getCSStatsUpdateObject(stats: any): any {
  return {
    participants: stats.participants,
    participants_ecosystem: stats.participants_ecosystem,
    participants_issuer_grantor: stats.participants_issuer_grantor,
    participants_issuer: stats.participants_issuer,
    participants_verifier_grantor: stats.participants_verifier_grantor,
    participants_verifier: stats.participants_verifier,
    participants_holder: stats.participants_holder,
    weight: Number(stats.weight ?? 0),
    issued: Number(stats.issued ?? 0),
    verified: Number(stats.verified ?? 0),
    ecosystem_slash_events: stats.ecosystem_slash_events,
    ecosystem_slashed_amount: Number(stats.ecosystem_slashed_amount ?? 0),
    ecosystem_slashed_amount_repaid: Number(stats.ecosystem_slashed_amount_repaid ?? 0),
    network_slash_events: stats.network_slash_events,
    network_slashed_amount: Number(stats.network_slashed_amount ?? 0),
    network_slashed_amount_repaid: Number(stats.network_slashed_amount_repaid ?? 0),
  };
}

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
  if (historyMetricColumnsExistCache === true) {
    return true;
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
    const allExist = checks.every(Boolean);
    if (allExist) historyMetricColumnsExistCache = true;
    return allExist;
  } catch {
    return false;
  }
}

async function getTrustRegistryHistoryColumns(db: any): Promise<Set<string>> {
  if (trHistoryColumnsCache) {
    return trHistoryColumnsCache;
  }
  const info = await db("trust_registry_history").columnInfo();
  trHistoryColumnsCache = new Set(Object.keys(info || {}));
  return trHistoryColumnsCache;
}

async function withDynamicTrustRegistryHistoryColumns(
  db: any,
  payload: Record<string, any>,
  trRow: Record<string, any>
): Promise<Record<string, any>> {
  const historyColumns = await getTrustRegistryHistoryColumns(db);
  return finalizeTrustRegistryHistoryInsert(historyColumns, payload, trRow) as Record<string, any>;
}

export async function syncTrustRegistryStatsAndHistoryFromSchemaChange(
  db: any,
  trIdRaw: unknown,
  blockHeightRaw: unknown
): Promise<void> {
  const trId = Number(trIdRaw);
  const blockHeight = Number(blockHeightRaw) || 0;
  if (!Number.isInteger(trId) || trId <= 0) return;

  const oldTr = await db("trust_registry").where("id", trId).first();
  if (!oldTr) return;

  let trStats: any;
  try {
    trStats = await calculateTrustRegistryStats(trId, undefined);
  } catch {
    return;
  }

  const trStatsUpdate: any = {
    participants: Number(trStats.participants ?? 0),
    participants_ecosystem: Number(trStats.participants_ecosystem ?? 0),
    participants_issuer_grantor: Number(trStats.participants_issuer_grantor ?? 0),
    participants_issuer: Number(trStats.participants_issuer ?? 0),
    participants_verifier_grantor: Number(trStats.participants_verifier_grantor ?? 0),
    participants_verifier: Number(trStats.participants_verifier ?? 0),
    participants_holder: Number(trStats.participants_holder ?? 0),
    active_schemas: Number(trStats.active_schemas ?? 0),
    archived_schemas: Number(trStats.archived_schemas ?? 0),
    weight: Number(trStats.weight ?? 0),
    issued: Number(trStats.issued ?? 0),
    verified: Number(trStats.verified ?? 0),
    ecosystem_slash_events: Number(trStats.ecosystem_slash_events ?? 0),
    ecosystem_slashed_amount: Number(trStats.ecosystem_slashed_amount ?? 0),
    ecosystem_slashed_amount_repaid: Number(trStats.ecosystem_slashed_amount_repaid ?? 0),
    network_slash_events: Number(trStats.network_slash_events ?? 0),
    network_slashed_amount: Number(trStats.network_slashed_amount ?? 0),
    network_slashed_amount_repaid: Number(trStats.network_slashed_amount_repaid ?? 0),
  };

  await db("trust_registry").where("id", trId).update(trStatsUpdate);
  const updatedTr = await db("trust_registry").where("id", trId).first();
  if (!updatedTr) return;

  const trChanges: Record<string, any> = {};
  for (const [key, value] of Object.entries(trStatsUpdate)) {
    const oldVal = Number(oldTr[key] ?? 0);
    const newVal = Number(value ?? 0);
    if (oldVal !== newVal) {
      trChanges[key] = newVal;
    }
  }
  if (Object.keys(trChanges).length === 0) return;

  const trHistoryPayload = await withDynamicTrustRegistryHistoryColumns(
    db,
    {
      tr_id: trId,
      did: updatedTr.did,
      corporation: updatedTr.corporation,
      created: updatedTr.created,
      modified: updatedTr.modified,
      archived: updatedTr.archived ?? null,
      aka: updatedTr.aka ?? null,
      language: updatedTr.language,
      active_version: updatedTr.active_version ?? null,
      participants: trStatsUpdate.participants,
      participants_ecosystem: trStatsUpdate.participants_ecosystem,
      participants_issuer_grantor: trStatsUpdate.participants_issuer_grantor,
      participants_issuer: trStatsUpdate.participants_issuer,
      participants_verifier_grantor: trStatsUpdate.participants_verifier_grantor,
      participants_verifier: trStatsUpdate.participants_verifier,
      participants_holder: trStatsUpdate.participants_holder,
      active_schemas: trStatsUpdate.active_schemas,
      archived_schemas: trStatsUpdate.archived_schemas,
      weight: trStatsUpdate.weight,
      issued: trStatsUpdate.issued,
      verified: trStatsUpdate.verified,
      ecosystem_slash_events: trStatsUpdate.ecosystem_slash_events,
      ecosystem_slashed_amount: trStatsUpdate.ecosystem_slashed_amount,
      ecosystem_slashed_amount_repaid: trStatsUpdate.ecosystem_slashed_amount_repaid,
      network_slash_events: trStatsUpdate.network_slash_events,
      network_slashed_amount: trStatsUpdate.network_slashed_amount,
      network_slashed_amount_repaid: trStatsUpdate.network_slashed_amount_repaid,
      event_type: "StatsUpdate",
      height: blockHeight,
      changes: JSON.stringify(trChanges),
      created_at: updatedTr.modified ?? updatedTr.created ?? new Date(),
    },
    updatedTr
  );

  const existingSameEvent = await db("trust_registry_history")
    .where({
      tr_id: trId,
      event_type: "StatsUpdate",
      height: blockHeight,
    })
    .orderBy("id", "desc")
    .first();
  const existingChanges = existingSameEvent?.changes ? String(existingSameEvent.changes) : null;
  const nextChanges = trHistoryPayload?.changes ? String(trHistoryPayload.changes) : null;
  if (existingSameEvent && existingChanges === nextChanges) {
    return;
  }

  await db("trust_registry_history").insert(trHistoryPayload);
}


export async function insertCredentialSchemaHistoryStatsRow(
  db: any,
  schemaId: number,
  blockHeight: number,
  stats: any
): Promise<void> {
  if (!schemaId || !Number.isInteger(schemaId) || schemaId <= 0) return;
  const schemaRow = await db("credential_schemas").where("id", schemaId).first();
  if (!schemaRow) return;
  const hasHeightColumn = await checkHeightColumnExists();
  const historyRow = mapToHistoryRow(schemaRow, {
    changes: JSON.stringify({ stats_update: true }),
    action: "stats_update",
    height: blockHeight,
  }, hasHeightColumn);
  if (stats) addStatsToHistoryRow(historyRow, stats);
  const historyRowForDb = await prepareCredentialSchemaHistoryRowForInsert(
    db,
    historyRow as Record<string, unknown>
  );
  await db("credential_schema_history").insert(historyRowForDb);
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
    issuer_grantor_validation_validity_period: row.issuer_grantor_validation_validity_period || 0,
    verifier_grantor_validation_validity_period: row.verifier_grantor_validation_validity_period || 0,
    issuer_validation_validity_period: row.issuer_validation_validity_period || 0,
    verifier_validation_validity_period: row.verifier_validation_validity_period || 0,
    holder_validation_validity_period: row.holder_validation_validity_period || 0,
    issuer_onboarding_mode:
      row.issuer_onboarding_mode ?? (row as { issuer_perm_management_mode?: unknown }).issuer_perm_management_mode ?? null,
    verifier_onboarding_mode:
      row.verifier_onboarding_mode ?? (row as { verifier_perm_management_mode?: unknown }).verifier_perm_management_mode ?? null,
    holder_onboarding_mode: (row as any).holder_onboarding_mode ?? null,
    pricing_asset_type: (row as any).pricing_asset_type ?? null,
    pricing_asset: (row as any).pricing_asset ?? null,
    digest_algorithm: (row as any).digest_algorithm ?? null,
    archived: row.archived ?? null,
    is_active: row.is_active ?? false,
    created: row.created ?? null,
    modified: row.modified ?? null,
    changes: overrides.changes ?? null,
    action: overrides.action ?? "unknown",
    created_at: row.modified ?? row.created ?? knex.fn.now(),
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
          const updatesAligned = await alignCredentialSchemaRowToInstalledColumns(
            trx,
            "credential_schemas",
            updates as Record<string, unknown>
          );
          const updatesForDb = await stripCsV4OptionalFields(
            trx,
            "credential_schemas",
            updatesAligned
          );
          const [updated] = await trx("credential_schemas")
            .where({ id: existingSchema.id })
            .update(updatesForDb)
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
          const insertAligned = await alignCredentialSchemaRowToInstalledColumns(
            trx,
            "credential_schemas",
            insertPayload as Record<string, unknown>
          );
          const insertForDb = await stripCsV4OptionalFields(
            trx,
            "credential_schemas",
            insertAligned
          );
          await ensureDepositDefaultIfColumnExists(
            trx,
            "credential_schemas",
            insertForDb,
            (insertPayload as Record<string, unknown>).deposit
          );
          const [inserted] = await trx("credential_schemas")
            .insert(insertForDb)
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
        
        let stats;
        try {
          stats = await calculateCredentialSchemaStats(finalRecord.id, blockHeight);
        } catch (statsError: any) {
          this.logger.warn(`Failed to calculate stats for CS ${finalRecord.id} at height ${blockHeight}: ${statsError?.message || String(statsError)}`);
          stats = getDefaultCSStats();
        }

        const oldStats = existingSchema ? {
          participants: existingSchema.participants ?? 0,
          participants_ecosystem: existingSchema.participants_ecosystem ?? 0,
          participants_issuer_grantor: existingSchema.participants_issuer_grantor ?? 0,
          participants_issuer: existingSchema.participants_issuer ?? 0,
          participants_verifier_grantor: existingSchema.participants_verifier_grantor ?? 0,
          participants_verifier: existingSchema.participants_verifier ?? 0,
          participants_holder: existingSchema.participants_holder ?? 0,
          weight: Number(existingSchema.weight ?? 0),
          issued: Number(existingSchema.issued ?? 0),
          verified: Number(existingSchema.verified ?? 0),
          ecosystem_slash_events: existingSchema.ecosystem_slash_events ?? 0,
          ecosystem_slashed_amount: Number(existingSchema.ecosystem_slashed_amount ?? 0),
          ecosystem_slashed_amount_repaid: Number(existingSchema.ecosystem_slashed_amount_repaid ?? 0),
          network_slash_events: existingSchema.network_slash_events ?? 0,
          network_slashed_amount: Number(existingSchema.network_slashed_amount ?? 0),
          network_slashed_amount_repaid: Number(existingSchema.network_slashed_amount_repaid ?? 0),
        } : null;

        const statsChanged = oldStats ? (
          oldStats.participants !== stats.participants ||
          oldStats.participants_ecosystem !== stats.participants_ecosystem ||
          oldStats.participants_issuer_grantor !== stats.participants_issuer_grantor ||
          oldStats.participants_issuer !== stats.participants_issuer ||
          oldStats.participants_verifier_grantor !== stats.participants_verifier_grantor ||
          oldStats.participants_verifier !== stats.participants_verifier ||
          oldStats.participants_holder !== stats.participants_holder ||
          oldStats.weight !== Number(stats.weight ?? 0) ||
          oldStats.issued !== Number(stats.issued ?? 0) ||
          oldStats.verified !== Number(stats.verified ?? 0) ||
          oldStats.ecosystem_slash_events !== stats.ecosystem_slash_events ||
          oldStats.ecosystem_slashed_amount !== Number(stats.ecosystem_slashed_amount ?? 0) ||
          oldStats.ecosystem_slashed_amount_repaid !== Number(stats.ecosystem_slashed_amount_repaid ?? 0) ||
          oldStats.network_slash_events !== stats.network_slash_events ||
          oldStats.network_slashed_amount !== Number(stats.network_slashed_amount ?? 0) ||
          oldStats.network_slashed_amount_repaid !== Number(stats.network_slashed_amount_repaid ?? 0)
        ) : true;

        if (Object.keys(creationChanges).length > 0 || statsChanged) {
          const historyRow = mapToHistoryRow(finalRecord, {
            changes: Object.keys(creationChanges).length > 0 ? JSON.stringify(creationChanges) : null,
            action: historyAction,
            height: blockHeight,
          }, hasHeightColumn);
          
          if (stats) {
            addStatsToHistoryRow(historyRow, stats);
          }

          const historyRowForDb = await prepareCredentialSchemaHistoryRowForInsert(
            trx,
            historyRow as Record<string, unknown>
          );
          await trx("credential_schema_history").insert(historyRowForDb);
        }

        try {
          await trx("credential_schemas")
            .where("id", finalRecord.id)
            .update(getCSStatsUpdateObject(stats));
          await syncTrustRegistryStatsAndHistoryFromSchemaChange(trx, finalRecord.tr_id, blockHeight);
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

      const updatesAligned = await alignCredentialSchemaRowToInstalledColumns(
        knex,
        "credential_schemas",
        updatesWithoutHeight as Record<string, unknown>
      );
      const updatesForDb = await stripCsV4OptionalFields(
        knex,
        "credential_schemas",
        updatesAligned
      );
      let [updated] = await knex("credential_schemas")
        .where({ id: existing.id })
        .update(updatesForDb)
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

      let stats;
      try {
        stats = await calculateCredentialSchemaStats(existing.id, blockHeight);
      } catch (statsError: any) {
        this.logger.warn(`Failed to calculate stats for CS ${existing.id} at height ${blockHeight}: ${statsError?.message || String(statsError)}`);
        stats = getDefaultCSStats();
      }

      const oldStats = {
        participants: existing.participants ?? 0,
        participants_ecosystem: existing.participants_ecosystem ?? 0,
        participants_issuer_grantor: existing.participants_issuer_grantor ?? 0,
        participants_issuer: existing.participants_issuer ?? 0,
        participants_verifier_grantor: existing.participants_verifier_grantor ?? 0,
        participants_verifier: existing.participants_verifier ?? 0,
        participants_holder: existing.participants_holder ?? 0,
        weight: Number(existing.weight ?? 0),
        issued: Number(existing.issued ?? 0),
        verified: Number(existing.verified ?? 0),
        ecosystem_slash_events: existing.ecosystem_slash_events ?? 0,
        ecosystem_slashed_amount: Number(existing.ecosystem_slashed_amount ?? 0),
        ecosystem_slashed_amount_repaid: Number(existing.ecosystem_slashed_amount_repaid ?? 0),
        network_slash_events: existing.network_slash_events ?? 0,
        network_slashed_amount: Number(existing.network_slashed_amount ?? 0),
        network_slashed_amount_repaid: Number(existing.network_slashed_amount_repaid ?? 0),
      };

      const statsChanged = 
        oldStats.participants !== stats.participants ||
        oldStats.participants_ecosystem !== stats.participants_ecosystem ||
        oldStats.participants_issuer_grantor !== stats.participants_issuer_grantor ||
        oldStats.participants_issuer !== stats.participants_issuer ||
        oldStats.participants_verifier_grantor !== stats.participants_verifier_grantor ||
        oldStats.participants_verifier !== stats.participants_verifier ||
        oldStats.participants_holder !== stats.participants_holder ||
        oldStats.weight !== Number(stats.weight ?? 0) ||
        oldStats.issued !== Number(stats.issued ?? 0) ||
        oldStats.verified !== Number(stats.verified ?? 0) ||
        oldStats.ecosystem_slash_events !== stats.ecosystem_slash_events ||
        oldStats.ecosystem_slashed_amount !== Number(stats.ecosystem_slashed_amount ?? 0) ||
        oldStats.ecosystem_slashed_amount_repaid !== Number(stats.ecosystem_slashed_amount_repaid ?? 0) ||
        oldStats.network_slash_events !== stats.network_slash_events ||
        oldStats.network_slashed_amount !== Number(stats.network_slashed_amount ?? 0) ||
        oldStats.network_slashed_amount_repaid !== Number(stats.network_slashed_amount_repaid ?? 0);

      if (Object.keys(changes).length > 0 || statsChanged) {
        const hasHeightColumn = await checkHeightColumnExists();
        
        const historyRow = mapToHistoryRow(updated, {
          changes: Object.keys(changes).length > 0 ? JSON.stringify(changes) : null,
          action: "update",
          height: blockHeight,
        }, hasHeightColumn);
        
        if (stats) {
          addStatsToHistoryRow(historyRow, stats);
        }

        const historyRowForDb = await prepareCredentialSchemaHistoryRowForInsert(
          knex,
          historyRow as Record<string, unknown>
        );
        await knex("credential_schema_history").insert(historyRowForDb);
      }

      try {
        await knex("credential_schemas")
          .where("id", existing.id)
          .update(getCSStatsUpdateObject(stats));
        await syncTrustRegistryStatsAndHistoryFromSchemaChange(knex, updated.tr_id, blockHeight);
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
      const hasHistoryMetricColumns = await checkHistoryMetricColumnsExist();
      
      let stats;
      try {
        stats = await calculateCredentialSchemaStats(id, blockHeight);
        } catch (statsError: any) {
          this.logger.warn(`Failed to calculate stats for CS ${id} at height ${blockHeight}: ${statsError?.message || String(statsError)}`);
          stats = getDefaultCSStats();
        }
        
        const historyRow = mapToHistoryRow(updated, {
          changes: JSON.stringify({
            archived: updated.archived,
          }),
          action: archiveFlag ? "archive" : "unarchive",
          height: blockHeight,
        }, hasHeightColumn);
        
        if (stats) {
          addStatsToHistoryRow(historyRow, stats);
        }

        const historyRowForDb = await prepareCredentialSchemaHistoryRowForInsert(
          knex,
          historyRow as Record<string, unknown>
        );
        await knex("credential_schema_history").insert(historyRowForDb);

      try {
        await knex("credential_schemas")
          .where("id", id)
          .update(getCSStatsUpdateObject(stats));
        await syncTrustRegistryStatsAndHistoryFromSchemaChange(knex, updated.tr_id, blockHeight);
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
      Object.assign(
        schema as Record<string, unknown>,
        normalizeCredentialSchemaV4LedgerFields(schema as Record<string, unknown>)
      );
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
        issuer_grantor_validation_validity_period: Number(schema.issuer_grantor_validation_validity_period ?? 0),
        verifier_grantor_validation_validity_period: Number(schema.verifier_grantor_validation_validity_period ?? 0),
        issuer_validation_validity_period: Number(schema.issuer_validation_validity_period ?? 0),
        verifier_validation_validity_period: Number(schema.verifier_validation_validity_period ?? 0),
        holder_validation_validity_period: Number(schema.holder_validation_validity_period ?? 0),
        issuer_onboarding_mode: String(schema.issuer_onboarding_mode ?? schema.issuerOnboardingMode ?? schema.issuer_perm_management_mode ?? schema.issuerPermManagementMode ?? "MODE_UNSPECIFIED"),
        verifier_onboarding_mode: String(schema.verifier_onboarding_mode ?? schema.verifierOnboardingMode ?? schema.verifier_perm_management_mode ?? schema.verifierPermManagementMode ?? "MODE_UNSPECIFIED"),
        holder_onboarding_mode:
          schema.holder_onboarding_mode != null || schema.holderOnboardingMode != null
            ? String(schema.holder_onboarding_mode ?? schema.holderOnboardingMode ?? "")
            : null,
        pricing_asset_type:
          schema.pricing_asset_type != null || schema.pricingAssetType != null
            ? String(schema.pricing_asset_type ?? schema.pricingAssetType ?? "")
            : null,
        pricing_asset:
          schema.pricing_asset != null || schema.pricingAsset != null
            ? String(schema.pricing_asset ?? schema.pricingAsset ?? "")
            : null,
        digest_algorithm:
          schema.digest_algorithm != null || schema.digestAlgorithm != null
            ? String(schema.digest_algorithm ?? schema.digestAlgorithm ?? "")
            : null,
        archived: normalizedArchived,
        created: schema.created ?? null,
        modified: schema.modified ?? null,
        is_active: derivedIsActive,
      };
      if (typeof schema.title === "string") payload.title = schema.title;
      if (typeof schema.description === "string") payload.description = schema.description;
      const existing = await knex("credential_schemas").where({ id }).first();
      const payloadAligned = await alignCredentialSchemaRowToInstalledColumns(
        knex,
        "credential_schemas",
        payload as Record<string, unknown>
      );
      const payloadForDb = await stripCsV4OptionalFields(
        knex,
        "credential_schemas",
        payloadAligned
      );
      const updates: Record<string, unknown> = { ...payloadForDb };
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
        const insertPayload = { ...payloadForDb };
        (insertPayload as Record<string, unknown>).json_schema = (insertPayload as Record<string, unknown>).json_schema ?? "{}";
        await ensureDepositDefaultIfColumnExists(
          knex,
          "credential_schemas",
          insertPayload as Record<string, unknown>,
          (schema as Record<string, unknown>).deposit
        );
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
        let stats;
        try {
          stats = await calculateCredentialSchemaStats(id, blockHeightNum);
        } catch (statsError: any) {
          this.logger.warn(`Failed to calculate stats for CS ${id} at height ${blockHeightNum} in syncFromLedger: ${statsError?.message || String(statsError)}`);
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
        
        if (stats) {
          addStatsToHistoryRow(historyRow, stats);
        }

        const historyRowForDb = await prepareCredentialSchemaHistoryRowForInsert(
          knex,
          historyRow as Record<string, unknown>
        );
        await knex("credential_schema_history").insert(historyRowForDb);
        
        try {
          await knex("credential_schemas")
            .where("id", id)
            .update({
              participants: stats.participants,
              participants_ecosystem: stats.participants_ecosystem,
              participants_issuer_grantor: stats.participants_issuer_grantor,
              participants_issuer: stats.participants_issuer,
              participants_verifier_grantor: stats.participants_verifier_grantor,
              participants_verifier: stats.participants_verifier,
              participants_holder: stats.participants_holder,
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
        } catch (statsUpdateError: any) {
          this.logger.warn(`Failed to update stats for CS ${id} in syncFromLedger: ${statsUpdateError?.message || String(statsUpdateError)}`);
        }
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
        const hasHistoryMetricColumns = await checkHistoryMetricColumnsExist();
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
          issuer_grantor_validation_validity_period: historyRecord.issuer_grantor_validation_validity_period,
          verifier_grantor_validation_validity_period: historyRecord.verifier_grantor_validation_validity_period,
          issuer_validation_validity_period: historyRecord.issuer_validation_validity_period,
          verifier_validation_validity_period: historyRecord.verifier_validation_validity_period,
          holder_validation_validity_period: historyRecord.holder_validation_validity_period,
          issuer_onboarding_mode: (historyRecord as any).issuer_onboarding_mode ?? null,
          verifier_onboarding_mode: (historyRecord as any).verifier_onboarding_mode ?? null,
          holder_onboarding_mode: (historyRecord as any).holder_onboarding_mode ?? null,
          pricing_asset_type: (historyRecord as any).pricing_asset_type ?? null,
          pricing_asset: (historyRecord as any).pricing_asset ?? null,
          digest_algorithm: (historyRecord as any).digest_algorithm ?? null,
          archived: historyRecord.archived,
          created: historyRecord.created,
          modified: historyRecord.modified,
        };

        let stats: ReturnType<typeof getDefaultCSStats>;
        if (hasHistoryMetricColumns) {
          stats = {
            participants: Number(historyRecord.participants ?? 0),
            participants_ecosystem: Number(historyRecord.participants_ecosystem ?? 0),
            participants_issuer_grantor: Number(historyRecord.participants_issuer_grantor ?? 0),
            participants_issuer: Number(historyRecord.participants_issuer ?? 0),
            participants_verifier_grantor: Number(historyRecord.participants_verifier_grantor ?? 0),
            participants_verifier: Number(historyRecord.participants_verifier ?? 0),
            participants_holder: Number(historyRecord.participants_holder ?? 0),
            weight: Number(historyRecord.weight ?? 0),
            issued: Number(historyRecord.issued ?? 0),
            verified: Number(historyRecord.verified ?? 0),
            ecosystem_slash_events: Number(historyRecord.ecosystem_slash_events ?? 0),
            ecosystem_slashed_amount: Number(historyRecord.ecosystem_slashed_amount ?? 0),
            ecosystem_slashed_amount_repaid: Number(historyRecord.ecosystem_slashed_amount_repaid ?? 0),
            network_slash_events: Number(historyRecord.network_slash_events ?? 0),
            network_slashed_amount: Number(historyRecord.network_slashed_amount ?? 0),
            network_slashed_amount_repaid: Number(historyRecord.network_slashed_amount_repaid ?? 0),
          };
        } else {
          stats = getDefaultCSStats();
        }

        return ApiResponder.success(ctx, {
          schema: mapCredentialSchemaApiFields({
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
          } as Record<string, unknown>),
        }, 200);
      }

      const schemaRecord = await knex("credential_schemas").where({ id }).first();
      if (!schemaRecord) {
        return ApiResponder.error(ctx, `Credential schema with id=${id} not found`, 404);
      }
      delete schemaRecord?.is_active;
      const storedSchemaString = getStoredSchemaString(schemaRecord.json_schema);

      const stats = {
        participants: Number(schemaRecord.participants ?? 0),
        participants_ecosystem: Number((schemaRecord as any).participants_ecosystem ?? 0),
        participants_issuer_grantor: Number((schemaRecord as any).participants_issuer_grantor ?? 0),
        participants_issuer: Number((schemaRecord as any).participants_issuer ?? 0),
        participants_verifier_grantor: Number((schemaRecord as any).participants_verifier_grantor ?? 0),
        participants_verifier: Number((schemaRecord as any).participants_verifier ?? 0),
        participants_holder: Number((schemaRecord as any).participants_holder ?? 0),
        weight: Number(schemaRecord.weight ?? 0),
        issued: Number(schemaRecord.issued ?? 0),
        verified: Number(schemaRecord.verified ?? 0),
        ecosystem_slash_events: Number(schemaRecord.ecosystem_slash_events ?? 0),
        ecosystem_slashed_amount: Number(schemaRecord.ecosystem_slashed_amount ?? 0),
        ecosystem_slashed_amount_repaid: Number(schemaRecord.ecosystem_slashed_amount_repaid ?? 0),
        network_slash_events: Number(schemaRecord.network_slash_events ?? 0),
        network_slashed_amount: Number(schemaRecord.network_slashed_amount ?? 0),
        network_slashed_amount_repaid: Number(schemaRecord.network_slashed_amount_repaid ?? 0),
      };

      return ApiResponder.success(ctx, {
        schema: mapCredentialSchemaApiFields({
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
        } as Record<string, unknown>),
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
      issuer_onboarding_mode: { type: "string", optional: true },
      verifier_onboarding_mode: { type: "string", optional: true },
      holder_onboarding_mode: { type: "string", optional: true },
      response_max_size: { type: "number", optional: true, default: 64 },
      sort: { type: "string", optional: true, default: "-modified" },
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
    issuer_onboarding_mode?: string;
    verifier_onboarding_mode?: string;
    holder_onboarding_mode?: string;
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
        issuer_onboarding_mode: issuerOnboardingModeParam,
        verifier_onboarding_mode: verifierOnboardingModeParam,
        holder_onboarding_mode: holderOnboardingModeParam,
      } = ctx.params;

      const issuerOmTrimmed =
        issuerOnboardingModeParam !== undefined ? String(issuerOnboardingModeParam).trim() : "";
      let effectiveIssuerOm: string | undefined;
      if (issuerOmTrimmed !== "") {
        effectiveIssuerOm = normalizeIssuerVerifierOnboardingModeForDbFilter(issuerOmTrimmed);
      }

      const verifierOmTrimmed =
        verifierOnboardingModeParam !== undefined ? String(verifierOnboardingModeParam).trim() : "";
      let effectiveVerifierOm: string | undefined;
      if (verifierOmTrimmed !== "") {
        effectiveVerifierOm = normalizeIssuerVerifierOnboardingModeForDbFilter(verifierOmTrimmed);
      }

      let effectiveHolderOnboarding: string | undefined;
      const holderOmTrimmed =
        holderOnboardingModeParam !== undefined ? String(holderOnboardingModeParam).trim() : "";
      if (holderOmTrimmed !== "") {
        effectiveHolderOnboarding = holderOmTrimmed;
      }

      const participantValidation = validateParticipantParam(participant, "participant");
      if (!participantValidation.valid) {
        return ApiResponder.error(ctx, participantValidation.error, 400);
      }
      const participantAccount = participantValidation.value;

      const effectiveSort = sort ?? "-modified";
      try {
        validateSortParameter(effectiveSort);
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
              if (effectiveIssuerOm !== undefined) qb.where("csh.issuer_onboarding_mode", effectiveIssuerOm);
              if (effectiveVerifierOm !== undefined) qb.where("csh.verifier_onboarding_mode", effectiveVerifierOm);
              if (effectiveHolderOnboarding !== undefined) qb.where("csh.holder_onboarding_mode", effectiveHolderOnboarding);
              applyMetricRangeFilters(qb);
            })
            .orderBy("csh.credential_schema_id", "asc")
            .modify((qb) => {
              if (hasHeightColumn) qb.orderBy("csh.height", "desc");
            })
            .orderBy("csh.created_at", "desc")
            .orderBy("csh.id", "desc")
            .as("latest");
          const orderedLatest = applyOrdering(knex.from(latestSub).select("*"), effectiveSort);
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
              if (effectiveIssuerOm !== undefined) qb.where("csh.issuer_onboarding_mode", effectiveIssuerOm);
              if (effectiveVerifierOm !== undefined) qb.where("csh.verifier_onboarding_mode", effectiveVerifierOm);
              if (effectiveHolderOnboarding !== undefined) qb.where("csh.holder_onboarding_mode", effectiveHolderOnboarding);
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
            issuer_grantor_validation_validity_period: historyRecord.issuer_grantor_validation_validity_period,
            verifier_grantor_validation_validity_period: historyRecord.verifier_grantor_validation_validity_period,
            issuer_validation_validity_period: historyRecord.issuer_validation_validity_period,
            verifier_validation_validity_period: historyRecord.verifier_validation_validity_period,
            holder_validation_validity_period: historyRecord.holder_validation_validity_period,
              issuer_onboarding_mode: (historyRecord as any).issuer_onboarding_mode ?? null,
              verifier_onboarding_mode: (historyRecord as any).verifier_onboarding_mode ?? null,
              holder_onboarding_mode: (historyRecord as any).holder_onboarding_mode ?? null,
              pricing_asset_type: (historyRecord as any).pricing_asset_type ?? null,
              pricing_asset: (historyRecord as any).pricing_asset ?? null,
              digest_algorithm: (historyRecord as any).digest_algorithm ?? null,
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
          issuer_grantor_validation_validity_period: any;
          verifier_grantor_validation_validity_period: any;
          issuer_validation_validity_period: any;
          verifier_validation_validity_period: any;
          holder_validation_validity_period: any;
          issuer_onboarding_mode: any;
          verifier_onboarding_mode: any;
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
            const statsRows = await knex("credential_schema_history as csh")
              .select(
                "csh.credential_schema_id",
                "csh.participants",
                "csh.participants_ecosystem",
                "csh.participants_issuer_grantor",
                "csh.participants_issuer",
                "csh.participants_verifier_grantor",
                "csh.participants_verifier",
                "csh.participants_holder",
                "csh.weight",
                "csh.issued",
                "csh.verified",
                "csh.ecosystem_slash_events",
                "csh.ecosystem_slashed_amount",
                "csh.ecosystem_slashed_amount_repaid",
                "csh.network_slash_events",
                "csh.network_slashed_amount",
                "csh.network_slashed_amount_repaid"
              )
              .whereIn("csh.credential_schema_id", filteredItems.map((i) => Number(i.id)))
              .andWhere("csh.height", "<=", blockHeight)
              .andWhereRaw(
                `NOT EXISTS (
                  SELECT 1 FROM credential_schema_history csh2
                  WHERE csh2.credential_schema_id = csh.credential_schema_id
                    AND csh2.height <= ?
                    AND (csh2.height > csh.height OR (csh2.height = csh.height AND csh2.created_at > csh.created_at))
                )`,
                [blockHeight]
              );

            const statsMap = new Map<number, any>();
            for (const row of statsRows) {
              statsMap.set(Number(row.credential_schema_id), row);
            }

            schemasWithStats = filteredItems.map((item) => {
              const stats = statsMap.get(Number(item.id)) || {};
              return {
                ...item,
                participants: Number(stats.participants ?? 0),
                participants_ecosystem: Number(stats.participants_ecosystem ?? 0),
                participants_issuer_grantor: Number(stats.participants_issuer_grantor ?? 0),
                participants_issuer: Number(stats.participants_issuer ?? 0),
                participants_verifier_grantor: Number(stats.participants_verifier_grantor ?? 0),
                participants_verifier: Number(stats.participants_verifier ?? 0),
                participants_holder: Number(stats.participants_holder ?? 0),
                weight: Number(stats.weight ?? 0),
                issued: Number(stats.issued ?? 0),
                verified: Number(stats.verified ?? 0),
                ecosystem_slash_events: Number(stats.ecosystem_slash_events ?? 0),
                ecosystem_slashed_amount: Number(stats.ecosystem_slashed_amount ?? 0),
                ecosystem_slashed_amount_repaid: Number(stats.ecosystem_slashed_amount_repaid ?? 0),
                network_slash_events: Number(stats.network_slash_events ?? 0),
                network_slashed_amount: Number(stats.network_slashed_amount ?? 0),
                network_slashed_amount_repaid: Number(stats.network_slashed_amount_repaid ?? 0),
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
                "network_slashed_amount_repaid"
              );

            for (const stat of schemaStats) {
              schemaStatsMap.set(stat.id, stat);
            }
          }

          const defaultStats = {
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
          schemasWithStats = filteredItems.map((item) => {
            const stats = schemaStatsMap.get(item.id) || defaultStats;
            const num = (v: any) => (typeof v === "number" ? v : Number(v || 0));
            return {
              ...item,
              participants: num(stats.participants),
              participants_ecosystem: num((stats as any).participants_ecosystem),
              participants_issuer_grantor: num((stats as any).participants_issuer_grantor),
              participants_issuer: num((stats as any).participants_issuer),
              participants_verifier_grantor: num((stats as any).participants_verifier_grantor),
              participants_verifier: num((stats as any).participants_verifier),
              participants_holder: num((stats as any).participants_holder),
              weight: num(stats.weight),
              issued: num(stats.issued),
              verified: num(stats.verified),
              ecosystem_slash_events: num(stats.ecosystem_slash_events),
              ecosystem_slashed_amount: num(stats.ecosystem_slashed_amount),
              ecosystem_slashed_amount_repaid: num(stats.ecosystem_slashed_amount_repaid),
              network_slash_events: num(stats.network_slash_events),
              network_slashed_amount: num(stats.network_slashed_amount),
              network_slashed_amount_repaid: num(stats.network_slashed_amount_repaid),
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
        const sortedItems = sortCredentialSchemaRows(typedFilteredItems, effectiveSort, limit);

        return ApiResponder.success(
          ctx,
          {
            schemas: sortedItems.map((s) => mapCredentialSchemaApiFields(s as Record<string, unknown>)),
          },
          200
        );
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

      if (effectiveIssuerOm !== undefined) {
        query.where("issuer_onboarding_mode", effectiveIssuerOm);
      }

      if (effectiveVerifierOm !== undefined) {
        query.where("verifier_onboarding_mode", effectiveVerifierOm);
      }

      if (effectiveHolderOnboarding !== undefined) {
        query.where("holder_onboarding_mode", effectiveHolderOnboarding);
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
        : sortCredentialSchemaRows(filteredItems as SchemaWithStats[], effectiveSort, limit);

      return ApiResponder.success(ctx, {
        schemas: sortedItems.map((s) => mapCredentialSchemaApiFields(s as Record<string, unknown>)),
      }, 200);
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
    const trPart = await resolveTrustRegistryParticipantColumn(knex);
    const controllerTrRows = await knex("trust_registry")
      .where(trPart, account)
      .select("id");
    const controllerTrIds = controllerTrRows.map((r: { id: number }) => r.id);
    const schemaIdsFromController =
      controllerTrIds.length === 0
        ? []
        : (await knex("credential_schemas").whereIn("tr_id", controllerTrIds).select("id")).map((r: { id: number }) => r.id);

    const permPart = await resolvePermissionsParticipantColumn(knex);
    const granteeRows = await knex("permissions").where(permPart, account).distinct("schema_id");
    const schemaIdsFromGrantee = granteeRows
      .map((r: { schema_id: string }) => (r.schema_id != null ? parseFloat(r.schema_id) : null))
      .filter((id): id is number => id != null && !Number.isNaN(id));

    return [...new Set([...schemaIdsFromController, ...schemaIdsFromGrantee])];
  }

  private async getCredentialSchemaIdsForParticipantAtHeight(account: string, blockHeight: number): Promise<number[]> {
    const trHistPart = await resolveTrustRegistryHistoryParticipantColumn(knex);
    const trHistoryRows = await knex("trust_registry_history")
      .where("height", "<=", blockHeight)
      .where(trHistPart, account)
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

    const permHistPart = await resolvePermissionHistoryParticipantColumn(knex);
    const granteePermRows = await knex("permission_history")
      .where("height", "<=", blockHeight)
      .where(permHistPart, account)
      .distinct("schema_id");
    const schemaIdsFromGrantee = granteePermRows
      .map((r: { schema_id: number }) => r.schema_id)
      .filter((id): id is number => id != null);

    return [...new Set([...schemaIdsFromController, ...schemaIdsFromGrantee])];
  }

  @Action({
    name: "getSchemaAuthorizationPolicy",
    params: { id: { type: "number", integer: true, positive: true } },
  })
  async getSchemaAuthorizationPolicy(ctx: Context<{ id: number }>) {
    const row = await knex("schema_authorization_policies")
      .where({ id: ctx.params.id })
      .first();
    if (!row) {
      return ApiResponder.error(ctx, "SchemaAuthorizationPolicy not found", 404);
    }
    return ApiResponder.success(ctx, { schema_authorization_policy: row }, 200);
  }

  @Action({
    name: "listSchemaAuthorizationPolicies",
    params: {
      schema_id: { type: "number", integer: true, positive: true, optional: true },
      role: { type: "string", optional: true },
    },
  })
  async listSchemaAuthorizationPolicies(ctx: Context<{
    schema_id?: number;
    role?: string;
  }>) {
    let q = knex("schema_authorization_policies").select("*");
    if (ctx.params.schema_id) {
      q = q.where("schema_id", ctx.params.schema_id);
    }
    if (ctx.params.role) {
      q = q.where("role", ctx.params.role);
    }
    const rows = await q.orderBy("schema_id", "asc").orderBy("version", "asc");
    return ApiResponder.success(ctx, { schema_authorization_policies: rows }, 200);
  }
}
