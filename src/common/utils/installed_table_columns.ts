
import type { Knex } from "knex";

type KnexWithTable = Knex | Knex.Transaction;

export function finalizeTrustRegistryHistoryInsert(
  historyColumns: Set<string>,
  payload: Record<string, unknown>,
  trRow: Record<string, unknown>
): Record<string, unknown> {
  const reservedColumns = new Set(["id", "tr_id", "event_type", "height", "changes", "created_at"]);
  const nextPayload: Record<string, unknown> = { ...payload };

  for (const column of historyColumns) {
    if (reservedColumns.has(column) || Object.prototype.hasOwnProperty.call(nextPayload, column)) {
      continue;
    }
    if (Object.prototype.hasOwnProperty.call(trRow, column)) {
      nextPayload[column] = trRow[column];
    }
  }

  const participantVal =
    trRow.corporation ??
    trRow.controller ??
    nextPayload.corporation ??
    nextPayload.controller ??
    null;

  const hasCorp = historyColumns.has("corporation");
  const hasCtrl = historyColumns.has("controller");

  if (hasCorp && hasCtrl) {
    const v =
      (participantVal as string | null) ??
      (nextPayload.corporation as string | undefined) ??
      (nextPayload.controller as string | undefined) ??
      null;
    if (v != null) {
      nextPayload.corporation = v;
      nextPayload.controller = v;
    }
  } else if (hasCorp) {
    const v =
      (participantVal as string | null) ??
      (nextPayload.corporation as string | undefined) ??
      (nextPayload.controller as string | undefined) ??
      null;
    if (v != null) nextPayload.corporation = v;
    delete nextPayload.controller;
  } else if (hasCtrl) {
    const v =
      (participantVal as string | null) ??
      (nextPayload.controller as string | undefined) ??
      (nextPayload.corporation as string | undefined) ??
      null;
    if (v != null) nextPayload.controller = v;
    delete nextPayload.corporation;
  } else {
    delete nextPayload.controller;
    delete nextPayload.corporation;
  }

  if (historyColumns.has("deposit")) {
    const dep = nextPayload.deposit ?? trRow.deposit ?? 0;
    nextPayload.deposit = Number(dep ?? 0);
  } else {
    delete nextPayload.deposit;
  }

  const filtered: Record<string, unknown> = {};
  for (const k of Object.keys(nextPayload)) {
    if (!historyColumns.has(k)) continue;
    const val = nextPayload[k];
    if (val === undefined) continue;
    filtered[k] = val;
  }
  return filtered;
}

async function tableColumnNames(db: KnexWithTable, table: string): Promise<Set<string>> {
  const info = await db(table).columnInfo();
  return new Set(Object.keys(info || {}));
}

export async function resolveTrustRegistryParticipantColumn(
  db: KnexWithTable
): Promise<"corporation" | "controller"> {
  const cols = await tableColumnNames(db, "trust_registry");
  return cols.has("corporation") ? "corporation" : "controller";
}

export async function resolveTrustRegistryHistoryParticipantColumn(
  db: KnexWithTable
): Promise<"corporation" | "controller"> {
  const cols = await tableColumnNames(db, "trust_registry_history");
  return cols.has("corporation") ? "corporation" : "controller";
}

export async function resolvePermissionsParticipantColumn(
  db: KnexWithTable
): Promise<"corporation" | "grantee" | "controller"> {
  const cols = await tableColumnNames(db, "permissions");
  if (cols.has("corporation")) return "corporation";
  if (cols.has("grantee")) return "grantee";
  if (cols.has("controller")) return "controller";
  return "grantee";
}

export async function resolvePermissionHistoryParticipantColumn(
  db: KnexWithTable
): Promise<"corporation" | "grantee" | "controller"> {
  const cols = await tableColumnNames(db, "permission_history");
  if (cols.has("corporation")) return "corporation";
  if (cols.has("grantee")) return "grantee";
  if (cols.has("controller")) return "controller";
  return "grantee";
}


export async function ensureDepositDefaultIfColumnExists(
  db: KnexWithTable,
  table: string,
  row: Record<string, unknown>,
  rawDeposit?: unknown
): Promise<void> {
  const cols = await tableColumnNames(db, table);
  if (!cols.has("deposit")) return;
  const deposit =
    row.deposit !== undefined && row.deposit !== null
      ? Number(row.deposit)
      : Number(rawDeposit ?? 0);
  Object.assign(row, { deposit });
}

function firstNonEmptyString(
  sources: Array<Record<string, unknown> | undefined>,
  keys: string[]
): string | undefined {
  for (const src of sources) {
    if (!src) continue;
    for (const k of keys) {
      const v = src[k];
      if (v === null || v === undefined) continue;
      const s = String(v).trim();
      if (s !== "") return s;
    }
  }
  return undefined;
}

export async function prepareTrustRegistrySnapshotRowForInsert(
  db: KnexWithTable,
  row: Record<string, unknown>,
  opts: { trRow: Record<string, unknown>; rawLedger?: Record<string, unknown> }
): Promise<Record<string, unknown>> {
  const next: Record<string, unknown> = { ...row };
  const info = await db("trust_registry_snapshot").columnInfo();
  const cols = new Set(Object.keys(info || {}));
  if (!cols.has("corporation") && (await db.schema.hasColumn("trust_registry_snapshot", "corporation"))) {
    cols.add("corporation");
  }
  if (!cols.has("controller") && (await db.schema.hasColumn("trust_registry_snapshot", "controller"))) {
    cols.add("controller");
  }
  const { trRow, rawLedger } = opts;
  const raw = rawLedger as Record<string, unknown> | undefined;
  const participant = firstNonEmptyString(
    [trRow, raw],
    ["corporation", "controller", "Corporation", "Controller"]
  );
  const didStr = String(trRow.did ?? "").trim();
  const pStr = participant ?? didStr;

  if (cols.has("corporation")) next.corporation = pStr;
  if (cols.has("controller")) next.controller = pStr;

  await ensureDepositDefaultIfColumnExists(db, "trust_registry_snapshot", next, trRow.deposit);

  const filtered: Record<string, unknown> = {};
  for (const k of Object.keys(next)) {
    if (!cols.has(k)) continue;
    const v = next[k];
    if (v === undefined) continue;
    filtered[k] = v;
  }
  return filtered;
}
