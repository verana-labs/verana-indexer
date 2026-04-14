
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

  if (historyColumns.has("corporation")) {
    const v =
      (trRow.corporation as string | undefined) ??
      (nextPayload.corporation as string | undefined) ??
      null;
    if (v != null) nextPayload.corporation = v;
  }
  delete nextPayload.controller;

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
  _db: KnexWithTable
): Promise<"corporation"> {
  return "corporation";
}

export async function resolveTrustRegistryHistoryParticipantColumn(
  _db: KnexWithTable
): Promise<"corporation"> {
  return "corporation";
}

export async function resolvePermissionsParticipantColumn(
  _db: KnexWithTable
): Promise<"corporation"> {
  return "corporation";
}

export async function resolvePermissionHistoryParticipantColumn(
  _db: KnexWithTable
): Promise<"corporation"> {
  return "corporation";
}

export async function resolveTrustDepositTableOwnerColumn(
  _db: KnexWithTable,
  _table: "trust_deposits" | "trust_deposit_history"
): Promise<"corporation"> {
  return "corporation";
}

export async function resolveTrustDepositTableBalanceColumn(
  _db: KnexWithTable,
  _table: "trust_deposits" | "trust_deposit_history"
): Promise<"deposit"> {
  return "deposit";
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
  const { trRow, rawLedger } = opts;
  const raw = rawLedger as Record<string, unknown> | undefined;
  const participant = firstNonEmptyString([trRow, raw], ["corporation"]);
  const didStr = String(trRow.did ?? "").trim();
  const pStr = participant ?? didStr;

  if (cols.has("corporation")) next.corporation = pStr;

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
