import type { Knex } from 'knex'

type KnexWithTable = Knex | Knex.Transaction

export function finalizeEcosystemHistoryInsert(
  historyColumns: Set<string>,
  payload: Record<string, unknown>,
  ecosystemRow: Record<string, unknown>
): Record<string, unknown> {
  const reservedColumns = new Set(['id', 'ecosystem_id', 'event_type', 'height', 'changes', 'created_at'])
  const nextPayload: Record<string, unknown> = { ...payload }

  for (const column of historyColumns) {
    if (reservedColumns.has(column) || Object.hasOwn(nextPayload, column)) {
      continue
    }
    if (Object.hasOwn(ecosystemRow, column)) {
      nextPayload[column] = ecosystemRow[column]
    }
  }

  if (historyColumns.has('corporation_id')) {
    const v =
      (ecosystemRow.corporation_id as number | undefined) ?? (nextPayload.corporation_id as number | undefined) ?? 0
    nextPayload.corporation_id = Number(v ?? 0) || 0
  }
  delete nextPayload.controller
  delete nextPayload.corporation

  if (historyColumns.has('deposit')) {
    const dep = nextPayload.deposit ?? ecosystemRow.deposit ?? 0
    nextPayload.deposit = Number(dep ?? 0)
  } else {
    delete nextPayload.deposit
  }

  const filtered: Record<string, unknown> = {}
  for (const k of Object.keys(nextPayload)) {
    if (!historyColumns.has(k)) continue
    const val = nextPayload[k]
    if (val === undefined) continue
    filtered[k] = val
  }
  return filtered
}

async function tableColumnNames(db: KnexWithTable, table: string): Promise<Set<string>> {
  const info = await db(table).columnInfo()
  return new Set(Object.keys(info || {}))
}

export async function resolveEcosystemParticipantColumn(_db: KnexWithTable): Promise<'corporation_id'> {
  return 'corporation_id'
}

export async function resolveEcosystemHistoryParticipantColumn(_db: KnexWithTable): Promise<'corporation_id'> {
  return 'corporation_id'
}

export async function resolveParticipantsParticipantColumn(_db: KnexWithTable): Promise<'corporation_id'> {
  return 'corporation_id'
}

export async function resolveParticipantHistoryParticipantColumn(_db: KnexWithTable): Promise<'corporation_id'> {
  return 'corporation_id'
}

export async function resolveTrustDepositTableOwnerColumn(
  _db: KnexWithTable,
  _table: 'trust_deposits' | 'trust_deposit_history'
): Promise<'corporation'> {
  return 'corporation'
}

export async function resolveTrustDepositTableBalanceColumn(
  _db: KnexWithTable,
  _table: 'trust_deposits' | 'trust_deposit_history'
): Promise<'deposit'> {
  return 'deposit'
}

export async function ensureDepositDefaultIfColumnExists(
  db: KnexWithTable,
  table: string,
  row: Record<string, unknown>,
  rawDeposit?: unknown
): Promise<void> {
  const cols = await tableColumnNames(db, table)
  if (!cols.has('deposit')) return
  const deposit = row.deposit !== undefined && row.deposit !== null ? Number(row.deposit) : Number(rawDeposit ?? 0)
  Object.assign(row, { deposit })
}

export async function prepareEcosystemSnapshotRowForInsert(
  db: KnexWithTable,
  row: Record<string, unknown>,
  opts: { ecosystemRow: Record<string, unknown>; rawLedger?: Record<string, unknown> }
): Promise<Record<string, unknown>> {
  const next: Record<string, unknown> = { ...row }
  const info = await db('ecosystem_snapshot').columnInfo()
  const cols = new Set(Object.keys(info || {}))
  const { ecosystemRow } = opts

  if (cols.has('corporation_id')) {
    next.corporation_id = Number(ecosystemRow.corporation_id ?? 0) || 0
  }
  delete next.corporation

  await ensureDepositDefaultIfColumnExists(db, 'ecosystem_snapshot', next, ecosystemRow.deposit)

  const filtered: Record<string, unknown> = {}
  for (const k of Object.keys(next)) {
    if (!cols.has(k)) continue
    const v = next[k]
    if (v === undefined) continue
    filtered[k] = v
  }
  return filtered
}
