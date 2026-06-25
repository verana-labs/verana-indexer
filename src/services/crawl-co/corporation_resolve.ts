import type { Knex } from 'knex'
import knexDefault from '../../common/utils/db_connection'
import { extractController } from '../../common/utils/extract_controller'

export async function resolveCorporationIdByAddress(
  address: string | null | undefined,
  db: Knex | Knex.Transaction = knexDefault
): Promise<number | null> {
  if (!address || typeof address !== 'string' || !address.trim()) return null
  const row = await db('corporation').select('id').where({ corporation: address.trim() }).first()
  const id = Number(row?.id)
  return Number.isInteger(id) && id > 0 ? id : null
}

export async function resolveAddressByCorporationId(
  corporationId: number | null | undefined,
  db: Knex | Knex.Transaction = knexDefault
): Promise<string | null> {
  if (!corporationId || !Number.isInteger(corporationId) || corporationId <= 0) return null
  const row = await db('corporation').select('corporation').where({ id: corporationId }).first()
  const addr = row?.corporation
  return typeof addr === 'string' && addr.trim() ? addr : null
}

export async function resolveCorporationIdForMessage(
  message: Record<string, any> | null | undefined,
  db: Knex | Knex.Transaction = knexDefault
): Promise<number> {
  const direct = Number(message?.corporation_id ?? message?.corporationId ?? 0) || 0
  if (direct > 0) return direct
  const controller = extractController(message ?? {})
  if (controller) {
    const id = await resolveCorporationIdByAddress(controller, db)
    if (id) return id
  }
  return 0
}

export async function resolveAddressesByCorporationIds(
  corporationIds: Array<number | null | undefined>,
  db: Knex | Knex.Transaction = knexDefault
): Promise<Map<number, string>> {
  const ids = Array.from(new Set(corporationIds.map((v) => Number(v)).filter((v) => Number.isInteger(v) && v > 0)))
  const map = new Map<number, string>()
  if (ids.length === 0) return map
  const rows = await db('corporation').select('id', 'corporation').whereIn('id', ids)
  for (const r of rows as Array<{ id: number; corporation: string | null }>) {
    if (typeof r.corporation === 'string' && r.corporation.trim()) {
      map.set(Number(r.id), r.corporation)
    }
  }
  return map
}
