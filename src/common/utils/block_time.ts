import type { Knex } from 'knex'
import knexDefault from './db_connection'

export type GetBlockChainTimeAsOfOptions = {
  db?: Knex | Knex.Transaction
  logContext?: string
  fallback?: Date
  logger?: { warn?: (msg: string, ...args: unknown[]) => void }
  atOrBefore?: boolean
}

export async function getBlockChainTimeAsOf(height: number, options?: GetBlockChainTimeAsOfOptions): Promise<Date> {
  const db = (options?.db ?? knexDefault) as Knex
  const logContext = options?.logContext ?? '[block_time]'
  const fallback = options?.fallback ?? new Date()
  const logWarn = options?.logger?.warn ?? (global as any)?.logger?.warn

  const atOrBefore = options?.atOrBefore === true
  const heightLabel = atOrBefore ? 'at or before' : 'at'

  try {
    const q = db('block').select('time')
    const blockRow = atOrBefore
      ? await q.where('height', '<=', height).orderBy('height', 'desc').first()
      : await q.where('height', height).first()
    if (blockRow?.time) {
      const t = new Date(blockRow.time)
      if (!Number.isNaN(t.getTime())) return t
      logWarn?.(`${logContext} block.time ${heightLabel} height ${height} is not a valid date; using fallback time`, {
        raw: blockRow.time,
      })
    }
  } catch (err: any) {
    logWarn?.(
      `${logContext} Failed to load block.time ${atOrBefore ? 'at or before' : 'for'} height ${height}; using fallback time`,
      err?.message ?? err
    )
  }

  return fallback
}

export type GetLatestIndexedBlockTimeOptions = Omit<GetBlockChainTimeAsOfOptions, 'atOrBefore'>

export async function getLatestIndexedBlockTime(options?: GetLatestIndexedBlockTimeOptions): Promise<Date> {
  const db = (options?.db ?? knexDefault) as Knex
  const logContext = options?.logContext ?? '[block_time]'
  const fallback = options?.fallback ?? new Date()
  const logWarn = options?.logger?.warn ?? (global as any)?.logger?.warn

  try {
    const blockRow = await db('block').select('time').orderBy('height', 'desc').first()
    if (blockRow?.time) {
      const t = new Date(blockRow.time)
      if (!Number.isNaN(t.getTime())) return t
      logWarn?.(`${logContext} latest block.time is not a valid date; using fallback time`, { raw: blockRow.time })
    }
  } catch (err: any) {
    logWarn?.(`${logContext} Failed to load the latest block.time; using fallback time`, err?.message ?? err)
  }

  return fallback
}

export async function resolveEvaluationTime(
  height: number | undefined,
  options?: GetLatestIndexedBlockTimeOptions
): Promise<Date> {
  if (height === undefined) return getLatestIndexedBlockTime(options)
  return getBlockChainTimeAsOf(height, { ...options, atOrBefore: true })
}
