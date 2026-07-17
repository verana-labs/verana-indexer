import knex from '../../common/utils/db_connection'
import { buildVtResponse } from './trust-vt-response'

export type TrustDataMode = 'none' | 'summary' | 'full'

type TrustRowLite = {
  did: string
  height: number
  resolve_result: unknown
  evaluated_at?: unknown
  created_at?: unknown
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (!value || typeof value !== 'object') return false
  if (Array.isArray(value)) return false
  if (value instanceof Date) return false
  const proto = Object.getPrototypeOf(value)
  return proto === Object.prototype || proto === null
}

export function parseTrustDataMode(raw: unknown): { ok: true; mode: TrustDataMode } | { ok: false; message: string } {
  if (raw === undefined || raw === null || raw === '') return { ok: true, mode: 'none' }
  const normalized = String(raw).trim().toLowerCase()
  if (normalized === 'null' || normalized === 'none') return { ok: true, mode: 'none' }
  if (normalized === 'summary') return { ok: true, mode: 'summary' }
  if (normalized === 'full') return { ok: true, mode: 'full' }
  return { ok: false, message: 'Invalid "trust_data". Allowed values: null, summary, full' }
}

function collectDidsDeep(value: unknown, dids: Set<string>, seen: WeakSet<object>): void {
  if (!value || typeof value !== 'object') return
  if (value instanceof Date) return
  if (seen.has(value as object)) return
  seen.add(value as object)

  if (Array.isArray(value)) {
    for (const item of value) collectDidsDeep(item, dids, seen)
    return
  }

  if (!isPlainObject(value)) return

  const record = value as Record<string, unknown>
  if (typeof record.did === 'string' && record.did.startsWith('did:')) {
    dids.add(record.did)
  }

  for (const nested of Object.values(record)) {
    collectDidsDeep(nested, dids, seen)
  }
}

async function fetchTrustResultsLatestByDid(dids: string[], blockHeight?: number): Promise<Map<string, TrustRowLite>> {
  const out = new Map<string, TrustRowLite>()
  const uniq = Array.from(new Set(dids.filter((d) => typeof d === 'string' && d.startsWith('did:'))))
  if (uniq.length === 0) return out

  const chunkSize = 500
  const isPg = String((knex as any)?.client?.config?.client || '').includes('pg')
  for (let i = 0; i < uniq.length; i += chunkSize) {
    const chunk = uniq.slice(i, i + chunkSize)
    let q: any = knex('trust_results')
      .select(['did', 'height', 'resolve_result', 'evaluated_at', 'created_at'])
      .whereIn('did', chunk)
    if (typeof blockHeight === 'number' && Number.isFinite(blockHeight) && blockHeight >= 0) {
      q = q.andWhere('height', '<=', Math.trunc(blockHeight))
    }
    if (isPg) {
      q = q.distinctOn('did').orderBy('did', 'asc').orderBy('height', 'desc')
    } else {
      q = q.orderBy('did', 'asc').orderBy('height', 'desc')
    }
    const rows = (await q) as any[]
    for (const row of rows) {
      const did = String(row?.did ?? '')
      const height = Number(row?.height ?? NaN)
      if (!did.startsWith('did:') || !Number.isFinite(height) || out.has(did)) continue
      out.set(did, {
        did,
        height: Math.trunc(height),
        resolve_result: row?.resolve_result,
        evaluated_at: row?.evaluated_at,
        created_at: row?.created_at,
      })
    }
  }

  return out
}

async function buildTrustPayload(
  row: TrustRowLite,
  mode: TrustDataMode,
  blockHeight?: number
): Promise<Record<string, unknown> | null> {
  if (mode === 'none') return null

  return buildVtResponse(
    {
      did: row.did,
      resolveResult: row.resolve_result,
      evaluatedAtBlock: row.height,
      evaluatedAtSource: (row.evaluated_at ?? row.created_at) as Date | string | null | undefined,
      atHeight: blockHeight,
    },
    mode
  )
}

function injectTrustDataDeep<T>(
  value: T,
  mode: TrustDataMode,
  trustDataByDid: Map<string, Record<string, unknown> | null>,
  seen: WeakMap<object, unknown>
): T {
  if (!value || typeof value !== 'object') return value
  if (value instanceof Date) return value
  if (seen.has(value as object)) return seen.get(value as object) as T

  if (Array.isArray(value)) {
    const clonedArray: unknown[] = []
    seen.set(value as object, clonedArray)
    for (const item of value) {
      clonedArray.push(injectTrustDataDeep(item, mode, trustDataByDid, seen))
    }
    return clonedArray as T
  }

  if (!isPlainObject(value)) return value

  const record = value as Record<string, unknown>
  const cloned: Record<string, unknown> = {}
  seen.set(value as object, cloned)

  for (const [key, nested] of Object.entries(record)) {
    cloned[key] = injectTrustDataDeep(nested, mode, trustDataByDid, seen)
  }

  const did = record.did
  if (mode !== 'none' && typeof did === 'string' && did.startsWith('did:')) {
    const payload = trustDataByDid.get(did) ?? null
    cloned.trust_data = payload
  }

  return cloned as T
}

export async function enrichTrustDataDeep<T>(value: T, mode: TrustDataMode, blockHeight?: number): Promise<T> {
  if (mode === 'none') return value
  const dids = new Set<string>()
  collectDidsDeep(value, dids, new WeakSet<object>())

  const trustRowsByDid = await fetchTrustResultsLatestByDid(Array.from(dids), blockHeight)
  const trustDataByDid = new Map<string, Record<string, unknown> | null>()
  await Promise.all(
    Array.from(dids).map(async (did) => {
      const row = trustRowsByDid.get(did)
      trustDataByDid.set(did, row ? await buildTrustPayload(row, mode, blockHeight) : null)
    })
  )

  return injectTrustDataDeep(value, mode, trustDataByDid, new WeakMap<object, unknown>())
}
