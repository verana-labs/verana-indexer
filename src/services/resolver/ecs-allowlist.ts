import knex from '../../common/utils/db_connection'
import config from '../../config.json' with { type: 'json' }

export type EcsEcosystem = {
  did: string
  vpr: string
}

const ALLOWED_ECOSYSTEM_IDS_TTL_MS = 30_000

let cachedAllowedEcosystemIds: { ids: Set<number>; expiresAt: number } | null = null

function ecsEcosystemsFromEnv(): EcsEcosystem[] {
  const chainId = (process.env.CHAIN_ID ?? '').trim()
  const dids = (process.env.ECS_ECOSYSTEM_DIDS ?? '')
    .split(',')
    .map((did) => did.trim())
    .filter(Boolean)
  if (!chainId || dids.length === 0) return []
  return dids.map((did) => ({ did, vpr: `vpr:verana:${chainId}` }))
}

export function getEcsEcosystems(): EcsEcosystem[] {
  const c = config as unknown as { resolver?: { ecsEcosystems?: EcsEcosystem[] } }
  const declared = c.resolver?.ecsEcosystems
  return declared && declared.length > 0 ? declared : ecsEcosystemsFromEnv()
}

export function isEcsAllowlistEnforced(): boolean {
  return getEcsEcosystems().length > 0
}

export async function getAllowedEcsEcosystemIds(): Promise<Set<number>> {
  const now = Date.now()
  if (cachedAllowedEcosystemIds && cachedAllowedEcosystemIds.expiresAt > now) return cachedAllowedEcosystemIds.ids

  const dids = getEcsEcosystems().map((ecosystem) => ecosystem.did)
  const ids = new Set<number>()
  if (dids.length > 0) {
    const rows = (await knex('ecosystem').whereIn('did', dids).select('id')) as Array<{ id: number }>
    for (const row of rows) {
      const id = Number(row.id)
      if (Number.isFinite(id) && id > 0) ids.add(id)
    }
  }

  cachedAllowedEcosystemIds = { ids, expiresAt: now + ALLOWED_ECOSYSTEM_IDS_TTL_MS }
  return ids
}

export async function isEcosystemEcsAllowlisted(ecosystemId: number): Promise<boolean> {
  if (!isEcsAllowlistEnforced()) return true
  if (!Number.isFinite(ecosystemId) || ecosystemId <= 0) return false
  return (await getAllowedEcsEcosystemIds()).has(ecosystemId)
}
