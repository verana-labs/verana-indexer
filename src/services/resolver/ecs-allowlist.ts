import knex from '../../common/utils/db_connection'
import config from '../../config.json' with { type: 'json' }

const ALLOWED_ECOSYSTEM_IDS_TTL_MS = 30_000

let cachedAllowedEcosystemIds: { ids: Set<number>; expiresAt: number } | null = null

function ecsEcosystemsFromEnv(): string[] {
  return (process.env.ECS_ECOSYSTEM_DIDS ?? '')
    .split(',')
    .map((did) => did.trim())
    .filter(Boolean)
}

export function getEcsEcosystems(): string[] {
  const c = config as unknown as { resolver?: { ecsEcosystems?: string[] } }
  const declared = c.resolver?.ecsEcosystems
  return declared && declared.length > 0 ? declared : ecsEcosystemsFromEnv()
}

export function isEcsAllowlistEnforced(): boolean {
  return getEcsEcosystems().length > 0
}

export async function getAllowedEcsEcosystemIds(): Promise<Set<number>> {
  const now = Date.now()
  if (cachedAllowedEcosystemIds && cachedAllowedEcosystemIds.expiresAt > now) return cachedAllowedEcosystemIds.ids

  const dids = getEcsEcosystems()
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
