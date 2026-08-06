import type {
  CredentialSchemaRef,
  IRegistryAdapter,
  Participant,
  ParticipantRole,
  VerifiablePublicRegistry,
} from '@verana-labs/verre'
import knex from '../../common/utils/db_connection'
import { toIso } from '../../common/utils/date_utils'
import { calculateParticipantState } from '../crawl-pp/pp_state_utils'

type SchemaRow = {
  id: number
  json_schema: unknown
}

type AdapterOptions = {
  enabled: boolean
}

type AnchoringParticipant = Pick<
  Participant,
  'id' | 'role' | 'created' | 'effective_from' | 'effective_until' | 'participant_state'
>

function asSchemaJsonString(value: unknown): string {
  if (typeof value === 'string') return value
  return JSON.stringify(value ?? {})
}

function positiveInt(value: unknown): number | null {
  const n = Number(value)
  if (!Number.isInteger(n) || n <= 0) return null
  return n
}

export function schemaIdFromUrl(url: string): number | null {
  const direct = positiveInt(url)
  if (direct) return direct
  const colonForm = /(?:^|[:/])cs(?:[:/]v\d+[:/]js)?[:/](\d+)$/.exec(url)
  if (colonForm) return positiveInt(colonForm[1])
  try {
    const segments = new URL(url).pathname.split('/').filter(Boolean)
    return positiveInt(segments.at(-1))
  } catch {
    return null
  }
}

async function findSchemaByUrl(url: string): Promise<SchemaRow | null> {
  const trimmed = String(url ?? '').trim()
  if (!trimmed) return null

  const directId = schemaIdFromUrl(trimmed)
  if (directId) {
    const row = await knex('credential_schemas')
      .select('id', 'json_schema')
      .where('id', directId)
      .whereNull('archived')
      .first()
    if (row) return { id: Number((row as any).id), json_schema: (row as any).json_schema }
  }

  const isPg = String((knex as { client?: { config?: { client?: string } } }).client?.config?.client || '').includes(
    'pg'
  )
  if (isPg) {
    const row = await knex('credential_schemas')
      .select('id', 'json_schema')
      .whereRaw(
        `
        (is_active IS NULL OR is_active = true)
        AND archived IS NULL
        AND (
          json_schema::jsonb->>'$id' = ?
          OR json_schema::jsonb->>'id' = ?
          OR json_schema::jsonb->>'@id' = ?
        )
        `,
        [trimmed, trimmed, trimmed]
      )
      .orderBy('id', 'desc')
      .first()
    if (!row) return null
    return { id: Number((row as any).id), json_schema: (row as any).json_schema }
  }

  const rows = await knex('credential_schemas').select('id', 'json_schema').whereNull('archived').limit(500)
  for (const row of rows as Array<{ id?: unknown; json_schema?: unknown }>) {
    const js = row.json_schema
    const s = typeof js === 'string' ? js : JSON.stringify(js ?? {})
    if (s.includes(trimmed)) {
      const id = Number(row.id)
      if (Number.isFinite(id) && id > 0) return { id, json_schema: js }
    }
  }
  return null
}

class IndexerRegistryAdapter implements IRegistryAdapter {
  private readonly schemaCache = new Map<string, SchemaRow | null>()

  private readonly credentialSchemaCache = new Map<number, CredentialSchemaRef | undefined>()

  private async cachedSchema(url: string): Promise<SchemaRow | null> {
    const key = String(url ?? '').trim()
    if (!key) return null
    if (this.schemaCache.has(key)) return this.schemaCache.get(key) ?? null
    const row = await findSchemaByUrl(key)
    this.schemaCache.set(key, row)
    return row
  }

  async fetchSchema(url: string): Promise<string> {
    const row = await this.cachedSchema(url)
    if (!row) {
      throw new Error(`Schema not found for URL: ${url}`)
    }
    return asSchemaJsonString(row.json_schema)
  }

  async fetchDigest(digestJCS: string): Promise<{ created: string; height?: number } | undefined> {
    if (!digestJCS) return undefined
    const row = (await knex('digests').select('created', 'height').where({ digest: digestJCS }).first()) as
      | { created?: Date | string | null; height?: number | null }
      | undefined
    const created = toIso(row?.created)
    if (!created) return undefined
    return { created, height: row?.height != null ? Number(row.height) : undefined }
  }

  async fetchCredentialSchema(schemaId: number): Promise<CredentialSchemaRef | undefined> {
    if (this.credentialSchemaCache.has(schemaId)) return this.credentialSchemaCache.get(schemaId)

    const row = (await knex('credential_schemas as cs')
      .leftJoin('ecosystem as e', 'e.id', 'cs.ecosystem_id')
      .select('cs.id', 'cs.ecosystem_id', 'cs.digest_algorithm', 'cs.json_schema', 'e.did as ecosystem_did')
      .where('cs.id', schemaId)
      .whereNull('cs.archived')
      .first()) as
      | { id?: unknown; ecosystem_id?: unknown; digest_algorithm?: unknown; json_schema?: unknown; ecosystem_did?: unknown }
      | undefined

    const result: CredentialSchemaRef | undefined = row
      ? {
          id: Number(row.id),
          ecosystemId: Number(row.ecosystem_id) || 0,
          ecosystemDid: typeof row.ecosystem_did === 'string' ? row.ecosystem_did : '',
          digestAlgorithm: typeof row.digest_algorithm === 'string' ? row.digest_algorithm : '',
          jsonSchema: asSchemaJsonString(row.json_schema),
        }
      : undefined
    this.credentialSchemaCache.set(schemaId, result)
    return result
  }

  async listParticipants(
    schemaId: number,
    did: string,
    role: ParticipantRole,
    when: string
  ): Promise<AnchoringParticipant[]> {
    const rows = (await knex('participants')
      .select('id', 'role', 'created', 'effective_from', 'effective_until', 'revoked', 'slashed', 'repaid')
      .where({ schema_id: schemaId, did, role: String(role) })
      .whereNotNull('effective_from')
      .andWhere('effective_from', '<=', when)
      .andWhere((qb) => qb.whereNull('effective_until').orWhere('effective_until', '>=', when))
      .andWhere((qb) => qb.whereNull('revoked').orWhere('revoked', '>', when))
      .andWhere((qb) => qb.whereNull('slashed').orWhere('slashed', '>', when))
      .andWhere((qb) => qb.whereNull('repaid').orWhere('repaid', '>', when))) as Array<{
      id: number
      role: string
      created: Date | string | null
      effective_from: Date | string | null
      effective_until: Date | string | null
      revoked: Date | string | null
      slashed: Date | string | null
      repaid: Date | string | null
    }>

    return rows.map((row) => ({
      id: Number(row.id),
      role: row.role as ParticipantRole,
      created: toIso(row.created) ?? '',
      effective_from: toIso(row.effective_from) ?? null,
      effective_until: toIso(row.effective_until) ?? null,
      participant_state: calculateParticipantState(
        {
          role: row.role as any,
          revoked: toIso(row.revoked) ?? null,
          slashed: toIso(row.slashed) ?? null,
          repaid: toIso(row.repaid) ?? null,
          effective_from: toIso(row.effective_from) ?? null,
          effective_until: toIso(row.effective_until) ?? null,
        },
        new Date(when)
      ) as Participant['participant_state'],
    }))
  }
}

export function attachRegistryAdapters(
  registries: VerifiablePublicRegistry[],
  options: AdapterOptions
): VerifiablePublicRegistry[] {
  if (!options.enabled) return registries
  const adapter = new IndexerRegistryAdapter()
  return registries.map((reg) => (reg.adapter ? reg : { ...reg, adapter }))
}
