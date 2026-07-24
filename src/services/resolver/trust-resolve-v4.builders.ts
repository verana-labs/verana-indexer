import { createHash } from 'node:crypto'
import { fetchJson } from '@verana-labs/verre'
import { canonicalizeJson, toCoin } from '../../common'
import { ALL_PARTICIPANT_ROLES, type ParticipantRole, type ParticipantState } from '../../common/types/types'
import { toDate, toIso } from '../../common/utils/date_utils'
import knex from '../../common/utils/db_connection'
import { isEcosystemEcsAllowlisted, isEcsAllowlistEnforced } from './ecs-allowlist'

/**
 * Derives the single `participant_state` enum from a permission row's
 * lifecycle timestamps, following the priority order defined in the indexer
 * spec (Participant state derivation table).
 */
export function deriveParticipantState(
  row: {
    slashed?: Date | string | null
    repaid?: Date | string | null
    revoked?: Date | string | null
    effective_from?: Date | string | null
    effective_until?: Date | string | null
  },
  now: Date
): ParticipantState {
  const slashed = toDate(row.slashed)
  const repaid = toDate(row.repaid)
  const revoked = toDate(row.revoked)
  const effectiveFrom = toDate(row.effective_from)
  const effectiveUntil = toDate(row.effective_until)

  if (slashed && repaid && repaid.getTime() >= slashed.getTime()) return 'REPAID'
  if (slashed && (!repaid || repaid.getTime() < slashed.getTime())) return 'SLASHED'
  if (revoked && revoked.getTime() <= now.getTime()) return 'REVOKED'
  if (effectiveUntil && effectiveUntil.getTime() <= now.getTime()) return 'EXPIRED'
  if (effectiveFrom && effectiveFrom.getTime() > now.getTime()) return 'FUTURE'
  if (
    (!effectiveFrom || effectiveFrom.getTime() <= now.getTime()) &&
    (!effectiveUntil || effectiveUntil.getTime() > now.getTime())
  ) {
    return 'ACTIVE'
  }
  return 'INACTIVE'
}

function participantsByRole(row: Record<string, unknown>): Record<string, number> | undefined {
  const mapping: Array<[ParticipantRole, string]> = [
    ['HOLDER', 'participants_holder'],
    ['ISSUER', 'participants_issuer'],
    ['VERIFIER', 'participants_verifier'],
    ['ISSUER_GRANTOR', 'participants_issuer_grantor'],
    ['VERIFIER_GRANTOR', 'participants_verifier_grantor'],
    ['ECOSYSTEM', 'participants_ecosystem'],
  ]
  const out: Record<string, number> = {}
  for (const [role, col] of mapping) {
    const n = Number(row[col])
    if (Number.isFinite(n) && n > 0) out[role] = Math.trunc(n)
  }
  return Object.keys(out).length > 0 ? out : undefined
}

function normalizeRole(type: unknown): ParticipantRole | null {
  const t = String(type ?? '').toUpperCase()
  return (ALL_PARTICIPANT_ROLES as string[]).includes(t) ? (t as ParticipantRole) : null
}

const columnCache = new Map<string, boolean>()

async function tableHasColumn(table: string, column: string): Promise<boolean> {
  const key = `${table}.${column}`
  const cached = columnCache.get(key)
  if (cached !== undefined) return cached
  let has = false
  try {
    has = await knex.schema.hasColumn(table, column)
  } catch {
    has = false
  }
  columnCache.set(key, has)
  return has
}

const blockTimeCache = new Map<number, Date | null>()

async function blockTimeAtHeight(height: number): Promise<Date | null> {
  if (blockTimeCache.has(height)) return blockTimeCache.get(height) ?? null
  let time: Date | null = null
  try {
    const row = (await knex('block').select('time').where('height', height).first()) as
      | { time?: Date | string }
      | undefined
    const d = row?.time != null ? new Date(row.time as Date | string) : null
    time = d && Number.isFinite(d.getTime()) ? d : null
  } catch {
    time = null
  }
  blockTimeCache.set(height, time)
  return time
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function applyHeightFilter(query: any, table: string, at?: number): Promise<void> {
  if (at == null || !Number.isFinite(at)) return
  if (await tableHasColumn(table, 'created')) {
    const time = await blockTimeAtHeight(at)
    if (time) {
      query.andWhere('created', '<=', time)
      return
    }
  }
  if (await tableHasColumn(table, 'height')) {
    query.andWhere('height', '<=', at)
  }
}

/**
 * Builds `participations[]` from the `participants` table for the resolved DID,
 * filtered to the requested participant states.
 */
export async function buildParticipations(
  did: string,
  now: Date,
  states: ParticipantState[],
  atHeight?: number
): Promise<Array<Record<string, unknown>>> {
  const stateSet = new Set(states.length > 0 ? states : ['ACTIVE'])

  const q = knex('participants').where({ did })
  await applyHeightFilter(q, 'participants', atHeight)
  const rows = (await q.orderBy('id', 'asc')) as Array<Record<string, unknown>>
  if (rows.length === 0) return []

  const schemaIds = [...new Set(rows.map((r) => Number(r.schema_id)).filter((n) => Number.isFinite(n)))]
  const schemaToEcosystemId = new Map<number, number>()
  if (schemaIds.length > 0) {
    const csQ = knex('credential_schemas').select('id', 'ecosystem_id').whereIn('id', schemaIds)
    await applyHeightFilter(csQ, 'credential_schemas', atHeight)
    const csRows = (await csQ) as Array<{ id: number; ecosystem_id: number }>
    for (const cs of csRows) schemaToEcosystemId.set(Number(cs.id), Number(cs.ecosystem_id))
  }

  const out: Array<Record<string, unknown>> = []
  for (const row of rows) {
    const role = normalizeRole(row.role)
    if (!role) continue
    const state = deriveParticipantState(row, now)
    if (!stateSet.has(state)) continue

    const schemaId = Number(row.schema_id)
    const ecosystemId = schemaToEcosystemId.get(schemaId)
    const isEcosystem = role === 'ECOSYSTEM'

    const entry: Record<string, unknown> = {
      id: Number(row.id),
      vsOperator: typeof row.vs_operator === 'string' ? row.vs_operator : null,
      role,
      state,
      credentialSchemaId: Number.isFinite(schemaId) ? schemaId : 0,
      ecosystemId: Number.isFinite(Number(ecosystemId)) ? Number(ecosystemId) : 0,
      weight: toCoin(row.weight ?? row.deposit),
      validatorParticipantId: isEcosystem
        ? null
        : row.validator_participant_id != null
          ? Number(row.validator_participant_id)
          : 0,
    }

    if (row.issued != null) entry.issuedCredentials = Math.trunc(Number(row.issued)) || 0
    if (row.verified != null) entry.verifiedCredentials = Math.trunc(Number(row.verified)) || 0
    const pbr = participantsByRole(row)
    if (pbr) entry.participants = pbr

    out.push(entry)
  }
  return out
}

export type EcosystemsOptions = {
  includeArchived: boolean
  credentialSchemas: { include: boolean; includeArchived: boolean }
}

const SRI_ALG_MAP: Record<string, 'sha256' | 'sha384' | 'sha512'> = {
  'sha-256': 'sha256',
  sha256: 'sha256',
  'sha-384': 'sha384',
  sha384: 'sha384',
  'sha-512': 'sha512',
  sha512: 'sha512',
}

async function computeSchemaDigestSri(jsonSchema: unknown, algorithm: string | null | undefined): Promise<string> {
  const alg =
    SRI_ALG_MAP[
      String(algorithm ?? '')
        .trim()
        .toLowerCase()
    ] ?? 'sha256'
  const parsed = typeof jsonSchema === 'string' ? safeParse(jsonSchema) : jsonSchema
  const canonical = await canonicalizeJson(parsed ?? {})
  const digest = createHash(alg).update(canonical, 'utf8').digest('base64')
  return `${alg}-${digest}`
}

function safeParse(s: string): unknown {
  try {
    return JSON.parse(s)
  } catch {
    return {}
  }
}

type GfvRow = { id: number; version: number; active_since?: Date | string }
type GfDocRow = { language: string; url: string; digest_sri: string }

function buildGovernanceFrameworkFrom(gfv: GfvRow, docs: GfDocRow[]): Record<string, unknown> | undefined {
  if (docs.length === 0) return undefined
  const framework: Record<string, unknown> = {
    version: Number(gfv.version),
    documents: docs.map((d) => ({ language: d.language, url: d.url, digestSri: d.digest_sri })),
  }
  const activeSince = toIso(gfv.active_since)
  if (activeSince) framework.activeSince = activeSince
  return framework
}

async function buildGovernanceFramework(ecosystemId: number, activeVersion: number | null | undefined) {
  if (activeVersion == null) return undefined
  const gfv = (await knex('governance_framework_version')
    .where({ ecosystem_id: ecosystemId, version: activeVersion })
    .first()) as GfvRow | undefined
  if (!gfv) return undefined

  const docs = (await knex('governance_framework_document')
    .where({ gfv_id: Number(gfv.id) })
    .orderBy('language', 'asc')) as GfDocRow[]
  return buildGovernanceFrameworkFrom(gfv, docs)
}

async function buildEcosystemSchemas(
  ecosystemId: number,
  includeArchived: boolean,
  atHeight?: number
): Promise<Array<Record<string, unknown>>> {
  const q = knex('credential_schemas').where({ ecosystem_id: ecosystemId })
  if (!includeArchived) q.whereNull('archived')
  await applyHeightFilter(q, 'credential_schemas', atHeight)
  const rows = (await q.orderBy('id', 'asc')) as Array<Record<string, unknown>>

  const out: Array<Record<string, unknown>> = []
  for (const row of rows) {
    const schema: Record<string, unknown> = {
      id: Number(row.id),
      type: 'JsonSchema',
      digestSri: await computeSchemaDigestSri(row.json_schema, row.digest_algorithm as string | null),
      archived: row.archived != null,
    }
    const pbr = participantsByRole(row)
    if (pbr) schema.participants = pbr
    if (row.issued != null) schema.issuedCredentials = Math.trunc(Number(row.issued)) || 0
    if (row.verified != null) schema.verifiedCredentials = Math.trunc(Number(row.verified)) || 0
    out.push(schema)
  }
  return out
}

export async function buildEcosystems(
  did: string,
  opts: EcosystemsOptions,
  atHeight?: number
): Promise<Array<Record<string, unknown>>> {
  const q = knex('ecosystem').where({ did })
  if (!opts.includeArchived) q.whereNull('archived')
  await applyHeightFilter(q, 'ecosystem', atHeight)
  const rows = (await q.orderBy('id', 'asc')) as Array<Record<string, unknown>>
  if (rows.length === 0) return []

  const out: Array<Record<string, unknown>> = []
  for (const row of rows) {
    const ecosystemId = Number(row.id)
    const entry: Record<string, unknown> = {
      id: ecosystemId,
      corporationId: Number.isFinite(Number(row.corporation_id)) ? Number(row.corporation_id) : 0,
      archived: row.archived != null,
    }
    const egf = await buildGovernanceFramework(ecosystemId, row.active_version as number | null)
    if (egf) entry.egf = egf
    if (opts.credentialSchemas.include) {
      entry.credentialSchemas = await buildEcosystemSchemas(
        ecosystemId,
        opts.credentialSchemas.includeArchived,
        atHeight
      )
    }
    const pbr = participantsByRole(row)
    if (pbr) entry.participants = pbr
    if (row.issued != null) entry.issuedCredentials = Math.trunc(Number(row.issued)) || 0
    if (row.verified != null) entry.verifiedCredentials = Math.trunc(Number(row.verified)) || 0
    out.push(entry)
  }
  return out
}

export async function resolveCorporationId(did: string, atHeight?: number): Promise<number> {
  if (!did) return 0

  const corpQ = knex('corporation').select('id').where({ did })
  await applyHeightFilter(corpQ, 'corporation', atHeight)
  const corp = (await corpQ.first()) as { id?: number } | undefined
  if (corp?.id != null && Number(corp.id) > 0) return Number(corp.id)

  const ecoQ = knex('ecosystem').select('corporation_id').where({ did })
  await applyHeightFilter(ecoQ, 'ecosystem', atHeight)
  const eco = (await ecoQ.first()) as { corporation_id?: number } | undefined
  if (eco?.corporation_id != null && Number(eco.corporation_id) > 0) return Number(eco.corporation_id)

  const partQ = knex('participants').select('corporation_id').where({ did })
  await applyHeightFilter(partQ, 'participants', atHeight)
  const part = (await partQ.first()) as { corporation_id?: number } | undefined
  if (part?.corporation_id != null && Number(part.corporation_id) > 0) return Number(part.corporation_id)

  return 0
}

async function buildCorporationGovernanceFramework(
  corporationId: number,
  atHeight?: number
): Promise<Record<string, unknown> | undefined> {
  const gfvQ = knex('co_governance_framework_version')
    .where({ corporation_id: corporationId })
    .orderBy('version', 'desc')
  await applyHeightFilter(gfvQ, 'co_governance_framework_version', atHeight)
  const gfv = (await gfvQ.first()) as GfvRow | undefined
  if (!gfv) return undefined

  const docs = (await knex('co_governance_framework_document')
    .where({ gfv_id: Number(gfv.id) })
    .orderBy('language', 'asc')) as GfDocRow[]
  return buildGovernanceFrameworkFrom(gfv, docs)
}

type CorporationRow = { id?: number; policy_address?: string | null; corporation?: string | null }

async function fetchCorporationByDid(did: string, atHeight?: number): Promise<CorporationRow | undefined> {
  const q = knex('corporation').where({ did })
  await applyHeightFilter(q, 'corporation', atHeight)
  return (await q.first()) as CorporationRow | undefined
}

async function fetchCorporationById(id: number, atHeight?: number): Promise<CorporationRow | undefined> {
  const q = knex('corporation').where({ id })
  await applyHeightFilter(q, 'corporation', atHeight)
  return (await q.first()) as CorporationRow | undefined
}

export async function buildCorporation(did: string, atHeight?: number): Promise<Record<string, unknown> | null> {
  if (!did) return null

  let corp = await fetchCorporationByDid(did, atHeight)
  if (!corp) {
    const corporationId = await resolveCorporationId(did, atHeight)
    if (corporationId > 0) corp = await fetchCorporationById(corporationId, atHeight)
  }
  if (!corp) return null

  const policyAddress =
    (typeof corp.policy_address === 'string' && corp.policy_address) ||
    (typeof corp.corporation === 'string' && corp.corporation) ||
    ''

  const entry: Record<string, unknown> = {
    id: Number(corp.id) || 0,
    policyAddress,
    deposit: toCoin(0),
  }

  if (policyAddress) {
    const td = (await knex('trust_deposits').where({ corporation: policyAddress }).first()) as
      | { deposit?: unknown; slashed_deposit?: unknown; slash_count?: unknown; last_slashed?: unknown }
      | undefined
    if (td) {
      entry.deposit = toCoin(td.deposit)
      const slashCount = Math.trunc(Number(td.slash_count))
      if (Number.isFinite(slashCount) && slashCount > 0) {
        entry.slashedEvents = slashCount
        entry.slashedValue = toCoin(td.slashed_deposit)
        const lastSlashed = toIso(td.last_slashed)
        if (lastSlashed) entry.lastSlashedAtTime = lastSlashed
      }
    }
  }

  const cgf = await buildCorporationGovernanceFramework(Number(corp.id) || 0, atHeight)
  if (cgf) entry.cgf = cgf

  return entry
}

const LINKED_VP_SERVICE_TYPE = 'LinkedVerifiablePresentation'

function isLinkedVpType(type: unknown): boolean {
  if (typeof type === 'string') return type === LINKED_VP_SERVICE_TYPE
  if (Array.isArray(type)) return type.includes(LINKED_VP_SERVICE_TYPE)
  return false
}

function didDocumentServices(resolveResult: unknown): Array<Record<string, unknown>> {
  if (!resolveResult || typeof resolveResult !== 'object') return []
  const didDocument = (resolveResult as { didDocument?: unknown }).didDocument
  if (!didDocument || typeof didDocument !== 'object') return []
  const services = (didDocument as { service?: unknown }).service
  if (!Array.isArray(services)) return []
  return services.filter((svc): svc is Record<string, unknown> => Boolean(svc) && typeof svc === 'object')
}

export function buildServices(resolveResult: unknown): Array<Record<string, unknown>> {
  return didDocumentServices(resolveResult).filter((svc) => !isLinkedVpType(svc.type))
}

export type PresentationsOptions = {
  unresolvableCredentialIds: boolean
  invalidCredentialIds: boolean
}

type EcsSchemaResolution = {
  credentialSchemaId: number
  ecosystemId: number
  isEcs: boolean
  ecsSchemaTitle: string | null
}

const IS_PG = String((knex as { client?: { config?: { client?: string } } }).client?.config?.client || '').includes(
  'pg'
)

function isEcsSchemaTitle(title: unknown): boolean {
  return typeof title === 'string' && Object.values(ECS_SCHEMA_TITLE_BY_TYPE).includes(title)
}

async function fetchVpCredentials(endpoint: string): Promise<Array<Record<string, unknown>>> {
  if (!endpoint) return []
  try {
    const vp = await fetchJson<Record<string, unknown>>(endpoint)
    const vc = (vp?.verifiableCredential ??
      (vp as { verifiableCredentials?: unknown })?.verifiableCredentials) as unknown
    const arr = Array.isArray(vc) ? vc : vc ? [vc] : []
    return arr.filter((c): c is Record<string, unknown> => Boolean(c) && typeof c === 'object')
  } catch {
    return []
  }
}

function credentialId(c: Record<string, unknown>): string {
  return typeof c.id === 'string' ? c.id : ''
}

function credentialIssuerDid(c: Record<string, unknown>): string | null {
  if (typeof c.issuer === 'string') return c.issuer
  const id = (c.issuer as { id?: unknown })?.id
  return typeof id === 'string' ? id : null
}

function credentialSubjectDid(c: Record<string, unknown>): string | null {
  const subject = c.credentialSubject
  const id = (Array.isArray(subject) ? subject[0]?.id : (subject as { id?: unknown })?.id) as unknown
  return typeof id === 'string' ? id : null
}

async function buildEcsResolutionFromRow(row: {
  id?: unknown
  ecosystem_id?: unknown
  json_schema?: unknown
}): Promise<EcsSchemaResolution> {
  const title = parseSchemaJson(row.json_schema)?.title
  const ecosystemId = Number(row.ecosystem_id) || 0
  const ecsSchemaTitle = isEcsSchemaTitle(title) ? (title as string) : null
  return {
    credentialSchemaId: Number(row.id) || 0,
    ecosystemId,
    isEcs: ecsSchemaTitle !== null && (await isEcosystemEcsAllowlisted(ecosystemId)),
    ecsSchemaTitle,
  }
}

async function resolveSchemaByNumericId(id: number): Promise<EcsSchemaResolution | null> {
  if (!Number.isInteger(id) || id <= 0) return null
  const row = (await knex('credential_schemas').select('id', 'ecosystem_id', 'json_schema').where('id', id).first()) as
    | { id?: unknown; ecosystem_id?: unknown; json_schema?: unknown }
    | undefined
  return row ? buildEcsResolutionFromRow(row) : null
}

async function resolveSchemaByUri(uri: string): Promise<EcsSchemaResolution | null> {
  if (!uri) return null

  let row: { id?: unknown; ecosystem_id?: unknown; json_schema?: unknown } | undefined
  if (IS_PG) {
    const res = await knex.raw(
      `
      SELECT id, ecosystem_id, json_schema FROM credential_schemas
      WHERE json_schema::jsonb->>'$id' = :uri
         OR json_schema::jsonb->>'id' = :uri
         OR json_schema::jsonb->>'@id' = :uri
      LIMIT 1
      `,
      { uri }
    )
    row = ((res as { rows?: Array<Record<string, unknown>> }).rows ?? [])[0]
  } else {
    const rows = (await knex('credential_schemas').select('id', 'ecosystem_id', 'json_schema')) as Array<
      Record<string, unknown>
    >
    row = rows.find((r) => {
      const js = parseSchemaJson(r.json_schema)
      return js?.$id === uri || js?.id === uri || js?.['@id'] === uri
    })
  }
  return row ? buildEcsResolutionFromRow(row) : null
}

function schemaRefNumericId(ref: string): number | null {
  const direct = Number(ref)
  if (Number.isInteger(direct) && direct > 0) return direct
  const tail = ref.split(/[/:]/).filter(Boolean).at(-1)
  const n = Number(tail)
  return Number.isInteger(n) && n > 0 ? n : null
}

async function dereferenceSchemaRef(csId: string): Promise<string | null> {
  try {
    const jsc = await fetchJson<Record<string, unknown>>(csId)
    const subject = (Array.isArray(jsc?.credentialSubject) ? jsc.credentialSubject[0] : jsc?.credentialSubject) as
      | { jsonSchema?: { $ref?: unknown }; id?: unknown }
      | undefined
    const ref = subject?.jsonSchema?.$ref ?? subject?.id
    return typeof ref === 'string' ? ref : null
  } catch {
    return null
  }
}

async function resolveCredentialSchema(cred: Record<string, unknown>): Promise<EcsSchemaResolution | null> {
  const cs = cred.credentialSchema
  const first = (Array.isArray(cs) ? cs[0] : cs) as { id?: unknown; type?: unknown } | undefined
  const rawId = typeof first?.id === 'string' ? first.id : ''
  if (!rawId) return null

  const needsDeref = first?.type === 'JsonSchemaCredential' || /^https?:/i.test(rawId)
  const schemaRef = needsDeref ? ((await dereferenceSchemaRef(rawId)) ?? rawId) : rawId

  const numericId = schemaRefNumericId(schemaRef)
  if (numericId) {
    const byId = await resolveSchemaByNumericId(numericId)
    if (byId) return byId
  }
  return resolveSchemaByUri(schemaRef)
}

async function resolveParticipantId(did: string, credentialSchemaId: number): Promise<number> {
  if (!did || credentialSchemaId <= 0) return 0
  const row = (await knex('participants').where({ did, schema_id: credentialSchemaId }).select('id').first()) as
    | { id?: number }
    | undefined
  return row?.id != null ? Number(row.id) || 0 : 0
}

export async function hasAllowlistedEcsServiceCredential(resolveResult: unknown): Promise<boolean> {
  const serviceSchemaTitle = ECS_SCHEMA_TITLE_BY_TYPE['ecs-service']

  for (const svc of didDocumentServices(resolveResult)) {
    if (!isLinkedVpType(svc.type)) continue
    const endpoint = typeof svc.serviceEndpoint === 'string' ? svc.serviceEndpoint : ''
    for (const cred of await fetchVpCredentials(endpoint)) {
      const schema = await resolveCredentialSchema(cred)
      if (schema?.isEcs && schema.ecsSchemaTitle === serviceSchemaTitle) return true
    }
  }
  return false
}

export async function buildPresentations(
  resolveResult: unknown,
  opts: PresentationsOptions
): Promise<Array<Record<string, unknown>>> {
  const didDocument = (resolveResult as { didDocument?: { id?: unknown } } | null)?.didDocument
  const didId = typeof didDocument?.id === 'string' ? didDocument.id : ''

  const out: Array<Record<string, unknown>> = []
  const seen = new Set<string>()
  for (const svc of didDocumentServices(resolveResult)) {
    if (!isLinkedVpType(svc.type)) continue
    const rawId = typeof svc.id === 'string' ? svc.id : ''
    const serviceId = rawId.startsWith('#') ? `${didId}${rawId}` : rawId
    if (!serviceId || seen.has(serviceId)) continue
    seen.add(serviceId)

    const endpoint = typeof svc.serviceEndpoint === 'string' ? svc.serviceEndpoint : ''
    const credentials = await fetchVpCredentials(endpoint)

    const vtcCredentials: Array<Record<string, unknown>> = []
    const unresolvableCredentialIds: string[] = []
    for (const cred of credentials) {
      const id = credentialId(cred)
      const schema = await resolveCredentialSchema(cred)
      if (!schema) {
        if (id) unresolvableCredentialIds.push(id)
        continue
      }
      if (schema.isEcs) continue // vtcCredentials surfaces non-ECS VTCs only

      const subjectDid = credentialSubjectDid(cred)
      const issuerDid = credentialIssuerDid(cred)
      vtcCredentials.push({
        id,
        credentialSchemaId: schema.credentialSchemaId,
        ecosystemId: schema.ecosystemId,
        participantId: subjectDid ? await resolveParticipantId(subjectDid, schema.credentialSchemaId) : 0,
        issuerParticipantId: issuerDid ? await resolveIssuerParticipantId(issuerDid, schema.credentialSchemaId) : 0,
      })
    }

    const entry: Record<string, unknown> = {
      id: endpoint,
      serviceId,
      vtcCredentials,
    }
    if (opts.unresolvableCredentialIds) entry.unresolvableCredentialIds = unresolvableCredentialIds
    // TODO: populate invalidCredentialIds once verre surfaces per-VC validation
    // results in its resolution. verre does not yet report which credentials in a
    // VP failed signature/format/expiry validation, so this stays empty for now.
    if (opts.invalidCredentialIds) entry.invalidCredentialIds = []
    out.push(entry)
  }
  return out
}

const ECS_SCHEMA_TITLE_BY_TYPE: Record<string, string> = {
  'ecs-service': 'ServiceCredential',
  'ecs-org': 'OrganizationCredential',
  'ecs-persona': 'PersonaCredential',
  'ecs-user-agent': 'UserAgentCredential',
}

function parseSchemaJson(value: unknown): Record<string, unknown> | null {
  if (value && typeof value === 'object') return value as Record<string, unknown>
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value)
      return parsed && typeof parsed === 'object' ? (parsed as Record<string, unknown>) : null
    } catch {
      return null
    }
  }
  return null
}

function ecsSchemaVersionFromId(schemaJson: unknown): string {
  const sid = parseSchemaJson(schemaJson)?.$id
  const m = typeof sid === 'string' ? sid.match(/\/cs\/(v\d+)\//) : null
  return m ? m[1] : ''
}

function toCredentialSubject(cred: Record<string, unknown>): Record<string, unknown> {
  const { schemaType, issuer, ...subject } = cred
  return subject
}

type EcsSchemaLink = {
  participantId: number
  credentialSchemaId: number
  ecosystemId: number
  ecsSchemaVersion: string
}

async function resolveEcsSchemaLink(subjectDid: string, ecsSchemaTitle: string): Promise<EcsSchemaLink | null> {
  const participants = (await knex('participants')
    .where({ did: subjectDid, role: 'HOLDER' })
    .select('id', 'schema_id')) as Array<{ id: number; schema_id: number }>
  if (participants.length === 0) return null

  const schemaIds = [...new Set(participants.map((p) => Number(p.schema_id)).filter((n) => Number.isFinite(n)))]
  if (schemaIds.length === 0) return null
  const schemas = (await knex('credential_schemas')
    .whereIn('id', schemaIds)
    .select('id', 'ecosystem_id', 'json_schema')) as Array<{ id: number; ecosystem_id: number; json_schema: unknown }>

  for (const p of participants) {
    const cs = schemas.find((s) => Number(s.id) === Number(p.schema_id))
    if (
      cs &&
      parseSchemaJson(cs.json_schema)?.title === ecsSchemaTitle &&
      (await isEcosystemEcsAllowlisted(Number(cs.ecosystem_id) || 0))
    ) {
      return {
        participantId: Number(p.id) || 0,
        credentialSchemaId: Number(cs.id) || 0,
        ecosystemId: Number(cs.ecosystem_id) || 0,
        ecsSchemaVersion: ecsSchemaVersionFromId(cs.json_schema),
      }
    }
  }
  return null
}

async function resolveIssuerParticipantId(issuerDid: string, credentialSchemaId: number): Promise<number> {
  if (!issuerDid || credentialSchemaId <= 0) return 0
  const row = (await knex('participants')
    .where({ did: issuerDid, schema_id: credentialSchemaId, role: 'ISSUER' })
    .select('id')
    .first()) as { id?: number } | undefined
  return row?.id != null ? Number(row.id) || 0 : 0
}

export async function buildEcsCredentials(resolveResult: unknown): Promise<Array<Record<string, unknown>>> {
  if (!resolveResult || typeof resolveResult !== 'object') return []
  const r = resolveResult as Record<string, unknown>

  const out: Array<Record<string, unknown>> = []
  for (const key of ['service', 'serviceProvider']) {
    const cred = r[key]
    if (!cred || typeof cred !== 'object') continue
    const c = cred as Record<string, unknown>
    const ecsSchema = ECS_SCHEMA_TITLE_BY_TYPE[String(c.schemaType ?? '').toLowerCase()]
    if (!ecsSchema) continue

    const subjectDid = typeof c.id === 'string' ? c.id : null
    const issuerDid = typeof c.issuer === 'string' ? c.issuer : null
    const link = subjectDid ? await resolveEcsSchemaLink(subjectDid, ecsSchema) : null
    if (isEcsAllowlistEnforced() && !link) continue

    const credentialSchemaId = link?.credentialSchemaId ?? 0
    const issuerParticipantId = issuerDid ? await resolveIssuerParticipantId(issuerDid, credentialSchemaId) : 0

    // TODO: validFrom/validUntil should mirror the VC body's validity window. verre's flattened
    // ICredential does not expose them and TrustResolution carries no raw VC, so we fall back to
    // now / now+1d. Pending to define the real source (raw VC via the VP, or a verre upgrade).
    const nowMs = Date.now()
    const entry: Record<string, unknown> = {
      ecsSchema,
      ecsSchemaVersion: link?.ecsSchemaVersion ?? '',
      credentialSchemaId,
      issuerParticipantId,
      ecosystemId: link?.ecosystemId ?? 0,
      participantId: link?.participantId ?? 0,
      validFrom: typeof c.validFrom === 'string' ? c.validFrom : new Date(nowMs).toISOString(),
      validUntil: typeof c.validUntil === 'string' ? c.validUntil : new Date(nowMs + 24 * 60 * 60 * 1000).toISOString(),
      credentialSubject: toCredentialSubject(c),
    }
    out.push(entry)
  }
  return out
}
