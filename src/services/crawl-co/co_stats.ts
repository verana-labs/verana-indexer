import { BULL_JOB_NAME } from '../../common'
import { getBlockChainTimeAsOf } from '../../common/utils/block_time'
import knex from '../../common/utils/db_connection'
import { Ecosystem } from '../../models/ecosystem'
import TrustDeposit from '../../models/trust_deposit'
import {
  calculateParticipantState,
  normalizeParticipantType,
  type ParticipantData,
  type ParticipantType,
} from '../crawl-pp/pp_state_utils'

export type GfDataMode = 'none' | 'only_active' | 'all'

export function parseGfDataMode(raw: unknown): { ok: true; mode: GfDataMode } | { ok: false; message: string } {
  if (raw === undefined || raw === null || raw === '') return { ok: true, mode: 'only_active' }
  const normalized = String(raw).trim().toLowerCase()
  if (normalized === 'none' || normalized === 'only_active' || normalized === 'all') {
    return { ok: true, mode: normalized as GfDataMode }
  }
  return { ok: false, message: 'Invalid "gf_data". Allowed values: none, only_active, all' }
}

export interface CorporationGfVersion {
  version: number
  active_since?: string | Date | null
  documents?: Array<Record<string, unknown>>
  [key: string]: unknown
}

export interface CorporationTrustDepositSnapshot {
  deposit: number
  share: number
  refunded: number
  slashed_deposit: number
  repaid_deposit: number
  slash_count: number
  last_slashed: string | null
  last_repaid: string | null
}

export interface CorporationParticipantStats {
  participants: number
  participants_ecosystem: number
  participants_issuer_grantor: number
  participants_issuer: number
  participants_verifier_grantor: number
  participants_verifier: number
  participants_holder: number
}

export function emptyParticipantStats(): CorporationParticipantStats {
  return {
    participants: 0,
    participants_ecosystem: 0,
    participants_issuer_grantor: 0,
    participants_issuer: 0,
    participants_verifier_grantor: 0,
    participants_verifier: 0,
    participants_holder: 0,
  }
}

const ROLE_TO_FIELD: Partial<Record<ParticipantType, keyof CorporationParticipantStats>> = {
  ECOSYSTEM: 'participants_ecosystem',
  ISSUER_GRANTOR: 'participants_issuer_grantor',
  ISSUER: 'participants_issuer',
  VERIFIER_GRANTOR: 'participants_verifier_grantor',
  VERIFIER: 'participants_verifier',
  HOLDER: 'participants_holder',
}

interface ParticipantRow {
  corporation_id?: number | string
  repaid?: string | null
  slashed?: string | null
  revoked?: string | null
  effective_from?: string | null
  effective_until?: string | null
  role: string
  op_state?: string
  op_exp?: string | null
  validator_participant_id?: string | null
}

function tallyActiveParticipant(stats: CorporationParticipantStats, row: ParticipantRow, now: Date): void {
  const state = calculateParticipantState(
    {
      repaid: row.repaid,
      slashed: row.slashed,
      revoked: row.revoked,
      effective_from: row.effective_from,
      effective_until: row.effective_until,
      role: row.role as ParticipantType,
      op_state: row.op_state as ParticipantData['op_state'],
      op_exp: row.op_exp,
      validator_participant_id: row.validator_participant_id,
    },
    now
  )
  if (state !== 'ACTIVE') return

  stats.participants += 1
  const roleField = ROLE_TO_FIELD[normalizeParticipantType(row.role)]
  if (roleField) stats[roleField] += 1
}

export async function calculateCorporationParticipantStats(
  corporationId: number | string,
  blockHeight?: number
): Promise<CorporationParticipantStats> {
  const stats = emptyParticipantStats()

  let now = new Date()
  if (typeof blockHeight === 'number') {
    now = await getBlockChainTimeAsOf(blockHeight, { logContext: '[co_stats]' })
  }

  const rows = await knex('participants').where('corporation_id', corporationId)
  for (const row of rows) {
    tallyActiveParticipant(stats, row, now)
  }

  return stats
}

export async function calculateCorporationParticipantStatsBatch(
  corporationIds: Array<number | string>,
  blockHeight?: number
): Promise<Map<string, CorporationParticipantStats>> {
  const result = new Map<string, CorporationParticipantStats>()
  for (const id of corporationIds) {
    result.set(String(id), emptyParticipantStats())
  }
  if (corporationIds.length === 0) return result

  let now = new Date()
  if (typeof blockHeight === 'number') {
    now = await getBlockChainTimeAsOf(blockHeight, { logContext: '[co_stats]' })
  }

  const rows = await knex('participants').whereIn('corporation_id', corporationIds)
  for (const row of rows) {
    const stats = result.get(String(row.corporation_id))
    if (stats) tallyActiveParticipant(stats, row, now)
  }

  return result
}

export async function countControlledEcosystems(corporationId: number | string): Promise<number> {
  return Ecosystem.query().where('corporation_id', corporationId).resultSize()
}

export async function countControlledEcosystemsBatch(
  corporationIds: Array<number | string>
): Promise<Map<string, number>> {
  const result = new Map<string, number>()
  for (const id of corporationIds) {
    result.set(String(id), 0)
  }
  if (corporationIds.length === 0) return result

  const rows = (await knex('ecosystem')
    .whereIn('corporation_id', corporationIds)
    .groupBy('corporation_id')
    .select('corporation_id')
    .count({ count: '*' })) as unknown as Array<{ corporation_id: number | string; count: string | number }>
  for (const row of rows) {
    result.set(String(row.corporation_id), Number(row.count))
  }
  return result
}

// Ecosystems owned by the corporation that existed and were not archived at the height.
export async function countControlledEcosystemsAtHeight(
  corporationId: number | string,
  blockHeight: number
): Promise<number> {
  const ranked = knex('ecosystem_history')
    .select('ecosystem_id', 'archived')
    .select(knex.raw('ROW_NUMBER() OVER (PARTITION BY ecosystem_id ORDER BY height DESC, created_at DESC) as rn'))
    .where('corporation_id', corporationId)
    .where('height', '<=', blockHeight)
  const rows = (await knex.from(ranked.as('h')).where('rn', 1).whereNull('archived')) as unknown[]
  return rows.length
}

export interface CorporationBaseAtHeight {
  id: number | string
  did: string | null
  policy_address: string | null
  language: string | null
  modified: string | Date | null
}

// Base corporation fields as of a height (from corporation_history); null if it did not exist yet.
export async function getCorporationBaseAtHeight(
  corporationId: number | string,
  blockHeight: number
): Promise<CorporationBaseAtHeight | null> {
  // Only Create/Update rows carry authoritative base fields; CGF activity rows
  // (AddCGFDocument, IncreaseCGFActiveVersion) do not change did/policy_address/language/modified.
  const row = await knex('corporation_history')
    .where('corporation_id', corporationId)
    .where('height', '<=', blockHeight)
    .whereIn('event_type', ['Create', 'Update'])
    .orderBy('height', 'desc')
    .orderBy('id', 'desc')
    .first()
  if (!row) return null
  return {
    id: corporationId,
    did: (row.did as string | null) ?? null,
    policy_address: (row.policy_address as string | null) ?? (row.corporation as string | null) ?? null,
    language: (row.language as string | null) ?? null,
    modified: (row.created_at as string | Date | null) ?? null,
  }
}

function byActiveSinceDesc(a: CorporationGfVersion, b: CorporationGfVersion): number {
  if (a.active_since && b.active_since) {
    const diff = new Date(b.active_since).getTime() - new Date(a.active_since).getTime()
    if (diff !== 0) return diff
  } else if (a.active_since) {
    return -1
  } else if (b.active_since) {
    return 1
  }
  return (b.version ?? 0) - (a.version ?? 0)
}

export function deriveActiveVersion(versions: CorporationGfVersion[], asOf?: Date): number | null {
  const active = versions
    .filter((v) => v.active_since && (!asOf || new Date(v.active_since) <= asOf))
    .sort(byActiveSinceDesc)
  return active.length > 0 ? active[0].version : null
}

export function emptyTrustDepositSnapshot(): CorporationTrustDepositSnapshot {
  return {
    deposit: 0,
    share: 0,
    refunded: 0,
    slashed_deposit: 0,
    repaid_deposit: 0,
    slash_count: 0,
    last_slashed: null,
    last_repaid: null,
  }
}

function mapTrustDepositRow(row: Record<string, unknown>): CorporationTrustDepositSnapshot {
  return {
    deposit: Number(row.deposit ?? 0),
    share: Number(row.share ?? 0),
    refunded: Number(row.claimable ?? 0),
    slashed_deposit: Number(row.slashed_deposit ?? 0),
    repaid_deposit: Number(row.repaid_deposit ?? 0),
    slash_count: Number(row.slash_count ?? 0),
    last_slashed: (row.last_slashed as string | null) ?? null,
    last_repaid: (row.last_repaid as string | null) ?? null,
  }
}

export async function getCorporationTrustDeposit(address: string | null): Promise<CorporationTrustDepositSnapshot> {
  if (!address) return emptyTrustDepositSnapshot()
  const row = await TrustDeposit.query().findOne({ corporation: address })
  return row ? mapTrustDepositRow(row as unknown as Record<string, unknown>) : emptyTrustDepositSnapshot()
}

export async function getCorporationTrustDepositBatch(
  addresses: Array<string | null>
): Promise<Map<string, CorporationTrustDepositSnapshot>> {
  const result = new Map<string, CorporationTrustDepositSnapshot>()
  const unique = [...new Set(addresses.filter((a): a is string => Boolean(a)))]
  if (unique.length === 0) return result

  const rows = await TrustDeposit.query().whereIn('corporation', unique)
  for (const row of rows) {
    result.set(row.corporation, mapTrustDepositRow(row as unknown as Record<string, unknown>))
  }
  return result
}

// Trust-deposit snapshot as of a block height (from trust_deposit_history).
export async function getCorporationTrustDepositAtHeight(
  address: string | null,
  blockHeight: number
): Promise<CorporationTrustDepositSnapshot> {
  if (!address) return emptyTrustDepositSnapshot()
  const row = await knex('trust_deposit_history')
    .where('corporation', address)
    .where('height', '<=', blockHeight)
    .orderBy('height', 'desc')
    .orderBy('id', 'desc')
    .first()
  return row ? mapTrustDepositRow(row as Record<string, unknown>) : emptyTrustDepositSnapshot()
}

function orderDocumentsByLanguage(
  documents: Array<Record<string, unknown>>,
  preferredLanguage: string
): Array<Record<string, unknown>> {
  const preferred = documents.filter((d) => d.language === preferredLanguage)
  const rest = documents.filter((d) => d.language !== preferredLanguage)
  return [...preferred, ...rest]
}

export function applyGfData(
  versions: CorporationGfVersion[],
  gfData: GfDataMode,
  preferredLanguage?: string,
  asOf?: Date
): CorporationGfVersion[] {
  // At a height, only versions that already existed (created <= asOf) count.
  const existing = asOf
    ? versions.filter((v) => v.created != null && new Date(v.created as string | Date) <= asOf)
    : versions

  let selected: CorporationGfVersion[]
  if (gfData === 'none') {
    selected = []
  } else if (gfData === 'only_active') {
    selected = existing
      .filter((v) => v.active_since && (!asOf || new Date(v.active_since) <= asOf))
      .sort(byActiveSinceDesc)
      .slice(0, 1)
  } else {
    selected = [...existing]
  }

  // ecosystem_id is null for CGF versions (VPR invariant: only EGF versions carry one).
  return selected.map((v) => {
    let documents = v.documents
    // At a height, exclude documents added after that block.
    if (asOf && documents) {
      documents = documents.filter((d) => d.created != null && new Date(d.created as string | Date) <= asOf)
    }
    if (preferredLanguage) {
      documents = orderDocumentsByLanguage(documents ?? [], preferredLanguage)
    }
    return { ...v, ecosystem_id: v.ecosystem_id ? v.ecosystem_id : null, documents }
  })
}

// Shared by Get and List so the two responses stay identical.
export function buildCorporationObject(params: {
  plain: Record<string, unknown>
  cgfVersions: CorporationGfVersion[]
  participantStats: CorporationParticipantStats
  controlledEcosystems: number
  trustDeposit: CorporationTrustDepositSnapshot
  gfData: GfDataMode
  preferredLanguage?: string
}): Record<string, unknown> {
  const { plain, cgfVersions, participantStats, controlledEcosystems, trustDeposit, gfData, preferredLanguage } = params
  const policyAddress = (plain.policy_address as string | null) ?? (plain.corporation as string | null) ?? null

  const corporation: Record<string, unknown> = {
    id: plain.id,
    did: plain.did,
    policy_address: policyAddress,
    language: plain.language ?? null,
    active_version: deriveActiveVersion(cgfVersions),
    created: plain.created,
    modified: plain.modified,
    controlled_ecosystems: controlledEcosystems,
    ...participantStats,
    ...trustDeposit,
  }

  if (gfData !== 'none') {
    corporation.versions = applyGfData(cgfVersions, gfData, preferredLanguage)
  }

  return corporation
}

export interface CorporationListPagination {
  limit: number
  minId?: string
  maxId?: string
  direction: 'asc' | 'desc'
}

function parseCursorParam(raw: unknown): { ok: true; value?: string } | { ok: false } {
  if (raw === undefined || raw === null || raw === '') return { ok: true }
  const value = String(raw).trim()
  if (!/^\d+$/.test(value)) return { ok: false }
  return { ok: true, value }
}

export function parseCorporationListPagination(params: {
  limit?: string | number
  min_id?: string | number
  max_id?: string | number
  sort?: string
}): { ok: true; value: CorporationListPagination } | { ok: false; message: string } {
  let limit = 64
  if (params.limit !== undefined && params.limit !== '') {
    const parsed = Number(params.limit)
    if (!Number.isInteger(parsed) || parsed < 1 || parsed > 1024) {
      return { ok: false, message: '"limit" must be an integer between 1 and 1024' }
    }
    limit = parsed
  }

  const minId = parseCursorParam(params.min_id)
  if (!minId.ok) return { ok: false, message: '"min_id" must be a non-negative integer' }
  const maxId = parseCursorParam(params.max_id)
  if (!maxId.ok) return { ok: false, message: '"max_id" must be a non-negative integer' }

  let direction: 'asc' | 'desc' = 'desc'
  if (params.sort !== undefined && params.sort !== '') {
    const sort = String(params.sort).trim()
    if (sort === 'id' || sort === '+id') direction = 'asc'
    else if (sort === '-id') direction = 'desc'
    else return { ok: false, message: 'Only "id" sort is supported (use "id", "+id", or "-id")' }
  }

  return { ok: true, value: { limit, minId: minId.value, maxId: maxId.value, direction } }
}

export async function getResolvedBlockHeight(blockHeight?: number): Promise<number> {
  if (typeof blockHeight === 'number') return blockHeight
  const checkpoint = await knex('block_checkpoint').where('job_name', BULL_JOB_NAME.HANDLE_TRANSACTION).first()
  if (checkpoint?.height != null) return Number(checkpoint.height)
  // fall back to latest block when the checkpoint row isn't written yet (fresh indexer)
  const latest = await knex('block').max('height as max').first()
  const maxValue = latest != null ? (latest as { max: string | number | null }).max : null
  return maxValue != null ? Number(maxValue) : 0
}
