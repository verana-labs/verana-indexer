import { BULL_JOB_NAME } from '../../common'
import { getBlockChainTimeAsOf } from '../../common/utils/block_time'
import knex from '../../common/utils/db_connection'
import { Ecosystem } from '../../models/ecosystem'
import TrustDeposit from '../../models/trust_deposit'
import { calculateParticipantState, normalizeParticipantType, type ParticipantType } from '../crawl-pp/pp_state_utils'

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

function emptyParticipantStats(): CorporationParticipantStats {
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
    const state = calculateParticipantState(
      {
        repaid: row.repaid,
        slashed: row.slashed,
        revoked: row.revoked,
        effective_from: row.effective_from,
        effective_until: row.effective_until,
        role: row.role,
        op_state: row.op_state,
        op_exp: row.op_exp,
        validator_participant_id: row.validator_participant_id,
      },
      now
    )
    if (state !== 'ACTIVE') continue

    stats.participants += 1
    const roleField = ROLE_TO_FIELD[normalizeParticipantType(row.role)]
    if (roleField) stats[roleField] += 1
  }

  return stats
}

export async function countControlledEcosystems(corporationId: number | string): Promise<number> {
  return Ecosystem.query().where('corporation_id', corporationId).resultSize()
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

export function deriveActiveVersion(versions: CorporationGfVersion[]): number | null {
  const active = versions.filter((v) => v.active_since).sort(byActiveSinceDesc)
  return active.length > 0 ? active[0].version : null
}

export async function getCorporationTrustDeposit(address: string | null): Promise<CorporationTrustDepositSnapshot> {
  const empty: CorporationTrustDepositSnapshot = {
    deposit: 0,
    share: 0,
    refunded: 0,
    slashed_deposit: 0,
    repaid_deposit: 0,
    slash_count: 0,
    last_slashed: null,
    last_repaid: null,
  }
  if (!address) return empty

  const row = await TrustDeposit.query().findOne({ corporation: address })
  if (!row) return empty

  return {
    deposit: Number(row.deposit ?? 0),
    share: Number(row.share ?? 0),
    refunded: Number(row.claimable ?? 0),
    slashed_deposit: Number(row.slashed_deposit ?? 0),
    repaid_deposit: Number(row.repaid_deposit ?? 0),
    slash_count: Number(row.slash_count ?? 0),
    last_slashed: row.last_slashed ?? null,
    last_repaid: row.last_repaid ?? null,
  }
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
  preferredLanguage?: string
): CorporationGfVersion[] {
  let selected: CorporationGfVersion[]
  if (gfData === 'none') {
    selected = []
  } else if (gfData === 'only_active') {
    selected = versions
      .filter((v) => v.active_since)
      .sort(byActiveSinceDesc)
      .slice(0, 1)
  } else {
    selected = [...versions]
  }

  // ecosystem_id is null for CGF versions (VPR invariant: only EGF versions carry one).
  return selected.map((v) => ({
    ...v,
    ecosystem_id: v.ecosystem_id ? v.ecosystem_id : null,
    documents: preferredLanguage ? orderDocumentsByLanguage(v.documents ?? [], preferredLanguage) : v.documents,
  }))
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
