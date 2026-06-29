import { BULL_JOB_NAME } from '../../common'
import { getBlockChainTimeAsOf } from '../../common/utils/block_time'
import knex from '../../common/utils/db_connection'
import { Ecosystem } from '../../models/ecosystem'
import TrustDeposit from '../../models/trust_deposit'
import { calculateParticipantState } from '../crawl-pp/pp_state_utils'

export type GfDataMode = 'none' | 'only_active' | 'all'

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

export async function calculateCorporationParticipantStats(
  corporationId: number,
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
    if (row.role === 'ECOSYSTEM') stats.participants_ecosystem += 1
    else if (row.role === 'ISSUER_GRANTOR') stats.participants_issuer_grantor += 1
    else if (row.role === 'ISSUER') stats.participants_issuer += 1
    else if (row.role === 'VERIFIER_GRANTOR') stats.participants_verifier_grantor += 1
    else if (row.role === 'VERIFIER') stats.participants_verifier += 1
    else if (row.role === 'HOLDER') stats.participants_holder += 1
  }

  return stats
}

export async function countControlledEcosystems(corporationId: number): Promise<number> {
  return Ecosystem.query().where('corporation_id', corporationId).resultSize()
}

function byActiveSinceDesc(a: CorporationGfVersion, b: CorporationGfVersion): number {
  if (!a.active_since && !b.active_since) return 0
  if (!a.active_since) return 1
  if (!b.active_since) return -1
  return new Date(b.active_since).getTime() - new Date(a.active_since).getTime()
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
  const row = await knex('block_checkpoint').where('job_name', BULL_JOB_NAME.HANDLE_TRANSACTION).first()
  return row?.height != null ? Number(row.height) : 0
}
