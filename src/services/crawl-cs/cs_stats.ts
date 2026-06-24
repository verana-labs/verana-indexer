import { getBlockChainTimeAsOf } from '../../common/utils/block_time'
import knex from '../../common/utils/db_connection'
import { calculateParticipantState } from '../crawl-pp/pp_state_utils'

function participantFromTrRow(row: Record<string, unknown> | null | undefined): string | null {
  if (!row) return null
  const n = Number(row.corporation_id ?? 0) || 0
  return n > 0 ? String(n) : null
}

const IS_PG_CLIENT = String((knex as any)?.client?.config?.client || '').includes('pg')
const MAX_SESSION_COUNTER_CACHE_ENTRIES = 24

type SessionCounters = {
  issuer: Map<number, number>
  verifier: Map<number, number>
}

const sessionCountersCache = new Map<string, Promise<SessionCounters>>()

function getSessionCounterCacheKey(blockHeight?: number): string {
  return typeof blockHeight === 'number' ? `h:${blockHeight}` : 'live'
}

export async function getParticipantSessionCounters(blockHeight?: number): Promise<SessionCounters> {
  const cacheKey = getSessionCounterCacheKey(blockHeight)
  const cached = sessionCountersCache.get(cacheKey)
  if (cached) return cached
  if (sessionCountersCache.size >= MAX_SESSION_COUNTER_CACHE_ENTRIES) {
    sessionCountersCache.clear()
  }

  const loadPromise = (async (): Promise<SessionCounters> => {
    let sessionRows: Array<{ session_records: any }> = []

    if (typeof blockHeight === 'number') {
      if (IS_PG_CLIENT) {
        sessionRows = await knex('participant_session_history as psh')
          .distinctOn('psh.session_id')
          .select('psh.session_records')
          .where('psh.height', '<=', blockHeight)
          .orderBy('psh.session_id', 'asc')
          .orderBy('psh.height', 'desc')
          .orderBy('psh.created_at', 'desc')
          .orderBy('psh.id', 'desc')
      } else {
        const ranked = knex('participant_session_history as psh')
          .select(
            'psh.session_records',
            knex.raw(
              'ROW_NUMBER() OVER (PARTITION BY psh.session_id ORDER BY psh.height DESC, psh.created_at DESC, psh.id DESC) as rn'
            )
          )
          .where('psh.height', '<=', blockHeight)
          .as('ranked')
        sessionRows = await knex.from(ranked).select('session_records').where('rn', 1)
      }
    } else {
      sessionRows = await knex('participant_sessions').select('session_records')
    }

    const issuer = new Map<number, number>()
    const verifier = new Map<number, number>()

    for (const session of sessionRows) {
      const recordsRaw =
        typeof (session as any).session_records === 'string'
          ? JSON.parse((session as any).session_records)
          : (session as any).session_records
      if (!Array.isArray(recordsRaw)) continue
      for (const entry of recordsRaw) {
        if (entry?.issuer_participant_id !== undefined && entry?.issuer_participant_id !== null) {
          const issuerParticipantId = Number(entry.issuer_participant_id)
          if (Number.isFinite(issuerParticipantId) && issuerParticipantId > 0) {
            issuer.set(issuerParticipantId, (issuer.get(issuerParticipantId) || 0) + 1)
          }
        }
        if (entry?.verifier_participant_id !== undefined && entry?.verifier_participant_id !== null) {
          const verifierParticipantId = Number(entry.verifier_participant_id)
          if (Number.isFinite(verifierParticipantId) && verifierParticipantId > 0) {
            verifier.set(verifierParticipantId, (verifier.get(verifierParticipantId) || 0) + 1)
          }
        }
      }
    }

    return { issuer, verifier }
  })()

  sessionCountersCache.set(cacheKey, loadPromise)
  return loadPromise
}

export interface CredentialSchemaStats {
  participants: number
  participants_ecosystem: number
  participants_issuer_grantor: number
  participants_issuer: number
  participants_verifier_grantor: number
  participants_verifier: number
  participants_holder: number
  weight: number
  issued: number
  verified: number
  ecosystem_slash_events: number
  ecosystem_slashed_amount: number
  ecosystem_slashed_amount_repaid: number
  network_slash_events: number
  network_slashed_amount: number
  network_slashed_amount_repaid: number
}

export async function getSchemaController(schemaId: number, blockHeight?: number): Promise<string | null> {
  if (typeof blockHeight === 'number') {
    const schemaHistory = await knex('credential_schema_history')
      .where('credential_schema_id', schemaId)
      .where('height', '<=', blockHeight)
      .orderBy('height', 'desc')
      .orderBy('created_at', 'desc')
      .first()

    if (!schemaHistory) {
      return null
    }

    const ecosystemHistory = await knex('ecosystem_history')
      .where('ecosystem_id', schemaHistory.ecosystem_id)
      .where('height', '<=', blockHeight)
      .orderBy('height', 'desc')
      .orderBy('created_at', 'desc')
      .first()

    return participantFromTrRow(ecosystemHistory as Record<string, unknown>) ?? null
  }

  const schema = await knex('credential_schemas').where('id', schemaId).first()

  if (!schema) {
    return null
  }

  const ec = await knex('ecosystem').where('id', schema.ecosystem_id).first()

  return participantFromTrRow(ec as Record<string, unknown>) ?? null
}

export async function getParticipantsForSchema(schemaId: number, blockHeight?: number): Promise<any[]> {
  if (typeof blockHeight === 'number') {
    if (IS_PG_CLIENT) {
      return await knex('participant_history as ph')
        .distinctOn('ph.participant_id')
        .select('ph.*')
        .where('ph.schema_id', Number(schemaId))
        .where('ph.height', '<=', blockHeight)
        .orderBy('ph.participant_id', 'asc')
        .orderBy('ph.height', 'desc')
        .orderBy('ph.created_at', 'desc')
        .orderBy('ph.id', 'desc')
    }
    const ranked = knex('participant_history as ph')
      .select(
        'ph.*',
        knex.raw(
          'ROW_NUMBER() OVER (PARTITION BY ph.participant_id ORDER BY ph.height DESC, ph.created_at DESC, ph.id DESC) as rn'
        )
      )
      .where('ph.schema_id', Number(schemaId))
      .where('ph.height', '<=', blockHeight)
      .as('ranked')
    return await knex.from(ranked).select('*').where('rn', 1)
  }
  return await knex('participants').where('schema_id', Number(schemaId)).select('*')
}

export async function calculateIssuedVerifiedForSchema(
  _schemaId: number,
  participantIds: Set<number>,
  blockHeight?: number
): Promise<{ issued: number; verified: number }> {
  let totalIssued = 0
  let totalVerified = 0

  if (participantIds.size === 0) {
    return { issued: 0, verified: 0 }
  }

  const counters = await getParticipantSessionCounters(blockHeight)
  for (const participantId of participantIds) {
    totalIssued += counters.issuer.get(Number(participantId)) || 0
    totalVerified += counters.verifier.get(Number(participantId)) || 0
  }

  return { issued: totalIssued, verified: totalVerified }
}

export async function calculateSlashStatsForSchema(
  schemaId: number,
  participantIds: Set<number>,
  _trController: string | null,
  blockHeight?: number
): Promise<{
  ecosystem_slash_events: number
  ecosystem_slashed_amount: number
  ecosystem_slashed_amount_repaid: number
  network_slash_events: number
  network_slashed_amount: number
  network_slashed_amount_repaid: number
}> {
  let ecosystemSlashEvents = 0
  let ecosystemSlashedAmount = 0
  let ecosystemSlashedAmountRepaid = 0
  let networkSlashEvents = 0
  let networkSlashedAmount = 0
  let networkSlashedAmountRepaid = 0

  if (participantIds.size === 0) {
    return {
      ecosystem_slash_events: 0,
      ecosystem_slashed_amount: 0,
      ecosystem_slashed_amount_repaid: 0,
      network_slash_events: 0,
      network_slashed_amount: 0,
      network_slashed_amount_repaid: 0,
    }
  }

  const participantIdArray = Array.from(participantIds)

  let slashEvents: any[]
  if (typeof blockHeight === 'number') {
    slashEvents = await knex('participant_history')
      .whereIn('participant_id', participantIdArray)
      .whereRaw('schema_id = ?', [Number(schemaId)])
      .where('event_type', 'SLASH_PARTICIPANT_TRUST_DEPOSIT')
      .where('height', '<=', blockHeight)
      .select('participant_id', 'role', 'slashed_deposit', 'repaid_deposit', 'height', 'created_at')
      .orderBy('participant_id', 'asc')
      .orderBy('height', 'asc')
      .orderBy('created_at', 'asc')
  } else {
    slashEvents = await knex('participant_history')
      .whereIn('participant_id', participantIdArray)
      .whereRaw('schema_id = ?', [Number(schemaId)])
      .where('event_type', 'SLASH_PARTICIPANT_TRUST_DEPOSIT')
      .select('participant_id', 'role', 'slashed_deposit', 'repaid_deposit', 'height', 'created_at')
      .orderBy('participant_id', 'asc')
      .orderBy('height', 'asc')
      .orderBy('created_at', 'asc')
  }

  const prevSlashedDeposits = new Map<string, number>()
  const prevRepaidDeposits = new Map<string, number>()

  for (const event of slashEvents) {
    const participantIdStr = String(event.participant_id)
    const prevSlashed = prevSlashedDeposits.get(participantIdStr) || 0
    const currentSlashed =
      typeof event.slashed_deposit === 'number' ? event.slashed_deposit : Number(event.slashed_deposit)
    const incrementalSlashed = currentSlashed - prevSlashed

    if (incrementalSlashed <= 0) {
      prevSlashedDeposits.set(participantIdStr, currentSlashed)
      const currentRepaid =
        typeof event.repaid_deposit === 'number' ? event.repaid_deposit : Number(event.repaid_deposit)
      prevRepaidDeposits.set(participantIdStr, currentRepaid)
      continue
    }

    prevSlashedDeposits.set(participantIdStr, currentSlashed)

    const isEcosystemSlash = event.role === 'ECOSYSTEM'

    const repaid = typeof event.repaid_deposit === 'number' ? event.repaid_deposit : Number(event.repaid_deposit)
    const prevRepaid = prevRepaidDeposits.get(participantIdStr) || 0
    const incrementalRepaid = repaid - prevRepaid
    prevRepaidDeposits.set(participantIdStr, repaid)

    if (isEcosystemSlash) {
      ecosystemSlashEvents++
      ecosystemSlashedAmount += incrementalSlashed
      if (incrementalRepaid > 0) ecosystemSlashedAmountRepaid += incrementalRepaid
    } else {
      networkSlashEvents++
      networkSlashedAmount += incrementalSlashed
      if (incrementalRepaid > 0) networkSlashedAmountRepaid += incrementalRepaid
    }
  }

  return {
    ecosystem_slash_events: ecosystemSlashEvents,
    ecosystem_slashed_amount: ecosystemSlashedAmount,
    ecosystem_slashed_amount_repaid: ecosystemSlashedAmountRepaid,
    network_slash_events: networkSlashEvents,
    network_slashed_amount: networkSlashedAmount,
    network_slashed_amount_repaid: networkSlashedAmountRepaid,
  }
}

export async function calculateCredentialSchemaStats(
  schemaId: number,
  blockHeight?: number
): Promise<CredentialSchemaStats> {
  const batch = await calculateCredentialSchemaStatsBatch([schemaId], blockHeight)
  return (
    batch.get(Number(schemaId)) || {
      participants: 0,
      participants_ecosystem: 0,
      participants_issuer_grantor: 0,
      participants_issuer: 0,
      participants_verifier_grantor: 0,
      participants_verifier: 0,
      participants_holder: 0,
      weight: 0,
      issued: 0,
      verified: 0,
      ecosystem_slash_events: 0,
      ecosystem_slashed_amount: 0,
      ecosystem_slashed_amount_repaid: 0,
      network_slash_events: 0,
      network_slashed_amount: 0,
      network_slashed_amount_repaid: 0,
    }
  )
}

export async function calculateCredentialSchemaStatsBatch(
  schemaIdsInput: number[],
  blockHeight?: number
): Promise<Map<number, CredentialSchemaStats>> {
  const schemaIds = Array.from(
    new Set(schemaIdsInput.map((id) => Number(id)).filter((id) => Number.isFinite(id) && id > 0))
  )
  const result = new Map<number, CredentialSchemaStats>()
  if (schemaIds.length === 0) return result

  let now = new Date()
  if (typeof blockHeight === 'number' && Number.isFinite(blockHeight) && blockHeight >= 0) {
    now = await getBlockChainTimeAsOf(blockHeight, { logContext: '[cs_stats]' })
  }

  let participants: any[] = []
  if (typeof blockHeight === 'number') {
    if (IS_PG_CLIENT) {
      participants = await knex('participant_history as ph')
        .distinctOn('ph.participant_id')
        .select('ph.*')
        .whereIn('ph.schema_id', schemaIds)
        .where('ph.height', '<=', blockHeight)
        .orderBy('ph.participant_id', 'asc')
        .orderBy('ph.height', 'desc')
        .orderBy('ph.created_at', 'desc')
        .orderBy('ph.id', 'desc')
    } else {
      const rankedParticipants = knex('participant_history as ph')
        .select(
          'ph.*',
          knex.raw(
            'ROW_NUMBER() OVER (PARTITION BY ph.participant_id ORDER BY ph.height DESC, ph.created_at DESC, ph.id DESC) as rn'
          )
        )
        .whereIn('ph.schema_id', schemaIds)
        .where('ph.height', '<=', blockHeight)
        .as('ranked')
      participants = await knex.from(rankedParticipants).select('*').where('rn', 1)
    }
  } else {
    participants = await knex('participants').whereIn('schema_id', schemaIds).select('*')
  }

  const counters = await getParticipantSessionCounters(blockHeight)
  // Key = numeric schema id, value = unique participant account identifier (address string).
  const activeParticipantsBySchema = new Map<number, Set<string>>()
  const activeParticipantsEcosystemBySchema = new Map<number, Set<string>>()
  const activeParticipantsIssuerGrantorBySchema = new Map<number, Set<string>>()
  const activeParticipantsIssuerBySchema = new Map<number, Set<string>>()
  const activeParticipantsVerifierGrantorBySchema = new Map<number, Set<string>>()
  const activeParticipantsVerifierBySchema = new Map<number, Set<string>>()
  const activeParticipantsHolderBySchema = new Map<number, Set<string>>()
  const participantIdsBySchema = new Map<number, Set<number>>()

  for (const schemaId of schemaIds) {
    result.set(schemaId, {
      participants: 0,
      participants_ecosystem: 0,
      participants_issuer_grantor: 0,
      participants_issuer: 0,
      participants_verifier_grantor: 0,
      participants_verifier: 0,
      participants_holder: 0,
      weight: 0,
      issued: 0,
      verified: 0,
      ecosystem_slash_events: 0,
      ecosystem_slashed_amount: 0,
      ecosystem_slashed_amount_repaid: 0,
      network_slash_events: 0,
      network_slashed_amount: 0,
      network_slashed_amount_repaid: 0,
    })
    activeParticipantsBySchema.set(schemaId, new Set<string>())
    activeParticipantsEcosystemBySchema.set(schemaId, new Set<string>())
    activeParticipantsIssuerGrantorBySchema.set(schemaId, new Set<string>())
    activeParticipantsIssuerBySchema.set(schemaId, new Set<string>())
    activeParticipantsVerifierGrantorBySchema.set(schemaId, new Set<string>())
    activeParticipantsVerifierBySchema.set(schemaId, new Set<string>())
    activeParticipantsHolderBySchema.set(schemaId, new Set<string>())
    participantIdsBySchema.set(schemaId, new Set<number>())
  }

  for (const participant of participants) {
    const schemaId = Number(participant.schema_id)
    if (!result.has(schemaId)) continue

    const participantId = Number(participant.participant_id || participant.id)
    if (!Number.isFinite(participantId) || participantId <= 0) continue

    participantIdsBySchema.get(schemaId)?.add(participantId)

    const participantState = calculateParticipantState(
      {
        repaid: participant.repaid,
        slashed: participant.slashed,
        revoked: participant.revoked,
        effective_from: participant.effective_from,
        effective_until: participant.effective_until,
        role: participant.role,
        op_state: participant.op_state,
        op_exp: participant.op_exp,
        validator_participant_id: participant.validator_participant_id,
      },
      now
    )

    const participantRow = participant as Record<string, unknown>
    const corpId = Number(participantRow.corporation_id ?? 0) || 0
    const corp = corpId > 0 ? String(corpId) : ''
    if (participantState === 'ACTIVE' && corp) {
      activeParticipantsBySchema.get(schemaId)?.add(corp)
      if (participant.role === 'ECOSYSTEM') activeParticipantsEcosystemBySchema.get(schemaId)?.add(corp)
      if (participant.role === 'ISSUER_GRANTOR') activeParticipantsIssuerGrantorBySchema.get(schemaId)?.add(corp)
      if (participant.role === 'ISSUER') activeParticipantsIssuerBySchema.get(schemaId)?.add(corp)
      if (participant.role === 'VERIFIER_GRANTOR') activeParticipantsVerifierGrantorBySchema.get(schemaId)?.add(corp)
      if (participant.role === 'VERIFIER') activeParticipantsVerifierBySchema.get(schemaId)?.add(corp)
      if (participant.role === 'HOLDER') activeParticipantsHolderBySchema.get(schemaId)?.add(corp)
    }

    const stats = result.get(schemaId)
    if (!stats) continue
    if (participant.weight != null) {
      stats.weight += typeof participant.weight === 'number' ? participant.weight : Number(participant.weight || 0)
    } else if (participant.deposit != null) {
      stats.weight += typeof participant.deposit === 'number' ? participant.deposit : Number(participant.deposit || 0)
    }

    stats.issued += counters.issuer.get(participantId) || 0
    stats.verified += counters.verifier.get(participantId) || 0

    if (typeof blockHeight === 'undefined') {
      stats.ecosystem_slash_events += Number(participant.ecosystem_slash_events ?? 0)
      stats.ecosystem_slashed_amount += Number(participant.ecosystem_slashed_amount ?? 0)
      stats.ecosystem_slashed_amount_repaid += Number(participant.ecosystem_slashed_amount_repaid ?? 0)
      stats.network_slash_events += Number(participant.network_slash_events ?? 0)
      stats.network_slashed_amount += Number(participant.network_slashed_amount ?? 0)
      stats.network_slashed_amount_repaid += Number(participant.network_slashed_amount_repaid ?? 0)
    }
  }

  const slashEvents: any[] =
    typeof blockHeight === 'number'
      ? await knex('participant_history')
          .select(
            'schema_id',
            'participant_id',
            'role',
            'slashed_deposit',
            'repaid_deposit',
            'height',
            'created_at',
            'id'
          )
          .whereIn('schema_id', schemaIds)
          .where('event_type', 'SLASH_PARTICIPANT_TRUST_DEPOSIT')
          .where('height', '<=', blockHeight)
          .orderBy('participant_id', 'asc')
          .orderBy('height', 'asc')
          .orderBy('created_at', 'asc')
          .orderBy('id', 'asc')
      : []

  const prevSlashedDeposits = new Map<number, number>()
  const prevRepaidDeposits = new Map<number, number>()

  for (const event of slashEvents) {
    const schemaId = Number(event.schema_id)
    const participantId = Number(event.participant_id)
    if (!result.has(schemaId) || !Number.isFinite(participantId)) continue

    if (!participantIdsBySchema.get(schemaId)?.has(participantId)) continue

    const prevSlashed = prevSlashedDeposits.get(participantId) || 0
    const currentSlashed =
      typeof event.slashed_deposit === 'number' ? event.slashed_deposit : Number(event.slashed_deposit || 0)
    const incrementalSlashed = currentSlashed - prevSlashed
    prevSlashedDeposits.set(participantId, currentSlashed)

    const repaid = typeof event.repaid_deposit === 'number' ? event.repaid_deposit : Number(event.repaid_deposit || 0)
    const prevRepaid = prevRepaidDeposits.get(participantId) || 0
    const incrementalRepaid = repaid - prevRepaid
    prevRepaidDeposits.set(participantId, repaid)

    if (incrementalSlashed <= 0) continue

    const stats = result.get(schemaId)
    if (!stats) continue
    const isEcosystemSlash = event.role === 'ECOSYSTEM'

    if (isEcosystemSlash) {
      stats.ecosystem_slash_events += 1
      stats.ecosystem_slashed_amount += incrementalSlashed
      if (incrementalRepaid > 0) stats.ecosystem_slashed_amount_repaid += incrementalRepaid
    } else {
      stats.network_slash_events += 1
      stats.network_slashed_amount += incrementalSlashed
      if (incrementalRepaid > 0) stats.network_slashed_amount_repaid += incrementalRepaid
    }
  }

  for (const [schemaId] of activeParticipantsBySchema.entries()) {
    const stats = result.get(schemaId)
    if (stats) {
      stats.participants_ecosystem = activeParticipantsEcosystemBySchema.get(schemaId)?.size || 0
      stats.participants_issuer_grantor = activeParticipantsIssuerGrantorBySchema.get(schemaId)?.size || 0
      stats.participants_issuer = activeParticipantsIssuerBySchema.get(schemaId)?.size || 0
      stats.participants_verifier_grantor = activeParticipantsVerifierGrantorBySchema.get(schemaId)?.size || 0
      stats.participants_verifier = activeParticipantsVerifierBySchema.get(schemaId)?.size || 0
      stats.participants_holder = activeParticipantsHolderBySchema.get(schemaId)?.size || 0
      stats.participants = activeParticipantsBySchema.get(schemaId)?.size || 0
    }
  }

  return result
}
