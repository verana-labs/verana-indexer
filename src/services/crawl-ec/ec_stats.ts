import knex from '../../common/utils/db_connection'
import { TR_STATS_FIELDS } from '../../common/utils/stats_fields'
import { Ecosystem } from '../../models/ecosystem'
import { resolveAddressByCorporationId } from '../crawl-co/corporation_resolve'
import { calculateCredentialSchemaStatsBatch, getParticipantSessionCounters } from '../crawl-cs/cs_stats'

const IS_PG_CLIENT = String((knex as any)?.client?.config?.client || '').includes('pg')

export interface EcosystemStats {
  participants: number
  participants_ecosystem: number
  participants_issuer_grantor: number
  participants_issuer: number
  participants_verifier_grantor: number
  participants_verifier: number
  participants_holder: number
  active_schemas: number
  archived_schemas: number
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

export { TR_STATS_FIELDS }

export async function getSchemasForEcosystem(ecosystemId: number, blockHeight?: number): Promise<any[]> {
  if (typeof blockHeight === 'number') {
    if (IS_PG_CLIENT) {
      return await knex('credential_schema_history as csh')
        .distinctOn('csh.credential_schema_id')
        .select('csh.*')
        .where('csh.ecosystem_id', String(ecosystemId))
        .where('csh.height', '<=', blockHeight)
        .orderBy('csh.credential_schema_id', 'asc')
        .orderBy('csh.height', 'desc')
        .orderBy('csh.created_at', 'desc')
        .orderBy('csh.id', 'desc')
    }
    const ranked = knex('credential_schema_history as csh')
      .select(
        'csh.*',
        knex.raw(
          'ROW_NUMBER() OVER (PARTITION BY csh.credential_schema_id ORDER BY csh.height DESC, csh.created_at DESC, csh.id DESC) as rn'
        )
      )
      .where('csh.ecosystem_id', String(ecosystemId))
      .where('csh.height', '<=', blockHeight)
      .as('ranked')
    return await knex.from(ranked).select('*').where('rn', 1)
  }
  return await knex('credential_schemas').where('ecosystem_id', String(ecosystemId)).select('*')
}

export async function getEcosystemController(ecosystemId: number, blockHeight?: number): Promise<string | null> {
  if (typeof blockHeight === 'number') {
    const ecosystemHistory = await knex('ecosystem_history')
      .where('ecosystem_id', ecosystemId)
      .where('height', '<=', blockHeight)
      .orderBy('height', 'desc')
      .orderBy('created_at', 'desc')
      .first()
    return resolveAddressByCorporationId(Number(ecosystemHistory?.corporation_id ?? 0) || 0)
  }
  const ec = await Ecosystem.query().findById(ecosystemId)
  return resolveAddressByCorporationId(Number(ec?.corporation_id ?? 0) || 0)
}

export async function getParticipantsForSchema(schemaId: number, blockHeight?: number): Promise<any[]> {
  if (typeof blockHeight === 'number') {
    if (IS_PG_CLIENT) {
      return await knex('participant_history as ph')
        .distinctOn('ph.participant_id')
        .select('ph.*')
        .where('ph.schema_id', schemaId)
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
      .where('ph.schema_id', schemaId)
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
      .whereRaw('schema_id = ?', [schemaId])
      .where('event_type', 'SLASH_PARTICIPANT_TRUST_DEPOSIT')
      .where('height', '<=', blockHeight)
      .select('participant_id', 'role', 'slashed_deposit', 'repaid_deposit', 'height', 'created_at')
      .orderBy('participant_id', 'asc')
      .orderBy('height', 'asc')
      .orderBy('created_at', 'asc')
  } else {
    slashEvents = await knex('participant_history')
      .whereIn('participant_id', participantIdArray)
      .whereRaw('schema_id = ?', [schemaId])
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

    const isEcosystemParticipant = event.role === 'ECOSYSTEM'

    if (isEcosystemParticipant) {
      networkSlashEvents++
      networkSlashedAmount += incrementalSlashed

      const repaid = typeof event.repaid_deposit === 'number' ? event.repaid_deposit : Number(event.repaid_deposit)
      const prevRepaid = prevRepaidDeposits.get(participantIdStr) || 0
      const incrementalRepaid = repaid - prevRepaid
      if (incrementalRepaid > 0) {
        networkSlashedAmountRepaid += incrementalRepaid
      }
      prevRepaidDeposits.set(participantIdStr, repaid)
    } else {
      ecosystemSlashEvents++
      ecosystemSlashedAmount += incrementalSlashed

      const repaid = typeof event.repaid_deposit === 'number' ? event.repaid_deposit : Number(event.repaid_deposit)
      const prevRepaid = prevRepaidDeposits.get(participantIdStr) || 0
      const incrementalRepaid = repaid - prevRepaid
      if (incrementalRepaid > 0) {
        ecosystemSlashedAmountRepaid += incrementalRepaid
      }
      prevRepaidDeposits.set(participantIdStr, repaid)
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

export async function calculateEcosystemStats(ecosystemId: number, blockHeight?: number): Promise<EcosystemStats> {
  const batch = await calculateEcosystemStatsBatch([ecosystemId], blockHeight)
  return (
    batch.get(Number(ecosystemId)) || {
      participants: 0,
      participants_ecosystem: 0,
      participants_issuer_grantor: 0,
      participants_issuer: 0,
      participants_verifier_grantor: 0,
      participants_verifier: 0,
      participants_holder: 0,
      active_schemas: 0,
      archived_schemas: 0,
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

export async function calculateEcosystemStatsBatch(
  ecosystemIdsInput: number[],
  blockHeight?: number
): Promise<Map<number, EcosystemStats>> {
  const ecosystemIds = Array.from(
    new Set(ecosystemIdsInput.map((id) => Number(id)).filter((id) => Number.isFinite(id) && id > 0))
  )
  const result = new Map<number, EcosystemStats>()
  if (ecosystemIds.length === 0) return result

  let schemas: any[] = []
  if (typeof blockHeight === 'number') {
    if (IS_PG_CLIENT) {
      schemas = await knex('credential_schema_history as csh')
        .distinctOn('csh.credential_schema_id')
        .select('csh.credential_schema_id', 'csh.ecosystem_id', 'csh.archived')
        .whereIn('csh.ecosystem_id', ecosystemIds)
        .where('csh.height', '<=', blockHeight)
        .orderBy('csh.credential_schema_id', 'asc')
        .orderBy('csh.height', 'desc')
        .orderBy('csh.created_at', 'desc')
        .orderBy('csh.id', 'desc')
    } else {
      const ranked = knex('credential_schema_history as csh')
        .select(
          'csh.credential_schema_id',
          'csh.ecosystem_id',
          'csh.archived',
          knex.raw(
            'ROW_NUMBER() OVER (PARTITION BY csh.credential_schema_id ORDER BY csh.height DESC, csh.created_at DESC, csh.id DESC) as rn'
          )
        )
        .whereIn('csh.ecosystem_id', ecosystemIds)
        .where('csh.height', '<=', blockHeight)
        .as('ranked')
      schemas = await knex.from(ranked).select('credential_schema_id', 'ecosystem_id', 'archived').where('rn', 1)
    }
  } else {
    schemas = await knex('credential_schemas')
      .select('id as credential_schema_id', 'ecosystem_id', 'archived')
      .whereIn('ecosystem_id', ecosystemIds)
  }

  for (const ecosystemId of ecosystemIds) {
    result.set(ecosystemId, {
      participants: 0,
      participants_ecosystem: 0,
      participants_issuer_grantor: 0,
      participants_issuer: 0,
      participants_verifier_grantor: 0,
      participants_verifier: 0,
      participants_holder: 0,
      active_schemas: 0,
      archived_schemas: 0,
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
  }

  const schemaIds: number[] = []
  const schemaToTr = new Map<number, number>()
  for (const schema of schemas) {
    const ecosystemId = Number(schema.ecosystem_id)
    const schemaId = Number(schema.credential_schema_id)
    if (!Number.isFinite(ecosystemId) || !Number.isFinite(schemaId)) continue
    schemaToTr.set(schemaId, ecosystemId)
    schemaIds.push(schemaId)

    const trStats = result.get(ecosystemId)
    if (!trStats) continue
    if (schema.archived !== null && schema.archived !== undefined) trStats.archived_schemas += 1
    else trStats.active_schemas += 1
  }

  const schemaStats = await calculateCredentialSchemaStatsBatch(schemaIds, blockHeight)
  for (const [schemaId, stats] of schemaStats.entries()) {
    const ecosystemId = schemaToTr.get(schemaId)
    if (!ecosystemId) continue
    const trStats = result.get(ecosystemId)
    if (!trStats) continue

    trStats.participants += Number(stats.participants || 0)
    trStats.participants_ecosystem += Number(stats.participants_ecosystem || 0)
    trStats.participants_issuer_grantor += Number(stats.participants_issuer_grantor || 0)
    trStats.participants_issuer += Number(stats.participants_issuer || 0)
    trStats.participants_verifier_grantor += Number(stats.participants_verifier_grantor || 0)
    trStats.participants_verifier += Number(stats.participants_verifier || 0)
    trStats.participants_holder += Number(stats.participants_holder || 0)
    trStats.weight += Number(stats.weight || 0)
    trStats.issued += Number(stats.issued || 0)
    trStats.verified += Number(stats.verified || 0)
    trStats.ecosystem_slash_events += Number(stats.ecosystem_slash_events || 0)
    trStats.ecosystem_slashed_amount += Number(stats.ecosystem_slashed_amount || 0)
    trStats.ecosystem_slashed_amount_repaid += Number(stats.ecosystem_slashed_amount_repaid || 0)
    trStats.network_slash_events += Number(stats.network_slash_events || 0)
    trStats.network_slashed_amount += Number(stats.network_slashed_amount || 0)
    trStats.network_slashed_amount_repaid += Number(stats.network_slashed_amount_repaid || 0)
  }

  return result
}
