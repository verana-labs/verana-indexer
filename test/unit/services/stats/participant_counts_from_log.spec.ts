import { ServiceBroker } from 'moleculer'
import knex from '../../../../src/common/utils/db_connection'
import Stats from '../../../../src/models/stats'
import CredentialSchemaDatabaseService from '../../../../src/services/crawl-cs/cs_database.service'
import EcosystemDatabaseService from '../../../../src/services/crawl-ec/ec_database.service'
import ParticipantAPIService from '../../../../src/services/crawl-pp/pp_apis.service'
import { applyScheduledParticipantFlipsForBlock } from '../../../../src/services/crawl-pp/pp_flip_processor'
import StatsAPIService from '../../../../src/services/stats/stats_api.service'
import StatsCalculationService from '../../../../src/services/stats/stats_calculation.service'
import { PARTICIPANT_COUNT_ENTITY_KIND } from '../../../../src/services/stats/stats_snapshot'

const ECOSYSTEM_ID = 87_100_001
const SCHEMA_ID = 87_100_002
const VALIDATOR_ID = 87_100_010
const ISSUER_A_ID = 87_100_011
const ISSUER_B_ID = 87_100_012
const VERIFIER_ID = 87_100_013
const CORPORATION_ID = 87_100_020
const PARTICIPANT_IDS = [VALIDATOR_ID, ISSUER_A_ID, ISSUER_B_ID, VERIFIER_ID]

const HEIGHT_BEFORE = 87_200_001
const HEIGHT_ENTER = 87_200_002
const HEIGHT_EXIT = 87_200_003
const BLOCK_HEIGHTS = [HEIGHT_BEFORE, HEIGHT_ENTER, HEIGHT_EXIT]

const TIME_BEFORE = new Date('2026-03-05T10:00:00.000Z')
const TIME_ENTER = new Date('2026-03-05T10:10:00.000Z')
const TIME_EXIT = new Date('2026-03-05T10:50:00.000Z')
const HOUR_BUCKET = new Date('2026-03-05T10:00:00.000Z')
const DAY_BUCKET = new Date('2026-03-05T00:00:00.000Z')
const PREVIOUS_DAY_BUCKET = new Date('2026-03-04T00:00:00.000Z')
const PREVIOUS_HOUR_BUCKET = new Date('2026-03-04T23:00:00.000Z')
const CREATED = new Date('2026-03-01T00:00:00.000Z')

const ROLE_TYPE = { ANY: 0, ECOSYSTEM: 1, ISSUER: 3, VERIFIER: 5 }

const FLIP_ENTER_ACTIVE = 1
const FLIP_EXIT_ACTIVE = 2

const DROPPED_PER_ROLE_COLUMNS = [
  'participants_ecosystem',
  'participants_issuer_grantor',
  'participants_issuer',
  'participants_verifier_grantor',
  'participants_verifier',
  'participants_holder',
].flatMap((metric) => [`cumulative_${metric}`, `delta_${metric}`])

const STATS_METRICS = [
  'participants',
  'active_ecosystems',
  'archived_ecosystems',
  'active_schemas',
  'archived_schemas',
  'weight',
  'issued',
  'verified',
  'ecosystem_slash_events',
  'ecosystem_slashed_amount',
  'ecosystem_slashed_amount_repaid',
  'network_slash_events',
  'network_slashed_amount',
  'network_slashed_amount_repaid',
]

const STALE_DENORMALIZED_COUNTS = {
  participants: 999,
  participants_ecosystem: 999,
  participants_issuer_grantor: 999,
  participants_issuer: 888,
  participants_verifier_grantor: 999,
  participants_verifier: 777,
  participants_holder: 999,
}

const broker = new ServiceBroker({ logger: false })
const credentialSchemaService: any = new CredentialSchemaDatabaseService(broker)
const ecosystemService: any = new EcosystemDatabaseService(broker)
const participantService: any = new ParticipantAPIService(broker)
const statsApiService: any = new StatsAPIService(broker)
const statsCalculationService: any = new StatsCalculationService(broker)

function participantRow(over: Record<string, unknown>) {
  return {
    schema_id: SCHEMA_ID,
    corporation_id: CORPORATION_ID,
    validator_participant_id: null,
    created: CREATED,
    modified: CREATED,
    effective_from: TIME_ENTER,
    effective_until: null,
    is_active_now: false,
    last_valid_flip_version: 1,
    ...STALE_DENORMALIZED_COUNTS,
    ...over,
  }
}

function participantHistoryRow(participantId: number, role: string) {
  return {
    participant_id: participantId,
    schema_id: SCHEMA_ID,
    corporation_id: CORPORATION_ID,
    role,
    event_type: 'CREATE',
    height: HEIGHT_BEFORE,
    created: CREATED,
    modified: CREATED,
    participants: STALE_DENORMALIZED_COUNTS.participants,
  }
}

function scheduledFlip(participantId: number, flipKind: number, flipAtTime: Date) {
  return { participant_id: participantId, version: 1, flip_at_time: flipAtTime, flip_kind: flipKind, status: 0 }
}

function statsRowForSchema(granularity: string, timestamp: Date, cumulativeParticipants: number) {
  return {
    granularity,
    timestamp,
    entity_type: 'CREDENTIAL_SCHEMA',
    entity_id: SCHEMA_ID,
    cumulative_participants: cumulativeParticipants,
  }
}

function globalStatsRow(timestamp: Date, cumulativeParticipants: number) {
  const row: Record<string, unknown> = {
    granularity: 'DAY',
    timestamp: timestamp.toISOString(),
    entity_type: 'GLOBAL',
    entity_id: null,
  }
  for (const metric of STATS_METRICS) {
    row[`cumulative_${metric}`] = metric === 'participants' ? cumulativeParticipants : 0
    row[`delta_${metric}`] = metric === 'participants' ? cumulativeParticipants : 0
  }
  return row
}

async function deleteFixture(): Promise<void> {
  await knex('participant_scheduled_flips').whereIn('participant_id', PARTICIPANT_IDS).del()
  await knex('participant_history').whereIn('participant_id', PARTICIPANT_IDS).del()
  await knex('participants').whereIn('id', PARTICIPANT_IDS).del()
  await knex('credential_schema_history').where('credential_schema_id', SCHEMA_ID).del()
  await knex('credential_schemas').where('id', SCHEMA_ID).del()
  await knex('ecosystem_history').where('ecosystem_id', ECOSYSTEM_ID).del()
  await knex('ecosystem').where('id', ECOSYSTEM_ID).del()
  await knex('entity_participant_changes')
    .whereIn('entity_id', [ECOSYSTEM_ID, SCHEMA_ID, ...PARTICIPANT_IDS])
    .del()
  await knex('stats').where('entity_type', 'CREDENTIAL_SCHEMA').where('entity_id', SCHEMA_ID).del()
  await knex('stats').where('entity_type', 'GLOBAL').whereIn('timestamp', [PREVIOUS_DAY_BUCKET, DAY_BUCKET]).del()
  await knex('block').whereIn('height', BLOCK_HEIGHTS).del()
}

async function insertFixture(): Promise<void> {
  await knex('block').insert(
    BLOCK_HEIGHTS.map((height, index) => ({
      height,
      hash: `participant-counts-${height}`,
      time: [TIME_BEFORE, TIME_ENTER, TIME_EXIT][index],
      proposer_address: 'participant-counts-proposer',
      data: {},
      tx_count: 0,
    }))
  )

  await knex('ecosystem').insert({
    id: ECOSYSTEM_ID,
    did: `did:example:ecosystem:${ECOSYSTEM_ID}`,
    created: CREATED,
    modified: CREATED,
    language: 'en',
    height: HEIGHT_BEFORE,
    corporation_id: CORPORATION_ID,
    ...STALE_DENORMALIZED_COUNTS,
  })
  await knex('ecosystem_history').insert({
    ecosystem_id: ECOSYSTEM_ID,
    did: `did:example:ecosystem:${ECOSYSTEM_ID}`,
    created: CREATED,
    modified: CREATED,
    language: 'en',
    event_type: 'CREATE',
    height: HEIGHT_BEFORE,
    corporation_id: CORPORATION_ID,
    ...STALE_DENORMALIZED_COUNTS,
  })

  const schemaColumns = {
    ecosystem_id: ECOSYSTEM_ID,
    issuer_grantor_validation_validity_period: 365,
    verifier_grantor_validation_validity_period: 365,
    issuer_validation_validity_period: 365,
    verifier_validation_validity_period: 365,
    holder_validation_validity_period: 365,
    issuer_onboarding_mode: 'OPEN',
    verifier_onboarding_mode: 'OPEN',
    is_active: true,
    created: CREATED,
    modified: CREATED,
  }
  await knex('credential_schemas').insert({
    id: SCHEMA_ID,
    ...schemaColumns,
    json_schema: JSON.stringify({ $id: `/vpr/v4/cs/js/${SCHEMA_ID}`, type: 'object' }),
    ...STALE_DENORMALIZED_COUNTS,
  })
  await knex('credential_schema_history').insert({
    credential_schema_id: SCHEMA_ID,
    ...schemaColumns,
    action: 'CREATE',
    height: HEIGHT_BEFORE,
    ...STALE_DENORMALIZED_COUNTS,
  })

  await knex('participants').insert([
    participantRow({ id: VALIDATOR_ID, role: 'ECOSYSTEM', did: `did:example:participant:${VALIDATOR_ID}` }),
    participantRow({
      id: ISSUER_A_ID,
      role: 'ISSUER',
      validator_participant_id: VALIDATOR_ID,
      did: `did:example:participant:${ISSUER_A_ID}`,
    }),
    participantRow({
      id: ISSUER_B_ID,
      role: 'ISSUER',
      validator_participant_id: VALIDATOR_ID,
      did: `did:example:participant:${ISSUER_B_ID}`,
    }),
    participantRow({
      id: VERIFIER_ID,
      role: 'VERIFIER',
      validator_participant_id: VALIDATOR_ID,
      corporation_id: 0,
      effective_until: TIME_EXIT,
      did: `did:example:participant:${VERIFIER_ID}`,
    }),
  ])
  await knex('participant_history').insert([
    participantHistoryRow(VALIDATOR_ID, 'ECOSYSTEM'),
    participantHistoryRow(ISSUER_A_ID, 'ISSUER'),
    participantHistoryRow(ISSUER_B_ID, 'ISSUER'),
    participantHistoryRow(VERIFIER_ID, 'VERIFIER'),
  ])

  await knex('participant_scheduled_flips').insert([
    ...PARTICIPANT_IDS.map((id) => scheduledFlip(id, FLIP_ENTER_ACTIVE, TIME_ENTER)),
    scheduledFlip(VERIFIER_ID, FLIP_EXIT_ACTIVE, TIME_EXIT),
  ])
}

function logValue(entityKind: number, entityId: number, roleType: number, height: number): Promise<number> {
  return knex('entity_participant_changes')
    .select('value')
    .where({ entity_kind: entityKind, entity_id: entityId, type: roleType })
    .where('height', '<=', height)
    .orderBy('height', 'desc')
    .first()
    .then((row) => Number(row?.value ?? 0))
}

function countParticipants(entityKind: number, entityId: number, roleType: number, height: number): Promise<number> {
  return statsApiService
    .getParticipantsAtHeight({
      params: { entity_kind: entityKind, entity_id: String(entityId), role_type: roleType },
      meta: { blockHeight: height },
    })
    .then((res: any) => Number(res.participants))
}

function snapshotCounts(entityType: string, entityId: number, height: number): Promise<Record<string, number>> {
  return statsApiService.getSnapshot({
    params: { entity_type: entityType, entity_id: String(entityId) },
    meta: { blockHeight: height },
  })
}

async function schemaFromApi(height: number): Promise<Record<string, number>> {
  const res = await credentialSchemaService.get({ params: { id: SCHEMA_ID }, meta: { blockHeight: height } })
  return res.schema
}

async function ecosystemFromApi(height: number): Promise<Record<string, number>> {
  const res = await ecosystemService.getEcosystem({
    params: { ecosystem_id: ECOSYSTEM_ID },
    meta: { blockHeight: height },
  })
  return res.ecosystem
}

async function participantFromApi(participantId: number, height: number): Promise<Record<string, number>> {
  const res = await participantService.getParticipant({
    params: { id: participantId },
    meta: { blockHeight: height },
  })
  return res.participant
}

describe('participant counts resolved from the counter log', () => {
  beforeAll(async () => {
    await deleteFixture()
    await insertFixture()
    await applyScheduledParticipantFlipsForBlock({ height: HEIGHT_BEFORE, blockTime: TIME_BEFORE })
    await applyScheduledParticipantFlipsForBlock({ height: HEIGHT_ENTER, blockTime: TIME_ENTER })
  })

  afterAll(async () => {
    await deleteFixture()
    await knex.destroy()
  })

  describe('the validator chain walk excludes the participant itself', () => {
    it('writes no PARTICIPANT row for a leaf entry', async () => {
      const leafRows = await knex('entity_participant_changes')
        .where('entity_kind', PARTICIPANT_COUNT_ENTITY_KIND.PARTICIPANT)
        .whereIn('entity_id', [ISSUER_A_ID, ISSUER_B_ID, VERIFIER_ID])

      expect(leafRows).toEqual([])
    })

    it('counts the active children of a validator without counting the validator', async () => {
      expect(await logValue(PARTICIPANT_COUNT_ENTITY_KIND.PARTICIPANT, VALIDATOR_ID, ROLE_TYPE.ANY, HEIGHT_ENTER)).toBe(
        3
      )
      expect(
        await logValue(PARTICIPANT_COUNT_ENTITY_KIND.PARTICIPANT, VALIDATOR_ID, ROLE_TYPE.ECOSYSTEM, HEIGHT_ENTER)
      ).toBe(0)
    })
  })

  describe('the counter log counts entries, not Corporations', () => {
    it('counts two entries of the same Corporation on one schema as two', async () => {
      const corporationIds = await knex('participants')
        .whereIn('id', [ISSUER_A_ID, ISSUER_B_ID])
        .pluck('corporation_id')
      expect(new Set(corporationIds.map(Number))).toEqual(new Set([CORPORATION_ID]))

      expect((await schemaFromApi(HEIGHT_ENTER)).participants_issuer).toBe(2)
    })

    it('counts an entry that has no owning Corporation', async () => {
      const verifier = await knex('participants').where('id', VERIFIER_ID).first()
      expect(Number(verifier.corporation_id)).toBe(0)

      expect((await schemaFromApi(HEIGHT_ENTER)).participants_verifier).toBe(1)
      expect((await schemaFromApi(HEIGHT_ENTER)).participants).toBe(4)
    })
  })

  describe('the entity endpoint, count-participants and the snapshot agree', () => {
    it('agrees on the Ecosystem', async () => {
      const inline = await ecosystemFromApi(HEIGHT_ENTER)
      const snapshot = await snapshotCounts('ECOSYSTEM', ECOSYSTEM_ID, HEIGHT_ENTER)

      expect(inline.participants).toBe(4)
      expect(
        await countParticipants(PARTICIPANT_COUNT_ENTITY_KIND.ECOSYSTEM, ECOSYSTEM_ID, ROLE_TYPE.ANY, HEIGHT_ENTER)
      ).toBe(inline.participants)
      expect(snapshot.participants).toBe(inline.participants)
      expect(
        await countParticipants(PARTICIPANT_COUNT_ENTITY_KIND.ECOSYSTEM, ECOSYSTEM_ID, ROLE_TYPE.ISSUER, HEIGHT_ENTER)
      ).toBe(inline.participants_issuer)
      expect(snapshot.participants_issuer).toBe(inline.participants_issuer)
    })

    it('agrees on the CredentialSchema', async () => {
      const inline = await schemaFromApi(HEIGHT_ENTER)
      const snapshot = await snapshotCounts('CREDENTIAL_SCHEMA', SCHEMA_ID, HEIGHT_ENTER)

      expect(inline.participants).toBe(4)
      expect(
        await countParticipants(PARTICIPANT_COUNT_ENTITY_KIND.CREDENTIAL_SCHEMA, SCHEMA_ID, ROLE_TYPE.ANY, HEIGHT_ENTER)
      ).toBe(inline.participants)
      expect(snapshot.participants).toBe(inline.participants)
      expect(
        await countParticipants(
          PARTICIPANT_COUNT_ENTITY_KIND.CREDENTIAL_SCHEMA,
          SCHEMA_ID,
          ROLE_TYPE.VERIFIER,
          HEIGHT_ENTER
        )
      ).toBe(inline.participants_verifier)
      expect(snapshot.participants_verifier).toBe(inline.participants_verifier)
    })

    it('agrees on the Participant', async () => {
      const inline = await participantFromApi(VALIDATOR_ID, HEIGHT_ENTER)
      const snapshot = await snapshotCounts('PARTICIPANT', VALIDATOR_ID, HEIGHT_ENTER)

      expect(inline.participants).toBe(3)
      expect(
        await countParticipants(PARTICIPANT_COUNT_ENTITY_KIND.PARTICIPANT, VALIDATOR_ID, ROLE_TYPE.ANY, HEIGHT_ENTER)
      ).toBe(inline.participants)
      expect(snapshot.participants).toBe(inline.participants)
      expect(
        await countParticipants(PARTICIPANT_COUNT_ENTITY_KIND.PARTICIPANT, VALIDATOR_ID, ROLE_TYPE.ISSUER, HEIGHT_ENTER)
      ).toBe(inline.participants_issuer)
      expect(snapshot.participants_issuer).toBe(inline.participants_issuer)
    })
  })

  describe('a transition driven only by block time', () => {
    beforeAll(async () => {
      await applyScheduledParticipantFlipsForBlock({ height: HEIGHT_EXIT, blockTime: TIME_EXIT })
    })

    it('drops the entry whose effective_until the block time crossed', async () => {
      const transactions = await knex('transaction').where('height', HEIGHT_EXIT)
      expect(transactions).toEqual([])

      const schema = await schemaFromApi(HEIGHT_EXIT)
      expect(schema.participants).toBe(3)
      expect(schema.participants_verifier).toBe(0)
      expect((await ecosystemFromApi(HEIGHT_EXIT)).participants).toBe(3)
      expect((await participantFromApi(VALIDATOR_ID, HEIGHT_EXIT)).participants).toBe(2)
    })

    it('keeps what the earlier block resolved to', async () => {
      expect((await schemaFromApi(HEIGHT_ENTER)).participants).toBe(4)
      expect((await schemaFromApi(HEIGHT_ENTER)).participants_verifier).toBe(1)
    })
  })

  describe('the bucket counters', () => {
    it('reads cumulative_participants from the log at the block closing the bucket', async () => {
      const stats = await statsCalculationService.computeCredentialSchemaStats('HOUR', String(SCHEMA_ID), HOUR_BUCKET)
      const snapshot = await snapshotCounts('CREDENTIAL_SCHEMA', SCHEMA_ID, HEIGHT_EXIT)

      expect(stats.cumulative_participants).toBe(3)
      expect(stats.cumulative_participants).toBe(snapshot.participants)
    })

    it('computes delta_participants against the previous bucket of the same granularity', async () => {
      await knex('stats').insert([
        statsRowForSchema('DAY', PREVIOUS_DAY_BUCKET, 1),
        statsRowForSchema('HOUR', PREVIOUS_HOUR_BUCKET, 99),
      ])

      const stats = await statsCalculationService.computeCredentialSchemaStats('DAY', String(SCHEMA_ID), DAY_BUCKET)

      expect(stats.cumulative_participants).toBe(3)
      expect(stats.delta_participants).toBe(2)
    })
  })

  describe('GLOBAL rows carry a null entity_id', () => {
    it('accepts a GLOBAL row with a null entity_id', () => {
      expect(() => Stats.fromJson(globalStatsRow(DAY_BUCKET, 3))).not.toThrow()
    })

    it('finds the GLOBAL row stored with a null entity_id', async () => {
      await knex('stats').insert([
        {
          granularity: 'DAY',
          timestamp: PREVIOUS_DAY_BUCKET,
          entity_type: 'GLOBAL',
          entity_id: null,
          cumulative_participants: 1,
        },
        {
          granularity: 'DAY',
          timestamp: DAY_BUCKET,
          entity_type: 'GLOBAL',
          entity_id: null,
          cumulative_participants: 4,
        },
      ])
      const stored = await knex('stats')
        .where('entity_type', 'GLOBAL')
        .whereIn('timestamp', [PREVIOUS_DAY_BUCKET, DAY_BUCKET])
      expect(stored.map((row) => row.entity_id)).toEqual([null, null])

      const single = await statsApiService.get({
        params: { granularity: 'DAY', timestamp: DAY_BUCKET.toISOString(), entity_type: 'GLOBAL' },
        meta: {},
      })
      expect(single.entity_id).toBeNull()
      expect(single.entity_type).toBe('GLOBAL')
      expect(single.cumulative_participants).toBe(4)

      const range = await statsApiService.stats({
        params: {
          granularity: 'DAY',
          timestamp_from: PREVIOUS_DAY_BUCKET.toISOString(),
          timestamp_until: new Date('2026-03-06T00:00:00.000Z').toISOString(),
          entity_type: 'GLOBAL',
          result_type: 'BUCKETS',
        },
        meta: {},
      })
      expect(range.buckets.map((bucket: any) => bucket.cumulative_participants)).toEqual([1, 4])
    })

    it('rejects a second GLOBAL row for the same bucket', async () => {
      await expect(
        knex('stats').insert({
          granularity: 'DAY',
          timestamp: DAY_BUCKET,
          entity_type: 'GLOBAL',
          entity_id: null,
          cumulative_participants: 4,
        })
      ).rejects.toThrow(/stats_unique_key/)
    })
  })

  describe('the per-role stats columns are gone', () => {
    it('keeps none of the twelve columns on the stats table', async () => {
      const columns = await knex('information_schema.columns')
        .where('table_name', 'stats')
        .whereIn('column_name', DROPPED_PER_ROLE_COLUMNS)
        .pluck('column_name')

      expect(columns).toEqual([])
    })

    it('requires none of the twelve columns on the model', () => {
      expect(Stats.jsonSchema.required).toEqual(expect.not.arrayContaining(DROPPED_PER_ROLE_COLUMNS))
    })
  })
})
