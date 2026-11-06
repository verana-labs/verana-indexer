jest.mock('../../../../src/common/utils/block_time', () => ({
  ...jest.requireActual('../../../../src/common/utils/block_time'),
  resolveEvaluationTime: jest.fn(async () => new Date('2026-11-01T00:00:00.000Z')),
}))

jest.mock('../../../../src/common/utils/params_service', () => {
  const actual = jest.requireActual('../../../../src/common/utils/params_service')
  return {
    ...actual,
    getModuleParams: jest.fn(async (module: string, blockHeight?: number) =>
      module === 'pp'
        ? { params: { PARTICIPANT_SET_EXPIRE_SOON_N_DAYS_BEFORE: 30 } }
        : actual.getModuleParams(module, blockHeight)
    ),
  }
})

import { ServiceBroker } from 'moleculer'
import knex from '../../../../src/common/utils/db_connection'
import ParticipantAPIService from '../../../../src/services/crawl-pp/pp_apis.service'

const ECOSYSTEM_ID = 476800
const SCHEMA_ID = 476900
const HISTORY_HEIGHT = 100
const QUERY_HEIGHT = 200

const VALIDATOR_CORPORATION = 2
const FOREIGN_CORPORATION = 99
const CORPORATION_WITHOUT_VALIDATOR_ENTRIES = 476999

const VALIDATOR_ENTRY = 476007
const FOREIGN_VALIDATOR_ENTRY = 476008
const PENDING_OF_CORPORATION_5 = 476032
const PENDING_OF_CORPORATION_6 = 476046
const PENDING_OF_CORPORATION_11 = 476050
const OWN_SLASHED = 476060
const OWN_EXPIRING = 476061
const SLASHED_UNDER_VALIDATOR = 476063

const PARTICIPANT_IDS = [
  VALIDATOR_ENTRY,
  FOREIGN_VALIDATOR_ENTRY,
  PENDING_OF_CORPORATION_5,
  PENDING_OF_CORPORATION_6,
  PENDING_OF_CORPORATION_11,
  OWN_SLASHED,
  OWN_EXPIRING,
  SLASHED_UNDER_VALIDATOR,
]

const created = new Date('2026-10-01T00:00:00.000Z')
const slashedAt = new Date('2026-10-15T00:00:00.000Z')
const expiresSoonAt = new Date('2026-11-10T00:00:00.000Z')

type ParticipantFixture = {
  id: number
  role: string
  corporation_id: number
  validator_participant_id: number | null
  op_state: string | null
  effective_from?: Date | null
  effective_until?: Date | null
  slashed?: Date | null
}

const fixtures: ParticipantFixture[] = [
  {
    id: VALIDATOR_ENTRY,
    role: 'ECOSYSTEM',
    corporation_id: VALIDATOR_CORPORATION,
    validator_participant_id: null,
    op_state: 'VALIDATED',
    effective_from: created,
  },
  {
    id: FOREIGN_VALIDATOR_ENTRY,
    role: 'ECOSYSTEM',
    corporation_id: FOREIGN_CORPORATION,
    validator_participant_id: null,
    op_state: 'VALIDATED',
    effective_from: created,
  },
  {
    id: PENDING_OF_CORPORATION_5,
    role: 'ISSUER',
    corporation_id: 5,
    validator_participant_id: VALIDATOR_ENTRY,
    op_state: 'PENDING',
  },
  {
    id: PENDING_OF_CORPORATION_6,
    role: 'ISSUER',
    corporation_id: 6,
    validator_participant_id: VALIDATOR_ENTRY,
    op_state: 'PENDING',
  },
  {
    id: PENDING_OF_CORPORATION_11,
    role: 'ISSUER',
    corporation_id: 11,
    validator_participant_id: VALIDATOR_ENTRY,
    op_state: 'PENDING',
  },
  {
    id: OWN_SLASHED,
    role: 'ISSUER',
    corporation_id: VALIDATOR_CORPORATION,
    validator_participant_id: FOREIGN_VALIDATOR_ENTRY,
    op_state: 'VALIDATED',
    effective_from: created,
    slashed: slashedAt,
  },
  {
    id: OWN_EXPIRING,
    role: 'ISSUER',
    corporation_id: VALIDATOR_CORPORATION,
    validator_participant_id: FOREIGN_VALIDATOR_ENTRY,
    op_state: 'VALIDATED',
    effective_from: created,
    effective_until: expiresSoonAt,
  },
  {
    id: SLASHED_UNDER_VALIDATOR,
    role: 'ISSUER',
    corporation_id: 7,
    validator_participant_id: VALIDATOR_ENTRY,
    op_state: 'VALIDATED',
    effective_from: created,
    slashed: slashedAt,
  },
]

const participantRow = (fixture: ParticipantFixture) => ({
  id: fixture.id,
  schema_id: SCHEMA_ID,
  role: fixture.role,
  did: `did:example:pending-flat-${fixture.id}`,
  corporation_id: fixture.corporation_id,
  validator_participant_id: fixture.validator_participant_id,
  op_state: fixture.op_state,
  effective_from: fixture.effective_from ?? null,
  effective_until: fixture.effective_until ?? null,
  slashed: fixture.slashed ?? null,
  created,
  modified: created,
})

const participantHistoryRow = (fixture: ParticipantFixture) => {
  const { id, ...columns } = participantRow(fixture)
  return { ...columns, participant_id: id, event_type: 'Create', height: HISTORY_HEIGHT }
}

describe('Pending Flat API', () => {
  const broker = new ServiceBroker({ logger: false })
  const service = broker.createService(ParticipantAPIService) as any

  const removeFixtures = async () => {
    await knex('participant_history').whereIn('participant_id', PARTICIPANT_IDS).del()
    await knex('participants').whereIn('id', PARTICIPANT_IDS).del()
    await knex('credential_schemas').where({ id: SCHEMA_ID }).del()
    await knex('ecosystem').where({ id: ECOSYSTEM_ID }).del()
  }

  const pendingFlatFor = async (corporationId: number, blockHeight?: number) => {
    const ctx: any = {
      params: { corporation_id: corporationId },
      meta: blockHeight === undefined ? {} : { blockHeight },
    }
    const res: any = await service.pendingFlat(ctx)
    expect(ctx.meta.$statusCode).toBe(200)
    return res
  }

  const pendingIdsFor = async (corporationId: number, blockHeight?: number) => {
    const res = await pendingFlatFor(corporationId, blockHeight)
    return ((res.ecosystems ?? []) as any[])
      .flatMap((ecosystem: any) => ecosystem.schemas ?? [])
      .filter((schema: any) => Number(schema.id) === SCHEMA_ID)
      .flatMap((schema: any) => schema.pending_participants ?? [])
      .map((participant: any) => Number(participant.id))
      .sort((a: number, b: number) => b - a)
  }

  beforeAll(async () => {
    await removeFixtures()
    await knex('ecosystem').insert({
      id: ECOSYSTEM_ID,
      did: `did:example:pending-flat-ecosystem-${ECOSYSTEM_ID}`,
      language: 'en',
      height: HISTORY_HEIGHT,
      created,
      modified: created,
    })
    await knex('credential_schemas').insert({
      id: SCHEMA_ID,
      ecosystem_id: ECOSYSTEM_ID,
      json_schema: JSON.stringify({ title: 'PendingFlatScope', description: 'scope fixture' }),
      is_active: true,
      issuer_grantor_validation_validity_period: 0,
      verifier_grantor_validation_validity_period: 0,
      issuer_validation_validity_period: 0,
      verifier_validation_validity_period: 0,
      holder_validation_validity_period: 0,
      issuer_onboarding_mode: 'GRANTOR_ONBOARDING_PROCESS',
      verifier_onboarding_mode: 'GRANTOR_ONBOARDING_PROCESS',
      created,
      modified: created,
    })
    await knex('participants').insert(fixtures.map(participantRow))
    await knex('participant_history').insert(fixtures.map(participantHistoryRow))
  })

  afterAll(async () => {
    await removeFixtures()
    await broker.stop()
    await knex.destroy()
  })

  afterEach(() => {
    jest.clearAllMocks()
  })

  it('returns empty when no participants found', async () => {
    const ctx: any = { params: { corporation_id: CORPORATION_WITHOUT_VALIDATOR_ENTRIES, limit: 10 }, meta: {} }
    const res = await service.pendingFlat(ctx)
    expect(res).toEqual({ ecosystems: [] })
  })

  it('rejects a non-positive corporation_id with 400', async () => {
    const ctx: any = { params: { corporation_id: 0 }, meta: {} }
    const res: any = await service.pendingFlat(ctx)
    expect(res).toMatchObject({ code: 400 })
  })

  it('rejects a limit above 1024 instead of clamping it', async () => {
    const ctx: any = { params: { corporation_id: 1, limit: 2000 }, meta: {} }
    const res: any = await service.pendingFlat(ctx)
    expect(res).toEqual({ error: '"limit" must be an integer between 1 and 1024', code: 400 })
  })

  it('rejects a limit of 0 instead of defaulting it to 64', async () => {
    const ctx: any = { params: { corporation_id: 1, limit: 0 }, meta: {} }
    const res: any = await service.pendingFlat(ctx)
    expect(res.code).toBe(400)
  })

  it('returns the participants whose validator entry belongs to the queried Corporation', async () => {
    const res = await pendingFlatFor(VALIDATOR_CORPORATION)
    const ecosystem = (res.ecosystems as any[]).find((entry: any) => Number(entry.id) === ECOSYSTEM_ID)
    const schema = (ecosystem.schemas as any[]).find((entry: any) => Number(entry.id) === SCHEMA_ID)

    expect(schema.pending_participants.map((participant: any) => Number(participant.id))).toEqual([
      PENDING_OF_CORPORATION_11,
      PENDING_OF_CORPORATION_6,
      PENDING_OF_CORPORATION_5,
    ])
    expect(schema.pending_tasks).toBe(3)
    expect(ecosystem.pending_tasks).toBe(3)
  })

  it('returns nothing to the Corporation that owns the pending application', async () => {
    expect(await pendingIdsFor(5)).toEqual([])
  })

  it('excludes the slashed entry owned by the queried Corporation', async () => {
    expect(await pendingIdsFor(VALIDATOR_CORPORATION)).not.toContain(OWN_SLASHED)
  })

  it('excludes the expiring entry owned by the queried Corporation', async () => {
    const expiringParticipant: any = await service.getParticipant({ params: { id: OWN_EXPIRING }, meta: {} })
    expect(expiringParticipant.participant.participant_state).toBe('ACTIVE')
    expect(expiringParticipant.participant.expire_soon).toBe(true)

    expect(await pendingIdsFor(VALIDATOR_CORPORATION)).not.toContain(OWN_EXPIRING)
  })

  it('excludes a slashed participant whose op_state is not PENDING from the validator queue', async () => {
    expect(await pendingIdsFor(VALIDATOR_CORPORATION)).not.toContain(SLASHED_UNDER_VALIDATOR)
  })

  it('applies the same scope on the At-Block-Height path', async () => {
    expect(await pendingIdsFor(VALIDATOR_CORPORATION, QUERY_HEIGHT)).toEqual([
      PENDING_OF_CORPORATION_11,
      PENDING_OF_CORPORATION_6,
      PENDING_OF_CORPORATION_5,
    ])
    expect(await pendingIdsFor(5, QUERY_HEIGHT)).toEqual([])
  })
})
