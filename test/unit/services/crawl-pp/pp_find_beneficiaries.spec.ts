jest.mock('../../../../src/common/utils/block_time', () => ({
  ...jest.requireActual('../../../../src/common/utils/block_time'),
  resolveEvaluationTime: jest.fn(async () => new Date('2026-10-01T00:00:00.000Z')),
}))

import { type Context, ServiceBroker } from 'moleculer'
import knex from '../../../../src/common/utils/db_connection'
import ParticipantAPIService from '../../../../src/services/crawl-pp/pp_apis.service'

const SCHEMA_ID = 449900
const ECOSYSTEM = 449001
const ISSUER_GRANTOR = 449002
const ISSUER = 449003
const VERIFIER_GRANTOR = 449004
const VERIFIER = 449005
const REVOKED_ISSUER_GRANTOR = 449006
const ISSUER_UNDER_REVOKED = 449007
const IDS = [
  ECOSYSTEM,
  ISSUER_GRANTOR,
  ISSUER,
  VERIFIER_GRANTOR,
  VERIFIER,
  REVOKED_ISSUER_GRANTOR,
  ISSUER_UNDER_REVOKED,
]

const created = new Date('2026-09-01T00:00:00.000Z')

const participant = (id: number, role: string, validatorParticipantId: number | null, revoked: Date | null = null) => ({
  id,
  schema_id: SCHEMA_ID,
  role,
  did: `did:example:beneficiaries-${id}`,
  validator_participant_id: validatorParticipantId,
  op_state: 'VALIDATED',
  revoked,
  created,
  modified: created,
  effective_from: created,
  corporation_id: 1,
})

describe('ParticipantAPIService findBeneficiaries', () => {
  const broker = new ServiceBroker({ logger: false })
  const service = broker.createService(ParticipantAPIService) as unknown as ParticipantAPIService

  const beneficiaryIds = async (params: { issuer_participant_id?: number; verifier_participant_id?: number }) => {
    const ctx = { params, meta: {} as { $statusCode?: number } }
    const res = (await service.findBeneficiaries(ctx as unknown as Context<typeof params>)) as {
      participants: { id: number | string }[]
    }
    expect(ctx.meta.$statusCode).toBe(200)
    return res.participants.map((p) => Number(p.id)).sort((a, b) => a - b)
  }

  beforeAll(async () => {
    await knex('participants').whereIn('id', IDS).del()
    await knex('participants').insert([
      participant(ECOSYSTEM, 'ECOSYSTEM', null),
      participant(ISSUER_GRANTOR, 'ISSUER_GRANTOR', ECOSYSTEM),
      participant(ISSUER, 'ISSUER', ISSUER_GRANTOR),
      participant(VERIFIER_GRANTOR, 'VERIFIER_GRANTOR', ECOSYSTEM),
      participant(VERIFIER, 'VERIFIER', VERIFIER_GRANTOR),
      participant(REVOKED_ISSUER_GRANTOR, 'ISSUER_GRANTOR', ECOSYSTEM, created),
      participant(ISSUER_UNDER_REVOKED, 'ISSUER', REVOKED_ISSUER_GRANTOR),
    ])
  })

  afterAll(async () => {
    await knex('participants').whereIn('id', IDS).del()
    await broker.stop()
    await knex.destroy()
  })

  it('issuance returns the issuer ancestors only', async () => {
    expect(await beneficiaryIds({ issuer_participant_id: ISSUER })).toEqual([ECOSYSTEM, ISSUER_GRANTOR])
  })

  it('verification without issuer returns the verifier ancestors only', async () => {
    expect(await beneficiaryIds({ verifier_participant_id: VERIFIER })).toEqual([ECOSYSTEM, VERIFIER_GRANTOR])
  })

  it('verification with issuer returns the issuer, both ancestor branches and no verifier', async () => {
    expect(await beneficiaryIds({ issuer_participant_id: ISSUER, verifier_participant_id: VERIFIER })).toEqual([
      ECOSYSTEM,
      ISSUER_GRANTOR,
      ISSUER,
      VERIFIER_GRANTOR,
    ])
  })

  it('skips a revoked ancestor but keeps walking past it', async () => {
    expect(await beneficiaryIds({ issuer_participant_id: ISSUER_UNDER_REVOKED })).toEqual([ECOSYSTEM])
  })
})
