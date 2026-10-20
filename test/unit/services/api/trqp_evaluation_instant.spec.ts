import { ServiceBroker } from 'moleculer'
import knex from '../../../../src/common/utils/db_connection'
import { Network } from '../../../../src/network'
import TrqpApiService from '../../../../src/services/api/trqp_apis.service'

const BLOCK_HEIGHT = 199_000_459
const BLOCK_TIME = new Date('2026-01-15T10:30:00Z')
const CREATED = new Date('2025-06-01T00:00:00Z')
const ARCHIVED_AT = new Date('2026-03-01T00:00:00Z')
const LATE_CREATED = new Date('2026-06-01T00:00:00Z')

const CORP_ID = 4590001
const OTHER_CORP_ID = 4590002
const LATE_CORP_ID = 4590003
const ECO_ID = 4590001
const LATE_ECO_ID = 4590002
const SCHEMA_ID = 4590001
const PARTICIPANT_ID = 4590001

const CORP_DID = 'did:web:corp-459.example'
const OTHER_CORP_DID = 'did:web:other-corp-459.example'
const LATE_CORP_DID = 'did:web:late-corp-459.example'
const ECO_DID = 'did:web:eco-459.example'
const LATE_ECO_DID = 'did:web:late-eco-459.example'
const RESOURCE = `vpr:verana:${Network.chainId}:cs:${SCHEMA_ID}`
const OUT_OF_SCOPE = 'out of v4 recognition scope (corporation → ecosystem only)'

type TrqpAnswer = {
  authorized?: boolean
  recognized?: boolean
  message?: string
  time_requested: string
  time_evaluated: string
  verana?: Record<string, unknown>
}

function corporationRow(id: number, did: string, created: Date) {
  return { id, did, created, modified: created, height: id }
}

function ecosystemRow(id: number, did: string, created: Date, archived: Date | null) {
  return {
    id,
    did,
    corporation_id: CORP_ID,
    created,
    modified: created,
    archived,
    language: 'en',
    height: id,
    active_version: 3,
  }
}

async function cleanup() {
  await knex('participants').where({ id: PARTICIPANT_ID }).del()
  await knex('credential_schemas').where({ id: SCHEMA_ID }).del()
  await knex('ecosystem').whereIn('id', [ECO_ID, LATE_ECO_ID]).del()
  await knex('corporation').whereIn('id', [CORP_ID, OTHER_CORP_ID, LATE_CORP_ID]).del()
  await knex('block').where({ height: BLOCK_HEIGHT }).del()
}

describe('TRQP evaluation instant', () => {
  const broker = new ServiceBroker({ logger: false })
  const service = broker.createService(TrqpApiService) as unknown as TrqpApiService

  beforeAll(async () => {
    await cleanup()
    await knex('block').insert({
      height: BLOCK_HEIGHT,
      hash: `trqp-459-${BLOCK_HEIGHT}`,
      time: BLOCK_TIME,
      proposer_address: 'p',
      data: {},
    })
    await knex('corporation').insert([
      corporationRow(CORP_ID, CORP_DID, CREATED),
      corporationRow(OTHER_CORP_ID, OTHER_CORP_DID, CREATED),
      corporationRow(LATE_CORP_ID, LATE_CORP_DID, LATE_CREATED),
    ])
    await knex('ecosystem').insert([
      ecosystemRow(ECO_ID, ECO_DID, CREATED, ARCHIVED_AT),
      ecosystemRow(LATE_ECO_ID, LATE_ECO_DID, LATE_CREATED, null),
    ])
    await knex('credential_schemas').insert({
      id: SCHEMA_ID,
      ecosystem_id: ECO_ID,
      json_schema: JSON.stringify({ title: 'Trqp459' }),
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
    })
    await knex('participants').insert({
      id: PARTICIPANT_ID,
      schema_id: SCHEMA_ID,
      role: 'ISSUER',
      did: CORP_DID,
      corporation_id: CORP_ID,
      created: CREATED,
      modified: CREATED,
      effective_from: CREATED,
      effective_until: new Date('2026-02-01T00:00:00Z'),
      deposit: 5000000,
    })
  })

  afterAll(async () => {
    await cleanup()
    await knex.destroy()
  })

  function authorize(context?: Record<string, string>): Promise<TrqpAnswer> {
    const ctx = {
      meta: {},
      params: { authority_id: ECO_DID, entity_id: CORP_DID, action: 'issue', resource: RESOURCE, context },
    }
    return service.authorize(ctx as unknown as Parameters<TrqpApiService['authorize']>[0]) as Promise<TrqpAnswer>
  }

  function recognize(
    context?: Record<string, string>,
    ids: { authority?: string; entity?: string } = {}
  ): Promise<TrqpAnswer> {
    const ctx = {
      meta: {},
      params: {
        authority_id: ids.authority ?? CORP_DID,
        entity_id: ids.entity ?? ECO_DID,
        action: 'issue',
        resource: RESOURCE,
        context,
      },
    }
    return service.recognize(ctx as unknown as Parameters<TrqpApiService['recognize']>[0]) as Promise<TrqpAnswer>
  }

  describe('default instant is the latest indexed block, not the service clock', () => {
    it('authorizes a participant that is ACTIVE at the latest block but expired by wall clock', async () => {
      const res = await authorize()

      expect(res.time_evaluated).toBe('2026-01-15T10:30:00Z')
      expect(res.time_requested).toBe('2026-01-15T10:30:00Z')
      expect(res.authorized).toBe(true)
      expect(res.verana).toMatchObject({ participant_state: 'ACTIVE', deposit: '5000000uvna' })
    })

    it('authorizes against context.time when it is given', async () => {
      const res = await authorize({ time: '2026-02-15T00:00:00Z' })

      expect(res.time_evaluated).toBe('2026-02-15T00:00:00Z')
      expect(res.authorized).toBe(false)
      expect(res.verana).toMatchObject({ participant_state: 'EXPIRED' })
    })

    it('recognizes against the latest block time, before the archival', async () => {
      const res = await recognize()

      expect(res.time_evaluated).toBe('2026-01-15T10:30:00Z')
      expect(res.recognized).toBe(true)
      expect(res.message).toBeUndefined()
      expect(res.verana).toEqual({ ecosystem_active_egf_version: 3, controlling_since: '2025-06-01T00:00:00Z' })
    })

    it('rejects a malformed context.time with 400', async () => {
      const ctx = {
        meta: {} as { $statusCode?: number },
        params: {
          authority_id: ECO_DID,
          entity_id: CORP_DID,
          action: 'issue',
          resource: RESOURCE,
          context: { time: '2026-02-15' },
        },
      }
      await service.authorize(ctx as unknown as Parameters<TrqpApiService['authorize']>[0])

      expect(ctx.meta.$statusCode).toBe(400)
    })
  })

  describe('recognition scope is evaluated at the evaluation instant', () => {
    it('recognizes an ecosystem archived after context.time', async () => {
      const res = await recognize({ time: '2026-02-28T23:59:59Z' })

      expect(res).toMatchObject({ recognized: true })
      expect(res.message).toBeUndefined()
    })

    it('is out of scope from the archival instant onwards', async () => {
      const atArchival = await recognize({ time: '2026-03-01T00:00:00Z' })
      const later = await recognize({ time: '2026-09-01T00:00:00Z' })

      expect(atArchival).toMatchObject({ recognized: false, message: OUT_OF_SCOPE })
      expect(later).toMatchObject({ recognized: false, message: OUT_OF_SCOPE })
      expect(later.verana).toBeUndefined()
    })

    it('is out of scope before the ecosystem existed', async () => {
      const res = await recognize({ time: '2025-01-01T00:00:00Z' })

      expect(res).toMatchObject({ recognized: false, message: OUT_OF_SCOPE })
    })

    it('is out of scope before the authority corporation existed', async () => {
      const beforeCorporation = await recognize({ time: '2026-02-01T00:00:00Z' }, { authority: LATE_CORP_DID })
      const afterCorporation = await recognize(
        { time: '2026-07-01T00:00:00Z' },
        { authority: LATE_CORP_DID, entity: LATE_ECO_DID }
      )

      expect(beforeCorporation).toMatchObject({ recognized: false, message: OUT_OF_SCOPE })
      expect(afterCorporation.recognized).toBe(false)
      expect(afterCorporation.message).toBeUndefined()
    })

    it('treats a late-created ecosystem as out of scope at the latest block and in scope later', async () => {
      const atLatestBlock = await recognize(undefined, { entity: LATE_ECO_DID })
      const afterCreation = await recognize({ time: '2026-07-01T00:00:00Z' }, { entity: LATE_ECO_DID })

      expect(atLatestBlock).toMatchObject({ recognized: false, message: OUT_OF_SCOPE })
      expect(afterCreation.recognized).toBe(false)
      expect(afterCreation.message).toBeUndefined()
      expect(afterCreation.verana).toEqual({
        ecosystem_active_egf_version: 3,
        controlling_since: '2026-06-01T00:00:00Z',
      })
    })

    it('answers a plain non-recognition when both entities are active but unrelated', async () => {
      const res = await recognize({ time: '2026-02-01T00:00:00Z' }, { authority: OTHER_CORP_DID })

      expect(res.recognized).toBe(false)
      expect(res.message).toBeUndefined()
      expect(res.verana).toBeUndefined()
    })
  })
})
