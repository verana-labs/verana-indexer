import { Buffer } from 'node:buffer'
import { ServiceBroker } from 'moleculer'
import { SERVICE } from '../../../../src/common'
import knex from '../../../../src/common/utils/db_connection'
import {
  extractOperatorAuthorizationTouches,
  extractVSOperatorAuthorizationIds,
  hasDelegationEvents,
} from '../../../../src/modules/de-height-sync/de_height_sync_service'
import DelegationApiService from '../../../../src/services/crawl-de/de_apis.service'

const b64 = (value: string) => Buffer.from(value, 'utf8').toString('base64')

function event(type: string, attributes: Record<string, string>, encoded = false) {
  return {
    type,
    attributes: Object.entries(attributes).map(([key, value]) =>
      encoded ? { key: b64(key), value: b64(value) } : { key, value }
    ),
  }
}

describe('de height-sync event extraction', () => {
  it('detects delegation events among unrelated ones', () => {
    expect(hasDelegationEvents([event('coin_spent', { amount: '1uvna' })])).toBe(false)
    expect(hasDelegationEvents([event('grant_operator_authorization', { authz_id: '1' })])).toBe(true)
  })

  it('extracts an operator-authorization grant from a plain event', () => {
    const events = [
      event('grant_operator_authorization', {
        authz_id: '42',
        corporation_id: '7',
        grantee: 'verana1operator',
        with_feegrant: 'false',
      }),
    ]

    expect(extractOperatorAuthorizationTouches(events)).toEqual([
      { authzId: 42, corporationId: 7, grantee: 'verana1operator', revoked: false },
    ])
  })

  it('decodes base64-encoded attributes as emitted in block_result', () => {
    const events = [
      event('grant_operator_authorization', { authz_id: '5', corporation_id: '3', grantee: 'verana1abc' }, true),
    ]

    expect(extractOperatorAuthorizationTouches(events)).toEqual([
      { authzId: 5, corporationId: 3, grantee: 'verana1abc', revoked: false },
    ])
  })

  it('marks revoke events so the row is deleted, and last touch per id wins', () => {
    const events = [
      event('grant_operator_authorization', { authz_id: '9', corporation_id: '3', grantee: 'verana1abc' }),
      event('revoke_operator_authorization', { authz_id: '9', corporation_id: '3', grantee: 'verana1abc' }),
    ]

    expect(extractOperatorAuthorizationTouches(events)).toEqual([
      { authzId: 9, corporationId: 3, grantee: 'verana1abc', revoked: true },
    ])
  })

  it('ignores touches with missing or invalid ids', () => {
    const events = [
      event('grant_operator_authorization', { corporation_id: '3', grantee: 'verana1abc' }),
      event('grant_operator_authorization', { authz_id: '0', corporation_id: '3', grantee: 'verana1abc' }),
    ]

    expect(extractOperatorAuthorizationTouches(events)).toEqual([])
  })

  it('collects unique VS-operator authorization ids across grant/revoke/update', () => {
    const events = [
      event('grant_vs_operator_authorization', { vsoa_id: '1', participant_id: '10' }),
      event('update_vs_operator_authorization', { vsoa_id: '1', participant_id: '11' }),
      event('revoke_vs_operator_authorization', { vsoa_id: '2', participant_id: '12' }),
    ]

    expect(extractVSOperatorAuthorizationIds(events).sort()).toEqual([1, 2])
  })
})

const EC_CREATE = '/verana.ec.v1.MsgCreateEcosystem'
const EC_UPDATE = '/verana.ec.v1.MsgUpdateEcosystem'
const CS_CREATE = '/verana.cs.v1.MsgCreateCredentialSchema'

const T1 = '2026-07-01T00:00:00.000Z'
const T2 = '2026-07-05T00:00:00.000Z'
const T3 = '2026-07-09T00:00:00.000Z'
const PAST = '2020-01-01T00:00:00.000Z'
const FUTURE = '2999-01-01T00:00:00.000Z'

function seedRow(row: Record<string, unknown>) {
  return {
    spend_limit: null,
    remaining_spend: null,
    fee_spend_limit: null,
    remaining_fee_spend: null,
    expiration: null,
    period: null,
    ...row,
    msg_types: JSON.stringify(row.msg_types),
  }
}

function listedIds(res: any): number[] {
  return (res.authorizations as any[]).map((a) => a.id)
}

describe('DelegationApiService.listOperatorAuthorizations', () => {
  const broker = new ServiceBroker({ logger: false })
  const serviceKey = SERVICE.V1.DelegationApiService.path

  beforeAll(async () => {
    broker.createService(DelegationApiService)
    await broker.start()

    await knex('operator_authorization_history').del()
    await knex('operator_authorizations').del()

    await knex('operator_authorizations').insert([
      seedRow({
        id: 1,
        corporation_id: 1,
        operator: 'verana1opA',
        msg_types: [EC_CREATE, EC_UPDATE],
        spend_limit: JSON.stringify([{ denom: 'uvna', amount: '1000' }]),
        remaining_spend: JSON.stringify([{ denom: 'uvna', amount: '800' }]),
        expiration: null,
        modified: T1,
        height: 100,
      }),
      seedRow({
        id: 2,
        corporation_id: 1,
        operator: 'verana1opB',
        msg_types: [CS_CREATE],
        expiration: FUTURE,
        modified: T2,
        height: 110,
      }),
      seedRow({
        id: 3,
        corporation_id: 2,
        operator: 'verana1opA',
        msg_types: [EC_CREATE],
        expiration: PAST,
        modified: T3,
        height: 120,
      }),
    ])
  })

  afterAll(async () => {
    await broker.stop()
  })

  const list = (params: Record<string, unknown> = {}) =>
    broker.call(`${serviceKey}.listOperatorAuthorizations`, params) as Promise<any>

  it('returns all rows newest-first by default (-id)', async () => {
    expect(listedIds(await list())).toEqual([3, 2, 1])
  })

  it('sorts ascending with sort=+id', async () => {
    expect(listedIds(await list({ sort: '+id' }))).toEqual([1, 2, 3])
  })

  it('rejects an unsupported sort column', async () => {
    expect((await list({ sort: 'operator' })).code).toBe(400)
  })

  it('filters by corporation_id', async () => {
    expect(listedIds(await list({ corporation_id: 1 }))).toEqual([2, 1])
  })

  it('filters by operator', async () => {
    expect(listedIds(await list({ operator: 'verana1opA' }))).toEqual([3, 1])
  })

  it('filters by msg_type membership in msg_types[]', async () => {
    expect(listedIds(await list({ msg_type: CS_CREATE }))).toEqual([2])
    expect(listedIds(await list({ msg_type: EC_UPDATE }))).toEqual([1])
  })

  it('only_active excludes expired authorizations (keeps null/future expiration)', async () => {
    expect(listedIds(await list({ only_active: true }))).toEqual([2, 1])
  })

  it('modified_after filters strictly after the given datetime', async () => {
    expect(listedIds(await list({ modified_after: T1 }))).toEqual([3, 2])
  })

  it('paginates with the half-open id cursor and limit', async () => {
    expect(listedIds(await list({ max_id: 3 }))).toEqual([2, 1])
    expect(listedIds(await list({ min_id: 2 }))).toEqual([3, 2])
    expect(listedIds(await list({ limit: 1 }))).toEqual([3])
  })

  it('serializes spend_limit/remaining_spend only when set', async () => {
    const res = await list({ corporation_id: 1, sort: '+id' })
    const [first, second] = res.authorizations
    expect(first.id).toBe(1)
    expect(first.spend_limit).toEqual([{ denom: 'uvna', amount: '1000' }])
    expect(first.remaining_spend).toEqual([{ denom: 'uvna', amount: '800' }])
    expect(second.id).toBe(2)
    expect(second).not.toHaveProperty('spend_limit')
  })

  it('does not expose the internal modified field in the response', async () => {
    const [row] = (await list({ limit: 1 })).authorizations
    expect(row).not.toHaveProperty('modified')
  })
})

function record(participantId: number, expiration: string | null, extra: Record<string, unknown> = {}) {
  return {
    participant_id: participantId,
    msg_types: [EC_CREATE],
    spend_limit: null,
    remaining_spend: null,
    fee_spend_limit: null,
    remaining_fee_spend: null,
    with_feegrant: false,
    expiration,
    period: null,
    ...extra,
  }
}

function seedVsoaRow(row: Record<string, unknown>) {
  return { ...row, records: JSON.stringify(row.records) }
}

describe('DelegationApiService.listVSOperatorAuthorizations', () => {
  const broker = new ServiceBroker({ logger: false })
  const serviceKey = SERVICE.V1.DelegationApiService.path

  beforeAll(async () => {
    broker.createService(DelegationApiService)
    await broker.start()

    await knex('vs_operator_authorization_history').del()
    await knex('vs_operator_authorizations').del()

    await knex('vs_operator_authorizations').insert([
      seedVsoaRow({
        id: 1,
        corporation_id: 1,
        vs_operator: 'verana1vsA',
        records: [
          record(10, null, {
            with_feegrant: true,
            spend_limit: [{ denom: 'uvna', amount: '500' }],
            remaining_spend: [{ denom: 'uvna', amount: '400' }],
          }),
        ],
        modified: T1,
        height: 100,
      }),
      seedVsoaRow({
        id: 2,
        corporation_id: 1,
        vs_operator: 'verana1vsB',
        records: [record(20, FUTURE)],
        modified: T2,
        height: 110,
      }),
      seedVsoaRow({
        id: 3,
        corporation_id: 2,
        vs_operator: 'verana1vsA',
        records: [record(10, PAST)],
        modified: T3,
        height: 120,
      }),
    ])
  })

  afterAll(async () => {
    await broker.stop()
  })

  const list = (params: Record<string, unknown> = {}) =>
    broker.call(`${serviceKey}.listVSOperatorAuthorizations`, params) as Promise<any>

  it('returns all rows newest-first by default (-id)', async () => {
    expect(listedIds(await list())).toEqual([3, 2, 1])
  })

  it('filters by corporation_id', async () => {
    expect(listedIds(await list({ corporation_id: 1 }))).toEqual([2, 1])
  })

  it('filters by vs_operator', async () => {
    expect(listedIds(await list({ vs_operator: 'verana1vsA' }))).toEqual([3, 1])
  })

  it('filters by participant_id membership in records[]', async () => {
    expect(listedIds(await list({ participant_id: 20 }))).toEqual([2])
    expect(listedIds(await list({ participant_id: 10 }))).toEqual([3, 1])
  })

  it('only_active keeps entries with at least one non-expired record', async () => {
    expect(listedIds(await list({ only_active: true }))).toEqual([2, 1])
  })

  it('modified_after filters strictly after the given datetime', async () => {
    expect(listedIds(await list({ modified_after: T1 }))).toEqual([3, 2])
  })

  it('paginates with the half-open id cursor and limit', async () => {
    expect(listedIds(await list({ max_id: 3 }))).toEqual([2, 1])
    expect(listedIds(await list({ min_id: 2 }))).toEqual([3, 2])
    expect(listedIds(await list({ limit: 1 }))).toEqual([3])
  })

  it('serializes nested records with with_feegrant and conditional spend_limit', async () => {
    const [row] = (await list({ corporation_id: 1, sort: '+id' })).authorizations
    expect(row.id).toBe(1)
    const [rec] = row.records
    expect(rec.participant_id).toBe(10)
    expect(rec.with_feegrant).toBe(true)
    expect(rec.spend_limit).toEqual([{ denom: 'uvna', amount: '500' }])
    expect(row.records[0]).not.toHaveProperty('fee_spend_limit')
  })
})

describe('DelegationApiService.getVSOperatorAuthorization', () => {
  const broker = new ServiceBroker({ logger: false })
  const serviceKey = SERVICE.V1.DelegationApiService.path

  beforeAll(async () => {
    broker.createService(DelegationApiService)
    await broker.start()

    await knex('vs_operator_authorization_history').del()
    await knex('vs_operator_authorizations').del()

    await knex('vs_operator_authorizations').insert([
      seedVsoaRow({
        id: 7,
        corporation_id: 1,
        vs_operator: 'verana1vsA',
        records: [record(10, FUTURE, { with_feegrant: true, spend_limit: [{ denom: 'uvna', amount: '500' }] })],
        modified: T2,
        height: 110,
      }),
    ])

    await knex('vs_operator_authorization_history').insert([
      seedVsoaRow({
        vs_operator_authorization_id: 7,
        corporation_id: 1,
        vs_operator: 'verana1vsA',
        records: [record(10, FUTURE)],
        modified: T1,
        revoked: false,
        height: 100,
      }),
      seedVsoaRow({
        vs_operator_authorization_id: 7,
        corporation_id: 1,
        vs_operator: 'verana1vsA',
        records: [record(10, FUTURE), record(20, FUTURE)],
        modified: T2,
        revoked: false,
        height: 110,
      }),
      seedVsoaRow({
        vs_operator_authorization_id: 8,
        corporation_id: 2,
        vs_operator: 'verana1vsB',
        records: [],
        modified: T3,
        revoked: true,
        height: 120,
      }),
    ])
  })

  afterAll(async () => {
    await broker.stop()
  })

  const get = (id: number, blockHeight?: number) =>
    broker.call(`${serviceKey}.getVSOperatorAuthorization`, { id }, { meta: { blockHeight } }) as Promise<any>

  it('returns the live row with its nested records', async () => {
    const { authorization } = await get(7)
    expect(authorization.id).toBe(7)
    expect(authorization.corporation_id).toBe(1)
    expect(authorization.vs_operator).toBe('verana1vsA')
    expect(authorization.records).toHaveLength(1)
    expect(authorization.records[0]).toMatchObject({
      participant_id: 10,
      with_feegrant: true,
      spend_limit: [{ denom: 'uvna', amount: '500' }],
    })
    expect(authorization.records[0]).not.toHaveProperty('fee_spend_limit')
  })

  it('returns 404 for an unknown id', async () => {
    expect(await get(999)).toMatchObject({ code: 404 })
  })

  it('resolves the state as of the requested block height', async () => {
    expect((await get(7, 100)).authorization.records.map((r: any) => r.participant_id)).toEqual([10])
    expect((await get(7, 110)).authorization.records.map((r: any) => r.participant_id)).toEqual([10, 20])
  })

  it('returns the authorization id at height, not the history row id', async () => {
    expect((await get(7, 110)).authorization.id).toBe(7)
  })

  it('returns 404 before the authorization existed', async () => {
    expect(await get(7, 99)).toMatchObject({ code: 404 })
  })

  it('returns 404 for a revoked authorization at height', async () => {
    expect(await get(8, 120)).toMatchObject({ code: 404 })
  })
})

afterAll(async () => {
  await knex.destroy()
})
