import { Buffer } from 'node:buffer'
import { ServiceBroker } from 'moleculer'
import { SERVICE } from '../../../../src/common'
import knex from '../../../../src/common/utils/db_connection'
import {
  extractFeeGrantTouches,
  extractFeeGrantUses,
  hasDelegationEvents,
} from '../../../../src/modules/de-height-sync/de_height_sync_service'
import DelegationApiService from '../../../../src/services/crawl-de/de_apis.service'
import DelegationDatabaseService from '../../../../src/services/crawl-de/de_database.service'

const b64 = (value: string) => Buffer.from(value, 'utf8').toString('base64')

function event(type: string, attributes: Record<string, string>, encoded = false) {
  return {
    type,
    attributes: Object.entries(attributes).map(([key, value]) =>
      encoded ? { key: b64(key), value: b64(value) } : { key, value }
    ),
  }
}

const EC_CREATE = '/verana.ec.v1.MsgCreateEcosystem'
const CS_CREATE = '/verana.cs.v1.MsgCreateCredentialSchema'

const T1 = '2026-07-01T00:00:00.000Z'
const T2 = '2026-07-05T00:00:00.000Z'
const T3 = '2026-07-09T00:00:00.000Z'
const PAST = '2020-01-01T00:00:00.000Z'
const FUTURE = '2999-01-01T00:00:00.000Z'

describe('de height-sync fee grant event extraction', () => {
  it('detects fee grant and use_feegrant events among unrelated ones', () => {
    expect(hasDelegationEvents([event('coin_spent', { amount: '1uvna' })])).toBe(false)
    expect(hasDelegationEvents([event('grant_fee_allowance', { corporation_id: '1' })])).toBe(true)
    expect(hasDelegationEvents([event('revoke_fee_allowance', { corporation_id: '1' })])).toBe(true)
    expect(hasDelegationEvents([event('use_feegrant', { granter: 'verana1policy' })])).toBe(true)
  })

  it('extracts a fee grant touch from a plain event', () => {
    const events = [event('grant_fee_allowance', { corporation_id: '7', grantee: 'verana1operator' })]

    expect(extractFeeGrantTouches(events)).toEqual([
      { grantorCorporationId: 7, grantee: 'verana1operator', revoked: false },
    ])
  })

  it('decodes base64-encoded fee grant attributes', () => {
    const events = [event('grant_fee_allowance', { corporation_id: '3', grantee: 'verana1abc' }, true)]

    expect(extractFeeGrantTouches(events)).toEqual([{ grantorCorporationId: 3, grantee: 'verana1abc', revoked: false }])
  })

  it('last touch per (corporation, grantee) pair wins', () => {
    const events = [
      event('grant_fee_allowance', { corporation_id: '3', grantee: 'verana1abc' }),
      event('revoke_fee_allowance', { corporation_id: '3', grantee: 'verana1abc' }),
      event('grant_fee_allowance', { corporation_id: '3', grantee: 'verana1other' }),
    ]

    expect(extractFeeGrantTouches(events)).toEqual([
      { grantorCorporationId: 3, grantee: 'verana1abc', revoked: true },
      { grantorCorporationId: 3, grantee: 'verana1other', revoked: false },
    ])
  })

  it('ignores fee grant touches with missing attributes', () => {
    const events = [
      event('grant_fee_allowance', { grantee: 'verana1abc' }),
      event('grant_fee_allowance', { corporation_id: '0', grantee: 'verana1abc' }),
      event('revoke_fee_allowance', { corporation_id: '3' }),
    ]

    expect(extractFeeGrantTouches(events)).toEqual([])
  })

  it('dedupes use_feegrant events per (granter, grantee) pair', () => {
    const events = [
      event('use_feegrant', { granter: 'verana1policy', grantee: 'verana1op' }),
      event('use_feegrant', { granter: 'verana1policy', grantee: 'verana1op' }, true),
      event('use_feegrant', { granter: 'verana1policy', grantee: 'verana1other' }),
      event('use_feegrant', { granter: 'verana1policy' }),
    ]

    expect(extractFeeGrantUses(events)).toEqual([
      { granter: 'verana1policy', grantee: 'verana1op' },
      { granter: 'verana1policy', grantee: 'verana1other' },
    ])
  })
})

function seedFeeGrantRow(row: Record<string, unknown>) {
  return {
    spend_limit: null,
    remaining_spend: null,
    expiration: null,
    period: null,
    ...row,
    msg_types: JSON.stringify(row.msg_types),
  }
}

function listedIds(res: any): number[] {
  return (res.fee_grants as any[]).map((g) => g.id)
}

describe('DelegationApiService.listFeeGrants', () => {
  const broker = new ServiceBroker({ logger: false })
  const serviceKey = SERVICE.V1.DelegationApiService.path

  beforeAll(async () => {
    broker.createService(DelegationApiService)
    await broker.start()

    await knex('fee_grant_history').del()
    await knex('fee_grants').del()

    await knex('fee_grants').insert([
      seedFeeGrantRow({
        id: 1,
        grantor_corporation_id: 1,
        grantee: 'verana1opA',
        msg_types: [EC_CREATE],
        spend_limit: JSON.stringify([{ denom: 'uvna', amount: '1000' }]),
        remaining_spend: JSON.stringify([{ denom: 'uvna', amount: '600' }]),
        expiration: null,
        modified: T1,
        height: 100,
      }),
      seedFeeGrantRow({
        id: 2,
        grantor_corporation_id: 1,
        grantee: 'verana1opB',
        msg_types: [CS_CREATE],
        expiration: FUTURE,
        modified: T2,
        height: 110,
      }),
      seedFeeGrantRow({
        id: 3,
        grantor_corporation_id: 2,
        grantee: 'verana1opA',
        msg_types: [EC_CREATE],
        expiration: PAST,
        modified: T3,
        height: 120,
      }),
      seedFeeGrantRow({
        id: 4,
        grantor_corporation_id: 2,
        grantee: 'verana1opC',
        msg_types: [EC_CREATE],
        spend_limit: JSON.stringify([{ denom: 'uvna', amount: '500' }]),
        remaining_spend: JSON.stringify([{ denom: 'uvna', amount: '500' }]),
        expiration: PAST,
        period: '604800s',
        modified: T3,
        height: 130,
      }),
    ])
  })

  afterAll(async () => {
    await broker.stop()
  })

  const list = (params: Record<string, unknown> = {}) =>
    broker.call(`${serviceKey}.listFeeGrants`, params) as Promise<any>

  it('returns all rows newest-first by default (-id)', async () => {
    expect(listedIds(await list())).toEqual([4, 3, 2, 1])
  })

  it('sorts ascending with sort=+id', async () => {
    expect(listedIds(await list({ sort: '+id' }))).toEqual([1, 2, 3, 4])
  })

  it('rejects an unsupported sort column', async () => {
    expect((await list({ sort: 'grantee' })).code).toBe(400)
  })

  it('filters by grantor_corporation_id', async () => {
    expect(listedIds(await list({ grantor_corporation_id: 1 }))).toEqual([2, 1])
  })

  it('filters by grantee', async () => {
    expect(listedIds(await list({ grantee: 'verana1opA' }))).toEqual([3, 1])
  })

  it('filters by msg_type membership in msg_types[]', async () => {
    expect(listedIds(await list({ msg_type: CS_CREATE }))).toEqual([2])
  })

  it('only_active excludes expired grants but keeps periodic ones past their cycle boundary', async () => {
    expect(listedIds(await list({ only_active: true }))).toEqual([4, 2, 1])
  })

  it('modified_after filters strictly after the given datetime', async () => {
    expect(listedIds(await list({ modified_after: T2 }))).toEqual([4, 3])
  })

  it('rejects an invalid modified_after datetime', async () => {
    expect((await list({ modified_after: 'not-a-date' })).code).toBe(400)
  })

  it('paginates with the half-open id cursor and limit', async () => {
    expect(listedIds(await list({ max_id: 3 }))).toEqual([2, 1])
    expect(listedIds(await list({ min_id: 3 }))).toEqual([4, 3])
    expect(listedIds(await list({ limit: 1 }))).toEqual([4])
  })

  it('serializes spend_limit/remaining_spend only when set', async () => {
    const res = await list({ grantor_corporation_id: 1, sort: '+id' })
    const [first, second] = res.fee_grants
    expect(first.id).toBe(1)
    expect(first.spend_limit).toEqual([{ denom: 'uvna', amount: '1000' }])
    expect(first.remaining_spend).toEqual([{ denom: 'uvna', amount: '600' }])
    expect(second.id).toBe(2)
    expect(second).not.toHaveProperty('spend_limit')
  })

  it('serializes period and expiration only when set', async () => {
    const res = await list({ sort: '+id' })
    const rows = res.fee_grants
    expect(rows[0]).not.toHaveProperty('expiration')
    expect(rows[0]).not.toHaveProperty('period')
    expect(rows[3].period).toBe('604800s')
    expect(rows[3].expiration).toBe(PAST)
  })

  it('does not expose the internal modified field in the response', async () => {
    const [row] = (await list({ limit: 1 })).fee_grants
    expect(row).not.toHaveProperty('modified')
  })

  it('resolves the list as of a requested block height from history', async () => {
    await knex('fee_grant_history').insert([
      seedFeeGrantRow({
        fee_grant_id: 1,
        grantor_corporation_id: 1,
        grantee: 'verana1opA',
        msg_types: [EC_CREATE],
        spend_limit: JSON.stringify([{ denom: 'uvna', amount: '1000' }]),
        remaining_spend: JSON.stringify([{ denom: 'uvna', amount: '1000' }]),
        modified: T1,
        revoked: false,
        height: 100,
      }),
      seedFeeGrantRow({
        fee_grant_id: 1,
        grantor_corporation_id: 1,
        grantee: 'verana1opA',
        msg_types: [EC_CREATE],
        spend_limit: JSON.stringify([{ denom: 'uvna', amount: '1000' }]),
        remaining_spend: JSON.stringify([{ denom: 'uvna', amount: '600' }]),
        modified: T2,
        revoked: false,
        height: 110,
      }),
      seedFeeGrantRow({
        fee_grant_id: 2,
        grantor_corporation_id: 1,
        grantee: 'verana1opB',
        msg_types: [CS_CREATE],
        modified: T3,
        revoked: true,
        height: 105,
      }),
    ])

    const atHeight = (blockHeight: number) =>
      broker.call(`${serviceKey}.listFeeGrants`, {}, { meta: { blockHeight } }) as Promise<any>

    const res100 = await atHeight(100)
    expect(listedIds(res100)).toEqual([1])
    expect(res100.fee_grants[0].remaining_spend).toEqual([{ denom: 'uvna', amount: '1000' }])

    const res110 = await atHeight(110)
    expect(listedIds(res110)).toEqual([1])
    expect(res110.fee_grants[0].remaining_spend).toEqual([{ denom: 'uvna', amount: '600' }])
  })
})

describe('DelegationDatabaseService fee grant actions', () => {
  const broker = new ServiceBroker({ logger: false })
  const serviceKey = SERVICE.V1.DelegationDatabaseService.path

  const snapshot = (overrides: Record<string, unknown> = {}) => ({
    msg_types: [EC_CREATE],
    spend_limit: [{ denom: 'uvna', amount: '1000' }],
    remaining_spend: [{ denom: 'uvna', amount: '1000' }],
    expiration: FUTURE,
    period: null,
    ...overrides,
  })

  const blockTime = (height: number) => new Date(Date.UTC(2026, 6, height)).toISOString()
  const seededHeights = [10, 11, 12, 13, 14, 15, 16, 17]

  beforeAll(async () => {
    broker.createService(DelegationDatabaseService)
    await broker.start()

    await knex('fee_grant_history').del()
    await knex('fee_grants').del()

    await knex('block').whereIn('height', seededHeights).del()
    await knex('block').insert(
      seededHeights.map((height) => ({
        height,
        hash: `feegrant-test-${height}`,
        time: blockTime(height),
        proposer_address: 'test',
        data: '{}',
        tx_count: 0,
      }))
    )
  })

  afterAll(async () => {
    await knex('block').whereIn('height', seededHeights).del()
    await broker.stop()
  })

  const call = (action: string, params: Record<string, unknown>) =>
    broker.call(`${serviceKey}.${action}`, params) as Promise<any>

  it('syncFeeGrant inserts a row and re-sync keeps the surrogate id', async () => {
    await call('syncFeeGrant', { grantorCorporationId: 1, grantee: 'verana1op', snapshot: snapshot(), blockHeight: 10 })

    const inserted = await knex('fee_grants').where({ grantor_corporation_id: 1, grantee: 'verana1op' }).first()
    expect(inserted).toBeTruthy()
    expect(inserted.msg_types).toEqual([EC_CREATE])

    await call('syncFeeGrant', {
      grantorCorporationId: 1,
      grantee: 'verana1op',
      snapshot: snapshot({ msg_types: [EC_CREATE, CS_CREATE] }),
      blockHeight: 11,
    })

    const updated = await knex('fee_grants').where({ grantor_corporation_id: 1, grantee: 'verana1op' }).first()
    expect(Number(updated.id)).toBe(Number(inserted.id))
    expect(updated.msg_types).toEqual([EC_CREATE, CS_CREATE])

    const history = await knex('fee_grant_history').where('fee_grant_id', inserted.id)
    expect(history).toHaveLength(2)
  })

  it('refreshFeeGrantSpend updates remaining_spend, expiration, and modified from the snapshot', async () => {
    await call('refreshFeeGrantSpend', {
      grantorCorporationId: 1,
      grantee: 'verana1op',
      snapshot: snapshot({ remaining_spend: [{ denom: 'uvna', amount: '400' }], expiration: T3 }),
      blockHeight: 12,
    })

    const row = await knex('fee_grants').where({ grantor_corporation_id: 1, grantee: 'verana1op' }).first()
    expect(row.remaining_spend).toEqual([{ denom: 'uvna', amount: '400' }])
    expect(new Date(row.expiration).toISOString()).toBe(T3)
    expect(Number(row.height)).toBe(12)
    expect(new Date(row.modified).toISOString()).toBe(blockTime(12))
  })

  it('refreshFeeGrantSpend zeroes remaining_spend when the allowance is gone', async () => {
    await call('refreshFeeGrantSpend', {
      grantorCorporationId: 1,
      grantee: 'verana1op',
      snapshot: null,
      blockHeight: 13,
    })

    const row = await knex('fee_grants').where({ grantor_corporation_id: 1, grantee: 'verana1op' }).first()
    expect(row.remaining_spend).toEqual([{ denom: 'uvna', amount: '0' }])
  })

  it('refreshFeeGrantSpend is a no-op for an untracked pair', async () => {
    const res = await call('refreshFeeGrantSpend', {
      grantorCorporationId: 99,
      grantee: 'verana1nobody',
      snapshot: null,
      blockHeight: 14,
    })
    expect(res.success).toBe(true)
    expect(await knex('fee_grants').where('grantor_corporation_id', 99).first()).toBeUndefined()
  })

  it('revokeFeeGrant deletes the row and a later re-grant mints a fresh id', async () => {
    const before = await knex('fee_grants').where({ grantor_corporation_id: 1, grantee: 'verana1op' }).first()

    await call('revokeFeeGrant', { grantorCorporationId: 1, grantee: 'verana1op', blockHeight: 15 })

    expect(await knex('fee_grants').where({ grantor_corporation_id: 1, grantee: 'verana1op' }).first()).toBeUndefined()
    const revokedHistory = await knex('fee_grant_history').where('fee_grant_id', before.id).where('revoked', true)
    expect(revokedHistory).toHaveLength(1)

    await call('syncFeeGrant', { grantorCorporationId: 1, grantee: 'verana1op', snapshot: snapshot(), blockHeight: 16 })

    const regranted = await knex('fee_grants').where({ grantor_corporation_id: 1, grantee: 'verana1op' }).first()
    expect(Number(regranted.id)).toBeGreaterThan(Number(before.id))
  })

  it('revokeFeeGrant is a no-op when no grant exists', async () => {
    const res = await call('revokeFeeGrant', { grantorCorporationId: 42, grantee: 'verana1ghost', blockHeight: 17 })
    expect(res.success).toBe(true)
  })
})

afterAll(async () => {
  await knex.destroy()
})
