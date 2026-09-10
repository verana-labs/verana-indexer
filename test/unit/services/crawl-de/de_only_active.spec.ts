import { ServiceBroker } from 'moleculer'
import { SERVICE } from '../../../../src/common'
import knex from '../../../../src/common/utils/db_connection'
import DelegationApiService from '../../../../src/services/crawl-de/de_apis.service'

const EC_CREATE = '/verana.ec.v1.MsgCreateEcosystem'
const MODIFIED = '2026-07-01T00:00:00.000Z'
const PAST = '2020-01-01T00:00:00.000Z'
const FUTURE = '2999-01-01T00:00:00.000Z'

function seedOperatorAuthorizationRow(row: Record<string, unknown>) {
  return {
    corporation_id: 1,
    msg_types: JSON.stringify([EC_CREATE]),
    spend_limit: JSON.stringify([{ denom: 'uvna', amount: '1000' }]),
    remaining_spend: JSON.stringify([{ denom: 'uvna', amount: '1000' }]),
    expiration: null,
    period: null,
    modified: MODIFIED,
    height: 100,
    ...row,
  }
}

function record(over: Record<string, unknown>) {
  return {
    participant_id: 1,
    msg_types: [EC_CREATE],
    spend_limit: [{ denom: 'uvna', amount: '1000' }],
    remaining_spend: [{ denom: 'uvna', amount: '1000' }],
    fee_spend_limit: null,
    remaining_fee_spend: null,
    with_feegrant: false,
    expiration: null,
    period: null,
    ...over,
  }
}

function seedVSOperatorAuthorizationRow(row: Record<string, unknown>, records: Record<string, unknown>[]) {
  return { corporation_id: 1, records: JSON.stringify(records), modified: MODIFIED, height: 100, ...row }
}

type ListResponse = { authorizations: { id: number }[] }

function listedIds(res: ListResponse): number[] {
  return res.authorizations.map((r) => r.id)
}

describe('Delegation only_active honours period (issue #432)', () => {
  const broker = new ServiceBroker({ logger: false })
  const serviceKey = SERVICE.V1.DelegationApiService.path

  beforeAll(async () => {
    broker.createService(DelegationApiService)
    await broker.start()

    await knex('operator_authorizations').del()
    await knex('vs_operator_authorizations').del()

    await knex('operator_authorizations').insert([
      seedOperatorAuthorizationRow({ id: 1, operator: 'verana1opNoExpiry' }),
      seedOperatorAuthorizationRow({ id: 2, operator: 'verana1opFuture', expiration: FUTURE }),
      seedOperatorAuthorizationRow({ id: 3, operator: 'verana1opExpired', expiration: PAST }),
      seedOperatorAuthorizationRow({ id: 4, operator: 'verana1opPeriodic', expiration: PAST, period: '604800s' }),
    ])

    await knex('vs_operator_authorizations').insert([
      seedVSOperatorAuthorizationRow({ id: 1, vs_operator: 'verana1vsNoExpiry' }, [record({})]),
      seedVSOperatorAuthorizationRow({ id: 2, vs_operator: 'verana1vsFuture' }, [record({ expiration: FUTURE })]),
      seedVSOperatorAuthorizationRow({ id: 3, vs_operator: 'verana1vsExpired' }, [
        record({ participant_id: 1, expiration: PAST }),
        record({ participant_id: 2, expiration: PAST }),
      ]),
      seedVSOperatorAuthorizationRow({ id: 4, vs_operator: 'verana1vsPeriodic' }, [
        record({ participant_id: 1, expiration: PAST }),
        record({ participant_id: 2, expiration: PAST, period: '604800s' }),
      ]),
    ])
  })

  afterAll(async () => {
    await knex('operator_authorizations').del()
    await knex('vs_operator_authorizations').del()
    await broker.stop()
  })

  it('listOperatorAuthorizations keeps a periodic authorization past its cycle boundary', async () => {
    const res = (await broker.call(`${serviceKey}.listOperatorAuthorizations`, {
      only_active: true,
      sort: '+id',
    })) as ListResponse
    expect(listedIds(res)).toEqual([1, 2, 4])
  })

  it('listVSOperatorAuthorizations keeps an entry whose only live record is periodic', async () => {
    const res = (await broker.call(`${serviceKey}.listVSOperatorAuthorizations`, {
      only_active: true,
      sort: '+id',
    })) as ListResponse
    expect(listedIds(res)).toEqual([1, 2, 4])
  })
})
