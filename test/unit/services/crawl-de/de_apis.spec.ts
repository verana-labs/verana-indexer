jest.mock('../../../../src/models/operator_authorization', () => ({
  __esModule: true,
  default: { query: jest.fn() },
}))
jest.mock('../../../../src/models/operator_authorization_history', () => ({
  __esModule: true,
  default: { query: jest.fn() },
}))
jest.mock('../../../../src/models/vs_operator_authorization', () => ({
  __esModule: true,
  default: { query: jest.fn() },
}))
jest.mock('../../../../src/models/vs_operator_authorization_history', () => ({
  __esModule: true,
  default: { query: jest.fn() },
}))

const mockKnexTable = jest.fn()
const mockKnexFrom = jest.fn()
jest.mock('../../../../src/common/utils/db_connection', () => ({
  __esModule: true,
  default: Object.assign((table: string) => mockKnexTable(table), { from: (sub: unknown) => mockKnexFrom(sub) }),
}))

jest.mock('../../../../src/common/utils/apiResponse', () => ({
  __esModule: true,
  default: {
    success: jest.fn((_ctx: unknown, data: unknown) => data),
    error: jest.fn((_ctx: unknown, message: string, code: number) => ({ error: message, code })),
  },
}))

jest.mock('../../../../src/common/utils/block_time', () => ({
  getBlockChainTimeAsOf: jest.fn(async () => new Date('2024-03-01T00:00:00.000Z')),
}))

import { ServiceBroker } from 'moleculer'
import OperatorAuthorization from '../../../../src/models/operator_authorization'
import OperatorAuthorizationHistory from '../../../../src/models/operator_authorization_history'
import VSOperatorAuthorization from '../../../../src/models/vs_operator_authorization'
import DelegationApiService from '../../../../src/services/crawl-de/de_apis.service'

// A stored row still carrying the v3 fee columns — the serializer must not surface them (spec #48).
function operatorAuthorizationRow(over: Record<string, unknown> = {}) {
  return {
    id: 7,
    corporation_id: 3,
    operator: 'verana1operator',
    msg_types: ['/verana.ec.v1.MsgCreateEcosystem'],
    spend_limit: [{ denom: 'uvna', amount: '1000' }],
    remaining_spend: [{ denom: 'uvna', amount: '400' }],
    fee_spend_limit: [{ denom: 'uvna', amount: '500' }],
    remaining_fee_spend: [{ denom: 'uvna', amount: '250' }],
    expiration: '2030-01-01T00:00:00.000Z',
    period: '86400s',
    revoked: false,
    ...over,
  }
}

function listQueryResolvesTo(rows: unknown[]) {
  const qb: any = {}
  qb.select = jest.fn(() => qb)
  qb.where = jest.fn(() => qb)
  qb.whereRaw = jest.fn(() => qb)
  qb.orderBy = jest.fn(() => qb)
  qb.distinctOn = jest.fn(() => qb)
  qb.as = jest.fn(() => qb)
  qb.limit = jest.fn(() => Promise.resolve(rows))
  mockKnexTable.mockReturnValue(qb)
  mockKnexFrom.mockReturnValue(qb)
  return qb
}

function findByIdResolvesTo(model: { query: jest.Mock }, row: unknown) {
  model.query.mockReturnValue({ findById: jest.fn(async () => row) })
}

function historyChainResolvesTo(row: unknown) {
  const qb: any = {}
  qb.where = jest.fn(() => qb)
  qb.orderBy = jest.fn(() => qb)
  qb.first = jest.fn(async () => row)
  ;(OperatorAuthorizationHistory as unknown as { query: jest.Mock }).query.mockReturnValue(qb)
  return qb
}

describe('DelegationApiService OperatorAuthorization responses (spec #48)', () => {
  const broker = new ServiceBroker({ logger: false })
  const service = new DelegationApiService(broker)

  beforeEach(() => {
    jest.clearAllMocks()
  })

  it('listOperatorAuthorizations omits fee fields but keeps spend/expiration/period', async () => {
    listQueryResolvesTo([operatorAuthorizationRow()])

    const ctx: any = { params: {}, meta: {} }
    const res: any = await service.listOperatorAuthorizations(ctx)

    expect(res.authorizations).toHaveLength(1)
    const entry = res.authorizations[0]
    expect(entry).toEqual({
      id: 7,
      corporation_id: 3,
      operator: 'verana1operator',
      msg_types: ['/verana.ec.v1.MsgCreateEcosystem'],
      spend_limit: [{ denom: 'uvna', amount: '1000' }],
      remaining_spend: [{ denom: 'uvna', amount: '400' }],
      expiration: '2030-01-01T00:00:00.000Z',
      period: '86400s',
    })
    expect(entry).not.toHaveProperty('fee_spend_limit')
    expect(entry).not.toHaveProperty('remaining_fee_spend')
  })

  it('getOperatorAuthorization omits fee fields on the latest-state path', async () => {
    findByIdResolvesTo(OperatorAuthorization as unknown as { query: jest.Mock }, operatorAuthorizationRow())

    const ctx: any = { params: { id: 7 }, meta: {} }
    const res: any = await service.getOperatorAuthorization(ctx)

    expect(res.authorization.id).toBe(7)
    expect(res.authorization).not.toHaveProperty('fee_spend_limit')
    expect(res.authorization).not.toHaveProperty('remaining_fee_spend')
  })

  it('getOperatorAuthorization omits fee fields on the At-Block-Height history path', async () => {
    historyChainResolvesTo(operatorAuthorizationRow({ operator_authorization_id: 7, id: 99 }))

    const ctx: any = { params: { id: 7 }, meta: { blockHeight: 50 } }
    const res: any = await service.getOperatorAuthorization(ctx)

    expect(res.authorization.id).toBe(7)
    expect(res.authorization).not.toHaveProperty('fee_spend_limit')
    expect(res.authorization).not.toHaveProperty('remaining_fee_spend')
  })

  it('listOperatorAuthorizations at-height serializes history rows — no fee fields, no raw columns', async () => {
    listQueryResolvesTo([
      operatorAuthorizationRow({ operator_authorization_id: 7, id: 99, height: 40, modified: '2024-02-01' }),
    ])

    const ctx: any = { params: {}, meta: { blockHeight: 50 } }
    const res: any = await service.listOperatorAuthorizations(ctx)

    expect(res.authorizations).toHaveLength(1)
    expect(res.authorizations[0]).toEqual({
      id: 7,
      corporation_id: 3,
      operator: 'verana1operator',
      msg_types: ['/verana.ec.v1.MsgCreateEcosystem'],
      spend_limit: [{ denom: 'uvna', amount: '1000' }],
      remaining_spend: [{ denom: 'uvna', amount: '400' }],
      expiration: '2030-01-01T00:00:00.000Z',
      period: '86400s',
    })
  })

  it('spend_limit stays optional: omitted entirely when the row has none', async () => {
    listQueryResolvesTo([operatorAuthorizationRow({ spend_limit: null, remaining_spend: null })])

    const ctx: any = { params: {}, meta: {} }
    const res: any = await service.listOperatorAuthorizations(ctx)

    expect(res.authorizations[0]).not.toHaveProperty('spend_limit')
    expect(res.authorizations[0]).not.toHaveProperty('remaining_spend')
  })

  it('ParticipantAuthorizationRecord entries keep their fee fields (IDX-DE-QRY-4 untouched)', async () => {
    findByIdResolvesTo(VSOperatorAuthorization as unknown as { query: jest.Mock }, {
      id: 5,
      corporation_id: 3,
      vs_operator: 'verana1vsop',
      revoked: false,
      records: [
        {
          participant_id: 11,
          msg_types: ['/verana.pp.v1.MsgRenewParticipant'],
          spend_limit: [{ denom: 'uvna', amount: '100' }],
          remaining_spend: [{ denom: 'uvna', amount: '90' }],
          fee_spend_limit: [{ denom: 'uvna', amount: '50' }],
          remaining_fee_spend: [{ denom: 'uvna', amount: '25' }],
          with_feegrant: true,
          expiration: '2030-01-01T00:00:00.000Z',
          period: '86400s',
        },
      ],
    })

    const ctx: any = { params: { id: 5 }, meta: {} }
    const res: any = await service.getVSOperatorAuthorization(ctx)

    const record = res.authorization.records[0]
    expect(record.fee_spend_limit).toEqual([{ denom: 'uvna', amount: '50' }])
    expect(record.remaining_fee_spend).toEqual([{ denom: 'uvna', amount: '25' }])
    expect(record.with_feegrant).toBe(true)
  })
})
