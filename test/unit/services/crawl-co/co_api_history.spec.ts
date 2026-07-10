jest.mock('../../../../src/models/corporation', () => ({ Corporation: { query: jest.fn() } }))
jest.mock('../../../../src/models/corporation_history', () => ({ CorporationHistory: { query: jest.fn() } }))

jest.mock('../../../../src/common/utils/apiResponse', () => ({
  __esModule: true,
  default: {
    success: jest.fn((_ctx: unknown, data: unknown) => data),
    error: jest.fn((_ctx: unknown, message: string, code: number) => ({ error: message, code })),
  },
}))

import { ServiceBroker } from 'moleculer'
import { Corporation } from '../../../../src/models/corporation'
import { CorporationHistory } from '../../../../src/models/corporation_history'
import CorporationApiService from '../../../../src/services/crawl-co/co_api.service'

function corporationExists(exists: boolean) {
  ;(Corporation.query as jest.Mock).mockReturnValue({
    findById: jest.fn(async () => (exists ? { id: 7 } : undefined)),
  })
}

function historyResolvesTo(rows: Record<string, unknown>[]) {
  const qb: any = {}
  qb.where = jest.fn(() => qb)
  qb.orderBy = jest.fn(() => qb)
  qb.limit = jest.fn(async () => rows.map((r) => ({ toJSON: () => r })))
  ;(CorporationHistory.query as jest.Mock).mockReturnValue(qb)
  return qb
}

function historyRow(over: Record<string, unknown> = {}) {
  return {
    id: '12',
    event_type: 'Create',
    height: '100',
    created_at: '2024-01-01T00:00:00.000Z',
    changes: '{"did":"did:example:co"}',
    account: 'verana1signer',
    ...over,
  }
}

describe('CorporationApiService.getCorporationHistory', () => {
  const broker = new ServiceBroker({ logger: false })
  const service = new CorporationApiService(broker)

  beforeEach(() => {
    jest.clearAllMocks()
    corporationExists(true)
    historyResolvesTo([])
  })

  it('returns ActivityTimelineResponse with mapped ActivityItems, newest-first by default', async () => {
    historyResolvesTo([
      historyRow({ id: '13', event_type: 'IncreaseCGFActiveVersion', changes: '{"active_version":2}' }),
      historyRow(),
    ])

    const ctx: any = { params: { id: '7' }, meta: {} }
    const res: any = await service.getCorporationHistory(ctx)

    expect(res.entity_type).toBe('Corporation')
    expect(res.entity_id).toBe('7')
    expect(res.activity).toHaveLength(2)
    expect(res.activity[0]).toEqual({
      id: 13,
      timestamp: '2024-01-01T00:00:00.000Z',
      block_height: 100,
      entity_type: 'Corporation',
      entity_id: '7',
      msg: 'IncreaseCGFActiveVersion',
      changes: { active_version: 2 },
      account: 'verana1signer',
    })
    expect(res.activity[1].msg).toBe('CreateCorporation')
  })

  it('maps stored Create/Update event types to spec msg names and omits absent account', async () => {
    historyResolvesTo([historyRow({ event_type: 'Update', account: null })])

    const ctx: any = { params: { id: '7' }, meta: {} }
    const res: any = await service.getCorporationHistory(ctx)

    expect(res.activity[0].msg).toBe('UpdateCorporation')
    expect('account' in res.activity[0]).toBe(false)
  })

  it('applies id-cursor pagination, sort and limit to the query', async () => {
    const qb = historyResolvesTo([])

    const ctx: any = { params: { id: '7', min_id: '5', max_id: '30', sort: '+id', limit: '10' }, meta: {} }
    await service.getCorporationHistory(ctx)

    expect(qb.where).toHaveBeenCalledWith('corporation_id', '7')
    expect(qb.where).toHaveBeenCalledWith('id', '>=', '5')
    expect(qb.where).toHaveBeenCalledWith('id', '<', '30')
    expect(qb.orderBy).toHaveBeenCalledWith('id', 'asc')
    expect(qb.limit).toHaveBeenCalledWith(10)
  })

  it('defaults to id desc and limit 64', async () => {
    const qb = historyResolvesTo([])

    const ctx: any = { params: { id: '7' }, meta: {} }
    await service.getCorporationHistory(ctx)

    expect(qb.orderBy).toHaveBeenCalledWith('id', 'desc')
    expect(qb.limit).toHaveBeenCalledWith(64)
  })

  it('filters by height when At-Block-Height is set', async () => {
    const qb = historyResolvesTo([])

    const ctx: any = { params: { id: '7' }, meta: { blockHeight: 50 } }
    await service.getCorporationHistory(ctx)

    expect(qb.where).toHaveBeenCalledWith('height', '<=', 50)
  })

  it('rejects a non-numeric id with 400 before any query', async () => {
    const ctx: any = { params: { id: 'did:example:co' }, meta: {} }
    const res: any = await service.getCorporationHistory(ctx)

    expect(res.code).toBe(400)
    expect(Corporation.query).not.toHaveBeenCalled()
  })

  it('rejects invalid pagination with 400', async () => {
    for (const params of [
      { id: '7', limit: '0' },
      { id: '7', min_id: 'abc' },
      { id: '7', sort: 'height' },
    ]) {
      const ctx: any = { params, meta: {} }
      const res: any = await service.getCorporationHistory(ctx)
      expect(res.code).toBe(400)
    }
  })

  it('returns 404 when the corporation does not exist', async () => {
    corporationExists(false)

    const ctx: any = { params: { id: '999' }, meta: {} }
    const res: any = await service.getCorporationHistory(ctx)

    expect(res).toEqual({ error: 'Corporation 999 not found', code: 404 })
    expect(CorporationHistory.query).not.toHaveBeenCalled()
  })
})
