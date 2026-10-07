jest.mock('../../../../src/models/exchange_rate', () => ({ __esModule: true, default: { query: jest.fn() } }))
jest.mock('../../../../src/models/exchange_rate_history', () => ({ __esModule: true, default: { query: jest.fn() } }))
jest.mock('../../../../src/common/utils/db_connection', () => ({ __esModule: true, default: jest.fn() }))

jest.mock('../../../../src/common/utils/apiResponse', () => ({
  __esModule: true,
  default: {
    success: jest.fn((_ctx: unknown, data: unknown) => data),
    error: jest.fn((_ctx: unknown, message: string, code: number) => ({ error: message, code })),
  },
}))

jest.mock('../../../../src/common/utils/blockHeight', () => ({
  ...jest.requireActual('../../../../src/common/utils/blockHeight'),
  getResolvedBlockHeight: jest.fn(async (height?: number) => height ?? 777),
}))

import { ServiceBroker } from 'moleculer'
import ExchangeRate from '../../../../src/models/exchange_rate'
import ExchangeRateHistory from '../../../../src/models/exchange_rate_history'
import ExchangeRateApiService from '../../../../src/services/crawl-xr/xr_apis.service'

function chainResolvesTo(model: unknown, row: Record<string, unknown> | undefined) {
  const qb: any = {}
  qb.where = jest.fn(() => qb)
  qb.orderBy = jest.fn(() => qb)
  qb.first = jest.fn(async () => row)
  ;(model as { query: jest.Mock }).query.mockReturnValue(qb)
  return qb
}

const rateRow = {
  id: 3,
  base_asset_type: 'TU',
  base_asset: 'tu',
  quote_asset_type: 'COIN',
  quote_asset: 'uvna',
  rate: '1000',
  rate_scale: 6,
  validity_duration: 3600,
  updated: '2026-10-01T00:00:00.000Z',
  expires: '2027-10-01T00:00:00.000Z',
  state: true,
}

describe('ExchangeRateApiService.getExchangeRate', () => {
  const broker = new ServiceBroker({ logger: false })
  const service = new ExchangeRateApiService(broker)

  beforeEach(() => jest.clearAllMocks())

  it('returns the exchange rate with the latest indexed height as block_height', async () => {
    chainResolvesTo(ExchangeRate, rateRow)

    const res: any = await service.getExchangeRate({ params: { id: '3' }, meta: {} } as any)

    expect(res.exchange_rate).toMatchObject({ id: 3, rate: '1000', rate_scale: 6, state: true })
    expect(res.block_height).toBe(777)
  })

  it('echoes At-Block-Height as block_height on the history path', async () => {
    chainResolvesTo(ExchangeRateHistory, { ...rateRow, id: 55, exchange_rate_id: 3 })

    const res: any = await service.getExchangeRate({ params: { id: '3' }, meta: { blockHeight: 200 } } as any)

    expect(res.exchange_rate.quote_asset).toBe('uvna')
    expect(res.block_height).toBe(200)
  })

  it('returns 404 without block_height when no rate matches', async () => {
    chainResolvesTo(ExchangeRate, undefined)

    const res: any = await service.getExchangeRate({ params: { id: '9' }, meta: {} } as any)

    expect(res).toEqual({ error: 'Exchange rate not found', code: 404 })
  })
})
