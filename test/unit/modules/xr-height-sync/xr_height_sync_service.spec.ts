import { Buffer } from 'node:buffer'
import { fetchExchangeRate } from '../../../../src/modules/xr-height-sync/xr_height_sync_helpers'
import { runHeightSyncXR } from '../../../../src/modules/xr-height-sync/xr_height_sync_service'

jest.mock('../../../../src/modules/xr-height-sync/xr_height_sync_helpers', () => ({
  ...jest.requireActual('../../../../src/modules/xr-height-sync/xr_height_sync_helpers'),
  fetchExchangeRate: jest.fn(),
}))

const b64 = (value: string) => Buffer.from(value, 'utf8').toString('base64')

const ledgerRate = (id: number) => ({
  id,
  baseAssetType: 1,
  baseAsset: 'tu',
  quoteAssetType: 2,
  quoteAsset: 'uvna',
  rate: '1000',
  rateScale: 6,
  validityDuration: { seconds: 3600, nanos: 0 },
  updated: new Date('2026-11-13T00:00:00Z'),
  expires: new Date('2026-11-13T01:00:00Z'),
  state: true,
})

describe('runHeightSyncXR', () => {
  beforeEach(() => jest.mocked(fetchExchangeRate).mockReset())

  it('syncs every rate named by the events, including ids past the 64th and base64-encoded attributes', async () => {
    jest.mocked(fetchExchangeRate).mockImplementation(async (id) => ledgerRate(id) as never)
    const broker = { call: jest.fn().mockResolvedValue({ success: true }) }

    await runHeightSyncXR(
      broker as never,
      {
        events: [
          { type: 'update_exchange_rate', attributes: [{ key: 'id', value: '1400' }] },
          { type: 'create_exchange_rate', attributes: [{ key: b64('id'), value: b64('65') }] },
        ],
      },
      100
    )

    expect(jest.mocked(fetchExchangeRate).mock.calls).toEqual([
      [1400, 100],
      [65, 100],
    ])
    expect(broker.call.mock.calls.map(([, params]) => [params.exchangeRate.id, params.eventType])).toEqual([
      [1400, 'update_exchange_rate'],
      [65, 'create_exchange_rate'],
    ])
  })

  it('fails when a rate the events name cannot be read', async () => {
    jest.mocked(fetchExchangeRate).mockRejectedValue(new Error('exchange rate 7 not found on the ledger at block 100'))

    await expect(
      runHeightSyncXR(
        { call: jest.fn() } as never,
        { events: [{ type: 'set_exchange_rate_state', attributes: [{ key: 'id', value: '7' }] }] },
        100
      )
    ).rejects.toThrow('exchange rate 7 not found')
  })
})
