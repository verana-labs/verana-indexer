import type { ExchangeRate as LedgerExchangeRate } from '@verana-labs/verana-types/codec/verana/xr/v1/tx'
import { serializeLedgerExchangeRate } from '../../../../src/modules/xr-height-sync/xr_height_sync_helpers'

jest.mock('../../../../src/common/utils/grpc_query', () => ({
  withAbciQueryClient: jest.fn(),
}))

function serialize(validityDuration: { seconds: number; nanos: number } | undefined) {
  const ledger = {
    id: 1,
    baseAssetType: 1,
    baseAsset: 'tu',
    quoteAssetType: 2,
    quoteAsset: 'uvna',
    rate: '1000',
    rateScale: 6,
    validityDuration,
    updated: new Date('2026-10-01T00:00:00Z'),
    expires: new Date('2026-10-01T01:00:00Z'),
    state: true,
  }
  return serializeLedgerExchangeRate(ledger as unknown as LedgerExchangeRate)
}

describe('serializeLedgerExchangeRate', () => {
  it('keeps validity_duration as a protobuf JSON duration, nanos included', () => {
    expect(serialize({ seconds: 3600, nanos: 0 }).validity_duration).toBe('3600s')
    expect(serialize({ seconds: 90, nanos: 500000000 }).validity_duration).toBe('90.5s')
    expect(serialize({ seconds: 60, nanos: 1 }).validity_duration).toBe('60.000000001s')
  })

  it('stores null instead of a zero duration', () => {
    expect(serialize(undefined).validity_duration).toBeNull()
    expect(serialize({ seconds: 0, nanos: 0 }).validity_duration).toBeNull()
  })
})
