import type { VSOperatorAuthorization as LedgerVSOperatorAuthorization } from '@verana-labs/verana-types/codec/verana/de/v1/types'
import { serializeLedgerVSOperatorAuthorization } from '../../../../src/modules/de-height-sync/de_height_sync_helpers'

jest.mock('../../../../src/common/utils/grpc_query', () => ({
  withAbciQueryClient: jest.fn(),
}))

function ledgerRecord(over: Record<string, unknown> = {}) {
  return {
    participantId: 3,
    msgTypes: ['/verana.ec.v1.MsgCreateEcosystem'],
    spendLimit: [],
    remainingSpend: [],
    feeSpendLimit: [{ denom: 'uvna', amount: '5' }],
    withFeegrant: true,
    expiration: undefined,
    period: undefined,
    ...over,
  }
}

function serialize(record: Record<string, unknown>) {
  const ledger = { id: 1, corporationId: 2, vsOperator: 'verana1vs', records: [record] }
  return serializeLedgerVSOperatorAuthorization(ledger as unknown as LedgerVSOperatorAuthorization).records[0]
}

describe('serializeLedgerVSOperatorAuthorization', () => {
  it('encodes period as protobuf JSON seconds with at most nine fractional digits', () => {
    expect(serialize(ledgerRecord({ period: { seconds: 315360000, nanos: 0 } })).period).toBe('315360000s')
    expect(serialize(ledgerRecord({ period: { seconds: 1, nanos: 500000000 } })).period).toBe('1.5s')
    expect(serialize(ledgerRecord({ period: { seconds: 0, nanos: 1 } })).period).toBe('0.000000001s')
    expect(serialize(ledgerRecord({ period: { seconds: 0, nanos: 0 } })).period).toBeNull()
  })

  it('keeps fee_spend_limit and carries no remaining_fee_spend', () => {
    const row = serialize(ledgerRecord({ remainingFeeSpend: [{ denom: 'uvna', amount: '4' }] }))
    expect(row.fee_spend_limit).toEqual([{ denom: 'uvna', amount: '5' }])
    expect(row).not.toHaveProperty('remaining_fee_spend')
  })
})
