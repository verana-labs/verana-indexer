import {
  AllowedMsgAllowance,
  BasicAllowance,
  PeriodicAllowance,
} from 'cosmjs-types/cosmos/feegrant/v1beta1/feegrant.js'
import { toTimestamp } from 'cosmjs-types/helpers.js'
import Long from 'long'
import { withAbciQueryClient } from '../../../../src/common/utils/grpc_query'
import { fetchFeeGrantAllowance } from '../../../../src/modules/de-height-sync/de_height_sync_helpers'

jest.mock('../../../../src/common/utils/grpc_query', () => ({
  withAbciQueryClient: jest.fn(),
}))

const mockAbci = withAbciQueryClient as jest.MockedFunction<typeof withAbciQueryClient>

const EC_CREATE = '/verana.ec.v1.MsgCreateEcosystem'
const RESET = new Date('2026-08-01T00:00:00.000Z')
const EXPIRY = new Date('2026-09-01T00:00:00.000Z')

function allowedMsgGrant(inner: { typeUrl: string; value: Uint8Array }) {
  return {
    allowance: {
      typeUrl: '/cosmos.feegrant.v1beta1.AllowedMsgAllowance',
      value: AllowedMsgAllowance.encode(
        AllowedMsgAllowance.fromPartial({ allowedMessages: [EC_CREATE], allowance: inner })
      ).finish(),
    },
  }
}

describe('fetchFeeGrantAllowance', () => {
  beforeEach(() => mockAbci.mockReset())

  it('decodes a periodic allowance: period_reset becomes expiration, period_can_spend the remaining', async () => {
    const periodic = PeriodicAllowance.encode(
      PeriodicAllowance.fromPartial({
        basic: BasicAllowance.fromPartial({}),
        period: { seconds: Long.fromNumber(604800), nanos: 0 },
        periodSpendLimit: [{ denom: 'uvna', amount: '1000' }],
        periodCanSpend: [{ denom: 'uvna', amount: '400' }],
        periodReset: toTimestamp(RESET),
      })
    ).finish()
    mockAbci.mockResolvedValue(
      allowedMsgGrant({ typeUrl: '/cosmos.feegrant.v1beta1.PeriodicAllowance', value: periodic })
    )

    expect(await fetchFeeGrantAllowance('verana1policy', 'verana1op', 100)).toEqual({
      msg_types: [EC_CREATE],
      spend_limit: [{ denom: 'uvna', amount: '1000' }],
      remaining_spend: [{ denom: 'uvna', amount: '400' }],
      expiration: RESET.toISOString(),
      period: '604800s',
    })
  })

  it('decodes a basic allowance: spend_limit doubles as remaining, absolute expiration, no period', async () => {
    const basic = BasicAllowance.encode(
      BasicAllowance.fromPartial({
        spendLimit: [{ denom: 'uvna', amount: '600' }],
        expiration: toTimestamp(EXPIRY),
      })
    ).finish()
    mockAbci.mockResolvedValue(allowedMsgGrant({ typeUrl: '/cosmos.feegrant.v1beta1.BasicAllowance', value: basic }))

    expect(await fetchFeeGrantAllowance('verana1policy', 'verana1op', 100)).toEqual({
      msg_types: [EC_CREATE],
      spend_limit: [{ denom: 'uvna', amount: '600' }],
      remaining_spend: [{ denom: 'uvna', amount: '600' }],
      expiration: EXPIRY.toISOString(),
      period: null,
    })
  })

  it('reports an unwrapped allowance as unrestricted (empty msg_types)', async () => {
    const basic = BasicAllowance.encode(
      BasicAllowance.fromPartial({ spendLimit: [{ denom: 'uvna', amount: '600' }] })
    ).finish()
    mockAbci.mockResolvedValue({
      allowance: { typeUrl: '/cosmos.feegrant.v1beta1.BasicAllowance', value: basic },
    })

    const snapshot = await fetchFeeGrantAllowance('verana1policy', 'verana1op', 100)
    expect(snapshot?.msg_types).toEqual([])
    expect(snapshot?.spend_limit).toEqual([{ denom: 'uvna', amount: '600' }])
  })

  it('returns undefined when the chain reports the allowance absent', async () => {
    mockAbci.mockRejectedValue(
      new Error('Query failed with (38): rpc error: code = Internal desc = fee-grant not found: not found')
    )

    expect(await fetchFeeGrantAllowance('verana1policy', 'verana1op', 100)).toBeUndefined()
  })

  it('rethrows transport errors instead of treating them as an absent allowance', async () => {
    mockAbci.mockRejectedValue(new Error('connection refused'))

    await expect(fetchFeeGrantAllowance('verana1policy', 'verana1op', 100)).rejects.toThrow('connection refused')
  })
})
