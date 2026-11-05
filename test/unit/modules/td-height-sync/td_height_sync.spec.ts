import type { ServiceBroker } from 'moleculer'
import { withAbciQueryClient } from '../../../../src/common/utils/grpc_query'
import { runHeightSyncTD } from '../../../../src/modules/td-height-sync/td_height_sync_service'

jest.mock('../../../../src/common/utils/grpc_query', () => ({
  ...jest.requireActual('../../../../src/common/utils/grpc_query'),
  withAbciQueryClient: jest.fn(),
}))
jest.mock('../../../../src/common/utils/db_connection', () => {
  const builder = {
    where: () => builder,
    select: () => builder,
    first: async () => ({ id: 7, corporation: 'verana1corp' }),
  }
  return { __esModule: true, default: () => builder }
})

const mockAbci = withAbciQueryClient as jest.MockedFunction<typeof withAbciQueryClient>

const NOT_FOUND = new Error(
  'Query failed with (38): rpc error: code = NotFound desc = trust deposit not found for corporation_id 7: key not found'
)
const FUTURE_HEIGHT = new Error(
  'Query failed with (26): cannot query with height in the future; please provide a valid height: invalid height'
)

const payload = {
  trustDepositList: [{ type: '/verana.td.v1.MsgReclaimTrustDepositYield', content: { corporation: '7' } }],
}

function mockBroker() {
  const call = jest.fn().mockResolvedValue({ success: true })
  const logger = { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() }
  return { broker: { call, logger } as unknown as ServiceBroker, call }
}

describe('runHeightSyncTD', () => {
  beforeEach(() => mockAbci.mockReset())

  it('stores the trust deposit the ledger returns', async () => {
    mockAbci.mockResolvedValue({ corporation: 'verana1corp', deposit: 10 })
    const { broker, call } = mockBroker()

    await runHeightSyncTD(broker, payload, 500)

    expect(call).toHaveBeenCalledWith(expect.stringContaining('syncFromLedger'), {
      ledgerTrustDeposit: { corporation: 'verana1corp', deposit: 10 },
      blockHeight: 500,
      eventType: expect.any(String),
    })
  })

  it('skips a corporation without a trust deposit', async () => {
    mockAbci.mockRejectedValue(NOT_FOUND)
    const { broker, call } = mockBroker()

    await expect(runHeightSyncTD(broker, payload, 500)).resolves.toBeUndefined()
    expect(call).not.toHaveBeenCalledWith(expect.stringContaining('syncFromLedger'), expect.anything())
  })

  it('fails the block when the ledger query fails', async () => {
    mockAbci.mockRejectedValue(FUTURE_HEIGHT)
    const { broker } = mockBroker()

    await expect(runHeightSyncTD(broker, payload, 500)).rejects.toThrow('Failed to sync deposit(s) 7 at block=500')
  })
})
