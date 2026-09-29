import { SERVICE } from '../../../../src/common'
import * as helpers from '../../../../src/modules/de-height-sync/de_height_sync_helpers'
import { runHeightSyncDE } from '../../../../src/modules/de-height-sync/de_height_sync_service'

jest.mock('../../../../src/modules/de-height-sync/de_height_sync_helpers', () => ({
  fetchOperatorAuthorization: jest.fn(),
  fetchVSOperatorAuthorization: jest.fn(),
  fetchFeeAllowance: jest.fn(),
  fetchFeeGrantAllowance: jest.fn(),
  fetchCorporationPolicyAddress: jest.fn(),
  serializeLedgerOperatorAuthorization: jest.fn((row: unknown) => row),
  serializeLedgerVSOperatorAuthorization: jest.fn((row: unknown) => row),
}))

jest.mock('../../../../src/models/corporation', () => ({
  Corporation: { query: () => ({ findById: async () => undefined, where: () => ({ first: async () => undefined }) }) },
}))

const mocked = helpers as jest.Mocked<typeof helpers>
const DB = SERVICE.V1.DelegationDatabaseService.path

function event(type: string, attributes: Record<string, string>) {
  return { type, attributes: Object.entries(attributes).map(([key, value]) => ({ key, value })) }
}

function fakeBroker() {
  const call = jest.fn(async () => ({ success: true }))
  return { call, logger: { warn: jest.fn() } } as unknown as Parameters<typeof runHeightSyncDE>[0]
}

describe('runHeightSyncDE on *_authorization_updated events (#442)', () => {
  beforeEach(() => jest.clearAllMocks())

  it('re-reads the operator authorization by id and syncs it', async () => {
    const ledger = { id: 9, corporation_id: 3, operator: 'verana1op', msg_types: [], spend_limit: null }
    mocked.fetchOperatorAuthorization.mockResolvedValue(ledger as never)
    const broker = fakeBroker()

    await runHeightSyncDE(broker, { events: [event('operator_authorization_updated', { authz_id: '9' })] }, 120)

    expect(mocked.fetchOperatorAuthorization).toHaveBeenCalledWith(9, 120)
    expect(broker.call).toHaveBeenCalledWith(`${DB}.syncOperatorAuthorization`, {
      authorization: ledger,
      feeAllowance: null,
      blockHeight: 120,
    })
  })

  it('re-reads the VS-operator authorization by id and syncs it', async () => {
    const ledger = { id: 4, corporation_id: 3, vs_operator: 'verana1vs', records: [] }
    mocked.fetchVSOperatorAuthorization.mockResolvedValue(ledger as never)
    const broker = fakeBroker()

    await runHeightSyncDE(
      broker,
      { events: [event('vs_operator_authorization_updated', { vsoa_id: '4', participant_id: '10' })] },
      121
    )

    expect(mocked.fetchVSOperatorAuthorization).toHaveBeenCalledWith(4, 121)
    expect(broker.call).toHaveBeenCalledWith(`${DB}.syncVSOperatorAuthorization`, {
      authorization: ledger,
      blockHeight: 121,
    })
  })
})
