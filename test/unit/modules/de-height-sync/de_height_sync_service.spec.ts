import type { ServiceBroker } from 'moleculer'
import { fetchOperatorAuthorization } from '../../../../src/modules/de-height-sync/de_height_sync_helpers'
import { runHeightSyncDE } from '../../../../src/modules/de-height-sync/de_height_sync_service'

jest.mock('../../../../src/modules/de-height-sync/de_height_sync_helpers', () => ({
  ...jest.requireActual('../../../../src/modules/de-height-sync/de_height_sync_helpers'),
  fetchOperatorAuthorization: jest.fn(),
}))

const mockFetch = fetchOperatorAuthorization as jest.MockedFunction<typeof fetchOperatorAuthorization>

const grant = (authzId: number) => ({
  type: 'grant_operator_authorization',
  attributes: [
    { key: 'authz_id', value: String(authzId) },
    { key: 'corporation_id', value: '7' },
    { key: 'grantee', value: `verana1op${authzId}` },
  ],
})

const ledgerAuthorization = (id: number) =>
  ({
    id,
    corporationId: 7,
    operator: `verana1op${id}`,
    msgTypes: [],
    spendLimit: [],
    remainingSpend: [],
  }) as unknown as Awaited<ReturnType<typeof fetchOperatorAuthorization>>

function mockBroker() {
  const call = jest.fn().mockResolvedValue({ success: true })
  return { broker: { call, logger: { warn: jest.fn() } } as unknown as ServiceBroker, call }
}

describe('runHeightSyncDE', () => {
  beforeEach(() => mockFetch.mockReset())

  it('writes every authorization the ledger returns', async () => {
    mockFetch.mockImplementation(async (id) => ledgerAuthorization(id))
    const { broker, call } = mockBroker()

    await runHeightSyncDE(broker, { events: [grant(1), grant(2)] }, 500)

    expect(call).toHaveBeenCalledTimes(2)
  })

  it('writes nothing when a later ledger read fails, so the retried block cannot duplicate history', async () => {
    const failure = new Error('Query failed with (26): cannot query with height in the future')
    mockFetch.mockResolvedValueOnce(ledgerAuthorization(1)).mockRejectedValueOnce(failure)
    const { broker, call } = mockBroker()

    await expect(runHeightSyncDE(broker, { events: [grant(1), grant(2)] }, 500)).rejects.toBe(failure)
    expect(call).not.toHaveBeenCalled()
  })
})
