import type { ServiceBroker } from 'moleculer'
import { fetchDigest } from '../../../../src/modules/di-height-sync/di_height_sync_helpers'
import { runHeightSyncDI } from '../../../../src/modules/di-height-sync/di_height_sync_service'

jest.mock('../../../../src/modules/di-height-sync/di_height_sync_helpers', () => ({
  ...jest.requireActual('../../../../src/modules/di-height-sync/di_height_sync_helpers'),
  fetchDigest: jest.fn(),
}))

const mockFetchDigest = fetchDigest as jest.MockedFunction<typeof fetchDigest>
const DIGEST = 'sha384-abc'
const events = [{ type: 'store_digest', attributes: [{ key: 'digest', value: DIGEST }] }]

describe('runHeightSyncDI', () => {
  const call = jest.fn()
  const broker = { call } as unknown as ServiceBroker

  beforeEach(() => {
    mockFetchDigest.mockReset()
    call.mockReset()
  })

  it('syncs a stored digest', async () => {
    mockFetchDigest.mockResolvedValue({ digest: DIGEST, created: undefined })

    await runHeightSyncDI(broker, { events }, 120)

    expect(mockFetchDigest).toHaveBeenCalledWith(DIGEST, 120)
    expect(call).toHaveBeenCalledTimes(1)
  })

  it('fails the block when a stored digest is missing from the ledger', async () => {
    mockFetchDigest.mockResolvedValue(undefined)

    await expect(runHeightSyncDI(broker, { events }, 120)).rejects.toThrow(`digest ${DIGEST} not found`)
    expect(call).not.toHaveBeenCalled()
  })
})
