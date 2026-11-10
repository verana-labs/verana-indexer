import { ServiceBroker } from 'moleculer'
import { VeranaEcosystemMessageTypes } from '../../../../src/common/verana-message-types'
import { getEcosystem } from '../../../../src/modules/ec-height-sync/ec_height_sync_helpers'
import EcosystemMessageProcessorService from '../../../../src/services/crawl-ec/ec_processor.service'

jest.mock('../../../../src/modules/ec-height-sync/ec_height_sync_helpers', () => ({
  ...jest.requireActual('../../../../src/modules/ec-height-sync/ec_height_sync_helpers'),
  getEcosystem: jest.fn(),
}))

describe('EcosystemMessageProcessorService failures', () => {
  const broker = new ServiceBroker({ logger: false })
  const service: any = broker.createService(EcosystemMessageProcessorService)

  afterAll(async () => {
    await broker.stop()
  })

  it('fails the batch when one of many messages fails', async () => {
    const ecosystemList = Array.from({ length: 10 }, (_, i) => ({
      id: i + 1,
      type: VeranaEcosystemMessageTypes.UpdateEcosystem,
      height: 100,
      content: { ecosystem_id: i + 1, did: `did:example:eco-${i}` },
    }))
    jest
      .spyOn(service, 'processUpdateTR')
      .mockResolvedValue(undefined)
      .mockRejectedValueOnce(new Error('connection terminated'))

    await expect(service.handleEcosystemMessages({ params: { ecosystemList } })).rejects.toThrow(
      'Failed to process 1 out of 10 Ecosystem messages'
    )
  })

  it('fails the height sync when the ledger has no ecosystem the tx names', async () => {
    const message = { type: VeranaEcosystemMessageTypes.CreateEcosystem, height: 58109, ecosystem_id: 7 }

    jest.mocked(getEcosystem).mockRejectedValueOnce(new Error('rpc error: code = NotFound desc = ecosystem not found'))
    await expect(service.processEcosystemHeightSync(message)).rejects.toThrow('code = NotFound')

    jest.mocked(getEcosystem).mockResolvedValueOnce(null)
    await expect(service.processEcosystemHeightSync(message)).rejects.toThrow(
      'Ledger returned no ecosystem for id=7 at height=58109'
    )
  })
})
