import { ServiceBroker } from 'moleculer'
import { VeranaEcosystemMessageTypes } from '../../../../src/common/verana-message-types'
import EcosystemMessageProcessorService from '../../../../src/services/crawl-ec/ec_processor.service'

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
})
