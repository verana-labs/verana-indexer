import { ServiceBroker } from 'moleculer'
import { VeranaCorporationMessageTypes } from '../../../../src/common/verana-message-types'
import CorporationMessageProcessorService from '../../../../src/services/crawl-co/co_processor.service'

describe('CorporationMessageProcessorService failures', () => {
  const broker = new ServiceBroker({ logger: false })
  const service: any = broker.createService(CorporationMessageProcessorService)

  afterAll(async () => {
    await broker.stop()
  })

  it('fails the batch when one of many messages fails', async () => {
    const corporationList = Array.from({ length: 10 }, (_, i) => ({
      id: i + 1,
      type: VeranaCorporationMessageTypes.UpdateCorporation,
      height: 100,
      content: { corporation: `verana1corp${i}` },
    }))
    jest
      .spyOn(service, 'processMessage')
      .mockResolvedValue(undefined)
      .mockRejectedValueOnce(new Error('connection terminated'))

    await expect(service.handleCorporationMessages({ params: { corporationList } })).rejects.toThrow(
      'Failed to process 1/10 Corporation messages: connection terminated'
    )
    expect(service.processMessage).toHaveBeenCalledTimes(10)
  })
})
