import { ServiceBroker } from 'moleculer'
import { VeranaCorporationMessageTypes } from '../../../../src/common/verana-message-types'
import CrawlTxService from '../../../../src/services/crawl-tx/crawl_tx.service'

const createCorporationEvent = (corporationId: string, msgIndex: string) => ({
  type: 'create_corporation',
  attributes: [
    { key: 'corporation_id', value: corporationId },
    { key: 'msg_index', value: msgIndex },
  ],
})

describe('crawl_tx routing of corporation messages', () => {
  const broker = new ServiceBroker({ logger: false })
  const service: any = broker.createService(CrawlTxService)

  beforeAll(() => service.getQueueManager().stopAll())
  afterAll(async () => {
    await broker.stop()
  })

  it('gives each message of a multi-message tx only the events of its own message', async () => {
    const txLevelEvent = { type: 'tx', attributes: [{ key: 'fee', value: '1000uvna' }] }
    const events = [txLevelEvent, createCorporationEvent('3', '0'), createCorporationEvent('4', '1')]
    const tx = {
      id: 1,
      code: 0,
      height: 6273,
      hash: 'HASH1',
      timestamp: '2026-11-10T00:00:00.000Z',
      data: { tx_response: { height: '6273', txhash: 'HASH1', events } },
    }
    const messages = [0, 1].map((index) => ({
      tx_id: 1,
      index,
      type: VeranaCorporationMessageTypes.CreateCorporation,
      content: { did: `did:example:corp-${index}` },
    }))

    const payload = await service.processMessageTypes(messages, [tx])

    expect(payload.corporationList.map((message: any) => message.txEvents)).toEqual([
      [txLevelEvent, createCorporationEvent('3', '0')],
      [txLevelEvent, createCorporationEvent('4', '1')],
    ])
  })
})
