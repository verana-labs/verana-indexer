import { ServiceBroker } from 'moleculer'
import { VeranaEcosystemMessageTypes } from '../../../../src/common/verana-message-types'
import CrawlTxService from '../../../../src/services/crawl-tx/crawl_tx.service'

const createEcosystemEvent = (ecosystemId: string, msgIndex: string) => ({
  type: 'create_ecosystem',
  attributes: [
    { key: 'ecosystem_id', value: ecosystemId },
    { key: 'msg_index', value: msgIndex },
  ],
})

describe('crawl_tx routing of ecosystem messages', () => {
  const broker = new ServiceBroker({ logger: false })
  const service: any = broker.createService(CrawlTxService)

  beforeAll(() => service.getQueueManager().stopAll())
  afterAll(async () => {
    await broker.stop()
  })

  it('gives each message of a multi-message tx the ecosystem id of its own event', async () => {
    const events = [createEcosystemEvent('3', '0'), createEcosystemEvent('4', '1')]
    const tx = {
      id: 1,
      code: 0,
      height: 53800,
      hash: 'HASH1',
      timestamp: '2026-11-10T00:00:00.000Z',
      data: { tx_response: { height: '53800', txhash: 'HASH1', events } },
    }
    const messages = [0, 1].map((index) => ({
      tx_id: 1,
      index,
      type: VeranaEcosystemMessageTypes.CreateEcosystem,
      content: { corporation: 'verana1corporation', did: `did:example:eco-${index}` },
    }))

    const payload = await service.processMessageTypes(messages, [tx])

    expect(payload.ecosystemList.map((message: any) => message.eventEcosystemIds)).toEqual([[3], [4]])
  })
})
