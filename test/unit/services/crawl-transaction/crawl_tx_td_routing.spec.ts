import { ServiceBroker } from 'moleculer'
import { VeranaTrustDepositMessageTypes } from '../../../../src/common/verana-message-types'
import CrawlTxService from '../../../../src/services/crawl-tx/crawl_tx.service'

function tx(id: number, code: number) {
  return {
    id,
    code,
    height: 53500,
    hash: `HASH${id}`,
    timestamp: '2026-11-10T00:00:00.000Z',
    data: { tx_response: { height: '53500', txhash: `HASH${id}`, events: [] } },
  }
}

function reclaimMessage(txId: number) {
  return {
    tx_id: txId,
    index: 0,
    type: VeranaTrustDepositMessageTypes.ReclaimYield,
    content: { corporation: 'verana1corporation', operator: 'verana1operator' },
  }
}

describe('crawl_tx routing of trust deposit messages', () => {
  const broker = new ServiceBroker({ logger: false })
  const service: any = broker.createService(CrawlTxService)

  beforeAll(() => service.getQueueManager().stopAll())
  afterAll(async () => {
    await broker.stop()
  })

  it('routes a trust deposit message from a successful tx', async () => {
    const payload = await service.processMessageTypes([reclaimMessage(1)], [tx(1, 0)])

    expect(payload.trustDepositList).toHaveLength(1)
  })

  it('ignores a trust deposit message from a failed tx', async () => {
    const payload = await service.processMessageTypes([reclaimMessage(1)], [tx(1, 5)])

    expect(payload.trustDepositList).toEqual([])
  })
})
