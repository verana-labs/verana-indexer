import { ServiceBroker } from 'moleculer'
import {
  VeranaEcosystemMessageTypes,
  VeranaGovernanceFrameworkMessageTypes,
} from '../../../../src/common/verana-message-types'
import CrawlTxService from '../../../../src/services/crawl-tx/crawl_tx.service'

const addGfDocument = VeranaGovernanceFrameworkMessageTypes.AddGovernanceFrameworkDocument
const increaseGf = VeranaGovernanceFrameworkMessageTypes.IncreaseActiveGovernanceFrameworkVersion

function tx(id: number, code: number, events: unknown[] = []) {
  return {
    id,
    code,
    height: 60193,
    hash: `HASH${id}`,
    timestamp: '2026-10-02T13:00:00.000Z',
    data: { tx_response: { height: '60193', txhash: `HASH${id}`, events } },
  }
}

function addMessage(txId: number, ecosystemId?: string) {
  return {
    tx_id: txId,
    index: 0,
    type: addGfDocument,
    content: {
      corporation: 'verana1pol',
      operator: 'verana1op',
      ...(ecosystemId === undefined ? {} : { ecosystem_id: ecosystemId }),
      doc_language: 'en',
      doc_url: 'https://example.com/egf-v2.json',
      doc_digest_sri: 'sha384-egf2',
      version: 2,
    },
  }
}

function increaseMessage(txId: number, ecosystemId: string) {
  return {
    tx_id: txId,
    index: 0,
    type: increaseGf,
    content: { corporation: 'verana1pol', operator: 'verana1op', ecosystem_id: ecosystemId },
  }
}

const types = (list: Array<{ type: string }>) => list.map((m) => m.type)

describe('crawl_tx routing of gf module messages', () => {
  const broker = new ServiceBroker({ logger: false })
  const service: any = broker.createService(CrawlTxService)

  beforeAll(() => service.getQueueManager().stopAll())
  afterAll(async () => {
    await broker.stop()
  })

  const route = (msgs: unknown[], txs: unknown[]) => service.processMessageTypes(msgs, txs)

  it('routes an EGF add (ecosystem_id > 0) to the ecosystem pipeline and the corporation pipeline', async () => {
    const events = [{ type: 'add_gf_document', attributes: [{ key: 'ecosystem_id', value: '1' }] }]
    const payload = await route([addMessage(1, '1')], [tx(1, 0, events)])

    expect(types(payload.ecosystemList)).toEqual([addGfDocument])
    expect(payload.ecosystemList[0].content.ecosystem_id).toBe('1')
    expect(payload.ecosystemList[0].txEvents).toEqual(events)
    expect(payload.ecosystemList[0].height).toBe(60193)
    expect(types(payload.corporationList)).toEqual([addGfDocument])
  })

  it('routes an EGF increase to both pipelines', async () => {
    const payload = await route([increaseMessage(1, '7')], [tx(1, 0)])

    expect(types(payload.ecosystemList)).toEqual([increaseGf])
    expect(types(payload.corporationList)).toEqual([increaseGf])
  })

  it('keeps a CGF add with ecosystem_id 0 out of the ecosystem pipeline', async () => {
    const payload = await route([addMessage(1, '0')], [tx(1, 0)])

    expect(payload.ecosystemList).toEqual([])
    expect(types(payload.corporationList)).toEqual([addGfDocument])
  })

  it('keeps a CGF add without ecosystem_id out of the ecosystem pipeline', async () => {
    const payload = await route([addMessage(1)], [tx(1, 0)])

    expect(payload.ecosystemList).toEqual([])
    expect(types(payload.corporationList)).toEqual([addGfDocument])
  })

  it('does not route a gf UpdateParams message to the ecosystem pipeline', async () => {
    const msg = {
      tx_id: 1,
      index: 0,
      type: VeranaGovernanceFrameworkMessageTypes.UpdateParams,
      content: { authority: 'verana1gov', params: {}, ecosystem_id: '1' },
    }
    const payload = await route([msg], [tx(1, 0)])

    expect(payload.ecosystemList).toEqual([])
  })

  it('leaves ec module routing unchanged', async () => {
    const msg = {
      tx_id: 1,
      index: 0,
      type: VeranaEcosystemMessageTypes.CreateEcosystem,
      content: { corporation: 'verana1pol', operator: 'verana1op', did: 'did:example:eco', language: 'en' },
    }
    const payload = await route([msg], [tx(1, 0)])

    expect(types(payload.ecosystemList)).toEqual([VeranaEcosystemMessageTypes.CreateEcosystem])
    expect(payload.corporationList).toEqual([])
  })

  it('ignores gf messages from failed transactions', async () => {
    const payload = await route([addMessage(1, '1')], [tx(1, 5)])

    expect(payload.ecosystemList).toEqual([])
    expect(payload.corporationList).toEqual([])
  })
})
