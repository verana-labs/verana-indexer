import { ServiceBroker } from 'moleculer'
import {
  VeranaCorporationMessageTypes,
  VeranaParticipantMessageTypes,
} from '../../../../src/common/verana-message-types'
import { readProposal } from '../../../../src/services/api/indexer_events_query'
import CrawlTxService from '../../../../src/services/crawl-tx/crawl_tx.service'

jest.mock('../../../../src/common/utils/error_handler', () => ({
  ...jest.requireActual('../../../../src/common/utils/error_handler'),
  handleErrorGracefully: jest.fn().mockResolvedValue(true),
}))

jest.mock('../../../../src/services/api/indexer_events_query', () => ({
  ...jest.requireActual('../../../../src/services/api/indexer_events_query'),
  readProposal: jest.fn(),
}))

const CORP = 'verana1n64en27u7qckklkk4twkkun5h6v5dsur7g6l4pfmfhydvfru9upq5w4nlu'

const execEvent = (proposalId: string, result: string, msgIndex: string) => ({
  type: 'cosmos.group.v1.EventExec',
  attributes: [
    { key: 'proposal_id', value: `"${proposalId}"` },
    { key: 'result', value: `"${result}"` },
    { key: 'msg_index', value: msgIndex },
  ],
})

const tx = (events: object[]) => ({
  id: 1,
  code: 0,
  height: 73152,
  hash: 'HASH1',
  timestamp: '2026-11-13T00:00:00.000Z',
  data: { tx_response: { height: '73152', txhash: 'HASH1', events } },
})

describe('crawl_tx routing of Verana messages run through authz or a group proposal', () => {
  const broker = new ServiceBroker({ logger: false })
  const service: any = broker.createService(CrawlTxService)

  beforeAll(() => service.getQueueManager().stopAll())
  afterAll(async () => {
    await broker.stop()
  })

  it('routes the Verana messages inside an authz MsgExec', async () => {
    const message = {
      tx_id: 1,
      index: 0,
      type: '/cosmos.authz.v1beta1.MsgExec',
      content: {
        grantee: 'verana1grantee',
        msgs: [{ '@type': VeranaParticipantMessageTypes.RevokeParticipant, id: 1, corporation: CORP, operator: 'op' }],
      },
    }

    const payload = await service.processMessageTypes([message], [tx([])])

    expect(payload.participantMessages).toHaveLength(1)
    expect(payload.participantMessages[0]).toEqual(
      expect.objectContaining({ type: VeranaParticipantMessageTypes.RevokeParticipant, msgIndex: 0 })
    )
    expect(payload.participantMessages[0].content.id).toBe(1)
  })

  it('records proposals that executed successfully and skips failed ones', async () => {
    const message = { tx_id: 1, index: 0, type: '/cosmos.group.v1.MsgSubmitProposal', content: {} }
    const events = [
      execEvent('12', 'PROPOSAL_EXECUTOR_RESULT_SUCCESS', '0'),
      execEvent('13', 'PROPOSAL_EXECUTOR_RESULT_FAILURE', '1'),
    ]

    const payload = await service.processMessageTypes([message], [tx(events)])

    expect(payload.proposalExecs.map((exec: any) => [exec.proposalId, exec.msgIndex])).toEqual([[12, 0]])
  })

  it('adds the messages of an executed proposal to the block payload', async () => {
    jest.mocked(readProposal).mockResolvedValueOnce({
      policyAddress: CORP,
      messages: [
        { '@type': VeranaParticipantMessageTypes.RevokeParticipant, corporation: CORP, operator: CORP, id: '4' },
        { '@type': VeranaCorporationMessageTypes.UpdateCorporation, corporation: CORP, operator: CORP, did: 'did:x' },
      ],
    })
    const payload: any = {
      participantMessages: [],
      corporationList: [],
      proposalExecs: [{ proposalId: 12, msgIndex: 0, tx: tx([]) }],
    }

    await service.addProposalMessages(payload)

    expect(payload.participantMessages.map((msg: any) => msg.content.id)).toEqual(['4'])
    expect(payload.corporationList.map((msg: any) => msg.content.did)).toEqual(['did:x'])
  })

  it('fails the block on a Verana message type it does not know, also inside an authz MsgExec', async () => {
    const message = {
      tx_id: 1,
      index: 0,
      type: '/cosmos.authz.v1beta1.MsgExec',
      content: { msgs: [{ '@type': '/verana.zz.v1.MsgSomethingNew', corporation: CORP }] },
    }

    await expect(service.processMessageTypes([message], [tx([])])).rejects.toThrow(
      'Unknown Verana message types: /verana.zz.v1.MsgSomethingNew'
    )
  })

  it('fails the block when an executed proposal cannot be loaded', async () => {
    jest.mocked(readProposal).mockResolvedValueOnce(undefined)

    await expect(
      service.addProposalMessages({ proposalExecs: [{ proposalId: 99, msgIndex: 0, tx: tx([]) }] })
    ).rejects.toThrow('Proposal 99 executed at block 73152 could not be loaded')
  })
})
