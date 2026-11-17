import { ServiceBroker } from 'moleculer'
import { SERVICE } from '../../../../src/common'
import knex from '../../../../src/common/utils/db_connection'
import CrawlTxService from '../../../../src/services/crawl-tx/crawl_tx.service'
import { loadGovProposalMessages, passedGovProposals } from '../../../../src/services/crawl-tx/gov_params'

describe('governance proposals that change module params', () => {
  const broker = new ServiceBroker({ logger: false })
  const service: any = broker.createService(CrawlTxService)
  const height = 9_500_000 + Math.floor(Math.random() * 100_000)
  const proposalId = 700_000 + Math.floor(Math.random() * 10_000)
  const previousLcd = process.env.LCD_ENDPOINT
  const proposal = {
    id: String(proposalId),
    status: 'PROPOSAL_STATUS_PASSED',
    messages: [
      {
        '@type': '/verana.cs.v1.MsgUpdateParams',
        authority: 'verana1gov',
        params: { credential_schema_schema_max_size: '9000' },
      },
      { '@type': '/cosmos.bank.v1beta1.MsgSend', from_address: 'a', to_address: 'b' },
    ],
  }
  const lcdAnswers = (...answers: Array<Response | Error>) => {
    const fetchMock = jest.spyOn(global, 'fetch')
    for (const answer of answers) {
      if (answer instanceof Error) fetchMock.mockRejectedValueOnce(answer)
      else fetchMock.mockResolvedValueOnce(answer)
    }
    return fetchMock
  }
  const found = () => new Response(JSON.stringify({ proposal }), { status: 200 })
  const notFound = () => new Response('{}', { status: 404 })

  const activeProposal = (id: number, result: string) => ({
    type: 'active_proposal',
    attributes: [
      { key: 'proposal_id', value: String(id), index: true },
      { key: 'proposal_result', value: result, index: true },
    ],
  })

  beforeAll(async () => {
    process.env.LCD_ENDPOINT = 'http://lcd.test'
    service.getQueueManager().stopAll()
    await knex('block').insert([
      {
        height,
        hash: `gov-${height}`,
        time: new Date(),
        proposer_address: 'v',
        data: {
          block_result: {
            finalize_block_events: [
              activeProposal(proposalId, 'proposal_passed'),
              activeProposal(proposalId + 1, 'proposal_rejected'),
            ],
          },
        },
      },
      { height: height + 1, hash: `gov-${height + 1}`, time: new Date(), proposer_address: 'v', data: {} },
    ])
  })

  afterEach(() => jest.restoreAllMocks())

  afterAll(async () => {
    if (previousLcd === undefined) Reflect.deleteProperty(process.env, 'LCD_ENDPOINT')
    else process.env.LCD_ENDPOINT = previousLcd
    await knex('block')
      .whereIn('height', [height, height + 1])
      .delete()
    await broker.stop()
    await knex.destroy()
  })

  it('finds only proposals that passed, at the block that ran them', async () => {
    expect(await passedGovProposals(height, height + 1)).toEqual([{ height, proposalId }])
  })

  it('loads the messages of a proposal from the chain at the block that ran it', async () => {
    const fetchMock = lcdAnswers(found())

    const messages = await loadGovProposalMessages(proposalId, height)

    expect(messages?.map((message) => message['@type'])).toEqual([
      '/verana.cs.v1.MsgUpdateParams',
      '/cosmos.bank.v1beta1.MsgSend',
    ])
    expect(fetchMock).toHaveBeenCalledWith(
      `http://lcd.test/cosmos/gov/v1/proposals/${proposalId}`,
      expect.objectContaining({ headers: { 'x-cosmos-block-height': String(height) } })
    )
  })

  it('falls back to the latest state when the node cannot answer at that block', async () => {
    const fetchMock = lcdAnswers(new Error('height pruned'), found())

    expect(await loadGovProposalMessages(proposalId, height)).toHaveLength(2)
    expect(fetchMock).toHaveBeenLastCalledWith(
      `http://lcd.test/cosmos/gov/v1/proposals/${proposalId}`,
      expect.objectContaining({ headers: {} })
    )
  })

  it('applies the UpdateParams of a passed proposal at its block', async () => {
    lcdAnswers(found())
    const call = jest.spyOn(broker, 'call').mockResolvedValue({ success: true } as never)

    await service.applyGovernanceParamChanges(height, height + 1)

    expect(call).toHaveBeenCalledTimes(1)
    expect(call).toHaveBeenCalledWith(`${SERVICE.V1.GenesisParamsService.path}.handleUpdateParams`, {
      message: {
        '@type': '/verana.cs.v1.MsgUpdateParams',
        type: '/verana.cs.v1.MsgUpdateParams',
        authority: 'verana1gov',
        params: { credential_schema_schema_max_size: '9000' },
      },
      height,
    })
  })

  it('fails the block when the proposal cannot be loaded', async () => {
    lcdAnswers(notFound(), notFound())

    await expect(service.applyGovernanceParamChanges(height, height)).rejects.toThrow(
      `Governance proposal ${proposalId} executed at block ${height} could not be loaded`
    )
  })

  it('fails the block when the params change is not applied', async () => {
    lcdAnswers(found())
    jest.spyOn(broker, 'call').mockResolvedValue({ success: false } as never)

    await expect(service.applyGovernanceParamChanges(height, height)).rejects.toThrow(
      `/verana.cs.v1.MsgUpdateParams from governance proposal ${proposalId} was not applied at block ${height}`
    )
  })
})
