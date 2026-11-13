import { ServiceBroker } from 'moleculer'
import { SERVICE } from '../../../../src/common'
import knex from '../../../../src/common/utils/db_connection'
import CrawlTxService from '../../../../src/services/crawl-tx/crawl_tx.service'
import { loadGovProposalMessages, passedGovProposals } from '../../../../src/services/crawl-tx/gov_params'

describe('governance proposals that change module params', () => {
  const broker = new ServiceBroker({ logger: false })
  const service: any = broker.createService(CrawlTxService)
  const height = 9_500_000 + Math.floor(Math.random() * 100_000)
  const txId = 93_000_000 + Math.floor(Math.random() * 1_000_000)
  const eventId = txId + 1
  const proposalId = 700_000 + Math.floor(Math.random() * 10_000)

  const activeProposal = (id: number, result: string) => ({
    type: 'active_proposal',
    attributes: [
      { key: 'proposal_id', value: String(id), index: true },
      { key: 'proposal_result', value: result, index: true },
    ],
  })

  beforeAll(async () => {
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
    await knex('transaction').insert({
      id: txId,
      height,
      hash: `GOV${txId}`,
      codespace: '',
      code: 0,
      gas_used: 1,
      gas_wanted: 1,
      gas_limit: 1,
      fee: {},
      timestamp: new Date(),
      data: {},
      index: 0,
    })
    await knex('transaction_message').insert({
      id: txId,
      tx_id: txId,
      index: 0,
      type: '/cosmos.gov.v1.MsgSubmitProposal',
      sender: 'verana1proposer',
      content: {
        messages: [
          {
            '@type': '/verana.cs.v1.MsgUpdateParams',
            authority: 'verana1gov',
            params: { credentialSchemaSchemaMaxSize: '9000' },
          },
          { '@type': '/cosmos.bank.v1beta1.MsgSend', from_address: 'a', to_address: 'b' },
        ],
      },
    })
    await knex('event').insert({
      id: eventId,
      tx_id: txId,
      tx_msg_index: 0,
      type: 'submit_proposal',
      block_height: height,
      source: 'TX_EVENT',
    })
    await knex('event_attribute').insert({
      event_id: eventId,
      index: 0,
      block_height: height,
      composite_key: 'submit_proposal.proposal_id',
      key: 'proposal_id',
      value: String(proposalId),
    })
  })

  afterAll(async () => {
    await knex('event_attribute').where({ block_height: height }).delete()
    await knex('event').where({ id: eventId }).delete()
    await knex('transaction_message').where({ tx_id: txId }).delete()
    await knex('transaction').where({ id: txId }).delete()
    await knex('block')
      .whereIn('height', [height, height + 1])
      .delete()
    await broker.stop()
    await knex.destroy()
  })

  it('finds only proposals that passed, at the block that ran them', async () => {
    expect(await passedGovProposals(height, height + 1)).toEqual([{ height, proposalId }])
  })

  it('loads the messages of a proposal from its submit tx', async () => {
    const messages = await loadGovProposalMessages(proposalId)
    expect(messages?.map((message) => message['@type'])).toEqual([
      '/verana.cs.v1.MsgUpdateParams',
      '/cosmos.bank.v1beta1.MsgSend',
    ])
  })

  it('applies the UpdateParams of a passed proposal at its block, with snake_case params', async () => {
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
    call.mockRestore()
  })

  it('fails the block when the params change is not applied', async () => {
    const call = jest.spyOn(broker, 'call').mockResolvedValue({ success: false } as never)

    await expect(service.applyGovernanceParamChanges(height, height)).rejects.toThrow(
      `/verana.cs.v1.MsgUpdateParams from governance proposal ${proposalId} was not applied at block ${height}`
    )
    call.mockRestore()
  })
})
