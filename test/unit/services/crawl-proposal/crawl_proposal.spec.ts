import { AfterAll, BeforeAll, Describe, Test } from '@jest-decorated/core';
import { ServiceBroker } from 'moleculer';
import { Block, BlockCheckpoint, Proposal, Transaction, Event, EventAttribute } from '../../../../src/models';
import { BULL_JOB_NAME } from '../../../../src/common';
import CrawlProposalService from '../../../../src/services/crawl-proposal/crawl_proposal.service';
import CrawlTallyProposalService from '../../../../src/services/crawl-proposal/crawl_tally_proposal.service';
import knex from '../../../../src/common/utils/db_connection';

@Describe('Test crawl_proposal service')
export default class CrawlProposalTest {
  private blocks: Block[] = [
    Block.fromJson({
      height: 3967529,
      hash: '4801997745BDD354C8F11CE4A4137237194099E664CD8F83A5FBA9041C43FE9A',
      time: '2023-01-12T01:53:57.216Z',
      proposer_address: 'aura1proposer',
      data: {},
    }),
    Block.fromJson({
      height: 3967530,
      hash: '4801997745BDD354C8F11CE4A4137237194099E664CD8F83A5FBA9041C43FE9F',
      time: '2023-01-12T01:53:57.216Z',
      proposer_address: 'aura1proposer',
      data: {},
    }),
  ];

  private txInsert = Transaction.fromJson({
    height: 3967529,
    hash: '4A8B0DE950F563553A81360D4782F6EC451F6BEF7AC50E2459D1997FA168997D',
    codespace: '',
    code: 0,
    gas_used: '123035',
    gas_wanted: '141106',
    gas_limit: '141106',
    fee: 353,
    timestamp: '2023-01-12T01:53:57.000Z',
    index: 0,
    data: {
      tx: {
        body: {
          messages: [
            {
              '@type': '/cosmos.gov.v1beta1.MsgSubmitProposal',
              initial_deposit: [{ denom: 'utaura', amount: '100000' }],
              proposer: 'aura1qwexv7c6sm95lwhzn9027vyu2ccneaqa7c24zk',
              content: {
                '@type': '/cosmos.gov.v1beta1.TextProposal',
                title: 'Community Pool Spend test 1',
                description: 'Test 1',
              },
            },
          ],
        },
      },
    },
  });

  broker = new ServiceBroker({
    logger: false,
    metrics: false,
    tracing: false,
    cacher: null,
    transporter: null,
  });

  crawlProposalService?: CrawlProposalService;
  crawlTallyProposalService?: CrawlTallyProposalService;

  private async seedCheckpoints() {
    await BlockCheckpoint.query()
      .insert({ job_name: BULL_JOB_NAME.CRAWL_PROPOSAL, height: 3967500 })
      .onConflict('job_name')
      .merge();

    await BlockCheckpoint.query()
      .insert({ job_name: BULL_JOB_NAME.HANDLE_TRANSACTION, height: 3967529 })
      .onConflict('job_name')
      .merge();
  }

  @BeforeAll()
  async initSuite() {
    jest.setTimeout(60_000);
    await this.broker.start();

    this.crawlProposalService = this.broker.createService(CrawlProposalService) as CrawlProposalService;

    this.crawlTallyProposalService = this.broker.createService(CrawlTallyProposalService) as CrawlTallyProposalService;

    try {
      await this.crawlProposalService.getQueueManager().stopAll();
    } catch {}
    try {
      await this.crawlTallyProposalService.getQueueManager().stopAll();
    } catch {}

    await knex.raw(
      'TRUNCATE TABLE block, block_signature, transaction, event, event_attribute, proposal, block_checkpoint RESTART IDENTITY CASCADE'
    );

    // --- Seed blocks + tx ---
    await Block.query().insert(this.blocks);
    const tx = await Transaction.query().insert(this.txInsert);

    // Log transaction data to verify
    console.log('Inserted transaction:', JSON.stringify(this.txInsert, null, 2));

    // --- Seed events & attributes so service can detect proposal ---
    const submitEvent = await Event.query().insert({
      tx_id: tx.id,
      tx_msg_index: 0,
      type: 'submit_proposal',
    });

    await EventAttribute.query().insert([
      {
        event_id: submitEvent.id,
        key: 'proposal_id',
        value: '1',
        block_height: 3967529,
        index: 0,
      },
      {
        event_id: submitEvent.id,
        key: 'proposal_type',
        value: 'Text',
        block_height: 3967529,
        index: 1,
      },
    ]);

    const depositEvent = await Event.query().insert({
      tx_id: tx.id,
      tx_msg_index: 0,
      type: 'proposal_deposit',
    });

    await EventAttribute.query().insert([
      {
        event_id: depositEvent.id,
        key: 'amount',
        value: '100000utaura',
        block_height: 3967529,
        index: 0,
      },
      {
        event_id: depositEvent.id,
        key: 'proposal_id',
        value: '1',
        block_height: 3967529,
        index: 1,
      },
    ]);

    await this.seedCheckpoints();
  }

  @AfterAll()
  async tearDown() {
    await knex.raw(
      'TRUNCATE TABLE block, block_signature, transaction, event, event_attribute, proposal, block_checkpoint RESTART IDENTITY CASCADE'
    );

    try {
      await this.crawlProposalService?.getQueueManager().stopAll();
    } catch {}
    try {
      await this.crawlTallyProposalService?.getQueueManager().stopAll();
    } catch {}

    await this.broker.stop();
    await knex.destroy();
  }

  @Test('Crawl new proposal success')
  public async testCrawlNewProposal() {
    await this.crawlProposalService?.handleCrawlProposals({});
    const p = await Proposal.query().where('proposal_id', 1).first();
    expect(p).toBeDefined();
    expect(p?.proposal_id).toEqual(1);
    expect(typeof p?.type).toBe('string');
    expect((p?.type ?? '').length).toBeGreaterThan(0);
    expect(typeof p?.title).toBe('string');
    expect((p?.title ?? '').length).toBeGreaterThan(0);
    expect(typeof p?.description).toBe('string');
    expect((p?.description ?? '').length).toBeGreaterThan(0);
    expect(p?.vote_counted).toEqual(false);
  }
}
