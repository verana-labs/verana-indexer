import { AfterAll, BeforeAll, Describe, Test } from '@jest-decorated/core';
import { ServiceBroker } from 'moleculer';
import ChainRegistry from '../../../../src/common/utils/chain.registry';
import CrawlTxService from '../../../../src/services/crawl-tx/crawl_tx.service';
import CrawlBlockService from '../../../../src/services/crawl-block/crawl_block.service';
import { Block, Event } from '../../../../src/models';
import knex from '../../../../src/common/utils/db_connection';
import { getProviderRegistry } from '../../../../src/common/utils/provider.registry';

@Describe('Test crawl block service (Verana)')
export default class CrawlBlockTest {
  // Verana-like block fixture (chain_id: verana-testnet-1, uvera amounts, verana1… addrs)
  blocks = [
    {
      block_id: {
        hash: 'C084A4FDBE3473CE55CF4EFFF2F8153B07B6CE740717ABD26B437898EC95CF1E',
        parts: { total: 1, hash: '1D23B62D986F5F1EB48C5E9CE1A61F72A0DB925EA31FFA27B0AFAE6D1A9AAC03' },
      },
      block: {
        header: {
          version: { block: '11' },
          chain_id: 'verana-testnet-1',
          height: '2001002',
          time: '2022-11-25T05:01:21.235286829Z',
          last_block_id: {
            hash: 'D633D37EA74467C060F4E06EE5C569C0C705EEECF8A6E94621E8A7532B01FFDA',
            parts: { total: 1, hash: '9B92B9789FDA3A3B51CF41A7009D10AE53E39F2199027ACFDF79913599669138' },
          },
          last_commit_hash: '439089FFB6392D07E83F0FA8C1D43FA0D98485E221F5F509D00315E8795C785B',
          data_hash: 'E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855',
          validators_hash: '26F58F551B888E91B9A6FC6863D0A879B24F548F17BF974BA9E91DA7FC8F4C56',
          next_validators_hash: 'B86EC94CA2C0D0423E5C6AD7FB4C2B734479E3404536B158EB0D5E6F3F523BEE',
          consensus_hash: '048091BC7DDC283F77BFBF91D73C44DA58C3DF8A9CBC867405D8B7F3DAADA22F',
          app_hash: '52C2F4C896828A00B221AFAAF9DA128B7B3FA38426F226F0FCA49EB9258FE0D6',
          last_results_hash: 'E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855',
          evidence_hash: 'E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855',
          proposer_address: 'BCF7CE808B45BFD44DECA498A1029DDE283654A3',
        },
        data: { txs: [] },
        evidence: { evidence: [] },
        last_commit: {
          height: '2001001',
          round: 0,
          block_id: {
            hash: 'D633D37EA74467C060F4E06EE5C569C0C705EEECF8A6E94621E8A7532B01FFDA',
            parts: { total: 1, hash: '9B92B9789FDA3A3B51CF41A7009D10AE53E39F2199027ACFDF79913599669138' },
          },
          signatures: [
            {
              block_id_flag: 2,
              validator_address: 'BCF7CE808B45BFD44DECA498A1029DDE283654A3',
              timestamp: '2022-11-25T05:01:21.291773259Z',
              signature: 'QyHw+4ui3q2pHv8Ff+rMJGatAcU/vjjRbrNsjkQ3tp36EIrrQ6RvW9BV7VxnzpOJM4ZRL+8R9cqSLqo9bYGrBw==',
            },
          ],
        },
      },
      block_result: {
        height: '2001002',
        txs_results: null,
        begin_block_events: [
          { type: 'coin_received', attributes: [
            { key: 'cmVjZWl2ZXI=', value: 'dmVyYW5hMWFkZHIx', index: true },
            { key: 'YW1vdW50',   value: 'MTAwdXZlcmE=',       index: true },
          ]},
          { type: 'coinbase', attributes: [
            { key: 'bWludGVy', value: 'dmVyYW5hMWFkZHIx', index: true },
            { key: 'YW1vdW50', value: 'MTAwdXZlcmE=',     index: true },
          ]},
          { type: 'coin_spent', attributes: [
            { key: 'c3BlbmRlcg==', value: 'dmVyYW5hMWFkZHIx', index: true },
            { key: 'YW1vdW50',     value: 'MTAwdXZlcmE=',     index: true },
          ]},
          { type: 'coin_received', attributes: [
            { key: 'cmVjZWl2ZXI=', value: 'dmVyYW5hMWFkZHIy', index: true },
            { key: 'YW1vdW50',     value: 'MTAwdXZlcmE=',     index: true },
          ]},
          { type: 'transfer', attributes: [
            { key: 'cmVjaXBpZW50', value: 'dmVyYW5hMWFkZHIy', index: true },
            { key: 'c2VuZGVy',     value: 'dmVyYW5hMWFkZHIx', index: true },
            { key: 'YW1vdW50',     value: 'MTAwdXZlcmE=',     index: true },
          ]},
          { type: 'message', attributes: [
            { key: 'c2VuZGVy', value: 'dmVyYW5hMWFkZHIx', index: true },
          ]},
          { type: 'mint', attributes: [
            { key: 'Ym9uZGVkX3JhdGlv',      value: 'MC4wMzEyMTU5OTU0NTI4ODc2NjA=', index: true },
            { key: 'aW5mbGF0aW9u',          value: 'MC4xMTM4NzUyMTYwMjA5OTc0MTY=', index: true },
            { key: 'YW5udWFsX3Byb3Zpc2lvbnM=', value: 'NTMyNjk4MTc5MzAzNDMuNDY0ODY2NDAyNzQ5ODIxOTI4', index: true },
            { key: 'YW1vdW50',              value: 'MTAwdXZlcmE=', index: true },
          ]},
          { type: 'coin_spent', attributes: [
            { key: 'c3BlbmRlcg==', value: 'dmVyYW5hMWFkZHIy', index: true },
            { key: 'YW1vdW50',     value: 'MTAwdXZlcmE=',     index: true },
          ]},
          { type: 'coin_received', attributes: [
            { key: 'cmVjZWl2ZXI=', value: 'dmVyYW5hMWFkZHIz', index: true },
            { key: 'YW1vdW50',     value: 'MTAwdXZlcmE=',     index: true },
          ]},
          { type: 'transfer', attributes: [
            { key: 'cmVjaXBpZW50', value: 'dmVyYW5hMWFkZHIz', index: true },
            { key: 'c2VuZGVy',     value: 'dmVyYW5hMWFkZHIy', index: true },
            { key: 'YW1vdW50',     value: 'MTAwdXZlcmE=',     index: true },
          ]},
          { type: 'message', attributes: [
            { key: 'c2VuZGVy', value: 'dmVyYW5hMWFkZHIy', index: true },
          ]},
          { type: 'proposer_reward', attributes: [
            { key: 'YW1vdW50',   value: 'NDE3NTc5LjEwOTE5OTQyMjQ0ODkzMTcwMnV2ZXJh', index: true },
            { key: 'dmFsaWRhdG9y', value: 'dmVyYW5hMWVwc2lsb24=', index: true },
          ]},
          { type: 'commission', attributes: [
            { key: 'YW1vdW50',   value: 'MjA4NzgubTU1dXZlcmE=', index: true },
            { key: 'dmFsaWRhdG9y', value: 'dmVyYW5hMWVwc2lsb24=', index: true },
          ]},
          { type: 'commission', attributes: [
            { key: 'YW1vdW50',   value: 'MjAzMzYuMzA0NTIwMzgwMDA4OTQ3ODQ3dXZlcmE=', index: true },
            { key: 'dmFsaWRhdG9y', value: 'dmVyYW5hMWRlbHRh', index: true },
          ]},
          { type: 'rewards', attributes: [
            { key: 'YW1vdW50',   value: 'MjkwNTE4LjYzNjAwNTQyODY5OTI1NDk1OXV2ZXJh', index: true },
            { key: 'dmFsaWRhdG9y', value: 'dmVyYW5hMWRlbHRh', index: true },
          ]},
          { type: 'message', attributes: [
            { key: 'c2VuZGVy', value: 'dmVyYW5hMWFkZHIx', index: true },
          ]},
        ],
        end_block_events: [
          { type: 'coin_spent', attributes: [
            { key: 'c3BlbmRlcg==', value: 'dmVyYW5hMWFkZHI0', index: true },
            { key: 'YW1vdW50',     value: 'MTAwdXZlcmE=',     index: true },
          ]},
          { type: 'coin_received', attributes: [
            { key: 'cmVjZWl2ZXI=', value: 'dmVyYW5hMWFkZHI1', index: true },
            { key: 'YW1vdW50',     value: 'MTAwdXZlcmE=',     index: true },
          ]},
          { type: 'transfer', attributes: [
            { key: 'cmVjaXBpZW50', value: 'dmVyYW5hMWFkZHI1', index: true },
            { key: 'c2VuZGVy',     value: 'dmVyYW5hMWFkZHI0', index: true },
            { key: 'YW1vdW50',     value: 'MTAwdXZlcmE=',     index: true },
          ]},
          { type: 'message', attributes: [
            { key: 'c2VuZGVy', value: 'dmVyYW5hMWFkZHI0', index: true },
          ]},
        ],
        validator_updates: [],
        consensus_param_updates: {
          block: { max_bytes: '22020096', max_gas: '-1' },
          evidence: { max_age_num_blocks: '100000', max_age_duration: '172800000000000', max_bytes: '1048576' },
          validator: { pub_key_types: ['ed25519'] },
        },
      },
    },
  ];

  // Broker with minimal features to avoid open handles
  broker = new ServiceBroker({
    logger: false,
    metrics: false,
    tracing: false,
    cacher: null,
    transporter: null,
  });

  crawlBlockService!: CrawlBlockService;
  crawlTxService!: CrawlTxService;

  @BeforeAll()
  async initSuite() {
    jest.setTimeout(60_000);
    await this.broker.start();

    // Stub the action CrawlBlockService calls so tests don't depend on the real tx service
    this.broker.createService({
      name: 'v1.CrawlTransactionService',
      actions: {
        async TriggerHandleTxJob() { return true; },
      },
    });

    // Real services
    this.crawlBlockService = this.broker.createService(CrawlBlockService) as CrawlBlockService;
    this.crawlTxService = this.broker.createService(CrawlTxService) as CrawlTxService;

    // Registry wiring
    const providerRegistry = await getProviderRegistry();
    const chainRegistry = new ChainRegistry(this.crawlTxService.logger, providerRegistry);
    chainRegistry.setCosmosSdkVersionByString('v0.45.7');
    this.crawlBlockService.setRegistry(chainRegistry);

    // Stop internal queues (guard)
    const stopQueues = async (svc: any) => {
      try { if (svc?.getQueueManager?.()) await svc.getQueueManager().stopAll(); } catch {}
    };
    await Promise.all([stopQueues(this.crawlBlockService), stopQueues(this.crawlTxService)]);

    // Clean DB unless we explicitly want to keep data for inspection
    if (process.env.KEEP_TEST_DATA !== '1') {
      await knex.raw(
        'TRUNCATE TABLE block, block_signature, transaction, event, event_attribute RESTART IDENTITY CASCADE'
      );
    }
  }

  @Test('Parse block and insert to DB (Verana)')
  public async testHandleBlocks() {
    await this.crawlBlockService.handleListBlock(this.blocks as any);

    // Verify block row in your schema (height, hash, time, proposer_address, data, tx_count)
    const rows = await knex('block')
      .select('*')
      .where('height', 2001002)
      .orderBy('height', 'asc');

    expect(rows.length).toBe(1);
    expect(rows[0].height).toBe(2001002);
    expect(typeof rows[0].hash).toBe('string');
    expect(typeof rows[0].proposer_address).toBe('string');
    expect(rows[0].tx_count === 0 || rows[0].tx_count == null).toBeTruthy();
    expect(rows[0].data).toBeTruthy();
    expect(new Date(rows[0].time).toISOString().startsWith('2022-11-25T05:01:21')).toBe(true);

    // Events
    const beginBlockEvents = await Event.query()
      .where('block_height', 2001002)
      .andWhere('source', Event.SOURCE.BEGIN_BLOCK_EVENT);

    const endBlockEvents = await Event.query()
      .where('block_height', 2001002)
      .andWhere('source', Event.SOURCE.END_BLOCK_EVENT);

    expect(beginBlockEvents.length).toBeGreaterThanOrEqual(15);
    expect(endBlockEvents.length).toBeGreaterThanOrEqual(4);

    const types = new Set(beginBlockEvents.map(e => e.type));
    expect(types.has('transfer')).toBe(true);
    expect(types.has('message')).toBe(true);
    expect(types.has('coin_received')).toBe(true);
  }

  @AfterAll()
  async tearDown() {
    const stopQueues = async (svc: any) => {
      try { if (svc?.getQueueManager?.()) await svc.getQueueManager().stopAll(); } catch {}
    };

    // Clean DB unless we want to keep the data around after the test
    if (process.env.KEEP_TEST_DATA !== '1') {
      await knex.raw(
        'TRUNCATE TABLE block, block_signature, transaction, event, event_attribute RESTART IDENTITY CASCADE'
      );
    }

    await Promise.all([stopQueues(this.crawlBlockService), stopQueues(this.crawlTxService)]);
    await this.broker.stop();
    jest.restoreAllMocks();
    await knex.destroy();
  }
}
