import { AfterAll, BeforeAll, Describe, Test } from '@jest-decorated/core';
import { ServiceBroker } from 'moleculer';
import { Validator } from '../../../../src/models';
import CrawlSigningInfoService from '../../../../src/services/crawl-validator/crawl_signing_info.service';
import knex from '../../../../src/common/utils/db_connection';

jest.setTimeout(30000);

jest.mock('../../../../src/common', () => {
  const actual = jest.requireActual('../../../../src/common');
  return {
    ...actual,
    getLcdClient: jest.fn().mockResolvedValue({
      provider: {
        cosmos: {
          base: {
            tendermint: {
              v1beta1: {
                getNodeInfo: async () => ({
                  application_version: { cosmos_sdk_version: 'v0.45.7' },
                }),
              },
            },
          },
          slashing: {
            v1beta1: {
              params: async () => ({
                params: { signed_blocks_window: '100' },
              }),
              signingInfos: async () => ({
                info: [],
                pagination: { next_key: null },
              }),
            },
          },
        },
      },
    }),
  };
});

@Describe('Test crawl_signing_info service')
export default class CrawlSigningInfoTest {
  validator: Validator = Validator.fromJson({
    commission: JSON.parse('{}'),
    operator_address: 'auravaloper1phaxpevm5wecex2jyaqty2a4v02qj7qmhyhvcg',
    consensus_address: 'auravalcons1rvq6km74pua3pt9g7u5svm4r6mrw8z08walfep',
    consensus_hex_address: '1B01AB6FD50F3B10ACA8F729066EA3D6C6E389E7',
    consensus_pubkey: {
      type: '/cosmos.crypto.ed25519.PubKey',
      key: 'AtzgNPEcMZlcSTaWjGO5ymvQ9/Sjp8N68/kJrx0ASI0=',
    },
    jailed: false,
    status: 'BOND_STATUS_BONDED',
    tokens: '100000000',
    delegator_shares: '100000000.000000000000000000',
    description: {
      moniker: 'mynode',
      identity: '',
      website: '',
      security_contact: '',
      details: '',
    },
    unbonding_height: 0,
    unbonding_time: '1970-01-01T00:00:00Z',
    min_self_delegation: '1',
    uptime: 100,
    account_address: 'aura1d3n0v5f23sqzkhlcnewhksaj8l3x7jey8hq0sc',
    percent_voting_power: 16.498804,
    start_height: 0,
    index_offset: 0,
    jailed_until: '1970-01-01T00:00:00Z',
    tombstoned: false,
    missed_blocks_counter: 0,
    self_delegation_balance: '102469134',
    delegators_count: 0,
    delegators_last_height: 0,
  });

  broker = new ServiceBroker({ logger: false });
  crawlSigningInfoService!: CrawlSigningInfoService;

  @BeforeAll()
  async initSuite() {
    await this.broker.start();
    this.crawlSigningInfoService = this.broker.createService(CrawlSigningInfoService) as CrawlSigningInfoService;

    // Stop background jobs for deterministic tests
    this.crawlSigningInfoService.getQueueManager().stopAll();

    // Clean table WITHOUT soft-delete (avoids missing delete_at errors)
    try {
      await knex.raw('TRUNCATE TABLE validator RESTART IDENTITY CASCADE');
    } catch (err: any) {
      if (err?.nativeError?.code !== '42P01') {
        throw err;
      }
    }

    // Seed
    await Validator.query().insert(this.validator);
  }

  @AfterAll()
  async tearDown() {
    // Clean hard + shutdown
    try {
      await knex.raw('TRUNCATE TABLE validator RESTART IDENTITY CASCADE');
    } catch (err: any) {
      if (err?.nativeError?.code !== '42P01') {
        throw err;
      }
    }
    await this.broker.stop();
    await knex.destroy();
  }

  @Test('Crawl validator signing info success')
  public async testCrawlSigningInfo() {
    await this.crawlSigningInfoService.handleJob({});

    const updated = await Validator.query().first();
    expect(updated?.start_height).toEqual(0);
    expect(updated?.tombstoned).toEqual(false);
  }
}
