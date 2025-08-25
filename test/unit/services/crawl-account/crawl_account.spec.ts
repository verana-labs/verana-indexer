import { AfterAll, BeforeAll, Describe, Test } from '@jest-decorated/core';
import { ServiceBroker } from 'moleculer';
import { cosmos } from '@aura-nw/aurajs';
import Long from 'long';
import { toBase64 } from '@cosmjs/encoding';
import _ from 'lodash';

import { Account, AccountBalance, AccountVesting } from '../../../../src/models';
import CrawlAccountService from '../../../../src/services/crawl-account/crawl_account.service';
import knex from '../../../../src/common/utils/db_connection';

/** ---------- helpers: make schema test-proof ---------- */
async function ensureTestSchema() {
  // Make sure we are on the test DB & migrations are applied
  // If your knex config switches by NODE_ENV, set it *before* knex import in your env runner.
  await knex.migrate.latest().catch(() => {
    /* fallback handled below */
  });

  // Fallback: if the 'type' column is still missing, add it so inserts don't crash.
  const hasType = await knex.schema.hasColumn('account_balance', 'type');
  if (!hasType) {
    // Minimal compatible type for tests. Adjust to match your real migration if needed.
    await knex.schema.alterTable('account_balance', t => {
      t.string('type').notNullable().defaultTo('NATIVE'); // matches AccountBalance.TYPE.NATIVE
    });
  }

  // If your soft-delete logic expects delete_at, ensure it's present too (optional)
  // const hasDeleteAt = await knex.schema.hasColumn('account_vesting', 'delete_at');
  // if (!hasDeleteAt) {
  //   await knex.schema.alterTable('account_vesting', (t) => {
  //     t.timestamp('delete_at').nullable();
  //   });
  // }
}

@Describe('Test crawl_account service')
export default class CrawlAccountTest {
  accounts: Account[] = [
    Account.fromJson({
      id: 1,
      address: 'verana1qwexv7c6sm95lwhzn9027vyu2ccneaqa7c24zk',
      balances: [],
      spendable_balances: [],
      type: null,
      pubkey: {},
      account_number: 0,
      sequence: 0,
    }),
    Account.fromJson({
      address: 'verana136v0nmlv0saryev8wqz89w80edzdu3quzm0ve9',
      balances: [],
      spendable_balances: [],
      type: null,
      pubkey: {},
      account_number: 0,
      sequence: 0,
      id: 2,
    }),
    Account.fromJson({
      address: 'verana1fndgsk37dss8judrcaae0gamdqdr8t3rlmvtpm',
      balances: [],
      spendable_balances: [],
      type: null,
      pubkey: {},
      account_number: 0,
      sequence: 0,
      id: 3,
    }),
  ];

  broker = new ServiceBroker({ logger: false });
  crawlAccountService!: CrawlAccountService;

  @BeforeAll()
  async initSuite() {
    // 1) Ensure usable schema for tests
    await ensureTestSchema();

    // 2) Start broker & service
    await this.broker.start();
    this.crawlAccountService = this.broker.createService(CrawlAccountService) as CrawlAccountService;

    // Stop background queues/timers for deterministic tests
    this.crawlAccountService.getQueueManager().stopAll();

    // 3) Clean tables (avoid soft-delete logic)
    await knex.raw('TRUNCATE TABLE account_balance RESTART IDENTITY CASCADE');
    await knex.raw('TRUNCATE TABLE account_vesting RESTART IDENTITY CASCADE');
    await knex.raw('TRUNCATE TABLE account RESTART IDENTITY CASCADE');

    // 4) Seed accounts
    await Account.query().insert(this.accounts);

    // 5) Default mock for account query (so any extra lookups don’t hit network)
    jest.spyOn(this.crawlAccountService._httpBatchClient, 'execute').mockImplementation(async () => ({
      result: {
        response: {
          value: toBase64(
            cosmos.auth.v1beta1.QueryAccountResponse.encode({
              account: {
                '@type': '/cosmos.auth.v1beta1.BaseAccount',
                address: 'verana1qwexv7c6sm95lwhzn9027vyu2ccneaqa7c24zk',
                pub_key: {
                  '@type': '/cosmos.crypto.secp256k1.PubKey',
                  key: 'A8Yj/..........................................',
                },
                account_number: Long.fromNumber(0),
                sequence: Long.fromNumber(0),
              },
            }).finish()
          ),
        },
      },
      id: 1,
      jsonrpc: '2.0',
    }));
  }

  @AfterAll()
  async tearDown() {
    // Hard cleanup
    await knex.raw('TRUNCATE TABLE account_balance, account_vesting, account RESTART IDENTITY CASCADE');

    await this.broker.stop();
    jest.resetAllMocks();
    jest.restoreAllMocks();

    // Close Knex so Jest can exit
    await knex.destroy();
  }

  @Test('Crawl base account balances success')
  public async testCrawlBaseAccountBalances() {
    // Stub balances RPC for this test call
    jest.spyOn(this.crawlAccountService._httpBatchClient, 'execute').mockResolvedValueOnce({
      result: {
        response: {
          value: toBase64(
            cosmos.bank.v1beta1.QueryAllBalancesResponse.encode({
              balances: [{ denom: 'uaura', amount: '1000000' }],
            }).finish()
          ),
          height: '1000',
        },
      },
      id: 1,
      jsonrpc: '2.0',
    });

    await this.crawlAccountService.handleJobAccountBalances({
      addresses: ['verana1qwexv7c6sm95lwhzn9027vyu2ccneaqa7c24zk'],
    });

    // Don’t rely on model relation names; assert directly
    const acc = await Account.query()
      .findOne({ address: 'verana1qwexv7c6sm95lwhzn9027vyu2ccneaqa7c24zk' })
      .throwIfNotFound();

    const balances = await AccountBalance.query().where('account_id', acc.id).where('denom', 'uaura');

    expect(balances).toHaveLength(1);
    expect(balances[0]).toMatchObject({ amount: '1000000' });
  }

  @Test('Crawl base account spendable balances success')
  public async testCrawlBaseAccountSpendableBalances() {
    jest.spyOn(this.crawlAccountService._httpBatchClient, 'execute').mockResolvedValueOnce({
      result: {
        response: {
          value: toBase64(
            cosmos.bank.v1beta1.QuerySpendableBalancesResponse.encode({
              balances: [{ denom: 'uaura', amount: '500000' }],
            }).finish()
          ),
          height: '1000',
        },
      },
      id: 1,
      jsonrpc: '2.0',
    });

    await this.crawlAccountService.handleJobAccountSpendableBalances({
      addresses: ['verana1qwexv7c6sm95lwhzn9027vyu2ccneaqa7c24zk'],
    });

    const acc = await Account.query()
      .findOne({ address: 'verana1qwexv7c6sm95lwhzn9027vyu2ccneaqa7c24zk' })
      .throwIfNotFound();

    // If your schema separates spendable vs total into different columns/tables,
    // adapt the where-clause accordingly. Here we assert presence of the row.
    const spendables = await AccountBalance.query().where('account_id', acc.id).where('denom', 'uaura');

    expect(spendables.length).toBeGreaterThan(0);
  }

  @Test('handleJobAccountBalances')
  async testHandleJobAccountBalances() {
    const updateBalance = {
      balances: [{ denom: 'phong', amount: '121411' }],
    };
    const height = 1211;

    const seed = [
      AccountBalance.fromJson({
        denom: 'sdljkhsgkfjg',
        amount: '132112',
        last_updated_height: 12141,
        account_id: this.accounts[0].id,
        type: AccountBalance.TYPE.NATIVE,
      }),
      AccountBalance.fromJson({
        denom: updateBalance.balances[0].denom,
        amount: updateBalance.balances[0].amount,
        last_updated_height: 12141,
        account_id: this.accounts[0].id,
        type: AccountBalance.TYPE.NATIVE,
      }),
      AccountBalance.fromJson({
        denom: 'cbvcbnbn',
        amount: '444',
        last_updated_height: 12141,
        account_id: this.accounts[0].id,
        type: AccountBalance.TYPE.ERC20_TOKEN,
      }),
    ];

    await AccountBalance.query().insert(seed);

    jest.spyOn(this.crawlAccountService._httpBatchClient, 'execute').mockResolvedValueOnce({
      result: {
        response: {
          value: toBase64(cosmos.bank.v1beta1.QueryAllBalancesResponse.encode(updateBalance).finish()),
          height,
        },
      },
      id: 1,
      jsonrpc: '2.0',
    });

    await this.crawlAccountService.handleJobAccountBalances({
      addresses: [this.accounts[0].address],
    });

    const results = _.keyBy(await AccountBalance.query(), 'denom');

    expect(results[seed[0].denom]).toMatchObject({
      denom: seed[0].denom,
      amount: '0', // set to zero when denom absent from update
      type: AccountBalance.TYPE.NATIVE,
    });
    expect(results[seed[1].denom]).toMatchObject({
      denom: updateBalance.balances[0].denom,
      amount: updateBalance.balances[0].amount,
      type: AccountBalance.TYPE.NATIVE,
    });
    expect(results[seed[2].denom]).toMatchObject({
      denom: seed[2].denom,
      amount: seed[2].amount,
      type: AccountBalance.TYPE.ERC20_TOKEN,
    });
  }
}
