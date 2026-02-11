import { AfterAll, BeforeAll, Describe, Test } from '@jest-decorated/core';
import { ServiceBroker } from 'moleculer';
import BigNumber from 'bignumber.js';
import { BULL_JOB_NAME, MSG_TYPE } from '../../../../src/common';
import {
  BlockCheckpoint,
  Delegator,
  Transaction,
  TransactionMessage,
  Validator,
} from '../../../../src/models';
import CrawlDelegatorsService from '../../../../src/services/crawl-validator/crawl_delegators.service';
import knex from '../../../../src/common/utils/db_connection';

@Describe('Test crawl_delegators service')
export default class CrawlDelegatorsTest {
  blockCheckpoint = BlockCheckpoint.fromJson({
    job_name: BULL_JOB_NAME.CRAWL_BLOCK,
    height: 3967500,
  });

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
  crawlDelegatorsService!: CrawlDelegatorsService;

  @BeforeAll()
  async initSuite() {
    await Promise.all([
      knex.raw('TRUNCATE TABLE validator RESTART IDENTITY CASCADE'),
      knex.raw('TRUNCATE TABLE block_checkpoint RESTART IDENTITY CASCADE'),
      knex.raw('TRUNCATE TABLE delegator RESTART IDENTITY CASCADE'),
      knex.raw('TRUNCATE TABLE transaction RESTART IDENTITY CASCADE'),
      knex.raw('TRUNCATE TABLE transaction_message RESTART IDENTITY CASCADE'),
    ]);

    await this.broker.start();

    this.crawlDelegatorsService = this.broker.createService(
      CrawlDelegatorsService
    ) as CrawlDelegatorsService;

    try {
      this.crawlDelegatorsService.getQueueManager().stopAll();
    } catch {}

    await Promise.all([
      Validator.query().insert(this.validator),
      BlockCheckpoint.query().insert(this.blockCheckpoint),
    ]);
  }

  @AfterAll()
  async tearDown() {
    try {
      this.crawlDelegatorsService.getQueueManager().stopAll();
    } catch {}
    await Promise.all([
      knex.raw('TRUNCATE TABLE validator, delegator RESTART IDENTITY CASCADE'),
      knex.raw(
        'TRUNCATE TABLE transaction, transaction_message, block_checkpoint RESTART IDENTITY CASCADE'
      ),
    ]);
    await this.broker.stop();
    await knex.destroy(); // ensure Jest exits
  }

  // ---------- Helpers (DB-driven / offline) ----------
  private async insertCrawlDelegatorDependingJob(
    desiredTxId: number,
    desiredHeight: number
  ): Promise<void> {
    const newTx = new Transaction();
    newTx.id = desiredTxId;
    newTx.height = desiredHeight;
    newTx.hash = String(Date.now());
    newTx.codespace = 'test';
    newTx.code = 0;
    newTx.gas_used = '1';
    newTx.gas_wanted = '1';
    newTx.gas_limit = '1';
    newTx.fee = '1';
    newTx.timestamp = new Date();
    newTx.data = {};
    await Transaction.query().insert(newTx);

    await BlockCheckpoint.query()
      .insert(
        BlockCheckpoint.fromJson({
          job_name: BULL_JOB_NAME.CRAWL_VALIDATOR,
          height: desiredHeight,
        })
      )
      .onConflict('job_name')
      .merge();
  }

  private async insertFakeTxMsg(
    msgType: string,
    sender: string,
    amount: string,
    validator: string
  ): Promise<TransactionMessage> {
    const txMsg = TransactionMessage.fromJson({
      tx_id: 1,
      index: 0,
      type: msgType,
      sender,
      content: {
        '@type': msgType,
        amount: { denom: 'uaura', amount },
        delegator_address: sender,
        validator_address: validator,
      },
    });
    return TransactionMessage.query().insert(txMsg);
  }

  // ---------------- FIXED: offline replacement for the flaky RPC test ----------------
  @Test('Crawl validator delegators success')
  public async testCrawlValidatorDelegators_offline() {
    // Reset state for this test
    await knex.raw(`
      TRUNCATE TABLE delegator, transaction, transaction_message, block_checkpoint RESTART IDENTITY CASCADE;
    `);
    await this.insertCrawlDelegatorDependingJob(100, 100);

    // Two delegators delegate to our validator (no network involved)
    await this.insertFakeTxMsg(
      MSG_TYPE.MSG_DELEGATE,
      'delegator_addr_1',
      '2000000',
      this.validator.operator_address
    );
    await this.insertFakeTxMsg(
      MSG_TYPE.MSG_DELEGATE,
      'delegator_addr_2',
      '2000000',
      this.validator.operator_address
    );

    await this.crawlDelegatorsService.getCheckpointUpdateDelegator();
    await this.crawlDelegatorsService.handleJob();

    const validator = await Validator.query().first();
    expect(validator?.delegators_count).toBe(2);

    const d1 = await Delegator.query().findOne({
      validator_id: validator?.id,
      delegator_address: 'delegator_addr_1',
    });
    const d2 = await Delegator.query().findOne({
      validator_id: validator?.id,
      delegator_address: 'delegator_addr_2',
    });
    expect(d1?.amount).toBe(2000000);
    expect(d2?.amount).toBe(2000000);
  }

  // ================================== EXISTING DB-DRIVEN TESTS ==================================
  private mockDelegatorAddress = 'mock_delegator_address';

  @Test('Test transaction message delegate type')
  public async test1(): Promise<void> {
    await knex.raw(`
      TRUNCATE TABLE validator, delegator, transaction, transaction_message, block_checkpoint RESTART IDENTITY CASCADE;
    `);
    const mockDelegateAmount = '100000000';
    await Validator.query().insert(this.validator);
    await this.insertCrawlDelegatorDependingJob(100, 100);
    await this.insertFakeTxMsg(
      MSG_TYPE.MSG_DELEGATE,
      this.mockDelegatorAddress,
      mockDelegateAmount,
      this.validator.operator_address
    );
    await this.crawlDelegatorsService.getCheckpointUpdateDelegator();
    await this.crawlDelegatorsService.handleJob();

    const validator = await Validator.query().findOne(
      'operator_address',
      this.validator.operator_address
    );
    const delegator = await Delegator.query().findOne({
      validator_id: validator?.id,
      delegator_address: this.mockDelegatorAddress,
    });

    expect(delegator?.amount).toBe(100000000);
    expect(validator?.delegators_count).toBe(1);
  }

  @Test('Test delegate and then delegate more')
  public async test2(): Promise<void> {
    await knex.raw(`
      TRUNCATE TABLE validator, delegator, transaction, transaction_message, block_checkpoint RESTART IDENTITY CASCADE;
    `);
    const mockDelegateAmount = '100000000';
    await this.insertCrawlDelegatorDependingJob(100, 100);
    await Validator.query().insert(this.validator);
    await this.insertFakeTxMsg(
      MSG_TYPE.MSG_DELEGATE,
      this.mockDelegatorAddress,
      mockDelegateAmount,
      this.validator.operator_address
    );
    await this.insertFakeTxMsg(
      MSG_TYPE.MSG_DELEGATE,
      this.mockDelegatorAddress,
      mockDelegateAmount,
      this.validator.operator_address
    );
    await this.crawlDelegatorsService.getCheckpointUpdateDelegator();
    await this.crawlDelegatorsService.handleJob();

    const validator = await Validator.query().findOne(
      'operator_address',
      this.validator.operator_address
    );
    const delegator = await Delegator.query().findOne({
      validator_id: validator?.id,
      delegator_address: this.mockDelegatorAddress,
    });

    expect(delegator?.amount).toBe(200000000);
    expect(validator?.delegators_count).toBe(1);
  }

  @Test('Test delegate, then delegate more, and then un delegate a half')
  public async test3(): Promise<void> {
    await knex.raw(`
      TRUNCATE TABLE validator, delegator, transaction, transaction_message, block_checkpoint RESTART IDENTITY CASCADE;
    `);
    const mockDelegateAmount = '100000000';
    await this.insertCrawlDelegatorDependingJob(100, 100);
    await Validator.query().insert(this.validator);
    await this.insertFakeTxMsg(
      MSG_TYPE.MSG_DELEGATE,
      this.mockDelegatorAddress,
      mockDelegateAmount,
      this.validator.operator_address
    );
    await this.insertFakeTxMsg(
      MSG_TYPE.MSG_DELEGATE,
      this.mockDelegatorAddress,
      mockDelegateAmount,
      this.validator.operator_address
    );
    await this.insertFakeTxMsg(
      MSG_TYPE.MSG_UNDELEGATE,
      this.mockDelegatorAddress,
      mockDelegateAmount,
      this.validator.operator_address
    );
    await this.crawlDelegatorsService.getCheckpointUpdateDelegator();
    await this.crawlDelegatorsService.handleJob();

    const validator = await Validator.query().findOne(
      'operator_address',
      this.validator.operator_address
    );
    const delegator = await Delegator.query().findOne({
      validator_id: validator?.id,
      delegator_address: this.mockDelegatorAddress,
    });

    expect(delegator?.amount).toBe(100000000);
    expect(validator?.delegators_count).toBe(1);
  }

  @Test('Test delegate, and then un delegate all')
  public async test4(): Promise<void> {
    await knex.raw(`
      TRUNCATE TABLE validator, delegator, transaction, transaction_message, block_checkpoint RESTART IDENTITY CASCADE;
    `);
    const mockDelegateAmount = '100000000';
    await this.insertCrawlDelegatorDependingJob(100, 100);
    await Validator.query().insert(this.validator);
    await this.insertFakeTxMsg(
      MSG_TYPE.MSG_DELEGATE,
      this.mockDelegatorAddress,
      mockDelegateAmount,
      this.validator.operator_address
    );
    await this.insertFakeTxMsg(
      MSG_TYPE.MSG_UNDELEGATE,
      this.mockDelegatorAddress,
      mockDelegateAmount,
      this.validator.operator_address
    );
    await this.crawlDelegatorsService.getCheckpointUpdateDelegator();
    await this.crawlDelegatorsService.handleJob();

    const validator = await Validator.query().findOne(
      'operator_address',
      this.validator.operator_address
    );
    const delegator = await Delegator.query().findOne({
      validator_id: validator?.id,
      delegator_address: this.mockDelegatorAddress,
    });

    expect(delegator).toBeUndefined();
    expect(validator?.delegators_count).toBe(0);
  }

  @Test('Test delegate, then un delegate and final cancel un delegate')
  public async test5(): Promise<void> {
    await knex.raw(`
      TRUNCATE TABLE validator, delegator, transaction, transaction_message, block_checkpoint RESTART IDENTITY CASCADE;
    `);
    const mockDelegateAmount = '100000000';
    await this.insertCrawlDelegatorDependingJob(100, 100);
    await Validator.query().insert(this.validator);
    await this.insertFakeTxMsg(
      MSG_TYPE.MSG_DELEGATE,
      this.mockDelegatorAddress,
      mockDelegateAmount,
      this.validator.operator_address
    );
    await this.insertFakeTxMsg(
      MSG_TYPE.MSG_UNDELEGATE,
      this.mockDelegatorAddress,
      mockDelegateAmount,
      this.validator.operator_address
    );
    await this.insertFakeTxMsg(
      MSG_TYPE.MSG_CANCEL_UNDELEGATE,
      this.mockDelegatorAddress,
      mockDelegateAmount,
      this.validator.operator_address
    );
    await this.crawlDelegatorsService.getCheckpointUpdateDelegator();
    await this.crawlDelegatorsService.handleJob();

    const validator = await Validator.query().findOne(
      'operator_address',
      this.validator.operator_address
    );
    const delegator = await Delegator.query().findOne({
      validator_id: validator?.id,
      delegator_address: this.mockDelegatorAddress,
    });

    expect(delegator?.amount).toBe(100000000);
    expect(validator?.delegators_count).toBe(1);
  }

  @Test('Test two delegate')
  public async test6(): Promise<void> {
    await knex.raw(`
      TRUNCATE TABLE validator, delegator, transaction, transaction_message, block_checkpoint RESTART IDENTITY CASCADE;
    `);
    const mockDelegateAmount = '100000000';
    await this.insertCrawlDelegatorDependingJob(100, 100);
    await Validator.query().insert(this.validator);
    await this.insertFakeTxMsg(
      MSG_TYPE.MSG_DELEGATE,
      `${this.mockDelegatorAddress}_1`,
      mockDelegateAmount,
      this.validator.operator_address
    );
    await this.insertFakeTxMsg(
      MSG_TYPE.MSG_DELEGATE,
      `${this.mockDelegatorAddress}_2`,
      mockDelegateAmount,
      this.validator.operator_address
    );
    await this.crawlDelegatorsService.getCheckpointUpdateDelegator();
    await this.crawlDelegatorsService.handleJob();

    const validator = await Validator.query().findOne(
      'operator_address',
      this.validator.operator_address
    );
    const delegator1 = await Delegator.query().findOne({
      validator_id: validator?.id,
      delegator_address: `${this.mockDelegatorAddress}_1`,
    });
    const delegator2 = await Delegator.query().findOne({
      validator_id: validator?.id,
      delegator_address: `${this.mockDelegatorAddress}_2`,
    });

    expect(delegator1?.amount).toBe(100000000);
    expect(delegator2?.amount).toBe(100000000);
    expect(validator?.delegators_count).toBe(2);
  }

  @Test('Test re delegate')
  public async test7(): Promise<void> {
    await knex.raw(`
      TRUNCATE TABLE validator, delegator, transaction, transaction_message, block_checkpoint RESTART IDENTITY CASCADE;
    `);
    const mockDelegateAmount = '100000000';
    await this.insertCrawlDelegatorDependingJob(100, 100);
    await Validator.query().insert(this.validator);
    const newValidator = JSON.parse(JSON.stringify(this.validator));
    newValidator.id = 2;
    newValidator.operator_address = `${newValidator.operator_address}_2`;
    newValidator.account_address = `${newValidator.account_address}_2`;
    newValidator.consensus_address = `${newValidator.consensus_address}_2`;
    newValidator.consensus_hex_address = `${newValidator.consensus_hex_address}_2`;
    await Validator.query().insert(newValidator);

    await this.insertFakeTxMsg(
      MSG_TYPE.MSG_DELEGATE,
      this.mockDelegatorAddress,
      mockDelegateAmount,
      this.validator.operator_address
    );

    const txMsg = TransactionMessage.fromJson({
      tx_id: 1,
      index: 0,
      type: MSG_TYPE.MSG_REDELEGATE,
      sender: this.mockDelegatorAddress,
      content: {
        '@type': MSG_TYPE.MSG_REDELEGATE,
        amount: { denom: 'uaura', amount: mockDelegateAmount },
        delegator_address: this.mockDelegatorAddress,
        validator_dst_address: newValidator.operator_address,
        validator_src_address: this.validator.operator_address,
      },
    });
    await TransactionMessage.query().insert(txMsg);

    await this.crawlDelegatorsService.getCheckpointUpdateDelegator();
    await this.crawlDelegatorsService.handleJob();

    const validatorSrc = await Validator.query().findOne(
      'operator_address',
      this.validator.operator_address
    );
    const validatorDst = await Validator.query().findOne(
      'operator_address',
      newValidator.operator_address
    );
    const delegatorSrc = await Delegator.query().findOne({
      validator_id: validatorSrc?.id,
      delegator_address: this.mockDelegatorAddress,
    });
    const delegatorDst = await Delegator.query().findOne({
      validator_id: validatorDst?.id,
      delegator_address: this.mockDelegatorAddress,
    });

    expect(delegatorSrc).toBeUndefined();
    expect(delegatorDst?.amount).toBe(100000000);
    expect(validatorSrc?.delegators_count).toBe(0);
    expect(validatorDst?.delegators_count).toBe(1);
  }

  @Test.skip('Test depending job')
  public async test8(): Promise<void> {
    await knex.raw(`
      TRUNCATE TABLE validator, delegator, transaction, transaction_message, block_checkpoint RESTART IDENTITY CASCADE;
    `);
    const mockDelegateAmount = '100000000';
    const mockCrawlValidatorTxId = 1;
    const mockCrawlValidatorHeight = 1;
    await Validator.query().insert(this.validator);
    await this.insertCrawlDelegatorDependingJob(
      mockCrawlValidatorTxId,
      mockCrawlValidatorHeight
    );
    await this.insertFakeTxMsg(
      MSG_TYPE.MSG_DELEGATE,
      this.mockDelegatorAddress,
      mockDelegateAmount,
      this.validator.operator_address
    );
    await this.crawlDelegatorsService.getCheckpointUpdateDelegator();
    await this.crawlDelegatorsService.handleJob();

    const validator = await Validator.query().findOne(
      'operator_address',
      this.validator.operator_address
    );
    const delegator = await Delegator.query().findOne({
      validator_id: validator?.id,
      delegator_address: this.mockDelegatorAddress,
    });

    // Not processed yet because of dependency height
    expect(delegator).toBeUndefined();

    await this.insertCrawlDelegatorDependingJob(
      mockCrawlValidatorTxId + 1,
      mockCrawlValidatorHeight + 1
    );
    await this.crawlDelegatorsService.handleJob();

    const testAgainDelegator = await Delegator.query().findOne({
      validator_id: validator?.id,
      delegator_address: this.mockDelegatorAddress,
    });
    const testAgainValidator = await Validator.query().findOne(
      'operator_address',
      this.validator.operator_address
    );
    expect(testAgainDelegator?.amount).toBe(mockDelegateAmount);
    expect(testAgainValidator?.delegators_count).toBe(1);
  }
}
