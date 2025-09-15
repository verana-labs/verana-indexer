import { AfterAll, BeforeAll, Describe, Test } from '@jest-decorated/core';
import { ServiceBroker } from 'moleculer';
import knex from '../../../../src/common/utils/db_connection';
import DidDatabaseService from '../../../../src/services/crawl-dids/dids.service';

@Describe('DidDatabaseService')
export default class DidDatabaseServiceSpec {
  broker = new ServiceBroker({ logger: false });
  db!: DidDatabaseService;

  @BeforeAll()
  async setup() {
    await this.broker.start();
    this.db = this.broker.createService(DidDatabaseService) as DidDatabaseService;
    await knex.raw('TRUNCATE TABLE dids RESTART IDENTITY CASCADE');
  }

  @AfterAll()
  async teardown() {
    await knex.raw('TRUNCATE TABLE dids RESTART IDENTITY CASCADE');
    await this.broker.stop();
  }

  @Test('upsert → get → upsert(merge) → delete')
  async crud() {
    const did = 'did:verana:test-001';
    const first = {
      did,
      controller: 'verana1controller000',
      created: new Date('2024-01-01T00:00:00Z'),
      modified: new Date('2024-01-01T00:00:00Z'),
      exp: new Date('2025-01-01T00:00:00Z'),
      deposit: '1000',
      height: 10,
    };

    // upsert (insert)
    await this.broker.call('v1.DidDatabaseService.upsert', first);
    let row = await knex('dids').where({ did }).first();
    expect(row.controller).toBe('verana1controller000');
    expect(row.deposit).toBe('1000');

    // get
    const got = (await this.broker.call('v1.DidDatabaseService.get', { did })) as any;
    expect(got?.did).toBe(did);

    // upsert (merge / update)
    await this.broker.call('v1.DidDatabaseService.upsert', {
      ...first,
      controller: 'verana1controller111',
      deposit: '2500',
    });
    row = await knex('dids').where({ did }).first();
    expect(row.controller).toBe('verana1controller111');
    expect(row.deposit).toBe('2500');

    // delete
    const delOk = (await this.broker.call('v1.DidDatabaseService.delete', { did })) as any;
    expect(delOk?.success).toBe(true);

    // delete non-existing
    const delMiss = (await this.broker.call('v1.DidDatabaseService.delete', { did })) as any;
    expect(delMiss?.success).toBe(false);
  }
}
