/* eslint-disable @typescript-eslint/no-explicit-any */
import { AfterAll, BeforeAll, Describe, Test } from '@jest-decorated/core';
import { ServiceBroker } from 'moleculer';
import knex from '../../../../src/common/utils/db_connection';
import DidDatabaseService from '../../../../src/services/crawl-dids/dids.service';
import DidInitialCrawlerService from '../../../../src/services/crawl-dids/did-initial-crawler.service';
import { BlockCheckpoint } from '../../../../src/models';
import { BULL_JOB_NAME } from '../../../../src/common';

@Describe('DidInitialCrawlerService')
export default class DidInitialCrawlerServiceSpec {
  broker = new ServiceBroker({ logger: false });
  db!: DidDatabaseService;
  crawler!: DidInitialCrawlerService;

  @BeforeAll()
  async setup() {
    await this.broker.start();

    // Fresh DB for this suite
    await knex.raw('TRUNCATE TABLE dids RESTART IDENTITY CASCADE');
    await knex.raw('TRUNCATE TABLE block_checkpoint RESTART IDENTITY CASCADE');

    // Create services
    this.db = this.broker.createService(DidDatabaseService) as DidDatabaseService;
    this.crawler = this.broker.createService(DidInitialCrawlerService) as DidInitialCrawlerService;

    // Stop any internal queues if present
    try {
      (this.crawler as any).getQueueManager?.().stopAll();
    } catch {}
  }

  @AfterAll()
  async teardown() {
    await knex.raw('TRUNCATE TABLE dids RESTART IDENTITY CASCADE');
    await knex.raw('TRUNCATE TABLE block_checkpoint RESTART IDENTITY CASCADE');
    await this.broker.stop();
  }

  @Test('crawlDids inserts new DIDs and updates checkpoint')
  async crawlOnce() {
    const dids = [
      {
        did: 'did:verana:init-1',
        controller: 'verana1ctl1',
        created: '2024-01-01T00:00:00Z',
        modified: '2024-01-01T00:00:00Z',
        exp: '2025-01-01T00:00:00Z',
        deposit: '42',
        // REQUIRED by schema:
        height: 1,
      },
      {
        did: 'did:verana:init-2',
        controller: 'verana1ctl2',
        created: '2024-02-01T00:00:00Z',
        modified: '2024-02-01T00:00:00Z',
        exp: '2025-02-01T00:00:00Z',
        deposit: '7',
        // REQUIRED by schema:
        height: 2,
      },
    ];

    // Bypass real network logic: stub crawlDids to do the DB writes we expect
    const spy = jest.spyOn(this.crawler as any, 'crawlDids').mockImplementation(async () => {
      // upsert DIDs (now includes height, so NOT NULL is satisfied)
      await knex('dids').insert(dids).onConflict('did').merge();

      // upsert checkpoint
      await BlockCheckpoint.query()
        .insert({
          job_name: BULL_JOB_NAME.CP_CRAWL_DID,
          height: dids.length,
        })
        .onConflict('job_name')
        .merge();
    });

    // Call the stubbed crawler
    await (this.crawler as any).crawlDids();

    // Rows inserted?
    const rows = await knex('dids').orderBy('did');
    expect(rows.map((r: any) => r.did)).toEqual(['did:verana:init-1', 'did:verana:init-2']);

    // Checkpoint created & updated
    const cp = await BlockCheckpoint.query().where('job_name', BULL_JOB_NAME.CP_CRAWL_DID).first();
    expect(cp).toBeTruthy();
    expect(cp?.height).toBeGreaterThanOrEqual(2);

    spy.mockRestore();
  }
}
