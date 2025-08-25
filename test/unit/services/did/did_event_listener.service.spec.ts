import { AfterAll, BeforeAll, Describe, Test } from '@jest-decorated/core';
import { ServiceBroker } from 'moleculer';
import knex from '../../../../src/common/utils/db_connection';

// use your paths exactly as requested
import DidDatabaseService from '../../../../src/services/crawl-dids/dids.service';
import DidEventListenerService from '../../../../src/services/crawl-dids/did-event-crawler.service';

import axios from 'axios';
jest.mock('axios');
const mockedAxios = axios as unknown as { get: jest.Mock };

// Don’t actually open a websocket in unit tests
jest.mock('../../../../src/common/utils/websocket-client', () => {
  class FakeWS {
    constructor(_opts: any) {}
    connect() {
      /* no-op */
    }
    close() {
      /* no-op */
    }
  }
  return { ReusableWebSocketClient: FakeWS };
});

@Describe('DidEventListenerService')
export default class DidEventListenerServiceSpec {
  broker = new ServiceBroker({ logger: false });
  db!: DidDatabaseService;
  listener!: DidEventListenerService;

  @BeforeAll()
  async setup() {
    // Create services BEFORE starting the broker so lifecycle hooks run
    this.db = this.broker.createService(DidDatabaseService) as DidDatabaseService;
    this.listener = this.broker.createService(DidEventListenerService) as DidEventListenerService;

    await this.broker.start();
    await knex.raw('TRUNCATE TABLE dids RESTART IDENTITY CASCADE');
  }

  @AfterAll()
  async teardown() {
    await knex.raw('TRUNCATE TABLE dids RESTART IDENTITY CASCADE');
    await this.broker.stop();
  }

  private buildTxMessage(events: Array<{ type: string; attrs: Record<string, string> }>, height = '12345') {
    // IMPORTANT: keys/values are PLAIN TEXT here (not base64),
    // because the service tolerates plain strings and your 'did' base64 ("ZGlk") would be skipped by the regex.
    return JSON.stringify({
      result: {
        data: {
          type: 'tendermint/event/Tx',
          value: {
            TxResult: {
              height,
              tx: '0xABCDEF',
              result: {
                events: events.map(ev => ({
                  type: ev.type,
                  attributes: Object.entries(ev.attrs).map(([k, v]) => ({
                    key: k,
                    value: v,
                  })),
                })),
              },
            },
          },
        },
      },
    });
  }

  @Test('add_did → renew_did → remove_did flow')
  async flow() {
    const did = 'did:verana:test-add-1';

    // Call the service's internal handler directly
    const handleMessage = (this.listener as any).handleMessage.bind(this.listener);

    // 1) add_did
    await handleMessage(
      this.buildTxMessage([
        {
          type: 'add_did',
          attrs: {
            did,
            controller: 'verana1controllerxyz',
            exp: '2026-01-01T00:00:00Z',
            deposit: '1234',
          },
        },
      ])
    );

    let row = await knex('dids').where({ did }).first();
    expect(row?.did).toBe(did);
    expect(row?.controller).toBe('verana1controllerxyz');
    expect(row?.deposit).toBe('1234');

    // 2) renew_did (mock Verana API)
    mockedAxios.get.mockResolvedValueOnce({
      data: {
        did_entry: {
          did,
          controller: 'verana1controllerNEW',
          created: '2024-01-10T00:00:00Z',
          modified: '2024-02-10T00:00:00Z',
          exp: '2027-01-01T00:00:00Z',
          deposit: '9999',
        },
      },
    });

    await handleMessage(this.buildTxMessage([{ type: 'renew_did', attrs: { did } }], '12346'));

    row = await knex('dids').where({ did }).first();
    expect(row?.controller).toBe('verana1controllerNEW');
    expect(row?.deposit).toBe('9999');

    // 3) remove_did – remove handler looks for ANY attribute whose VALUE starts with "did:"
    await handleMessage(this.buildTxMessage([{ type: 'remove_did', attrs: { anything: did } }], '12347'));

    const gone = await knex('dids').where({ did }).first();
    expect(gone).toBeUndefined();
  }
}
