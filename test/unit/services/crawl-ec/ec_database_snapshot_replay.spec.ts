import { ServiceBroker } from 'moleculer'
import knex from '../../../../src/common/utils/db_connection'
import EcosystemDatabaseService from '../../../../src/services/crawl-ec/ec_database.service'

const ECO = 9
const HEIGHT = 420
const created = '2026-10-06T19:12:56.270Z'

const ledgerEcosystem = {
  id: ECO,
  did: 'did:example:eco-snapshot-replay',
  corporation_id: 6,
  created,
  modified: created,
  archived: null,
  language: 'en',
  active_version: 1,
  versions: [
    {
      id: 41,
      version: 1,
      created,
      active_since: created,
      documents: [{ id: 42, created, language: 'en', url: 'https://example.com/gf.json', digest_sri: 'sha384-v1' }],
    },
  ],
}

describe('EcosystemDatabaseService.syncFromLedger snapshot replay', () => {
  const broker = new ServiceBroker({ logger: false })
  const service: any = broker.createService(EcosystemDatabaseService)
  const previousHeightSync = process.env.USE_HEIGHT_SYNC_TR

  const sync = () =>
    service.syncFromLedger({
      params: { ledgerResponse: { ecosystem: ledgerEcosystem }, blockHeight: HEIGHT },
      meta: {},
    })
  const rowsAtHeight = async (table: string) => knex(table).where({ ecosystem_id: ECO, height: HEIGHT })

  beforeAll(async () => {
    process.env.USE_HEIGHT_SYNC_TR = 'true'
    for (const table of ['ecosystem_snapshot_diff', 'ecosystem_snapshot', 'ecosystem_history']) {
      await knex(table)
        .whereIn('ecosystem_id', [ECO, ECO + 1])
        .del()
    }
    await knex('ecosystem')
      .whereIn('id', [ECO, ECO + 1])
      .del()
  })

  afterAll(async () => {
    process.env.USE_HEIGHT_SYNC_TR = previousHeightSync
    await knex.raw('ALTER TABLE ecosystem_snapshot_diff DROP CONSTRAINT IF EXISTS fail_snapshot_diff')
    await knex.raw('ALTER TABLE ecosystem_history DROP CONSTRAINT IF EXISTS fail_ecosystem_history')
    await broker.stop()
    await knex.destroy()
  })

  it('writes no snapshot when the diff insert fails, and both rows on the retry', async () => {
    await knex.raw('ALTER TABLE ecosystem_snapshot_diff ADD CONSTRAINT fail_snapshot_diff CHECK (false) NOT VALID')
    const failed = await sync()
    expect(failed?.success).not.toBe(true)
    expect(await rowsAtHeight('ecosystem_snapshot')).toHaveLength(0)
    expect(await rowsAtHeight('ecosystem_snapshot_diff')).toHaveLength(0)

    await knex.raw('ALTER TABLE ecosystem_snapshot_diff DROP CONSTRAINT fail_snapshot_diff')
    const retried = await sync()
    expect(retried?.success).toBe(true)
    const [snapshot] = await rowsAtHeight('ecosystem_snapshot')
    const diffs = await rowsAtHeight('ecosystem_snapshot_diff')
    expect(diffs).toHaveLength(1)
    expect(Number(diffs[0].next_snapshot_id)).toBe(Number(snapshot.id))

    await sync()
    expect(await rowsAtHeight('ecosystem_snapshot')).toHaveLength(1)
    expect(await rowsAtHeight('ecosystem_snapshot_diff')).toHaveLength(1)
  })

  it('writes no ecosystem row when its history insert fails, and the Create history on the retry', async () => {
    const ecosystemId = ECO + 1
    const syncOther = () =>
      service.syncFromLedger({
        params: {
          ledgerResponse: {
            ecosystem: {
              ...ledgerEcosystem,
              id: ecosystemId,
              did: 'did:example:eco-history-replay',
              versions: [
                {
                  ...ledgerEcosystem.versions[0],
                  id: 43,
                  documents: [{ ...ledgerEcosystem.versions[0].documents[0], id: 44 }],
                },
              ],
            },
          },
          blockHeight: HEIGHT,
        },
        meta: {},
      })

    await knex.raw('ALTER TABLE ecosystem_history ADD CONSTRAINT fail_ecosystem_history CHECK (false) NOT VALID')
    const failed = await syncOther()
    expect(failed?.success).not.toBe(true)
    expect(await knex('ecosystem').where({ id: ecosystemId }).first()).toBeUndefined()

    await knex.raw('ALTER TABLE ecosystem_history DROP CONSTRAINT fail_ecosystem_history')
    const retried = await syncOther()
    expect(retried?.success).toBe(true)
    const history = await knex('ecosystem_history').where({ ecosystem_id: ecosystemId, height: HEIGHT })
    expect(history.map((row) => row.event_type)).toEqual(['Create'])
  })

  it('creates another ecosystem in a block that already wrote one', async () => {
    const ecosystemId = ECO + 2
    const result = await service.syncFromLedger({
      params: {
        ledgerResponse: {
          ecosystem: {
            ...ledgerEcosystem,
            id: ecosystemId,
            did: 'did:example:eco-same-block',
            versions: [
              {
                ...ledgerEcosystem.versions[0],
                id: 45,
                documents: [{ ...ledgerEcosystem.versions[0].documents[0], id: 46 }],
              },
            ],
          },
        },
        blockHeight: HEIGHT,
      },
      meta: {},
    })

    expect(result?.success).toBe(true)
    expect(Number((await knex('ecosystem').where({ id: ecosystemId }).first())?.height)).toBe(HEIGHT)
  })
})
