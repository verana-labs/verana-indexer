import { ServiceBroker } from 'moleculer'
import knex from '../../../../src/common/utils/db_connection'
import EcosystemDatabaseService from '../../../../src/services/crawl-ec/ec_database.service'
import EcosystemHistoryService from '../../../../src/services/crawl-ec/ec_history.service'

type SyncContext = Parameters<EcosystemDatabaseService['syncFromLedger']>[0]
type GetContext = Parameters<EcosystemDatabaseService['getEcosystem']>[0]
type HistoryContext = Parameters<EcosystemHistoryService['getTRHistory']>[0]
type Versions = Array<{ id: number; documents: Array<Record<string, unknown>> }>

// A document has the same fields at a past height as at the latest block (IDX-GF-QRY-1).
const ECO = 4842
const HEIGHT = 560000
const created = '2026-11-19T12:43:14.549Z'

const ledgerEcosystem = {
  id: ECO,
  did: 'did:web:eco484-past-height.example',
  corporation_id: 22,
  created,
  modified: created,
  archived: null,
  language: 'en',
  active_version: 1,
  versions: [
    {
      id: 48420,
      version: 1,
      created,
      active_since: created,
      documents: [{ id: 48421, created, language: 'en', url: 'https://example.com/v1.md', digest_sri: 'sha384-v1' }],
    },
  ],
}

const expectedDocument = {
  id: 48421,
  gfv_id: 48420,
  created,
  language: 'en',
  url: 'https://example.com/v1.md',
  digest_sri: 'sha384-v1',
}

describe('Ecosystem documents at a past height', () => {
  const broker = new ServiceBroker({ logger: false })
  const service = broker.createService(EcosystemDatabaseService) as unknown as EcosystemDatabaseService
  const history = broker.createService(EcosystemHistoryService) as unknown as EcosystemHistoryService
  const previousHeightSync = process.env.USE_HEIGHT_SYNC_TR

  beforeAll(async () => {
    process.env.USE_HEIGHT_SYNC_TR = 'true'
    for (const table of ['ecosystem_snapshot_diff', 'ecosystem_snapshot', 'ecosystem_history']) {
      await knex(table).where({ ecosystem_id: ECO }).del()
    }
    await knex('ecosystem').where({ id: ECO }).del()
    const synced = await service.syncFromLedger({
      params: { ledgerResponse: { ecosystem: ledgerEcosystem }, blockHeight: HEIGHT },
      meta: {},
    } as unknown as SyncContext)
    expect(synced).toMatchObject({ success: true })
  })

  afterAll(async () => {
    process.env.USE_HEIGHT_SYNC_TR = previousHeightSync
    await broker.stop()
    await knex.destroy()
  })

  it('have gfv_id, like at the latest block', async () => {
    const res = (await service.getEcosystem({
      params: { ecosystem_id: ECO, gf_data: 'all' },
      meta: { blockHeight: HEIGHT + 5 },
    } as unknown as GetContext)) as { ecosystem: { versions: Versions } }

    expect(res.ecosystem.versions[0].documents).toEqual([expectedDocument])
  })

  it('have gfv_id in the history of the ecosystem', async () => {
    const res = (await history.getTRHistory({
      params: { ecosystem_id: ECO },
      meta: {},
    } as unknown as HistoryContext)) as { activity: Array<{ changes: { versions: Versions } }> }

    expect(res.activity[0].changes.versions[0].documents).toEqual([expectedDocument])
  })
})
