import { ServiceBroker } from 'moleculer'
import knex from '../../../../src/common/utils/db_connection'
import EcosystemDatabaseService from '../../../../src/services/crawl-ec/ec_database.service'

type SyncContext = Parameters<EcosystemDatabaseService['syncFromLedger']>[0]

// The chain puts no length limit on a DID.
const ECO = 484
const created = '2026-11-19T17:20:05.412Z'
const LONG_DID = `did:example:${'a'.repeat(288)}`

const ledgerEcosystem = {
  id: ECO,
  did: LONG_DID,
  corporation_id: 2,
  created,
  modified: created,
  archived: null,
  language: 'en',
  active_version: 1,
  versions: [
    {
      id: 4841,
      version: 1,
      created,
      active_since: created,
      documents: [{ id: 4841, created, language: 'en', url: 'https://example.com/v1.json', digest_sri: 'sha384-v1' }],
    },
  ],
}

describe('EcosystemDatabaseService.syncFromLedger with a DID longer than 255 characters', () => {
  const broker = new ServiceBroker({ logger: false })
  const service = broker.createService(EcosystemDatabaseService) as unknown as EcosystemDatabaseService
  const previousHeightSync = process.env.USE_HEIGHT_SYNC_TR

  beforeAll(async () => {
    process.env.USE_HEIGHT_SYNC_TR = 'true'
    for (const table of ['ecosystem_snapshot_diff', 'ecosystem_snapshot', 'ecosystem_history']) {
      await knex(table).where({ ecosystem_id: ECO }).del()
    }
    await knex('ecosystem').where({ id: ECO }).del()
  })

  afterAll(async () => {
    process.env.USE_HEIGHT_SYNC_TR = previousHeightSync
    await broker.stop()
    await knex.destroy()
  })

  it('stores the ecosystem, its history and its snapshot', async () => {
    const result = await service.syncFromLedger({
      params: { ledgerResponse: { ecosystem: ledgerEcosystem }, blockHeight: 492 },
      meta: {},
    } as unknown as SyncContext)

    expect(result).toMatchObject({ success: true })
    expect(await knex('ecosystem').where({ id: ECO }).pluck('did')).toEqual([LONG_DID])
    expect(await knex('ecosystem_history').where({ ecosystem_id: ECO }).pluck('did')).toEqual([LONG_DID])
    expect(await knex('ecosystem_snapshot').where({ ecosystem_id: ECO }).pluck('did')).toEqual([LONG_DID])
  })
})
