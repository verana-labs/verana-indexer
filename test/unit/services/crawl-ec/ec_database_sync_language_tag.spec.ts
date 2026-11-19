import { ServiceBroker } from 'moleculer'
import knex from '../../../../src/common/utils/db_connection'
import EcosystemDatabaseService from '../../../../src/services/crawl-ec/ec_database.service'

type SyncContext = Parameters<EcosystemDatabaseService['syncFromLedger']>[0]

// The chain accepts any BCP 47 language tag, so region and script subtags must sync like a two-letter tag.
const ECO = 14
const REGIONAL_ECO = 15
const created = '2026-11-19T12:43:14.549Z'
const draftCreated = '2026-11-19T12:45:37.616Z'
const LONG_TAG = 'hy-Latn-IT-arevela'

const versionOne = {
  id: 39,
  version: 1,
  created,
  active_since: created,
  documents: [{ id: 39, created, language: 'en', url: 'https://example.com/v1.md', digest_sri: 'sha384-v1' }],
}

const draftVersionTwo = {
  id: 40,
  version: 2,
  created: draftCreated,
  active_since: null,
  documents: [
    { id: 40, created: draftCreated, language: 'en-US', url: 'https://example.com/v2.md', digest_sri: 'sha384-v2' },
    { id: 41, created: draftCreated, language: LONG_TAG, url: 'https://example.com/v2-hy.md', digest_sri: 'sha384-hy' },
  ],
}

const ledgerEcosystem = (id: number, language: string, versions: object[]) => ({
  id,
  did: `did:web:eco482-${id}.example`,
  corporation_id: 22,
  created,
  modified: created,
  archived: null,
  language,
  active_version: 1,
  versions,
})

describe('EcosystemDatabaseService.syncFromLedger with BCP 47 language tags', () => {
  const broker = new ServiceBroker({ logger: false })
  const service = broker.createService(EcosystemDatabaseService) as unknown as EcosystemDatabaseService
  const sync = (ecosystem: object, blockHeight: number) =>
    service.syncFromLedger({
      params: { ledgerResponse: { ecosystem }, blockHeight },
      meta: {},
    } as unknown as SyncContext)

  const previousHeightSync = process.env.USE_HEIGHT_SYNC_TR

  beforeAll(async () => {
    process.env.USE_HEIGHT_SYNC_TR = 'true'
    for (const table of [
      'ecosystem_snapshot_diff',
      'ecosystem_snapshot',
      'ecosystem_document',
      'ecosystem_version',
      'governance_framework_document_history',
      'governance_framework_version_history',
      'ecosystem_history',
      'governance_framework_document',
      'governance_framework_version',
      'ecosystem',
    ]) {
      await knex(table)
        .del()
        .catch(() => {})
    }
  })

  afterAll(async () => {
    process.env.USE_HEIGHT_SYNC_TR = previousHeightSync
    await broker.stop()
    await knex.destroy()
  })

  it('syncs a draft version whose documents carry region and script subtags', async () => {
    expect(await sync(ledgerEcosystem(ECO, 'en', [versionOne]), 569407)).toMatchObject({ success: true })
    expect(await sync(ledgerEcosystem(ECO, 'en', [versionOne, draftVersionTwo]), 569408)).toMatchObject({
      success: true,
    })

    const expected = ['en-US', LONG_TAG]
    expect(await knex('ecosystem_document').where({ version_id: 40 }).orderBy('id').pluck('language')).toEqual(expected)
    expect(
      await knex('governance_framework_document').whereIn('gfd_id', [40, 41]).orderBy('gfd_id').pluck('language')
    ).toEqual(expected)
    expect(
      await knex('governance_framework_document_history')
        .where({ ecosystem_id: ECO, height: 569408 })
        .orderBy('id')
        .pluck('language')
    ).toEqual(expected)
  })

  it('syncs an ecosystem whose primary language carries a region subtag', async () => {
    expect(await sync(ledgerEcosystem(REGIONAL_ECO, 'pt-BR', []), 569409)).toMatchObject({ success: true })

    expect(await knex('ecosystem').where({ id: REGIONAL_ECO }).pluck('language')).toEqual(['pt-BR'])
    expect(await knex('ecosystem_snapshot').where({ ecosystem_id: REGIONAL_ECO }).pluck('language')).toEqual(['pt-BR'])
    expect(await knex('ecosystem_history').where({ ecosystem_id: REGIONAL_ECO }).pluck('language')).toEqual(['pt-BR'])
  })
})
