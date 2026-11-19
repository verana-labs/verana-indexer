import { ServiceBroker } from 'moleculer'
import knex from '../../../../src/common/utils/db_connection'
import EcosystemDatabaseService from '../../../../src/services/crawl-ec/ec_database.service'

type SyncContext = Parameters<EcosystemDatabaseService['syncFromLedger']>[0]

// The chain identifies a document by its id: a replacement keeps the id, and two languages can share a url.
const created = '2026-11-19T16:47:17.656Z'
const draftCreated = '2026-11-19T16:52:40.101Z'

const doc = (id: number, language: string, name: string, docCreated = draftCreated) => ({
  id,
  created: docCreated,
  language,
  url: `https://example.com/${name}.json`,
  digest_sri: `sha384-${name}`,
})

const ledgerEcosystem = (id: number, draftDocuments: object[]) => ({
  id,
  did: `did:web:eco482-replace-${id}.example`,
  corporation_id: 22,
  created,
  modified: created,
  archived: null,
  language: 'en',
  active_version: 1,
  versions: [
    { id: id * 10, version: 1, created, active_since: created, documents: [doc(id * 10, 'en', `v1-${id}`, created)] },
    { id: id * 10 + 1, version: 2, created: draftCreated, active_since: null, documents: draftDocuments },
  ],
})

describe('EcosystemDatabaseService.syncFromLedger keyed by the chain document id', () => {
  const broker = new ServiceBroker({ logger: false })
  const service = broker.createService(EcosystemDatabaseService) as unknown as EcosystemDatabaseService
  const sync = (ecosystemId: number, draftDocuments: object[], blockHeight: number) =>
    service.syncFromLedger({
      params: { ledgerResponse: { ecosystem: ledgerEcosystem(ecosystemId, draftDocuments) }, blockHeight },
      meta: {},
    } as unknown as SyncContext)
  const draftDocuments = (ecosystemId: number) =>
    knex('ecosystem_document')
      .where({ version_id: ecosystemId * 10 + 1 })
      .orderBy('id')
      .select('id', 'language', 'url')

  beforeAll(async () => {
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
    await broker.stop()
    await knex.destroy()
  })

  it('keeps one row when a draft document moves to a new url and digest', async () => {
    expect(await sync(21, [doc(215, 'en-GB', 'first')], 108)).toMatchObject({ success: true })
    expect(await sync(21, [doc(215, 'en-GB', 'second')], 109)).toMatchObject({ success: true })

    expect(await draftDocuments(21)).toEqual([{ id: 215, language: 'en-GB', url: 'https://example.com/second.json' }])
    expect(
      await knex('governance_framework_document').where({ gfd_id: 215 }).select('language', 'url', 'digest_sri')
    ).toEqual([{ language: 'en-GB', url: 'https://example.com/second.json', digest_sri: 'sha384-second' }])
  })

  it('keeps both documents when two languages share one url', async () => {
    expect(await sync(22, [doc(225, 'en-GB', 'shared'), doc(226, 'en-AU', 'shared')], 110)).toMatchObject({
      success: true,
    })

    expect(await draftDocuments(22)).toEqual([
      { id: 225, language: 'en-GB', url: 'https://example.com/shared.json' },
      { id: 226, language: 'en-AU', url: 'https://example.com/shared.json' },
    ])
  })
})
