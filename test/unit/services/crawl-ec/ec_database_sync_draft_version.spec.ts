import { ServiceBroker } from 'moleculer'
import knex from '../../../../src/common/utils/db_connection'
import EcosystemDatabaseService from '../../../../src/services/crawl-ec/ec_database.service'

// A draft EGF version (not yet activated) must stay inactive in the ledger-synced mirror tables.
const ECO = 5
const created = '2026-10-06T19:12:56.270Z'
const draftCreated = '2026-10-06T19:13:01.288Z'

const ledgerEcosystem = {
  id: ECO,
  did: 'did:example:eco444b',
  corporation_id: 6,
  created,
  modified: created,
  archived: null,
  language: 'en',
  active_version: 1,
  versions: [
    {
      id: 17,
      version: 1,
      created,
      active_since: created,
      documents: [{ id: 18, created, language: 'en', url: 'https://example.com/gf.json', digest_sri: 'sha384-v1' }],
    },
    {
      id: 18,
      version: 2,
      created: draftCreated,
      active_since: null,
      documents: [
        { id: 19, created: draftCreated, language: 'en', url: 'https://example.com/v2.json', digest_sri: 'sha384-v2' },
      ],
    },
  ],
}

describe('EcosystemDatabaseService.syncFromLedger with a draft EGF version', () => {
  const broker = new ServiceBroker({ logger: false })
  const service: any = broker.createService(EcosystemDatabaseService)

  beforeAll(async () => {
    for (const table of [
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
    const res = await service.syncFromLedger({
      params: { ledgerResponse: { ecosystem: ledgerEcosystem }, blockHeight: 310 },
      meta: {},
    })
    expect(res?.success).toBe(true)
  })

  afterAll(async () => {
    await broker.stop()
    await knex.destroy()
  })

  it('keeps the draft version inactive in ecosystem_version', async () => {
    const rows = await knex('ecosystem_version').where({ ecosystem_id: ECO }).orderBy('version')
    expect(rows.map((r: any) => [Number(r.id), Number(r.version), r.active_since === null])).toEqual([
      [17, 1, false],
      [18, 2, true],
    ])
  })

  it('keeps the draft version inactive in governance_framework_version', async () => {
    const draft = await knex('governance_framework_version').where({ ecosystem_id: ECO, version: 2 }).first()
    expect(Number(draft.gfv_id)).toBe(18)
    expect(draft.active_since).toBeNull()
  })

  it('keeps the draft version inactive in the ecosystem snapshot', async () => {
    const snapshot = await knex('ecosystem_snapshot').where({ ecosystem_id: ECO }).orderBy('height', 'desc').first()
    const v2 = snapshot.versions_snapshot.find((v: any) => Number(v.version) === 2)
    expect(v2.active_since).toBeNull()
  })
})
