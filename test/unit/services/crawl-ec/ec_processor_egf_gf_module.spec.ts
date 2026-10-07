import { ServiceBroker } from 'moleculer'
import { ModulesParamsNamesTypes } from '../../../../src/common'
import knex from '../../../../src/common/utils/db_connection'
import { VeranaGovernanceFrameworkMessageTypes } from '../../../../src/common/verana-message-types'
import EcosystemMessageProcessorService from '../../../../src/services/crawl-ec/ec_processor.service'
import GovernanceFrameworkApiService from '../../../../src/services/crawl-gf/gf_api.service'

// Devnet repro from issue #444: gf.v1 add of EGF v2 for ecosystem 1 at height 60193 (gfv_id 15, gfd_id 15).
const timestamp = new Date('2026-10-02T13:18:16.000Z')
const ECO = 1
const OTHER_ECO = 2
const CORP = 'verana1pol'

function addGfDocumentEvent(ecosystemId: number, gfvId: number, gfdId: number, version: number, language: string) {
  return {
    type: 'add_gf_document',
    attributes: [
      { key: 'corporation', value: CORP },
      { key: 'operator', value: 'verana1op' },
      { key: 'ecosystem_id', value: String(ecosystemId) },
      { key: 'gfv_id', value: String(gfvId) },
      { key: 'gfd_id', value: String(gfdId) },
      { key: 'version', value: String(version) },
      { key: 'language', value: language },
      { key: 'doc_url', value: `https://example.com/egf-v${version}-${language}.json` },
      { key: 'doc_digest_sri', value: `sha384-v${version}-${language}` },
      { key: 'msg_index', value: '0' },
    ],
  }
}

function increaseGfActiveEvent(ecosystemId: number, gfvId: number, version: number) {
  return {
    type: 'increase_active_gf_version',
    attributes: [
      { key: 'corporation', value: CORP },
      { key: 'operator', value: 'verana1op' },
      { key: 'ecosystem_id', value: String(ecosystemId) },
      { key: 'gfv_id', value: String(gfvId) },
      { key: 'version', value: String(version) },
      { key: 'active_since', value: '2026-10-02T13:20:00Z' },
    ],
  }
}

function addMessage(version: number, language: string, txEvents: unknown[], extra: Record<string, unknown> = {}) {
  return {
    type: VeranaGovernanceFrameworkMessageTypes.AddGovernanceFrameworkDocument,
    height: 60193,
    timestamp,
    txEvents,
    content: {
      corporation: CORP,
      operator: 'verana1op',
      ecosystem_id: String(ECO),
      doc_language: language,
      doc_url: `https://example.com/egf-v${version}-${language}.json`,
      doc_digest_sri: `sha384-v${version}-${language}`,
      version,
    },
    ...extra,
  }
}

function increaseMessage(txEvents: unknown[]) {
  return {
    type: VeranaGovernanceFrameworkMessageTypes.IncreaseActiveGovernanceFrameworkVersion,
    height: 60200,
    timestamp: new Date('2026-10-02T13:20:00.000Z'),
    txEvents,
    content: { corporation: CORP, operator: 'verana1op', ecosystem_id: String(ECO) },
  }
}

async function seedVersion(ecosystemId: number, version: number, over: Record<string, unknown> = {}) {
  const [row] = await knex('governance_framework_version')
    .insert({ ecosystem_id: ecosystemId, version, created: timestamp, active_since: null, gfv_id: null, ...over })
    .returning('*')
  return row
}

describe('EcosystemMessageProcessorService EGF via gf module (issue #444)', () => {
  const broker = new ServiceBroker({ logger: false })
  const service: any = broker.createService(EcosystemMessageProcessorService)
  const gfApi: any = broker.createService(GovernanceFrameworkApiService)

  const process = (message: unknown) => service.handleEcosystemMessages({ params: { ecosystemList: [message] } })

  afterAll(async () => {
    await broker.stop()
    await knex.destroy()
  })

  beforeEach(async () => {
    for (const table of [
      'governance_framework_document_history',
      'governance_framework_version_history',
      'ecosystem_history',
      'governance_framework_document',
      'governance_framework_version',
      'ecosystem',
      'module_params',
    ]) {
      await knex(table)
        .del()
        .catch(() => {})
    }
    await knex('module_params').insert({
      module: ModulesParamsNamesTypes.EC,
      params: JSON.stringify({ params: {} }),
    })
    await knex('ecosystem').insert([
      {
        id: ECO,
        did: 'did:web:eco-one.example',
        corporation_id: 1,
        created: timestamp,
        modified: timestamp,
        language: 'en',
        height: 10,
        active_version: 1,
      },
      {
        id: OTHER_ECO,
        did: 'did:web:eco-two.example',
        corporation_id: 1,
        created: timestamp,
        modified: timestamp,
        language: 'en',
        height: 11,
        active_version: 1,
      },
    ])
    const v1 = await seedVersion(ECO, 1, { active_since: timestamp, gfv_id: 2 })
    await knex('governance_framework_document').insert({
      gfv_id: v1.id,
      created: timestamp,
      language: 'en',
      url: 'https://example.com/egf-v1-en.json',
      digest_sri: 'sha384-v1-en',
      gfd_id: 2,
    })
    await seedVersion(OTHER_ECO, 1, { active_since: timestamp, gfv_id: 3 })
  })

  describe('AddGovernanceFrameworkDocument', () => {
    it('stores the new EGF version and document with their chain ids', async () => {
      await expect(process(addMessage(2, 'en', [addGfDocumentEvent(ECO, 15, 15, 2, 'en')]))).resolves.toBeUndefined()

      const v2 = await knex('governance_framework_version').where({ ecosystem_id: ECO, version: 2 }).first()
      expect(Number(v2.gfv_id)).toBe(15)
      expect(v2.active_since).toBeNull()

      const docs = await knex('governance_framework_document').where({ gfv_id: v2.id })
      expect(docs).toHaveLength(1)
      expect(Number(docs[0].gfd_id)).toBe(15)
      expect(docs[0].language).toBe('en')
      expect(docs[0].url).toBe('https://example.com/egf-v2-en.json')
      expect(docs[0].digest_sri).toBe('sha384-v2-en')

      const gfvHistory = await knex('governance_framework_version_history').where({ ecosystem_id: ECO, version: 2 })
      expect(gfvHistory.map((h: any) => h.event_type)).toEqual(['AddGFV'])
      const gfdHistory = await knex('governance_framework_document_history').where({ ecosystem_id: ECO, gfv_id: v2.id })
      expect(gfdHistory.map((h: any) => h.event_type)).toEqual(['AddGFD'])

      const ec = await knex('ecosystem').where({ id: ECO }).first()
      expect(Number(ec.active_version)).toBe(1)
    })

    it('exposes the new version through the governance-framework list and get endpoints', async () => {
      await process(addMessage(2, 'en', [addGfDocumentEvent(ECO, 15, 15, 2, 'en')]))

      const list = await gfApi.listGovernanceFrameworkVersionsV4({ params: { ecosystem_id: String(ECO) }, meta: {} })
      expect(list.versions.map((v: any) => v.id)).toEqual([15, 2])
      expect(list.versions[0]).toMatchObject({
        id: 15,
        ecosystem_id: ECO,
        corporation_id: null,
        version: 2,
        active_since: null,
      })
      expect(list.versions[0].documents).toEqual([
        expect.objectContaining({ id: 15, gfv_id: 15, language: 'en', url: 'https://example.com/egf-v2-en.json' }),
      ])

      const got = await gfApi.getGovernanceFrameworkVersionV4({ params: { id: '15' }, meta: {} })
      expect(got.version).toMatchObject({ id: 15, ecosystem_id: ECO, corporation_id: null, version: 2 })
    })

    it('adds a second-language document to an existing draft version and backfills its chain id', async () => {
      const draft = await seedVersion(ECO, 2)

      await process(addMessage(2, 'fr', [addGfDocumentEvent(ECO, 15, 16, 2, 'fr')]))

      const rows = await knex('governance_framework_version').where({ ecosystem_id: ECO, version: 2 })
      expect(rows).toHaveLength(1)
      expect(Number(rows[0].id)).toBe(Number(draft.id))
      expect(Number(rows[0].gfv_id)).toBe(15)

      const docs = await knex('governance_framework_document').where({ gfv_id: draft.id }).orderBy('id')
      expect(docs.map((d: any) => [d.language, Number(d.gfd_id)])).toEqual([['fr', 16]])
    })

    it('uses the add_gf_document event of this ecosystem when the tx carries events for several', async () => {
      const events = [addGfDocumentEvent(OTHER_ECO, 99, 98, 2, 'en'), addGfDocumentEvent(ECO, 15, 15, 2, 'en')]

      await process(addMessage(2, 'en', events))

      const v2 = await knex('governance_framework_version').where({ ecosystem_id: ECO, version: 2 }).first()
      expect(Number(v2.gfv_id)).toBe(15)
      const other = await knex('governance_framework_version').where({ ecosystem_id: OTHER_ECO, version: 2 }).first()
      expect(other).toBeUndefined()
    })

    it('targets the ecosystem named in the message even when tx events name another ecosystem first', async () => {
      const events = [
        { type: 'update_ecosystem', attributes: [{ key: 'ecosystem_id', value: String(OTHER_ECO) }] },
        addGfDocumentEvent(ECO, 15, 15, 2, 'en'),
      ]

      await process(addMessage(2, 'en', events, { eventEcosystemIds: [OTHER_ECO, ECO] }))

      const v2 = await knex('governance_framework_version').where({ ecosystem_id: ECO, version: 2 }).first()
      expect(Number(v2.gfv_id)).toBe(15)
      const other = await knex('governance_framework_version').where({ ecosystem_id: OTHER_ECO, version: 2 }).first()
      expect(other).toBeUndefined()
    })
  })

  describe('IncreaseActiveGovernanceFrameworkVersion', () => {
    it('activates the version named by the increase event and moves ecosystem.active_version', async () => {
      const draft = await seedVersion(ECO, 2)

      await expect(process(increaseMessage([increaseGfActiveEvent(ECO, 15, 2)]))).resolves.toBeUndefined()

      const ec = await knex('ecosystem').where({ id: ECO }).first()
      expect(Number(ec.active_version)).toBe(2)
      const v2 = await knex('governance_framework_version').where({ id: draft.id }).first()
      expect(v2.active_since).not.toBeNull()
      expect(Number(v2.gfv_id)).toBe(15)

      const ecHistory = await knex('ecosystem_history').where({ ecosystem_id: ECO, event_type: 'IncreaseGFV' }).first()
      expect(ecHistory).toBeDefined()
      expect(ecHistory.changes.active_version).toBe(2)
      const gfvHistory = await knex('governance_framework_version_history').where({
        ecosystem_id: ECO,
        version: 2,
        event_type: 'ActivateGFV',
      })
      expect(gfvHistory).toHaveLength(1)
    })

    it('exposes the activated version as active_only through the governance-framework list', async () => {
      await seedVersion(ECO, 2, { gfv_id: 15 })

      await process(increaseMessage([increaseGfActiveEvent(ECO, 15, 2)]))

      const list = await gfApi.listGovernanceFrameworkVersionsV4({
        params: { ecosystem_id: String(ECO), active_only: 'true' },
        meta: {},
      })
      expect(list.versions.map((v: any) => v.id)).toEqual([15])
      expect(list.versions[0].active_since).not.toBeNull()
    })

    it('falls back to active_version + 1 when the tx carries no events', async () => {
      const draft = await seedVersion(ECO, 2, { gfv_id: 15 })

      await process(increaseMessage([]))

      const ec = await knex('ecosystem').where({ id: ECO }).first()
      expect(Number(ec.active_version)).toBe(2)
      const v2 = await knex('governance_framework_version').where({ id: draft.id }).first()
      expect(v2.active_since).not.toBeNull()
    })

    it('creates the version row from the event when the indexer never saw the add', async () => {
      await process(increaseMessage([increaseGfActiveEvent(ECO, 15, 2)]))

      const v2 = await knex('governance_framework_version').where({ ecosystem_id: ECO, version: 2 }).first()
      expect(v2).toBeDefined()
      expect(Number(v2.gfv_id)).toBe(15)
      expect(v2.active_since).not.toBeNull()
      const ec = await knex('ecosystem').where({ id: ECO }).first()
      expect(Number(ec.active_version)).toBe(2)
    })

    it('ignores an increase event that belongs to another ecosystem', async () => {
      await seedVersion(ECO, 2, { gfv_id: 15 })

      await process(increaseMessage([increaseGfActiveEvent(OTHER_ECO, 40, 3)]))

      const ec = await knex('ecosystem').where({ id: ECO }).first()
      expect(Number(ec.active_version)).toBe(2)
      const other = await knex('ecosystem').where({ id: OTHER_ECO }).first()
      expect(Number(other.active_version)).toBe(1)
      const otherV3 = await knex('governance_framework_version').where({ ecosystem_id: OTHER_ECO, version: 3 }).first()
      expect(otherV3).toBeUndefined()
    })
  })
})
