import { ServiceBroker } from 'moleculer'
import knex from '../../../../src/common/utils/db_connection'
import {
  VeranaCorporationMessageTypes,
  VeranaGovernanceFrameworkMessageTypes,
} from '../../../../src/common/verana-message-types'
import CorporationMessageProcessorService from '../../../../src/services/crawl-co/co_processor.service'

const timestamp = new Date('2024-06-01T00:00:00.000Z')

function addGfDocumentEvent(gfvId: number, gfdId: number, version: number, language: string) {
  return {
    type: 'add_gf_document',
    attributes: [
      { key: 'gfv_id', value: String(gfvId) },
      { key: 'gfd_id', value: String(gfdId) },
      { key: 'version', value: String(version) },
      { key: 'language', value: language },
    ],
  }
}

describe('gf chain id capture (co processor)', () => {
  const broker = new ServiceBroker({ logger: false })
  const service: any = broker.createService(CorporationMessageProcessorService)

  afterAll(async () => {
    await broker.stop()
    await knex.destroy()
  })

  beforeEach(async () => {
    for (const table of [
      'corporation_history',
      'co_governance_framework_document',
      'co_governance_framework_version',
      'corporation_member',
      'corporation',
    ]) {
      await knex(table)
        .del()
        .catch(() => {})
    }
  })

  it('stores chain gfv_id/gfd_id for the CGF v1 seeded by CreateCorporation', async () => {
    await service.handleCorporationMessages({
      params: {
        corporationList: [
          {
            type: VeranaCorporationMessageTypes.CreateCorporation,
            height: 10,
            txEvents: [
              {
                type: 'create_corporation',
                attributes: [
                  { key: 'corporation_id', value: '1' },
                  { key: 'policy_address', value: 'verana1pol' },
                ],
              },
              addGfDocumentEvent(1362, 2001, 1, 'en'),
            ],
            content: {
              did: 'did:example:co',
              signer: 'verana1signer',
              language: 'en',
              doc_url: 'http://example.com/cgf-v1.pdf',
              doc_digest_sri: 'sha384-v1',
              timestamp,
            },
          },
        ],
      },
    } as any)

    const gfv = await knex('co_governance_framework_version').where({ corporation_id: 1, version: 1 }).first()
    expect(Number(gfv.gfv_id)).toBe(1362)
    const gfd = await knex('co_governance_framework_document').where({ gfv_id: gfv.id }).first()
    expect(Number(gfd.gfd_id)).toBe(2001)
  })

  it('stores chain ids for a new version created by AddGovernanceFrameworkDocument', async () => {
    await knex('corporation').insert({
      id: 1,
      did: 'did:example:co',
      policy_address: 'verana1pol',
      corporation: 'verana1pol',
      language: 'en',
      created: timestamp,
      modified: timestamp,
      height: 10,
    })

    await service.handleCorporationMessages({
      params: {
        corporationList: [
          {
            type: VeranaGovernanceFrameworkMessageTypes.AddGovernanceFrameworkDocument,
            height: 20,
            txEvents: [addGfDocumentEvent(9, 15, 2, 'en')],
            content: {
              corporation: 'verana1pol',
              ecosystem_id: 0,
              version: 2,
              doc_language: 'en',
              doc_url: 'http://example.com/cgf-v2.pdf',
              doc_digest_sri: 'sha384-v2',
              timestamp,
            },
          },
        ],
      },
    } as any)

    const gfv = await knex('co_governance_framework_version').where({ corporation_id: 1, version: 2 }).first()
    expect(Number(gfv.gfv_id)).toBe(9)
    const gfd = await knex('co_governance_framework_document').where({ gfv_id: gfv.id }).first()
    expect(Number(gfd.gfd_id)).toBe(15)
  })

  it('backfills gfv_id on an existing version when a later document arrives', async () => {
    await knex('corporation').insert({
      id: 1,
      did: 'did:example:co',
      policy_address: 'verana1pol',
      corporation: 'verana1pol',
      language: 'en',
      created: timestamp,
      modified: timestamp,
      height: 10,
    })
    await knex('co_governance_framework_version').insert({
      corporation_id: 1,
      ecosystem_id: 0,
      version: 2,
      created: timestamp,
    })

    await service.handleCorporationMessages({
      params: {
        corporationList: [
          {
            type: VeranaGovernanceFrameworkMessageTypes.AddGovernanceFrameworkDocument,
            height: 21,
            txEvents: [addGfDocumentEvent(9, 16, 2, 'fr')],
            content: {
              corporation: 'verana1pol',
              ecosystem_id: 0,
              version: 2,
              doc_language: 'fr',
              doc_url: 'http://example.com/cgf-v2-fr.pdf',
              doc_digest_sri: 'sha384-v2-fr',
              timestamp,
            },
          },
        ],
      },
    } as any)

    const gfv = await knex('co_governance_framework_version').where({ corporation_id: 1, version: 2 }).first()
    expect(Number(gfv.gfv_id)).toBe(9)
  })
})
