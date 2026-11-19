import { ServiceBroker } from 'moleculer'
import knex from '../../../../src/common/utils/db_connection'
import {
  VeranaCorporationMessageTypes,
  VeranaGovernanceFrameworkMessageTypes,
} from '../../../../src/common/verana-message-types'
import CorporationMessageProcessorService from '../../../../src/services/crawl-co/co_processor.service'

type MessagesContext = Parameters<CorporationMessageProcessorService['handleCorporationMessages']>[0]

// The chain allows a corporation language of up to 17 characters and puts no limit on a document tag.
const timestamp = new Date('2026-11-19T16:53:19.144Z')
const CORPORATION_TAG = 'zh-Hant-TW'
const DOCUMENT_TAG = 'hy-Latn-IT-arevela'

describe('CorporationMessageProcessorService with BCP 47 language tags', () => {
  const broker = new ServiceBroker({ logger: false })
  const service = broker.createService(
    CorporationMessageProcessorService
  ) as unknown as CorporationMessageProcessorService
  const handle = (corporationList: object[]) =>
    service.handleCorporationMessages({ params: { corporationList } } as unknown as MessagesContext)

  beforeAll(async () => {
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

  afterAll(async () => {
    await broker.stop()
    await knex.destroy()
  })

  it('indexes a corporation and a document whose tags carry script and region subtags', async () => {
    await handle([
      {
        type: VeranaCorporationMessageTypes.CreateCorporation,
        height: 99,
        txEvents: [
          {
            type: 'create_corporation',
            attributes: [
              { key: 'corporation_id', value: '3' },
              { key: 'policy_address', value: 'verana1corp482' },
            ],
          },
        ],
        content: {
          did: 'did:example:corp-482',
          signer: 'verana1signer',
          language: CORPORATION_TAG,
          doc_url: 'https://example.com/cgf-v1.json',
          doc_digest_sri: 'sha384-v1',
          timestamp,
        },
      },
      {
        type: VeranaGovernanceFrameworkMessageTypes.AddGovernanceFrameworkDocument,
        height: 105,
        txEvents: [],
        content: {
          corporation: 'verana1corp482',
          ecosystem_id: 0,
          version: 2,
          doc_language: DOCUMENT_TAG,
          doc_url: 'https://example.com/cgf-v2.json',
          doc_digest_sri: 'sha384-v2',
          timestamp,
        },
      },
    ])

    expect(await knex('corporation').where({ id: 3 }).pluck('language')).toEqual([CORPORATION_TAG])
    expect(
      await knex('corporation_history').where({ corporation_id: 3, event_type: 'Create' }).pluck('language')
    ).toEqual([CORPORATION_TAG])
    expect(await knex('co_governance_framework_document').orderBy('id').pluck('language')).toEqual([
      CORPORATION_TAG,
      DOCUMENT_TAG,
    ])
  })
})
