import { ServiceBroker } from 'moleculer'
import knex from '../../../../src/common/utils/db_connection'
import { VeranaGovernanceFrameworkMessageTypes } from '../../../../src/common/verana-message-types'
import CorporationMessageProcessorService from '../../../../src/services/crawl-co/co_processor.service'

describe('CorporationMessageProcessorService CGF history', () => {
  const broker = new ServiceBroker({ logger: false })
  const service: any = broker.createService(CorporationMessageProcessorService)

  const timestamp = new Date('2024-06-01T00:00:00.000Z')

  afterAll(async () => {
    await broker.stop()
    await knex.destroy()
  })

  beforeEach(async () => {
    await knex('corporation_history')
      .del()
      .catch(() => {})
    await knex('co_governance_framework_document')
      .del()
      .catch(() => {})
    await knex('co_governance_framework_version')
      .del()
      .catch(() => {})
    await knex('corporation_member')
      .del()
      .catch(() => {})
    await knex('corporation')
      .del()
      .catch(() => {})

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
  })

  it('records AddCGFDocument history with changes and account for CGF (ecosystem_id 0)', async () => {
    await service.handleCorporationMessages({
      params: {
        corporationList: [
          {
            type: VeranaGovernanceFrameworkMessageTypes.AddGovernanceFrameworkDocument,
            height: 20,
            content: {
              corporation: 'verana1pol',
              operator: 'verana1signer',
              ecosystem_id: 0,
              version: 2,
              doc_language: 'en',
              doc_url: 'http://example.com/cgf-v2.pdf',
              doc_digest_sri: 'sha384-abc',
              timestamp,
            },
          },
        ],
      },
    } as any)

    const row = await knex('corporation_history').where({ corporation_id: 1, event_type: 'AddCGFDocument' }).first()
    expect(row).toBeDefined()
    expect(Number(row.height)).toBe(20)
    expect(row.account).toBe('verana1signer')
    expect(row.changes).toMatchObject({
      version: 2,
      language: 'en',
      url: 'http://example.com/cgf-v2.pdf',
      digest_sri: 'sha384-abc',
    })
  })

  it('records IncreaseCGFActiveVersion history with the new active_version', async () => {
    await knex('co_governance_framework_version').insert({
      corporation_id: 1,
      ecosystem_id: 0,
      version: 1,
      created: timestamp,
    })

    await service.handleCorporationMessages({
      params: {
        corporationList: [
          {
            type: VeranaGovernanceFrameworkMessageTypes.IncreaseActiveGovernanceFrameworkVersion,
            height: 30,
            content: {
              corporation: 'verana1pol',
              operator: 'verana1signer',
              ecosystem_id: 0,
              timestamp,
            },
          },
        ],
      },
    } as any)

    const row = await knex('corporation_history')
      .where({ corporation_id: 1, event_type: 'IncreaseCGFActiveVersion' })
      .first()
    expect(row).toBeDefined()
    expect(Number(row.height)).toBe(30)
    expect(row.account).toBe('verana1signer')
    expect(row.changes).toMatchObject({ active_version: 1 })

    const corp = await knex('corporation').where({ id: 1 }).first()
    expect(Number(corp.active_version)).toBe(1)
  })

  it('does not record corporation history for EGF documents (ecosystem_id != 0)', async () => {
    await service.handleCorporationMessages({
      params: {
        corporationList: [
          {
            type: VeranaGovernanceFrameworkMessageTypes.AddGovernanceFrameworkDocument,
            height: 40,
            content: {
              corporation: 'verana1pol',
              operator: 'verana1signer',
              ecosystem_id: 5,
              version: 1,
              doc_url: 'http://example.com/egf.pdf',
              doc_digest_sri: 'sha384-egf',
              timestamp,
            },
          },
        ],
      },
    } as any)

    const rows = await knex('corporation_history').where({ corporation_id: 1 })
    expect(rows).toHaveLength(0)
  })
})
