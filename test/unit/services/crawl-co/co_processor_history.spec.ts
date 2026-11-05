import { ServiceBroker } from 'moleculer'
import knex from '../../../../src/common/utils/db_connection'
import {
  VeranaCorporationMessageTypes,
  VeranaGovernanceFrameworkMessageTypes,
} from '../../../../src/common/verana-message-types'
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

  it('stores a CGF document and its history once when the block is replayed', async () => {
    const addDocument = {
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
    }

    await service.handleCorporationMessages({ params: { corporationList: [addDocument] } } as any)
    await service.handleCorporationMessages({ params: { corporationList: [addDocument] } } as any)

    expect(await knex('co_governance_framework_document')).toHaveLength(1)
    expect(await knex('corporation_history').where({ corporation_id: 1, event_type: 'AddCGFDocument' })).toHaveLength(1)
  })

  it('records an identical CGF document re-added in a later block', async () => {
    const addDocument = (height: number, at: Date) => ({
      type: VeranaGovernanceFrameworkMessageTypes.AddGovernanceFrameworkDocument,
      height,
      content: {
        corporation: 'verana1pol',
        operator: 'verana1signer',
        ecosystem_id: 0,
        version: 2,
        doc_language: 'en',
        doc_url: 'http://example.com/cgf-v2.pdf',
        doc_digest_sri: 'sha384-abc',
        timestamp: at,
      },
    })

    await service.handleCorporationMessages({ params: { corporationList: [addDocument(20, timestamp)] } } as any)
    await service.handleCorporationMessages({
      params: { corporationList: [addDocument(21, new Date('2024-06-01T00:00:05.000Z'))] },
    } as any)

    expect(await knex('corporation_history').where({ corporation_id: 1, event_type: 'AddCGFDocument' })).toHaveLength(2)
  })

  it('records IncreaseCGFActiveVersion history once when the block is replayed', async () => {
    await knex('co_governance_framework_version').insert({
      corporation_id: 1,
      ecosystem_id: 0,
      version: 2,
      created: timestamp,
    })
    const increase = {
      type: VeranaGovernanceFrameworkMessageTypes.IncreaseActiveGovernanceFrameworkVersion,
      height: 30,
      content: { corporation: 'verana1pol', operator: 'verana1signer', ecosystem_id: 0, timestamp },
      txEvents: [
        {
          type: 'increase_active_gf_version',
          attributes: [
            { key: 'corporation', value: 'verana1pol' },
            { key: 'ecosystem_id', value: '0' },
            { key: 'version', value: '2' },
          ],
        },
      ],
    }

    await service.handleCorporationMessages({ params: { corporationList: [increase] } } as any)
    await service.handleCorporationMessages({ params: { corporationList: [increase] } } as any)

    const rows = await knex('corporation_history').where({ corporation_id: 1, event_type: 'IncreaseCGFActiveVersion' })
    expect(rows).toHaveLength(1)
    expect(rows[0].changes).toMatchObject({ active_version: 2 })
  })

  it('records a corporation creation once when the block is replayed', async () => {
    const create = {
      type: VeranaCorporationMessageTypes.CreateCorporation,
      height: 50,
      content: {
        signer: 'verana1signer',
        did: 'did:example:new',
        language: 'en',
        doc_url: 'http://example.com/cgf-v1.pdf',
        doc_digest_sri: 'sha384-v1',
        decision_policy: { '@type': '/cosmos.group.v1.ThresholdDecisionPolicy', threshold: '1' },
        members: [{ address: 'verana1member', weight: '1' }],
        timestamp,
      },
      txEvents: [
        {
          type: 'create_corporation',
          attributes: [
            { key: 'corporation_id', value: '2' },
            { key: 'policy_address', value: 'verana1newpol' },
          ],
        },
      ],
    }

    await service.handleCorporationMessages({ params: { corporationList: [create] } } as any)
    await service.handleCorporationMessages({ params: { corporationList: [create] } } as any)

    expect(await knex('corporation_history').where({ corporation_id: 2 })).toHaveLength(1)
    expect(await knex('co_governance_framework_document')).toHaveLength(1)
  })
})
