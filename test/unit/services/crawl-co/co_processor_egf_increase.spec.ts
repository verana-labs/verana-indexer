import { ServiceBroker } from 'moleculer'
import knex from '../../../../src/common/utils/db_connection'
import { VeranaGovernanceFrameworkMessageTypes } from '../../../../src/common/verana-message-types'
import CorporationMessageProcessorService from '../../../../src/services/crawl-co/co_processor.service'

describe('CorporationMessageProcessorService EGF increase active version', () => {
  const broker = new ServiceBroker({ logger: false })
  const service: any = broker.createService(CorporationMessageProcessorService)

  const timestamp = new Date('2026-08-26T11:34:49.769Z')

  const increaseGfActiveEvent = (version: number, gfvId: number, ecosystemId: number) => [
    {
      type: 'increase_active_gf_version',
      attributes: [
        { key: 'corporation', value: 'verana1pol' },
        { key: 'operator', value: 'verana1op' },
        { key: 'ecosystem_id', value: String(ecosystemId) },
        { key: 'gfv_id', value: String(gfvId) },
        { key: 'version', value: String(version) },
        { key: 'active_since', value: '2026-08-26T11:34:49Z' },
      ],
    },
  ]

  const increaseMessage = (ecosystemId: number, txEvents?: unknown[]) => ({
    type: VeranaGovernanceFrameworkMessageTypes.IncreaseActiveGovernanceFrameworkVersion,
    height: 310075,
    content: {
      corporation: 'verana1pol',
      operator: 'verana1op',
      ecosystem_id: ecosystemId,
      timestamp,
    },
    txEvents,
  })

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
      id: 12,
      did: 'did:example:co',
      policy_address: 'verana1pol',
      corporation: 'verana1pol',
      language: 'en',
      created: timestamp,
      modified: timestamp,
      height: 10,
    })
  })

  it('activates the event version for an EGF whose v1 mirror row was never seeded (devnet block 310075)', async () => {
    await knex('co_governance_framework_version').insert({
      corporation_id: 12,
      ecosystem_id: 13,
      version: 2,
      created: timestamp,
      gfv_id: null,
    })

    await expect(
      service.handleCorporationMessages({
        params: { corporationList: [increaseMessage(13, increaseGfActiveEvent(2, 26, 13))] },
      } as any)
    ).resolves.toBeUndefined()

    const v2 = await knex('co_governance_framework_version')
      .where({ corporation_id: 12, ecosystem_id: 13, version: 2 })
      .first()
    expect(v2.active_since).not.toBeNull()
    expect(Number(v2.gfv_id)).toBe(26)

    const history = await knex('corporation_history').where({ corporation_id: 12 })
    expect(history).toHaveLength(0)
    const corp = await knex('corporation').where({ id: 12 }).first()
    expect(corp.active_version).toBeNull()
  })

  it('creates the version row from the event when no EGF mirror rows exist at all', async () => {
    await expect(
      service.handleCorporationMessages({
        params: { corporationList: [increaseMessage(13, increaseGfActiveEvent(2, 26, 13))] },
      } as any)
    ).resolves.toBeUndefined()

    const v2 = await knex('co_governance_framework_version')
      .where({ corporation_id: 12, ecosystem_id: 13, version: 2 })
      .first()
    expect(v2).toBeDefined()
    expect(Number(v2.gfv_id)).toBe(26)
    expect(v2.active_since).not.toBeNull()
  })
})
