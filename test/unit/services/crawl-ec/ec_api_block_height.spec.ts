import { ServiceBroker } from 'moleculer'
import { BULL_JOB_NAME } from '../../../../src/common/constant'
import knex from '../../../../src/common/utils/db_connection'
import EcosystemDatabaseService from '../../../../src/services/crawl-ec/ec_database.service'

// Real checkpoint row, no resolver mock: the echo must come from block_checkpoint.
const ECO = 31
const CHECKPOINT = 4242
const created = new Date('2026-10-01T00:00:00.000Z')

describe('EcosystemDatabaseService.getEcosystem block_height echo', () => {
  const broker = new ServiceBroker({ logger: false })
  const service: any = broker.createService(EcosystemDatabaseService)

  beforeAll(async () => {
    for (const table of [
      'ecosystem_history',
      'governance_framework_document',
      'governance_framework_version',
      'ecosystem',
    ]) {
      await knex(table)
        .del()
        .catch(() => {})
    }
    await knex('block_checkpoint').where({ job_name: BULL_JOB_NAME.HANDLE_TRANSACTION }).del()
    await knex('block_checkpoint').insert({ job_name: BULL_JOB_NAME.HANDLE_TRANSACTION, height: CHECKPOINT })
    await knex('ecosystem').insert({
      id: ECO,
      did: 'did:web:eco-block-height.example',
      corporation_id: 1,
      created,
      modified: created,
      language: 'en',
      height: 100,
      active_version: 1,
    })
    await knex('ecosystem_history').insert({
      ecosystem_id: ECO,
      did: 'did:web:eco-block-height.example',
      corporation_id: 1,
      created,
      modified: created,
      language: 'en',
      active_version: 1,
      event_type: 'Create',
      height: 100,
      created_at: created,
    })
  })

  afterAll(async () => {
    await knex('block_checkpoint').where({ job_name: BULL_JOB_NAME.HANDLE_TRANSACTION }).del()
    await knex('ecosystem_history').where({ ecosystem_id: ECO }).del()
    await knex('ecosystem').where({ id: ECO }).del()
    await broker.stop()
    await knex.destroy()
  })

  it('echoes the latest indexed height from the real checkpoint row as block_height', async () => {
    const res: any = await service.getEcosystem({ params: { ecosystem_id: ECO }, meta: {} })

    expect(res.ecosystem.id).toBe(ECO)
    expect(res.block_height).toBe(CHECKPOINT)
  })

  it('echoes At-Block-Height as block_height on the point-in-time path', async () => {
    const res: any = await service.getEcosystem({ params: { ecosystem_id: ECO }, meta: { blockHeight: 150 } })

    expect(res.ecosystem.id).toBe(ECO)
    expect(res.block_height).toBe(150)
  })

  it('keeps block_height when gf_data is none', async () => {
    const res: any = await service.getEcosystem({ params: { ecosystem_id: ECO, gf_data: 'none' }, meta: {} })

    expect(res.ecosystem.versions).toBeUndefined()
    expect(res.block_height).toBe(CHECKPOINT)
  })
})
