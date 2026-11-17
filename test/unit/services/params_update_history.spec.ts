import { ServiceBroker } from 'moleculer'
import knex from '../../../src/common/utils/db_connection'
import { getModuleParams } from '../../../src/common/utils/params_service'
import GenesisParamsService from '../../../src/services/params/params.service'

describe('GenesisParamsService.handleUpdateParams', () => {
  const broker = new ServiceBroker({ logger: false })
  const service: any = broker.createService(GenesisParamsService)
  const modules = ['cs', 'xr']
  let saved: { params: Array<Record<string, unknown>>; history: Array<Record<string, unknown>> } = {
    params: [],
    history: [],
  }

  beforeAll(async () => {
    saved = {
      params: await knex('module_params').whereIn('module', modules),
      history: await knex('module_params_history').whereIn('module', modules),
    }
    await knex('module_params_history').whereIn('module', modules).delete()
    await knex('module_params').whereIn('module', modules).delete()
    await knex('module_params').insert([
      {
        module: 'cs',
        params: JSON.stringify({ params: { credential_schema_schema_max_size: '8192' }, schemas: [] }),
        updated_at: new Date(),
      },
      {
        module: 'xr',
        params: JSON.stringify({ params: { max_validity_duration: '31536000s' } }),
        updated_at: new Date(),
      },
    ])
  })

  afterAll(async () => {
    await knex('module_params_history').whereIn('module', modules).delete()
    await knex('module_params').whereIn('module', modules).delete()
    if (saved.params.length > 0) await knex('module_params').insert(saved.params)
    if (saved.history.length > 0) await knex('module_params_history').insert(saved.history)
    await broker.stop()
    await knex.destroy()
  })

  it('records the change against params seeded from genesis, so At-Block-Height sees it', async () => {
    const result = await service.handleUpdateParams({
      params: {
        message: { type: '/verana.cs.v1.MsgUpdateParams', params: { credential_schema_schema_max_size: '9000' } },
        height: 81712,
      },
    })

    expect(result.success).toBe(true)
    const history = await knex('module_params_history').where({ module: 'cs', height: 81712 }).first()
    expect(history?.event_type).toBe('UPDATE_PARAMS')
    expect(history?.changes).toEqual({ credential_schema_schema_max_size: { old: '8192', new: '9000' } })
    expect((await getModuleParams('cs', 81712))?.params.credential_schema_schema_max_size).toBe(9000)
  })

  it('applies an exchange-rate module params update', async () => {
    const result = await service.handleUpdateParams({
      params: {
        message: { type: '/verana.xr.v1.MsgUpdateParams', params: { max_validity_duration: '7200s' } },
        height: 81800,
      },
    })

    expect(result.success).toBe(true)
    const row = await knex('module_params').where({ module: 'xr' }).first()
    expect(typeof row.params === 'string' ? JSON.parse(row.params) : row.params).toEqual({
      max_validity_duration: '7200s',
    })
  })
})
