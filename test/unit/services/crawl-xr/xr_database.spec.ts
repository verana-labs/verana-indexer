import { ServiceBroker } from 'moleculer'
import knex from '../../../../src/common/utils/db_connection'
import ExchangeRateDatabaseService from '../../../../src/services/crawl-xr/xr_database.service'

const ID = 9001

const created = {
  id: ID,
  base_asset_type: 'TU',
  base_asset: 'tu',
  quote_asset_type: 'COIN',
  quote_asset: 'uvna',
  rate: '1500000',
  rate_scale: 6,
  validity_duration: '60.5s',
  updated: '2026-11-05T07:35:01.692Z',
  expires: '2026-11-05T07:36:02.192Z',
  state: false,
}
const enabled = { ...created, state: true, updated: '2026-11-05T07:37:42.215Z' }
const refreshed = {
  ...enabled,
  rate: '1600000',
  updated: '2026-11-05T07:38:07.291Z',
  expires: '2026-11-05T07:39:07.791Z',
}

async function historyRows() {
  return knex('exchange_rate_history')
    .where({ exchange_rate_id: ID })
    .orderBy('height')
    .select('height', 'event_type', 'validity_duration', 'rate', 'state', 'changes')
}

describe('ExchangeRateDatabaseService.syncFromLedger history', () => {
  const broker = new ServiceBroker({ logger: false })
  const service: any = broker.createService(ExchangeRateDatabaseService)
  const sync = (exchangeRate: typeof created, blockHeight: number, eventType?: string) =>
    service.syncFromLedger({ params: { exchangeRate, blockHeight, eventType } })

  beforeEach(async () => {
    await knex('exchange_rate_history').where({ exchange_rate_id: ID }).del()
    await knex('exchange_rates').where({ id: ID }).del()
  })

  afterAll(async () => {
    await knex('exchange_rate_history').where({ exchange_rate_id: ID }).del()
    await knex('exchange_rates').where({ id: ID }).del()
  })

  it('writes no history row when an unrelated xr event resyncs an unchanged rate', async () => {
    await sync(created, 24, 'create_exchange_rate')
    await sync(created, 40)
    await sync(created, 41)

    const rows = await historyRows()
    expect(rows.map((r) => r.height)).toEqual([24])
  })

  it('records only the fields that changed, timestamps as ISO strings', async () => {
    await sync(created, 24, 'create_exchange_rate')
    await sync(enabled, 56, 'set_exchange_rate_state')
    await sync(refreshed, 61, 'update_exchange_rate')

    const rows = await historyRows()
    expect(rows.map((r) => r.height)).toEqual([24, 56, 61])
    expect(rows[0].changes).toEqual({
      rate: '1500000',
      rate_scale: 6,
      validity_duration: '60.5s',
      updated: created.updated,
      expires: created.expires,
      state: false,
    })
    expect(rows[1].changes).toEqual({
      updated: { before: created.updated, after: enabled.updated },
      state: { before: false, after: true },
    })
    expect(rows[2].changes).toEqual({
      rate: { before: '1500000', after: '1600000' },
      updated: { before: enabled.updated, after: refreshed.updated },
      expires: { before: enabled.expires, after: refreshed.expires },
    })
    expect(rows[2].validity_duration).toBe('60.5s')
  })

  it('rewrites identical history when the blocks are replayed', async () => {
    await sync(created, 24, 'create_exchange_rate')
    await sync(enabled, 56, 'set_exchange_rate_state')
    await sync(refreshed, 61, 'update_exchange_rate')
    const before = await historyRows()

    await sync(created, 24, 'create_exchange_rate')
    await sync(enabled, 56, 'set_exchange_rate_state')
    await sync(refreshed, 61, 'update_exchange_rate')

    expect(await historyRows()).toEqual(before)
    const current = await knex('exchange_rates').where({ id: ID }).first()
    expect(current.rate).toBe('1600000')
    expect(current.state).toBe(true)
  })
})
