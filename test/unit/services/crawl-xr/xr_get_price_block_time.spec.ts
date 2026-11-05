import { ServiceBroker } from 'moleculer'
import knex from '../../../../src/common/utils/db_connection'
import ExchangeRateApiService from '../../../../src/services/crawl-xr/xr_apis.service'

const RATE_ID = 4690001
const VALID_HEIGHT = 199_000_469
const EXPIRED_HEIGHT = 199_000_470
const VALID_BLOCK_TIME = new Date('2026-01-15T10:30:00.000Z')
const EXPIRES = new Date('2026-01-15T10:31:00.000Z')
const EXPIRED_BLOCK_TIME = new Date('2026-01-15T10:32:00.000Z')

const pair = {
  base_asset_type: 'TU',
  base_asset: 'tu-469',
  quote_asset_type: 'COIN',
  quote_asset: 'uvna-469',
}

async function cleanup() {
  await knex('exchange_rate_history').where({ exchange_rate_id: RATE_ID }).del()
  await knex('block').whereIn('height', [VALID_HEIGHT, EXPIRED_HEIGHT]).del()
}

describe('ExchangeRateApiService.getPrice against indexed block time', () => {
  const broker = new ServiceBroker({ logger: false })
  const service = broker.createService(ExchangeRateApiService) as unknown as ExchangeRateApiService
  const getPrice = async (blockHeight: number) => {
    const ctx = {
      params: { ...pair, amount: '1000000' },
      meta: { blockHeight } as { blockHeight: number; $statusCode?: number },
    }
    const body = await service.getPrice(ctx as unknown as Parameters<ExchangeRateApiService['getPrice']>[0])
    return { status: ctx.meta.$statusCode, body }
  }

  beforeAll(async () => {
    await cleanup()
    await knex('block').insert([
      { height: VALID_HEIGHT, hash: `xr-469-${VALID_HEIGHT}`, time: VALID_BLOCK_TIME, proposer_address: 'p', data: {} },
      {
        height: EXPIRED_HEIGHT,
        hash: `xr-469-${EXPIRED_HEIGHT}`,
        time: EXPIRED_BLOCK_TIME,
        proposer_address: 'p',
        data: {},
      },
    ])
    await knex('exchange_rate_history').insert({
      exchange_rate_id: RATE_ID,
      ...pair,
      rate: '1500000',
      rate_scale: 6,
      validity_duration: '60s',
      updated: VALID_BLOCK_TIME,
      expires: EXPIRES,
      state: true,
      event_type: 'update_exchange_rate',
      height: VALID_HEIGHT,
    })
  })

  afterAll(async () => {
    await cleanup()
  })

  it('prices a rate whose expiry is after the block time even though the wall clock is past it', async () => {
    expect(EXPIRES.getTime()).toBeLessThan(Date.now())

    const { status, body } = await getPrice(VALID_HEIGHT)

    expect(status).toBe(200)
    expect(body).toMatchObject({ price: '1500000', block_height: VALID_HEIGHT })
  })

  it('answers 410 at a later block whose time is past the expiry', async () => {
    const { status } = await getPrice(EXPIRED_HEIGHT)

    expect(status).toBe(410)
  })
})
