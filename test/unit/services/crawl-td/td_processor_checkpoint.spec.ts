import { ServiceBroker } from 'moleculer'
import { BULL_JOB_NAME, SERVICE } from '../../../../src/common'
import knex from '../../../../src/common/utils/db_connection'
import { fetchTrustDeposit } from '../../../../src/modules/td-height-sync/td_height_sync_helpers'
import CrawlTrustDepositService from '../../../../src/services/crawl-td/td_processor.service'

jest.mock('../../../../src/modules/td-height-sync/td_height_sync_helpers', () => ({
  ...jest.requireActual('../../../../src/modules/td-height-sync/td_height_sync_helpers'),
  fetchTrustDeposit: jest.fn(),
}))
jest.mock('../../../../src/common/utils/db_query_helper', () => ({
  ...jest.requireActual('../../../../src/common/utils/db_query_helper'),
  delay: jest.fn().mockResolvedValue(undefined),
}))

const ACCOUNT = 'verana1evvrzxw9yg5staqdvumd6fupy3jhaxfflla7st'
const mockFetch = fetchTrustDeposit as jest.MockedFunction<typeof fetchTrustDeposit>
const ledgerState = { corporation: ACCOUNT, deposit: 1000 } as Awaited<ReturnType<typeof fetchTrustDeposit>>

function adjustBlock(height: number) {
  return {
    height,
    hash: `TDHASH${height}`,
    time: '2026-11-10T00:00:00.000Z',
    proposer_address: 'proposer',
    data: {
      block_result: {
        txs_results: [
          {
            events: [
              {
                type: 'adjust_trust_deposit',
                attributes: [
                  { key: 'corporation_id', value: '7' },
                  { key: 'account', value: ACCOUNT },
                ],
              },
            ],
          },
        ],
      },
    },
  }
}

describe('CrawlTrustDepositService.processBlocks checkpoint', () => {
  const broker = new ServiceBroker({ logger: false })
  const service: any = broker.createService(CrawlTrustDepositService)
  const call = jest.spyOn(broker, 'call')
  const previousHeightSync = process.env.USE_HEIGHT_SYNC_TD

  const checkpoint = async () =>
    Number(
      (await knex('block_checkpoint').where({ job_name: BULL_JOB_NAME.HANDLE_TRUST_DEPOSIT }).first())?.height ?? -1
    )
  const syncedHeights = () =>
    call.mock.calls
      .filter(([action]) => action === `${SERVICE.V1.TrustDepositDatabaseService.path}.syncFromLedger`)
      .map(([, params]) => (params as { blockHeight: number }).blockHeight)

  async function setup(txHeight: number) {
    await knex.raw('TRUNCATE TABLE block, block_checkpoint RESTART IDENTITY CASCADE')
    await knex('block').insert([101, 102, 103].map(adjustBlock))
    await knex('block_checkpoint').insert([
      { job_name: BULL_JOB_NAME.HANDLE_TRUST_DEPOSIT, height: 100 },
      { job_name: BULL_JOB_NAME.HANDLE_TRANSACTION, height: txHeight },
    ])
  }

  beforeAll(() => {
    process.env.USE_HEIGHT_SYNC_TD = 'true'
  })

  beforeEach(() => {
    mockFetch.mockReset()
    call.mockReset().mockResolvedValue({ success: true } as never)
  })

  afterAll(async () => {
    process.env.USE_HEIGHT_SYNC_TD = previousHeightSync
    await knex.raw('TRUNCATE TABLE block, block_checkpoint RESTART IDENTITY CASCADE')
    await broker.stop()
    await knex.destroy()
  })

  it('checkpoints only the blocks before a failed deposit query', async () => {
    await setup(103)
    mockFetch.mockImplementation(async (_id, height) => {
      if (height === 102) throw new Error('Query failed with (6): connection reset')
      return ledgerState
    })

    await service.processBlocks(undefined, false)

    expect(await checkpoint()).toBe(101)
    expect(syncedHeights()).toEqual([101])
  })

  it('treats a deposit missing from the ledger as a failure', async () => {
    await setup(103)
    mockFetch.mockResolvedValue(null)

    await service.processBlocks(undefined, false)

    expect(await checkpoint()).toBe(100)
    expect(syncedHeights()).toEqual([])
  })

  it('does not process blocks the transaction pipeline has not reached', async () => {
    await setup(102)
    mockFetch.mockResolvedValue(ledgerState)

    await service.processBlocks(undefined, false)

    expect(await checkpoint()).toBe(102)
    expect(syncedHeights()).toEqual([101, 102])
  })
})
