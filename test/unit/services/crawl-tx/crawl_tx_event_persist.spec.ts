import { BULL_JOB_NAME, SERVICE } from '../../../../src/common'

jest.mock('../../../../src/common/utils/db_connection', () => ({
  __esModule: true,
  default: Object.assign(jest.fn(), {
    raw: jest.fn().mockResolvedValue({ rows: [] }),
    transaction: jest.fn(async (fn: any) => fn({})),
  }),
}))

jest.mock('../../../../src/common/utils/chain.registry', () => ({
  __esModule: true,
  default: jest.fn(() => ({})),
}))

const txQuery = {
  select: jest.fn(),
  where: jest.fn(),
  orderBy: jest.fn(),
  first: jest.fn(),
  max: jest.fn(),
  timeout: jest.fn(),
  limit: jest.fn(),
}

jest.mock('../../../../src/models', () => ({
  __esModule: true,
  Transaction: {
    query: () => txQuery,
  },
  BlockCheckpoint: {
    getCheckpoint: jest.fn(),
    query: jest.fn(),
  },
}))

const PERSIST = `${SERVICE.V1.IndexerEventsService.path}.persistBlockEvents`

describe('CrawlTxService stores block events before the checkpoint', () => {
  let svc: any
  let checkpointChain: any

  beforeEach(async () => {
    jest.clearAllMocks()
    for (const method of ['select', 'where', 'orderBy', 'max', 'timeout'] as const) {
      txQuery[method].mockReturnValue(txQuery)
    }
    txQuery.first.mockResolvedValueOnce({ max_id: 2 })
    txQuery.limit.mockResolvedValueOnce([
      { id: 1, height: 5, index: 0, timestamp: '2026-11-13T00:00:00.000Z' },
      { id: 2, height: 5, index: 1, timestamp: '2026-11-13T00:00:00.000Z' },
    ])

    const { default: CrawlTxService } = await import('../../../../src/services/crawl-tx/crawl_tx.service')
    const { BlockCheckpoint } = await import('../../../../src/models')
    ;(BlockCheckpoint.getCheckpoint as any).mockResolvedValue([
      0,
      100,
      { job_name: BULL_JOB_NAME.HANDLE_TRANSACTION, height: 0, updated_at: new Date() },
    ])
    checkpointChain = {
      select: jest.fn(() => checkpointChain),
      where: jest.fn(() => checkpointChain),
      first: jest.fn(async () => ({ height: 10 })),
      insert: jest.fn(() => checkpointChain),
      onConflict: jest.fn(() => checkpointChain),
      merge: jest.fn(() => checkpointChain),
      timeout: jest.fn(() => checkpointChain),
      transacting: jest.fn(async () => undefined),
    }
    ;(BlockCheckpoint.query as any).mockReturnValue(checkpointChain)

    svc = Object.create(CrawlTxService.prototype)
    svc.logger = { info: jest.fn(), warn: jest.fn(), debug: jest.fn(), error: jest.fn() }
    svc.broker = { call: jest.fn().mockResolvedValue(undefined) }
    svc.applyScheduledFlipsForRange = jest.fn().mockResolvedValue(undefined)
    svc.applyScheduledFlipsForBlockHeight = jest.fn().mockResolvedValue(undefined)
    svc.processTransactionsForBlock = jest.fn().mockResolvedValue({})
    svc.processPayloads = jest.fn().mockResolvedValue(undefined)
    svc._completedPayloadStages = new Map()
    svc._hasUniqueConstraintCache = true
    svc._isFreshStart = false
  })

  it('fails the block and keeps the checkpoint when its events cannot be stored', async () => {
    svc.broker.call.mockImplementation(async (action: string) => {
      if (action === PERSIST) throw new Error('statement timeout')
    })

    await expect(svc.jobHandlerCrawlTxBody()).rejects.toThrow('statement timeout')
    expect(checkpointChain.insert).not.toHaveBeenCalled()
  })

  it('stores the events of the block before writing its checkpoint', async () => {
    await svc.jobHandlerCrawlTxBody()

    const persistCall = svc.broker.call.mock.calls.findIndex(([action]: [string]) => action === PERSIST)
    expect(svc.broker.call.mock.calls[persistCall][1]).toEqual({ height: 5 })
    expect(svc.broker.call.mock.invocationCallOrder[persistCall]).toBeLessThan(
      checkpointChain.insert.mock.invocationCallOrder[0]
    )
  })
})
