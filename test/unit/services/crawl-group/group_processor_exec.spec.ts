const tables: Record<string, unknown[]> = {}
const updates: Array<{ table: string; patch: Record<string, unknown> }> = []

function chainable(table: string, rows: unknown[]) {
  let current = [...rows]
  const qb: any = {}
  const apply =
    (method: string) =>
    (...args: unknown[]) => {
      if (method === 'where' && args.length === 2 && typeof args[0] === 'string') {
        const column = String(args[0]).split('.').pop() as string
        current = current.filter((row: any) => String(row?.[column]) === String(args[1]))
      }
      return qb
    }
  for (const method of ['select', 'where', 'whereIn', 'whereRaw', 'orderBy', 'limit', 'innerJoin']) {
    qb[method] = apply(method)
  }
  qb.update = jest.fn(async (patch: Record<string, unknown>) => {
    updates.push({ table, patch })
    for (const row of current) Object.assign(row as object, patch)
    return current.length
  })
  qb.insert = jest.fn(() => qb)
  qb.onConflict = jest.fn(() => qb)
  qb.merge = jest.fn(async () => [])
  qb.ignore = jest.fn(async () => [])
  qb.first = jest.fn(async () => current[0])
  qb.then = (resolve: (v: unknown) => void, reject?: (e: unknown) => void) =>
    Promise.resolve(current).then(resolve, reject)
  return qb
}

jest.mock('../../../../src/common/utils/db_connection', () => ({
  __esModule: true,
  default: (table: string) => chainable(table.split(' ')[0], tables[table.split(' ')[0]] ?? []),
}))
jest.mock('../../../../src/common/utils/checkpoint_manager', () => ({
  CheckpointManager: class {
    async ensureCheckpoint() {}
    async updateCheckpoint() {}
  },
}))
jest.mock('../../../../src/common/utils/start_mode_detector', () => ({
  detectStartMode: jest.fn(async () => 'normal'),
}))
jest.mock('../../../../src/services/manager/indexer_status.manager', () => ({
  indexerStatusManager: { isCrawlingActive: () => false },
}))
jest.mock('../../../../src/common/utils/account_balance_utils', () => ({ getLcdBaseUrl: () => 'http://lcd.test' }))

import { ServiceBroker } from 'moleculer'
import GroupProcessorService from '../../../../src/services/crawl-group/group_processor.service'

const NOW = '2026-08-12T00:00:00.000Z'
const OPEN_WINDOW = '2026-08-12T01:00:00.000Z'
const CLOSED_WINDOW = '2026-08-11T23:00:00.000Z'

function execEvent(result: string) {
  return {
    event_id: 1,
    type: 'cosmos.group.v1.EventExec',
    tx_id: 1,
    tx_msg_index: 0,
    block_height: 100,
    attrs: { proposal_id: '"7"', result: `"PROPOSAL_EXECUTOR_RESULT_${result}"` },
  }
}

function proposalRow(over: Record<string, unknown> = {}) {
  return {
    id: 7,
    corporation_id: 1,
    status: 'SUBMITTED',
    executor_result: 'NOT_RUN',
    voting_period_end: OPEN_WINDOW,
    ...over,
  }
}

let fetchCalls: Array<{ url: string; height?: string }> = []
function stubFetch(handler: (call: { url: string; height?: string }) => { ok: boolean; body?: unknown }) {
  fetchCalls = []
  global.fetch = jest.fn(async (url: unknown, init?: unknown) => {
    const headers = ((init as { headers?: Record<string, string> })?.headers ?? {}) as Record<string, string>
    const call = { url: String(url), height: headers['x-cosmos-block-height'] }
    fetchCalls.push(call)
    const result = handler(call)
    return { ok: result.ok, json: async () => result.body ?? {} } as unknown as Response
  }) as unknown as typeof fetch
}

describe('GroupProcessorService.handleExecEvent', () => {
  const service = new GroupProcessorService(new ServiceBroker({ logger: false })) as any

  beforeEach(() => {
    for (const key of Object.keys(tables)) delete tables[key]
    updates.length = 0
    tables.group_vote = []
    tables.corporation_member = []
    tables.corporation_group = [{ corporation_id: 1, total_weight: '3', decision_policy: { threshold: '2' } }]
    stubFetch(() => ({ ok: false }))
  })

  afterEach(() => jest.restoreAllMocks())

  it('SUCCESS marks the proposal ACCEPTED and does not re-read chain state', async () => {
    tables.group_proposal = [proposalRow()]
    await service.handleExecEvent(execEvent('SUCCESS'), 100, NOW)

    expect(updates[0].patch).toMatchObject({ status: 'ACCEPTED', executor_result: 'SUCCESS' })
    expect(fetchCalls).toHaveLength(0)
  })

  it('FAILURE also implies ACCEPTED, because x/group only executes an accepted proposal', async () => {
    tables.group_proposal = [proposalRow()]
    await service.handleExecEvent(execEvent('FAILURE'), 100, NOW)

    expect(updates[0].patch).toMatchObject({ status: 'ACCEPTED', executor_result: 'FAILURE' })
  })

  it('NOT_RUN leaves the status alone and settles from chain state', async () => {
    tables.group_proposal = [proposalRow()]
    stubFetch(() => ({
      ok: true,
      body: { proposal: { status: 'PROPOSAL_STATUS_SUBMITTED', executor_result: 'PROPOSAL_EXECUTOR_RESULT_NOT_RUN' } },
    }))

    await service.handleExecEvent(execEvent('NOT_RUN'), 100, NOW)

    expect(updates[0].patch.status).toBeUndefined()
    expect(fetchCalls[0].height).toBe('100')
  })

  // The regression the audit caught: an unreachable LCD must never invent a terminal status.
  it('does not fabricate a terminal status when the chain read fails on an open proposal', async () => {
    tables.group_proposal = [proposalRow({ voting_period_end: OPEN_WINDOW })]
    stubFetch(() => ({ ok: false }))

    await service.handleExecEvent(execEvent('NOT_RUN'), 100, NOW)

    const statusWrites = updates.filter((u) => u.patch.status !== undefined)
    expect(statusWrites).toEqual([])
  })

  it('does not overwrite an already-terminal status when the chain read fails', async () => {
    tables.group_proposal = [proposalRow({ status: 'ACCEPTED', voting_period_end: CLOSED_WINDOW })]
    stubFetch(() => ({ ok: false }))

    await service.handleExecEvent(execEvent('FAILURE'), 100, NOW)

    expect(tables.group_proposal[0]).toMatchObject({ status: 'ACCEPTED', executor_result: 'FAILURE' })
  })

  it('settles a pruned proposal only once its voting window has closed', async () => {
    tables.group_proposal = [proposalRow({ voting_period_end: CLOSED_WINDOW })]
    tables.group_vote = [{ proposal_id: 7, voter: 'verana1a', option: 'YES' }]
    tables.corporation_member = [
      { corporation_id: 1, address: 'verana1a', weight: '2' },
      { corporation_id: 1, address: 'verana1b', weight: '1' },
    ]
    stubFetch(() => ({ ok: false }))

    await service.handleExecEvent(execEvent('NOT_RUN'), 100, NOW)

    const settled = updates.find((u) => u.patch.status !== undefined)
    expect(settled?.patch).toMatchObject({ status: 'ACCEPTED' })
  })

  it('retries the chain read without the height before giving up', async () => {
    tables.group_proposal = [proposalRow()]
    stubFetch((call) =>
      call.height
        ? { ok: false }
        : { ok: true, body: { proposal: { status: 'PROPOSAL_STATUS_SUBMITTED', voting_period_end: OPEN_WINDOW } } }
    )

    await service.handleExecEvent(execEvent('NOT_RUN'), 100, NOW)

    expect(fetchCalls.map((c) => c.height)).toEqual(['100', undefined])
  })
})
