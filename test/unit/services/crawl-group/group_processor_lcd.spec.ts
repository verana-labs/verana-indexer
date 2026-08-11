jest.mock('../../../../src/common/utils/db_connection', () => ({ __esModule: true, default: () => ({}) }))
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
jest.mock('../../../../src/common/utils/account_balance_utils', () => ({
  getLcdBaseUrl: () => 'http://lcd.test',
}))

import { ServiceBroker } from 'moleculer'
import GroupProcessorService from '../../../../src/services/crawl-group/group_processor.service'

type FetchCall = { url: string; height?: string }

function stubFetch(handler: (call: FetchCall) => { ok: boolean; body?: unknown }) {
  const calls: FetchCall[] = []
  global.fetch = jest.fn(async (url: unknown, init?: unknown) => {
    const headers = ((init as { headers?: Record<string, string> })?.headers ?? {}) as Record<string, string>
    const call = { url: String(url), height: headers['x-cosmos-block-height'] }
    calls.push(call)
    const result = handler(call)
    return {
      ok: result.ok,
      json: async () => result.body ?? {},
    } as unknown as Response
  }) as unknown as typeof fetch
  return calls
}

// A pruning node answers current-state queries but 501s historical ones. Membership refresh must
// still land instead of silently leaving stale members.
describe('GroupProcessorService LCD height fallback', () => {
  const service = new GroupProcessorService(new ServiceBroker({ logger: false })) as any
  const membersBody = {
    members: [{ member: { address: 'verana1a', weight: '2', metadata: 'm', added_at: '2026-08-10T00:00:00Z' } }],
  }

  afterEach(() => {
    jest.restoreAllMocks()
  })

  it('falls back to current state when the historical query fails', async () => {
    const calls = stubFetch((call) => (call.height ? { ok: false } : { ok: true, body: membersBody }))

    const members = await service.fetchGroupMembers(9, 30)

    expect(members).toEqual([{ address: 'verana1a', weight: '2', metadata: 'm', added_at: '2026-08-10T00:00:00Z' }])
    expect(calls).toHaveLength(2)
    expect(calls[0].height).toBe('30')
    expect(calls[1].height).toBeUndefined()
  })

  it('uses the historical answer when the node serves it, without a second call', async () => {
    const calls = stubFetch(() => ({ ok: true, body: membersBody }))

    const members = await service.fetchGroupMembers(9, 30)

    expect(members).toHaveLength(1)
    expect(calls).toHaveLength(1)
    expect(calls[0].height).toBe('30')
  })

  it('returns an empty list when both attempts fail', async () => {
    const calls = stubFetch(() => ({ ok: false }))

    expect(await service.fetchGroupMembers(9, 30)).toEqual([])
    expect(calls).toHaveLength(2)
  })

  it('does not issue a duplicate call when no height was requested', async () => {
    const calls = stubFetch(() => ({ ok: false }))

    expect(await service.fetchGroupMembers(9)).toEqual([])
    expect(calls).toHaveLength(1)
  })
})
