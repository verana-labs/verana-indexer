const tables: Record<string, unknown[]> = {}
const deleted: string[] = []
const inserted: Array<{ table: string; rows: unknown }> = []

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
  for (const method of ['select', 'where', 'whereIn', 'whereRaw', 'orderBy', 'limit']) qb[method] = apply(method)
  qb.first = jest.fn(async () => current[0])
  qb.update = jest.fn(async (patch: Record<string, unknown>) => {
    for (const row of current) Object.assign(row as object, patch)
    return current.length
  })
  qb.delete = jest.fn(async () => {
    deleted.push(table)
    return current.length
  })
  qb.insert = jest.fn((rows: unknown) => {
    inserted.push({ table, rows })
    return qb
  })
  qb.onConflict = jest.fn(() => qb)
  qb.merge = jest.fn(async () => [])
  qb.ignore = jest.fn(async () => [])
  qb.then = (resolve: (v: unknown) => void, reject?: (e: unknown) => void) =>
    Promise.resolve(current).then(resolve, reject)
  return qb
}

jest.mock('../../../../src/common/utils/db_connection', () => {
  const factory = (table: string) => chainable(table.split(' ')[0], tables[table.split(' ')[0]] ?? [])
  return {
    __esModule: true,
    default: Object.assign(factory, {
      transaction: async (fn: (trx: unknown) => Promise<unknown>) => fn(factory),
    }),
  }
})
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

  it('returns null (not an empty list) when both attempts fail', async () => {
    const calls = stubFetch(() => ({ ok: false }))

    expect(await service.fetchGroupMembers(9, 30)).toBeNull()
    expect(calls).toHaveLength(2)
  })

  it('does not issue a duplicate call when no height was requested', async () => {
    const calls = stubFetch(() => ({ ok: false }))

    expect(await service.fetchGroupMembers(9)).toBeNull()
    expect(calls).toHaveLength(1)
  })

  it('distinguishes a genuinely empty group from a failed lookup', async () => {
    stubFetch(() => ({ ok: true, body: { members: [] } }))
    expect(await service.fetchGroupMembers(9, 30)).toEqual([])
  })
})

// The consumer half: a leave must rewrite membership, including down to zero members.
describe('GroupProcessorService.handleGroupUpdated membership rewrite', () => {
  const service = new GroupProcessorService(new ServiceBroker({ logger: false })) as any

  beforeEach(() => {
    for (const key of Object.keys(tables)) delete tables[key]
    tables.corporation_group = [{ corporation_id: 1, group_id: 9, group_version: 3, total_weight: '3' }]
    deleted.length = 0
    inserted.length = 0
  })

  afterEach(() => jest.restoreAllMocks())

  const leaveEvent = {
    event_id: 1,
    type: 'cosmos.group.v1.EventLeaveGroup',
    tx_id: 1,
    tx_msg_index: 0,
    block_height: 100,
    attrs: { group_id: '"9"' },
  }

  it('replaces membership with the post-leave list', async () => {
    stubFetch((call) =>
      call.url.includes('group_members')
        ? { ok: true, body: { members: [{ member: { address: 'verana1a', weight: '1', metadata: '' } }] } }
        : { ok: true, body: { info: { version: '4', total_weight: '1' } } }
    )

    await service.handleGroupUpdated(leaveEvent, 100, '2026-08-12T00:00:00.000Z')

    expect(deleted).toContain('corporation_member')
    const members = inserted.filter((i) => i.table === 'corporation_member').flatMap((i) => i.rows as any[])
    expect(members.map((m) => m.address)).toEqual(['verana1a'])
  })

  it('clears membership when the last member leaves', async () => {
    stubFetch((call) =>
      call.url.includes('group_members')
        ? { ok: true, body: { members: [] } }
        : { ok: true, body: { info: { version: '5', total_weight: '0' } } }
    )

    await service.handleGroupUpdated(leaveEvent, 100, '2026-08-12T00:00:00.000Z')

    expect(deleted).toContain('corporation_member')
    expect(inserted.filter((i) => i.table === 'corporation_member')).toEqual([])
  })

  it('leaves membership untouched when the lookup fails', async () => {
    stubFetch(() => ({ ok: false }))

    await service.handleGroupUpdated(leaveEvent, 100, '2026-08-12T00:00:00.000Z')

    expect(deleted).not.toContain('corporation_member')
  })
})
