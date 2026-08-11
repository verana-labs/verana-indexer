const tables: Record<string, unknown[]> = {}

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
      if (method === 'whereIn' && args.length === 2 && Array.isArray(args[1])) {
        const column = String(args[0]).split('.').pop() as string
        const wanted = (args[1] as unknown[]).map(String)
        current = current.filter((row: any) => wanted.includes(String(row?.[column])))
      }
      return qb
    }
  for (const method of [
    'select',
    'where',
    'andWhere',
    'whereIn',
    'whereRaw',
    'orderBy',
    'limit',
    'innerJoin',
    'leftJoin',
  ]) {
    qb[method] = apply(method)
  }
  qb.first = jest.fn(async () => current[0])
  qb.then = (resolve: (v: unknown) => void, reject?: (e: unknown) => void) =>
    Promise.resolve(current).then(resolve, reject)
  qb.insert = jest.fn(() => qb)
  qb.onConflict = jest.fn(() => qb)
  qb.ignore = jest.fn(() => qb)
  qb.returning = jest.fn(async () => [])
  return qb
}

jest.mock('../../../../src/common/utils/db_connection', () => ({
  __esModule: true,
  default: (table: string) => chainable(table.split(' ')[0], tables[table.split(' ')[0]] ?? []),
}))

import { buildGroupEventsForTest } from '../../../../src/services/api/indexer_events_query'

const SUBMIT = '/cosmos.group.v1.MsgSubmitProposal'
const VOTE = '/cosmos.group.v1.MsgVote'
const UPDATE_MEMBERS = '/cosmos.group.v1.MsgUpdateGroupMembers'

// Carries both the raw join columns the query filters on (height/code/type) and the aliased output names.
function messageRow(over: Record<string, unknown> = {}) {
  const row = {
    message_index: 0,
    message_type: SUBMIT,
    sender: 'verana1sender',
    content: { group_policy_address: 'verana1policy' },
    block_height: 40,
    tx_hash: 'HASH1',
    tx_index: 0,
    timestamp: '2026-08-10T15:45:00.000Z',
    ...over,
  }
  return { ...row, height: row.block_height, code: 0, type: row.message_type }
}

describe('buildGroupEvents (IDX-INDEXER-SUB-1 x/group routing)', () => {
  beforeEach(() => {
    for (const key of Object.keys(tables)) delete tables[key]
    tables.corporation = [
      { id: 3, did: 'did:example:corp', policy_address: 'verana1policy', corporation: 'verana1policy' },
    ]
    // innerJoin is a no-op in the mock, so the joined corporation did rides along on the fixture
    tables.corporation_group = [
      { corporation_id: 3, group_id: 9, policy_address: 'verana1policy', did: 'did:example:corp' },
    ]
  })

  it('routes a policy-addressed message to its Corporation with module group', async () => {
    tables.transaction_message = [messageRow({ message_type: UPDATE_MEMBERS, content: { group_id: 9 } })]
    const [event] = await buildGroupEventsForTest(40)

    expect(event.module).toBe('group')
    expect(event.action).toBe('UpdateGroupMembers')
    expect(event.did).toBe('did:example:corp')
    expect(event.corporationId).toBe(3)
    expect(event.entityType).toBe('CorporationGroup')
    // CorporationGroup entities are keyed by corporation_id, never the x/group group_id
    expect(event.entityId).toBe('3')
  })

  it('anchors on the corporation table alone, before the group processor has written corporation_group', async () => {
    tables.corporation_group = []
    tables.transaction_message = [messageRow()]
    tables.event = []
    const [event] = await buildGroupEventsForTest(40)

    expect(event.corporationId).toBe(3)
    expect(event.did).toBe('did:example:corp')
  })

  it('stamps the chain-assigned proposal id on SubmitProposal events', async () => {
    tables.transaction_message = [messageRow()]
    tables.event = [
      {
        value: '"12"',
        tx_msg_index: 0,
        hash: 'HASH1',
        type: 'cosmos.group.v1.EventSubmitProposal',
        key: 'proposal_id',
      },
    ]
    const [event] = await buildGroupEventsForTest(40)

    expect(event.entityType).toBe('GroupProposal')
    expect(event.entityId).toBe('12')
  })

  it('resolves a Vote to its proposal and Corporation', async () => {
    tables.transaction_message = [messageRow({ message_type: VOTE, content: { proposal_id: '12', voter: 'verana1v' } })]
    tables.group_proposal = [{ id: 12, group_policy_address: 'verana1policy' }]
    const [event] = await buildGroupEventsForTest(40)

    expect(event.action).toBe('Vote')
    expect(event.entityType).toBe('GroupProposal')
    expect(event.entityId).toBe('12')
    expect(event.corporationId).toBe(3)
    expect(event.did).toBe('did:example:corp')
  })

  it('leaves group events that anchor no Corporation unscoped (wildcard-only)', async () => {
    tables.corporation = []
    tables.corporation_group = []
    tables.transaction_message = [messageRow({ content: { group_policy_address: 'verana1unrelated' } })]
    tables.event = []
    const [event] = await buildGroupEventsForTest(40)

    expect(event.did).toBeNull()
    expect(event.relatedDids).toEqual([])
    expect(event.corporationId).toBeUndefined()
  })

  it('resolves a group_id through the create events when corporation_group is not yet written', async () => {
    tables.corporation_group = []
    tables.transaction_message = [messageRow({ message_type: UPDATE_MEMBERS, content: { group_id: 9 } })]
    // EventCreateGroup(group_id 9) and EventCreateGroupPolicy(address) from the same create tx
    tables.event = [
      { id: 10, tx_id: 4, tx_msg_index: 0, type: 'cosmos.group.v1.EventCreateGroup', key: 'group_id', value: '"9"' },
      {
        id: 11,
        tx_id: 4,
        tx_msg_index: 0,
        type: 'cosmos.group.v1.EventCreateGroupPolicy',
        key: 'address',
        value: '"verana1policy"',
      },
    ]
    const [event] = await buildGroupEventsForTest(40)

    expect(event.corporationId).toBe(3)
    expect(event.did).toBe('did:example:corp')
  })

  it('returns nothing when the block has no group messages', async () => {
    tables.transaction_message = []
    expect(await buildGroupEventsForTest(40)).toEqual([])
  })
})
