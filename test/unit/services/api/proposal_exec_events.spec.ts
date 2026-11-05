import { toBase64 } from '@cosmjs/encoding'
import { Registry } from '@cosmjs/proto-signing'
import { defaultRegistryTypes } from '@cosmjs/stargate'
import { MsgUpdateCorporation } from '@verana-labs/verana-types/codec/verana/co/v1/tx'
import { MsgSlashTrustDeposit } from '@verana-labs/verana-types/codec/verana/td/v1/tx'
import Long from 'long'

const tables: Record<string, unknown[]> = {}

function chainable(rows: unknown[]) {
  let current = [...rows]
  const qb: any = {}
  const filter = (...args: unknown[]) => {
    if (args.length === 2 && typeof args[0] === 'string') {
      const column = String(args[0]).split('.').pop() as string
      current = current.filter((row: any) => String(row?.[column]) === String(args[1]))
    } else if (args.length === 1 && args[0] && typeof args[0] === 'object') {
      for (const [column, value] of Object.entries(args[0] as Record<string, unknown>)) {
        current = current.filter((row) => String((row as Record<string, unknown>)?.[column]) === String(value))
      }
    }
    return qb
  }
  qb.where = filter
  qb.andWhere = filter
  qb.whereIn = (column: string, values: unknown[]) => {
    const key = column.split('.').pop() as string
    const wanted = values.map(String)
    current = current.filter((row: any) => wanted.includes(String(row?.[key])))
    return qb
  }
  for (const method of ['select', 'whereRaw', 'orWhere', 'orderBy', 'limit', 'innerJoin', 'leftJoin']) {
    qb[method] = () => qb
  }
  qb.first = jest.fn(async () => current[0])
  qb.then = (resolve: (v: unknown) => void, reject?: (e: unknown) => void) =>
    Promise.resolve(current).then(resolve, reject)
  return qb
}

jest.mock('../../../../src/common/utils/db_connection', () => ({
  __esModule: true,
  default: (table: string) => chainable(tables[table.split(' ')[0]] ?? []),
}))

import { buildProposalExecEventsForTest } from '../../../../src/services/api/indexer_events_query'

const POLICY = 'verana1policy'
const UPDATE_CORPORATION = '/verana.co.v1.MsgUpdateCorporation'

function updateCorporationAny(did: string) {
  const value = MsgUpdateCorporation.encode({ corporation: POLICY, operator: POLICY, did }).finish()
  return { type_url: UPDATE_CORPORATION, value: toBase64(value) }
}

function slashAny(corporationId: number) {
  const value = MsgSlashTrustDeposit.encode({
    authority: POLICY,
    corporationId,
    deposit: '1000',
    reason: 'e2e',
  }).finish()
  return { type_url: '/verana.td.v1.MsgSlashTrustDeposit', value: toBase64(value) }
}

function updateGroupMembersAny(groupId: number) {
  const value = new Registry(defaultRegistryTypes).encode({
    typeUrl: '/cosmos.group.v1.MsgUpdateGroupMembers',
    value: {
      admin: POLICY,
      groupId: Long.fromNumber(groupId),
      memberUpdates: [{ address: 'verana1member', weight: '0', metadata: '' }],
    },
  })
  return { type_url: '/cosmos.group.v1.MsgUpdateGroupMembers', value: toBase64(value) }
}

// Carries the join columns the query filters on (block_height/code/type) next to the aliased output names.
function execEvent(over: Record<string, unknown> = {}) {
  return {
    id: 50,
    event_id: 50,
    tx_id: 8,
    tx_msg_index: 1,
    tx_hash: 'EXECHASH',
    tx_index: 2,
    block_height: 60,
    timestamp: '2026-11-05T10:00:00.000Z',
    code: 0,
    type: 'cosmos.group.v1.EventExec',
    ...over,
  }
}

function execAttributes(result: string, eventId = 50, proposalId = '12') {
  return [
    { event_id: eventId, block_height: 60, key: 'proposal_id', value: `"${proposalId}"` },
    { event_id: eventId, block_height: 60, key: 'result', value: `"${result}"` },
  ]
}

// EventSubmitProposal rides on event_attribute because innerJoin is a no-op in the mock.
function submitAttribute(proposalId = '12') {
  return {
    event_id: 40,
    block_height: 55,
    type: 'cosmos.group.v1.EventSubmitProposal',
    key: 'proposal_id',
    value: `"${proposalId}"`,
    tx_id: 4,
    tx_msg_index: 0,
  }
}

describe('buildProposalExecEvents (IDX-INDEXER-SUB-1 group-executed Msgs)', () => {
  beforeEach(() => {
    for (const key of Object.keys(tables)) delete tables[key]
    tables.corporation = [{ id: 3, did: 'did:example:corp', policy_address: POLICY, corporation: POLICY }]
  })

  it('emits a VPR event per watched wrapped Msg of a successfully executed proposal', async () => {
    tables.event = [execEvent()]
    tables.event_attribute = [...execAttributes('PROPOSAL_EXECUTOR_RESULT_SUCCESS'), submitAttribute()]
    tables.transaction_message = [
      {
        tx_id: 4,
        index: 0,
        type: '/cosmos.group.v1.MsgSubmitProposal',
        content: {
          group_policy_address: POLICY,
          messages: [{ type_url: '/cosmos.bank.v1beta1.MsgSend', value: '' }, updateCorporationAny('did:example:corp')],
        },
      },
    ]

    const events = await buildProposalExecEventsForTest(60)

    expect(events).toHaveLength(1)
    const [event] = events
    expect(event.module).toBe('corporation')
    expect(event.action).toBe('UpdateCorporation')
    expect(event.messageType).toBe(UPDATE_CORPORATION)
    expect(event.txHash).toBe('EXECHASH')
    expect(event.txIndex).toBe(2)
    expect(event.messageIndex).toBe(1)
    expect(event.sender).toBe(POLICY)
    expect(event.did).toBe('did:example:corp')
    expect(event.corporationId).toBe(3)
    expect(event.proposalId).toBe('12')
    expect(event.proposalMessageIndex).toBe(1)
  })

  it('routes a wrapped SlashTrustDeposit to the slashed Corporation, not the council', async () => {
    tables.corporation.push({
      id: 9,
      did: 'did:example:slashed',
      policy_address: 'verana1other',
      corporation: 'verana1other',
    })
    tables.event = [execEvent()]
    tables.event_attribute = [...execAttributes('PROPOSAL_EXECUTOR_RESULT_SUCCESS'), submitAttribute()]
    tables.transaction_message = [
      {
        tx_id: 4,
        index: 0,
        type: '/cosmos.group.v1.MsgSubmitProposal',
        content: { group_policy_address: POLICY, messages: [slashAny(9)] },
      },
    ]

    const [event] = await buildProposalExecEventsForTest(60)

    expect(event.module).toBe('trust-deposit')
    expect(event.action).toBe('SlashTrustDeposit')
    expect(event.sender).toBe(POLICY)
    expect(event.corporationId).toBe(9)
    expect(event.entityId).toBe('9')
    expect(event.did).toBe('did:example:slashed')
    expect(event.relatedDids).not.toContain('did:example:corp')
  })

  it('emits group admin Msgs wrapped in the proposal as group events of the Corporation', async () => {
    tables.corporation_group = [{ group_id: 5, corporation_id: 3, did: 'did:example:corp' }]
    tables.event = [execEvent()]
    tables.event_attribute = [...execAttributes('PROPOSAL_EXECUTOR_RESULT_SUCCESS'), submitAttribute()]
    tables.transaction_message = [
      {
        tx_id: 4,
        index: 0,
        type: '/cosmos.group.v1.MsgSubmitProposal',
        content: {
          group_policy_address: POLICY,
          messages: [{ type_url: '/cosmos.group.v1.MsgVote', value: '' }, updateGroupMembersAny(5)],
        },
      },
    ]

    const events = await buildProposalExecEventsForTest(60)

    expect(events).toHaveLength(1)
    const [event] = events
    expect(event.module).toBe('group')
    expect(event.action).toBe('UpdateGroupMembers')
    expect(event.entityType).toBe('CorporationGroup')
    expect(event.entityId).toBe('3')
    expect(event.corporationId).toBe(3)
    expect(event.did).toBe('did:example:corp')
    expect(event.sender).toBe(POLICY)
    expect(event.messageIndex).toBe(1)
    expect(event.proposalId).toBe('12')
    expect(event.proposalMessageIndex).toBe(1)
  })

  it('anchors a wrapped decision policy update by its policy address', async () => {
    tables.event = [execEvent()]
    tables.event_attribute = execAttributes('PROPOSAL_EXECUTOR_RESULT_SUCCESS')
    tables.group_proposal = [
      {
        id: 12,
        group_policy_address: POLICY,
        messages: [
          {
            '@type': '/cosmos.group.v1.MsgUpdateGroupPolicyDecisionPolicy',
            admin: POLICY,
            groupPolicyAddress: POLICY,
            decisionPolicy: { '@type': '/cosmos.group.v1.ThresholdDecisionPolicy', threshold: '2' },
          },
        ],
      },
    ]

    const [event] = await buildProposalExecEventsForTest(60)

    expect(event.action).toBe('UpdateGroupPolicyDecisionPolicy')
    expect(event.entityType).toBe('CorporationGroup')
    expect(event.corporationId).toBe(3)
    expect(event.proposalMessageIndex).toBe(0)
  })

  it('emits nothing when the execution failed', async () => {
    tables.event = [execEvent()]
    tables.event_attribute = [...execAttributes('PROPOSAL_EXECUTOR_RESULT_FAILURE'), submitAttribute()]
    tables.transaction_message = [
      {
        tx_id: 4,
        index: 0,
        type: '/cosmos.group.v1.MsgSubmitProposal',
        content: { group_policy_address: POLICY, messages: [updateCorporationAny('did:example:corp')] },
      },
    ]

    expect(await buildProposalExecEventsForTest(60)).toEqual([])
  })

  it('ignores EventExec from a failed tx', async () => {
    tables.event = [execEvent({ code: 5 })]
    tables.event_attribute = [...execAttributes('PROPOSAL_EXECUTOR_RESULT_SUCCESS'), submitAttribute()]

    expect(await buildProposalExecEventsForTest(60)).toEqual([])
  })

  it('falls back to group_proposal.messages when the submit tx is not indexed', async () => {
    tables.event = [execEvent()]
    tables.event_attribute = execAttributes('PROPOSAL_EXECUTOR_RESULT_SUCCESS')
    tables.group_proposal = [
      {
        id: 12,
        group_policy_address: POLICY,
        messages: [{ '@type': UPDATE_CORPORATION, corporation: POLICY, operator: POLICY, did: 'did:example:corp' }],
      },
    ]

    const [event] = await buildProposalExecEventsForTest(60)

    expect(event.action).toBe('UpdateCorporation')
    expect(event.corporationId).toBe(3)
    expect(event.proposalMessageIndex).toBe(0)
  })

  it('keeps same-type wrapped Msgs apart by their position in the proposal', async () => {
    tables.event = [execEvent()]
    tables.event_attribute = [...execAttributes('PROPOSAL_EXECUTOR_RESULT_SUCCESS'), submitAttribute()]
    tables.transaction_message = [
      {
        tx_id: 4,
        index: 0,
        type: '/cosmos.group.v1.MsgSubmitProposal',
        content: {
          group_policy_address: POLICY,
          messages: [updateCorporationAny('did:example:a'), updateCorporationAny('did:example:b')],
        },
      },
    ]

    const events = await buildProposalExecEventsForTest(60)

    expect(events.map((event) => event.proposalMessageIndex)).toEqual([0, 1])
    expect(events.map((event) => event.messageIndex)).toEqual([1, 1])
  })

  it('returns nothing when the block executed no proposal', async () => {
    tables.event = []
    expect(await buildProposalExecEventsForTest(60)).toEqual([])
  })
})
