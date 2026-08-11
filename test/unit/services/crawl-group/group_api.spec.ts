const tables: Record<string, unknown[]> = {}
const calls: Array<{ table: string; method: string; args: unknown[] }> = []

// Records every builder call so tests can assert the filters actually reach the query,
// and applies the filters it can evaluate in-memory so results reflect them.
function chainable(table: string, rows: unknown[]) {
  let current = [...rows]
  const qb: any = {}
  const record =
    (method: string) =>
    (...args: unknown[]) => {
      calls.push({ table, method, args })
      if (method === 'where' && args.length === 2 && typeof args[0] === 'string') {
        const column = String(args[0]).split('.').pop() as string
        current = current.filter((row: any) => String(row?.[column]) === String(args[1]))
      }
      if (method === 'whereIn' && args.length === 2 && Array.isArray(args[1])) {
        const column = String(args[0]).split('.').pop() as string
        const wanted = (args[1] as unknown[]).map(String)
        current = current.filter((row: any) => wanted.includes(String(row?.[column])))
      }
      if (method === 'limit' && typeof args[0] === 'number') current = current.slice(0, args[0])
      return qb
    }
  for (const method of [
    'select',
    'where',
    'whereIn',
    'whereRaw',
    'whereNotNull',
    'whereExists',
    'whereNotExists',
    'orderBy',
    'limit',
    'innerJoin',
    'distinctOn',
    'from',
    'as',
  ]) {
    qb[method] = record(method)
  }
  qb.first = jest.fn(async () => current[0])
  qb.then = (resolve: (v: unknown) => void, reject?: (e: unknown) => void) =>
    Promise.resolve(current).then(resolve, reject)
  return qb
}

jest.mock('../../../../src/common/utils/db_connection', () => ({
  __esModule: true,
  default: Object.assign((table: string) => chainable(table.split(' ')[0], tables[table.split(' ')[0]] ?? []), {
    from: () => chainable('__from', tables.__from ?? []),
    raw: (sql: string, bindings?: unknown) => ({ sql, bindings }),
  }),
}))

jest.mock('../../../../src/common/utils/apiResponse', () => ({
  __esModule: true,
  default: {
    success: jest.fn((_ctx: unknown, data: unknown) => data),
    error: jest.fn((_ctx: unknown, message: string, code: number) => ({ error: message, code })),
  },
}))

jest.mock('../../../../src/common/utils/block_time', () => ({
  getBlockChainTimeAsOf: jest.fn(async () => new Date('2026-08-10T16:00:00.000Z')),
}))

import { ServiceBroker } from 'moleculer'
import { getBlockChainTimeAsOf } from '../../../../src/common/utils/block_time'
import GroupApiService from '../../../../src/services/crawl-group/group_api.service'

function called(table: string, method: string, predicate: (args: unknown[]) => boolean) {
  return calls.some((call) => call.table === table && call.method === method && predicate(call.args))
}

function proposalRow(over: Record<string, unknown> = {}) {
  return {
    id: 4,
    corporation_id: 1,
    group_policy_address: 'verana1policy',
    metadata: '',
    proposers: ['verana1admin'],
    submit_time: '2026-08-10T15:45:00.000Z',
    group_version: 1,
    group_policy_version: 2,
    status: 'ACCEPTED',
    executor_result: 'SUCCESS',
    voting_period_end: '2026-08-10T15:50:00.000Z',
    messages: [{ '@type': '/verana.de.v1.MsgGrantOperatorAuthorization' }],
    final_tally_result: { yes_count: '2', no_count: '0', abstain_count: '0', no_with_veto_count: '0' },
    modified: '2026-08-10T15:46:00.000Z',
    height: 40,
    ...over,
  }
}

describe('GroupApiService', () => {
  const broker = new ServiceBroker({ logger: false })
  const service = new GroupApiService(broker)

  beforeEach(() => {
    jest.clearAllMocks()
    calls.length = 0
    for (const key of Object.keys(tables)) delete tables[key]
  })

  describe('IDX-GR-QRY-1 getCorporationGroup', () => {
    beforeEach(() => {
      tables.corporation_group = [
        {
          corporation_id: 1,
          group_id: 9,
          policy_address: 'verana1policy',
          group_version: 3,
          policy_version: 2,
          total_weight: '3',
          decision_policy: { '@type': '/cosmos.group.v1.ThresholdDecisionPolicy', threshold: '2' },
          created: '2026-08-10T15:44:00.000Z',
        },
      ]
      tables.corporation_member = [
        {
          corporation_id: 1,
          address: 'verana1admin',
          weight: '1',
          metadata: 'member_1',
          created: '2026-08-10T15:44:00.000Z',
        },
      ]
    })

    it('returns the spec shape with nested policy and members', async () => {
      const ctx: any = { params: { corporation_id: 1 }, meta: {} }
      const res: any = await service.getCorporationGroup(ctx)

      expect(res.group).toMatchObject({
        corporation_id: 1,
        group_id: 9,
        version: 3,
        total_weight: '3',
        policy: { address: 'verana1policy', version: 2 },
      })
      expect(res.group.members).toEqual([
        { address: 'verana1admin', weight: '1', metadata: 'member_1', added_at: '2026-08-10T15:44:00.000Z' },
      ])
    })

    it('404s when no group is anchored', async () => {
      tables.corporation_group = []
      const ctx: any = { params: { corporation_id: 42 }, meta: {} }
      expect(await service.getCorporationGroup(ctx)).toMatchObject({ code: 404 })
    })

    it('At-Block-Height serves the history snapshot and keeps policy.address from the live row', async () => {
      tables.__from = [
        {
          corporation_id: 1,
          group_id: 9,
          group_version: 2,
          policy_version: 1,
          total_weight: '2',
          decision_policy: { threshold: '2' },
          members: [{ address: 'verana1admin', weight: '1', metadata: '', added_at: '2026-08-10T15:44:00.000Z' }],
          height: 30,
        },
      ]
      const ctx: any = { params: { corporation_id: 1 }, meta: { blockHeight: 35 } }
      const res: any = await service.getCorporationGroup(ctx)

      expect(res.group).toMatchObject({
        version: 2,
        total_weight: '2',
        policy: { address: 'verana1policy', version: 1 },
      })
      expect(res.group.members).toHaveLength(1)
      expect(called('corporation_group_history', 'where', (args) => args[0] === 'height' && args[1] === '<='))
    })

    it('At-Block-Height 404s before the group existed', async () => {
      tables.__from = []
      const ctx: any = { params: { corporation_id: 1 }, meta: { blockHeight: 5 } }
      expect(await service.getCorporationGroup(ctx)).toMatchObject({ code: 404 })
    })
  })

  describe('IDX-GR-QRY-2 listCorporationsByMember', () => {
    it('rejects a missing/blank account', async () => {
      const ctx: any = { params: { account: '  ' }, meta: {} }
      expect(await service.listCorporationsByMember(ctx)).toMatchObject({ code: 400 })
    })

    it('filters by account and returns memberships without the address key', async () => {
      tables.corporation_member = [
        {
          corporation_id: 3,
          address: 'verana1member',
          weight: '1',
          metadata: 'member_2',
          created: '2026-08-10T15:44:00.000Z',
        },
        { corporation_id: 9, address: 'verana1other', weight: '1', metadata: '', created: '2026-08-10T15:44:00.000Z' },
      ]
      const ctx: any = { params: { account: 'verana1member' }, meta: {} }
      const res: any = await service.listCorporationsByMember(ctx)

      expect(res.memberships).toEqual([
        { corporation_id: 3, weight: '1', metadata: 'member_2', added_at: '2026-08-10T15:44:00.000Z' },
      ])
      expect(
        called('corporation_member', 'where', (args) => args[0] === 'cm.address' && args[1] === 'verana1member')
      ).toBe(true)
    })

    it('At-Block-Height resolves membership from the history snapshot', async () => {
      tables.__from = [
        {
          corporation_id: 7,
          members: [
            { address: 'verana1member', weight: '2', metadata: 'm', added_at: '2026-08-10T15:44:00.000Z' },
            { address: 'verana1other', weight: '1', metadata: '', added_at: '2026-08-10T15:44:00.000Z' },
          ],
        },
      ]
      const ctx: any = { params: { account: 'verana1member' }, meta: { blockHeight: 30 } }
      const res: any = await service.listCorporationsByMember(ctx)

      expect(res.memberships).toEqual([
        { corporation_id: 7, weight: '2', metadata: 'm', added_at: '2026-08-10T15:44:00.000Z' },
      ])
    })

    it('rejects invalid pagination', async () => {
      const ctx: any = { params: { account: 'verana1member', limit: '0' }, meta: {} }
      expect(await service.listCorporationsByMember(ctx)).toMatchObject({ code: 400 })
    })
  })

  describe('IDX-GR-QRY-3 listProposals', () => {
    it('rejects an unknown status and an invalid modified_after', async () => {
      expect(await service.listProposals({ params: { status: 'OPEN' }, meta: {} } as any)).toMatchObject({ code: 400 })
      expect(await service.listProposals({ params: { modified_after: 'nope' }, meta: {} } as any)).toMatchObject({
        code: 400,
      })
    })

    it('pushes corporation_id, status, proposer and modified_after into the query', async () => {
      tables.group_proposal = []
      const ctx: any = {
        params: {
          corporation_id: 1,
          status: 'SUBMITTED',
          proposer: 'verana1admin',
          modified_after: '2026-08-01T00:00:00Z',
        },
        meta: {},
      }
      await service.listProposals(ctx)

      expect(called('group_proposal', 'where', (args) => args[0] === 'corporation_id' && args[1] === 1)).toBe(true)
      expect(called('group_proposal', 'where', (args) => args[0] === 'status' && args[1] === 'SUBMITTED')).toBe(true)
      expect(
        called(
          'group_proposal',
          'whereRaw',
          (args) => String(args[0]).includes('proposers') && JSON.stringify(args[1]).includes('verana1admin')
        )
      ).toBe(true)
      expect(
        called(
          'group_proposal',
          'where',
          (args) => args[0] === 'modified' && args[1] === '>' && args[2] === '2026-08-01T00:00:00.000Z'
        )
      ).toBe(true)
    })

    it('closed proposals report tally equal to final_tally_result, ignoring current weights', async () => {
      tables.group_proposal = [proposalRow()]
      tables.group_vote = []
      tables.corporation_member = [{ corporation_id: 1, address: 'verana1admin', weight: '5' }]

      const res: any = await service.listProposals({ params: {}, meta: {} } as any)
      expect(res.proposals[0].tally).toEqual(res.proposals[0].final_tally_result)
      expect(res.proposals[0].tally.yes_count).toBe('2')
    })

    it('open proposals report a weight-summed running tally', async () => {
      tables.group_proposal = [
        proposalRow({ status: 'SUBMITTED', executor_result: 'NOT_RUN', final_tally_result: null }),
      ]
      tables.group_vote = [
        { proposal_id: 4, voter: 'verana1heavy', option: 'YES' },
        { proposal_id: 4, voter: 'verana1light', option: 'NO' },
      ]
      tables.corporation_member = [
        { corporation_id: 1, address: 'verana1heavy', weight: '7' },
        { corporation_id: 1, address: 'verana1light', weight: '2' },
      ]

      const res: any = await service.listProposals({ params: {}, meta: {} } as any)
      expect(res.proposals[0].tally).toEqual({
        yes_count: '7',
        no_count: '2',
        abstain_count: '0',
        no_with_veto_count: '0',
      })
    })

    it('serializes the spec proposal shape including decoded messages', async () => {
      tables.group_proposal = [proposalRow()]
      const res: any = await service.listProposals({ params: {}, meta: {} } as any)
      expect(res.proposals[0]).toMatchObject({
        id: 4,
        corporation_id: 1,
        group_policy_address: 'verana1policy',
        status: 'ACCEPTED',
        executor_result: 'SUCCESS',
        group_policy_version: 2,
      })
      expect(res.proposals[0].messages).toEqual([{ '@type': '/verana.de.v1.MsgGrantOperatorAuthorization' }])
    })

    describe('pending_voter', () => {
      const subBuilder = () => {
        const sub: any = {}
        for (const m of ['select', 'from', 'where', 'whereRaw']) sub[m] = jest.fn(() => sub)
        return sub
      }
      const runSubquery = (method: string) => {
        const call = calls.find((c) => c.method === method)
        expect(call).toBeDefined()
        const sub = subBuilder()
        ;(call!.args[0] as (qb: unknown) => void)(sub)
        return sub
      }

      it('pushes status and voting-window predicates into SQL', async () => {
        tables.group_proposal = []
        await service.listProposals({ params: { pending_voter: 'verana1voter' }, meta: {} } as any)

        expect(called('group_proposal', 'where', (args) => args[0] === 'status' && args[1] === 'SUBMITTED')).toBe(true)
        expect(called('group_proposal', 'whereNotNull', (args) => args[0] === 'voting_period_end')).toBe(true)
        expect(called('group_proposal', 'where', (args) => args[0] === 'voting_period_end' && args[1] === '>')).toBe(
          true
        )
      })

      it('excludes proposals the account already voted on via NOT EXISTS on group_vote', async () => {
        tables.group_proposal = []
        await service.listProposals({ params: { pending_voter: 'verana1voter' }, meta: {} } as any)

        const sub = runSubquery('whereNotExists')
        expect(sub.from).toHaveBeenCalledWith('group_vote as gv')
        expect(sub.where).toHaveBeenCalledWith('gv.voter', 'verana1voter')
        // correlated to the outer row, else it degenerates into "has this account voted on anything"
        expect(sub.whereRaw).toHaveBeenCalledWith('gv.proposal_id = group_proposal.id')
      })

      it('requires current membership via EXISTS on corporation_member', async () => {
        tables.group_proposal = []
        await service.listProposals({ params: { pending_voter: 'verana1voter' }, meta: {} } as any)

        const sub = runSubquery('whereExists')
        expect(sub.from).toHaveBeenCalledWith('corporation_member as cm')
        expect(sub.where).toHaveBeenCalledWith('cm.address', 'verana1voter')
        expect(sub.whereRaw).toHaveBeenCalledWith('cm.corporation_id = group_proposal.corporation_id')
      })

      it('still applies cursors, sort and limit in SQL', async () => {
        tables.group_proposal = []
        await service.listProposals({
          params: { pending_voter: 'verana1voter', min_id: '5', max_id: '9', limit: '3', sort: '+id' },
          meta: {},
        } as any)

        expect(
          called('group_proposal', 'where', (args) => args[0] === 'id' && args[1] === '>=' && args[2] === '5')
        ).toBe(true)
        expect(
          called('group_proposal', 'where', (args) => args[0] === 'id' && args[1] === '<' && args[2] === '9')
        ).toBe(true)
        expect(called('group_proposal', 'orderBy', (args) => args[0] === 'id' && args[1] === 'asc')).toBe(true)
        expect(called('group_proposal', 'limit', (args) => args[0] === 3)).toBe(true)
      })

      it('at height, bounds votes by block height and reads membership from the history snapshot', async () => {
        tables.__from = []
        await service.listProposals({
          params: { pending_voter: 'verana1voter' },
          meta: { blockHeight: 50 },
        } as any)

        expect(getBlockChainTimeAsOf).toHaveBeenCalledWith(50, expect.anything())
        const votes = runSubquery('whereNotExists')
        expect(votes.where).toHaveBeenCalledWith('gv.height', '<=', 50)
        expect(votes.whereRaw).toHaveBeenCalledWith('gv.proposal_id = ph.proposal_id')
        const members = runSubquery('whereExists')
        expect(members.whereRaw).toHaveBeenCalledWith('gh.members @> ?::jsonb', [
          JSON.stringify([{ address: 'verana1voter' }]),
        ])
        expect(called('__from', 'limit', (args) => typeof args[0] === 'number')).toBe(true)
      })

      it('does not query chain time when pending_voter is absent', async () => {
        tables.__from = []
        await service.listProposals({ params: {}, meta: { blockHeight: 50 } } as any)
        expect(getBlockChainTimeAsOf).not.toHaveBeenCalled()
      })
    })

    it('At-Block-Height reads proposals from the history snapshots', async () => {
      tables.__from = [{ proposal_id: 4, snapshot: proposalRow({ status: 'SUBMITTED', final_tally_result: null }) }]
      tables.group_vote = []
      const ctx: any = { params: {}, meta: { blockHeight: 50 } }
      const res: any = await service.listProposals(ctx)

      expect(res.proposals.map((p: any) => p.id)).toEqual([4])
      expect(called('group_proposal_history', 'where', (args) => args[0] === 'height' && args[1] === '<=')).toBe(true)
    })
  })

  describe('IDX-GR-QRY-4 getProposal', () => {
    it('404s for unknown proposals', async () => {
      tables.group_proposal = []
      expect(await service.getProposal({ params: { id: 999 }, meta: {} } as any)).toMatchObject({ code: 404 })
    })

    it('returns the proposal with its tally', async () => {
      tables.group_proposal = [proposalRow()]
      const res: any = await service.getProposal({ params: { id: 4 }, meta: {} } as any)
      expect(res.proposal).toMatchObject({ id: 4, status: 'ACCEPTED' })
      expect(res.proposal.tally).toEqual(res.proposal.final_tally_result)
    })
  })

  describe('IDX-GR-QRY-5 listVotes', () => {
    it('requires proposal_id or voter', async () => {
      expect(await service.listVotes({ params: {}, meta: {} } as any)).toMatchObject({ code: 400 })
    })

    it('serializes votes with the indexer-assigned cursor id', async () => {
      tables.group_vote = [
        {
          id: 11,
          proposal_id: 4,
          voter: 'verana1admin',
          option: 'YES',
          metadata: null,
          submit_time: '2026-08-10T15:45:30.000Z',
        },
      ]
      const res: any = await service.listVotes({ params: { proposal_id: 4 }, meta: {} } as any)
      expect(res.votes).toEqual([
        {
          id: 11,
          proposal_id: 4,
          voter: 'verana1admin',
          option: 'YES',
          metadata: '',
          submit_time: '2026-08-10T15:45:30.000Z',
        },
      ])
    })

    it('filters by voter and bounds votes by At-Block-Height', async () => {
      tables.group_vote = []
      const ctx: any = { params: { voter: 'verana1admin' }, meta: { blockHeight: 40 } }
      await service.listVotes(ctx)
      expect(called('group_vote', 'where', (args) => args[0] === 'voter' && args[1] === 'verana1admin')).toBe(true)
      expect(called('group_vote', 'where', (args) => args[0] === 'height' && args[1] === '<=' && args[2] === 40)).toBe(
        true
      )
    })

    it('rejects bad pagination with 400', async () => {
      expect(await service.listVotes({ params: { proposal_id: 4, limit: '0' }, meta: {} } as any)).toMatchObject({
        code: 400,
      })
    })
  })
})
