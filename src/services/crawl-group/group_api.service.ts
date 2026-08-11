import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended'
import type { Knex } from 'knex'
import { Context, ServiceBroker } from 'moleculer'
import BaseService from '../../base/base.service'
import { SERVICE } from '../../common'
import ApiResponder from '../../common/utils/apiResponse'
import { getBlockChainTimeAsOf } from '../../common/utils/block_time'
import { getBlockHeight } from '../../common/utils/blockHeight'
import { dateToIsoOrNull } from '../../common/utils/date_utils'
import knex from '../../common/utils/db_connection'
import { compareById, parseCorporationListPagination } from '../crawl-co/co_stats'
import { normalizeTallyResult, PROPOSAL_STATUSES } from './group_helpers'

type GroupMember = { address: string; weight: string; metadata: string; added_at: string | null }

function serializeMember(member: Record<string, unknown>): GroupMember {
  return {
    address: String(member.address ?? ''),
    weight: String(member.weight ?? '0'),
    metadata: member.metadata != null ? String(member.metadata) : '',
    added_at: dateToIsoOrNull(member.added_at ?? member.created ?? null),
  }
}

function serializeCorporationGroup(row: Record<string, unknown>, members: GroupMember[]) {
  return {
    corporation_id: Number(row.corporation_id),
    group_id: Number(row.group_id),
    version: Number(row.group_version),
    total_weight: String(row.total_weight ?? '0'),
    created_at: dateToIsoOrNull(row.created ?? null),
    policy: {
      address: String(row.policy_address ?? ''),
      version: Number(row.policy_version),
      decision_policy: (row.decision_policy as Record<string, unknown> | null) ?? null,
    },
    members,
  }
}

function serializeProposal(row: Record<string, unknown>, tally: Record<string, string>) {
  const finalTally = row.final_tally_result as Record<string, unknown> | null
  return {
    id: Number(row.id),
    corporation_id: Number(row.corporation_id),
    group_policy_address: String(row.group_policy_address ?? ''),
    metadata: row.metadata != null ? String(row.metadata) : '',
    proposers: Array.isArray(row.proposers) ? row.proposers : [],
    submit_time: dateToIsoOrNull(row.submit_time ?? null),
    group_version: row.group_version != null ? Number(row.group_version) : null,
    group_policy_version: row.group_policy_version != null ? Number(row.group_policy_version) : null,
    status: String(row.status ?? 'SUBMITTED'),
    voting_period_end: dateToIsoOrNull(row.voting_period_end ?? null),
    executor_result: String(row.executor_result ?? 'NOT_RUN'),
    messages: Array.isArray(row.messages) ? row.messages : [],
    final_tally_result: finalTally ?? null,
    tally,
  }
}

function serializeVote(row: Record<string, unknown>) {
  return {
    id: Number(row.id),
    proposal_id: Number(row.proposal_id),
    voter: String(row.voter ?? ''),
    option: String(row.option ?? ''),
    metadata: row.metadata != null ? String(row.metadata) : '',
    submit_time: dateToIsoOrNull(row.submit_time ?? null),
  }
}

@Service({
  name: SERVICE.V1.GroupApiService.key,
  version: 1,
})
export default class GroupApiService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker)
  }

  private latestGroupHistoryQuery(blockHeight: number) {
    const latest = knex('corporation_group_history')
      .distinctOn('corporation_id')
      .select('*')
      .where('height', '<=', blockHeight)
      .orderBy('corporation_id', 'asc')
      .orderBy('height', 'desc')
    return knex.from(latest.as('gh')).select('*')
  }

  private latestProposalHistoryQuery(blockHeight: number) {
    const latest = knex('group_proposal_history')
      .distinctOn('proposal_id')
      .select('*')
      .where('height', '<=', blockHeight)
      .orderBy('proposal_id', 'asc')
      .orderBy('height', 'desc')
    return knex.from(latest.as('ph'))
  }

  // Weighted running tally per proposal (x/group semantics: counts are member-weight sums).
  private async computeTallies(
    proposals: Array<Record<string, unknown>>,
    blockHeight: number | undefined
  ): Promise<Map<number, Record<string, string>>> {
    const result = new Map<number, Record<string, string>>()
    if (proposals.length === 0) return result
    const proposalIds = proposals.map((proposal) => Number(proposal.id))

    let votesQuery = knex('group_vote').select('proposal_id', 'voter', 'option').whereIn('proposal_id', proposalIds)
    if (blockHeight !== undefined) votesQuery = votesQuery.where('height', '<=', blockHeight)
    const votes = await votesQuery

    const corporationIds = [...new Set(proposals.map((proposal) => Number(proposal.corporation_id)))]
    const weightByCorporation = new Map<number, Map<string, number>>()
    if (blockHeight !== undefined) {
      const historyRows = await this.latestGroupHistoryQuery(blockHeight).whereIn('gh.corporation_id', corporationIds)
      for (const row of historyRows) {
        const members = Array.isArray(row.members) ? (row.members as Array<Record<string, unknown>>) : []
        weightByCorporation.set(
          Number(row.corporation_id),
          new Map(members.map((member) => [String(member.address), Number(member.weight) || 0]))
        )
      }
    } else {
      const memberRows = await knex('corporation_member')
        .select('corporation_id', 'address', 'weight')
        .whereIn('corporation_id', corporationIds)
      for (const member of memberRows) {
        const bucket = weightByCorporation.get(Number(member.corporation_id)) ?? new Map<string, number>()
        bucket.set(String(member.address), Number(member.weight) || 0)
        weightByCorporation.set(Number(member.corporation_id), bucket)
      }
    }

    for (const proposal of proposals) {
      const proposalId = Number(proposal.id)
      // Spec: once a proposal closes, tally equals the chain's final_tally_result — never recompute it
      // from current weights, which drift as membership changes.
      const finalTally = normalizeTallyResult(proposal.final_tally_result)
      if (String(proposal.status) !== 'SUBMITTED' && finalTally) {
        result.set(proposalId, finalTally)
        continue
      }
      const weights = weightByCorporation.get(Number(proposal.corporation_id)) ?? new Map<string, number>()
      const sums: Record<string, number> = { YES: 0, NO: 0, ABSTAIN: 0, NO_WITH_VETO: 0 }
      for (const vote of votes.filter((v) => Number(v.proposal_id) === proposalId)) {
        if (sums[vote.option] !== undefined) sums[vote.option] += weights.get(String(vote.voter)) ?? 0
      }
      result.set(proposalId, {
        yes_count: String(sums.YES),
        no_count: String(sums.NO),
        abstain_count: String(sums.ABSTAIN),
        no_with_veto_count: String(sums.NO_WITH_VETO),
      })
    }
    return result
  }

  // IDX-GR-QRY-1 Get Corporation Group
  @Action({
    rest: 'GET get/:corporation_id',
    params: { corporation_id: { type: 'number', integer: true, positive: true, convert: true } },
  })
  public async getCorporationGroup(ctx: Context<{ corporation_id: number }>) {
    try {
      const corporationId = ctx.params.corporation_id
      const blockHeight = getBlockHeight(ctx)

      if (blockHeight !== undefined) {
        const row = await this.latestGroupHistoryQuery(blockHeight).where('gh.corporation_id', corporationId).first()
        if (!row) return ApiResponder.error(ctx, `Corporation ${corporationId} group not found`, 404)
        // policy_address and created are immutable, so they come from the live row; history carries neither.
        const live = await knex('corporation_group')
          .select('created', 'policy_address')
          .where('corporation_id', corporationId)
          .first()
        const members = (Array.isArray(row.members) ? row.members : []).map(serializeMember)
        return ApiResponder.success(ctx, {
          group: serializeCorporationGroup(
            { ...row, created: live?.created ?? null, policy_address: live?.policy_address ?? null },
            members
          ),
        })
      }

      const row = await knex('corporation_group').where('corporation_id', corporationId).first()
      if (!row) return ApiResponder.error(ctx, `Corporation ${corporationId} group not found`, 404)
      const members = await knex('corporation_member')
        .select('address', 'weight', 'metadata', 'created')
        .where('corporation_id', corporationId)
        .orderBy('id', 'asc')
      return ApiResponder.success(ctx, {
        group: serializeCorporationGroup(row, members.map(serializeMember)),
      })
    } catch (err) {
      this.logger.error('Error in Group.getCorporationGroup:', err)
      return ApiResponder.error(ctx, 'Internal Server Error', 500)
    }
  }

  // IDX-GR-QRY-2 List Corporations By Member
  @Action({
    rest: 'GET corporations-by-member',
    params: {
      account: { type: 'string' },
      limit: { type: 'string', optional: true, convert: true },
      min_id: { type: 'string', optional: true, convert: true },
      max_id: { type: 'string', optional: true, convert: true },
      sort: { type: 'string', optional: true },
    },
  })
  public async listCorporationsByMember(
    ctx: Context<{ account: string; limit?: string; min_id?: string; max_id?: string; sort?: string }>
  ) {
    try {
      const account = String(ctx.params.account ?? '').trim()
      if (!account) return ApiResponder.error(ctx, '"account" is required', 400)

      const pageParsed = parseCorporationListPagination(ctx.params)
      if (!pageParsed.ok) return ApiResponder.error(ctx, pageParsed.message, 400)
      const { limit, minId, maxId, direction } = pageParsed.value
      const blockHeight = getBlockHeight(ctx)

      if (blockHeight !== undefined) {
        const rows = await this.latestGroupHistoryQuery(blockHeight).whereRaw('gh.members @> ?::jsonb', [
          JSON.stringify([{ address: account }]),
        ])
        const memberships = rows
          .map((row) => {
            const members = Array.isArray(row.members) ? (row.members as Array<Record<string, unknown>>) : []
            const member = members.find((entry) => String(entry.address) === account)
            if (!member) return null
            return {
              id: Number(row.corporation_id),
              corporation_id: Number(row.corporation_id),
              ...serializeMember(member),
            }
          })
          .filter(Boolean) as Array<{ id: number; corporation_id: number } & GroupMember>
        const paged = memberships
          .filter(
            (entry) =>
              (minId === undefined || entry.id >= Number(minId)) && (maxId === undefined || entry.id < Number(maxId))
          )
          .sort((a, b) => compareById(a.id, b.id, direction))
          .slice(0, limit)
          .map(({ id: _id, address: _address, ...rest }) => rest)
        return ApiResponder.success(ctx, { memberships: paged })
      }

      let query = knex('corporation_member as cm')
        .innerJoin('corporation_group as cg', 'cg.corporation_id', 'cm.corporation_id')
        .where('cm.address', account)
        .select('cm.corporation_id', 'cm.weight', 'cm.metadata', 'cm.created')
      if (minId !== undefined) query = query.where('cm.corporation_id', '>=', minId)
      if (maxId !== undefined) query = query.where('cm.corporation_id', '<', maxId)
      const rows = await query.orderBy('cm.corporation_id', direction).limit(limit)

      return ApiResponder.success(ctx, {
        memberships: rows.map((row) => {
          const { address: _address, ...rest } = serializeMember(row)
          return { corporation_id: Number(row.corporation_id), ...rest }
        }),
      })
    } catch (err) {
      this.logger.error('Error in Group.listCorporationsByMember:', err)
      return ApiResponder.error(ctx, 'Internal Server Error', 500)
    }
  }

  // IDX-GR-QRY-3 List Proposals
  @Action({
    rest: 'GET proposals',
    params: {
      corporation_id: { type: 'number', integer: true, positive: true, optional: true, convert: true },
      status: { type: 'string', optional: true },
      proposer: { type: 'string', optional: true },
      pending_voter: { type: 'string', optional: true },
      modified_after: { type: 'string', optional: true },
      limit: { type: 'string', optional: true, convert: true },
      min_id: { type: 'string', optional: true, convert: true },
      max_id: { type: 'string', optional: true, convert: true },
      sort: { type: 'string', optional: true },
    },
  })
  public async listProposals(
    ctx: Context<{
      corporation_id?: number
      status?: string
      proposer?: string
      pending_voter?: string
      modified_after?: string
      limit?: string
      min_id?: string
      max_id?: string
      sort?: string
    }>
  ) {
    try {
      const p = ctx.params
      const pageParsed = parseCorporationListPagination(p)
      if (!pageParsed.ok) return ApiResponder.error(ctx, pageParsed.message, 400)
      const { limit, minId, maxId, direction } = pageParsed.value

      if (p.status !== undefined && !(PROPOSAL_STATUSES as readonly string[]).includes(p.status)) {
        return ApiResponder.error(ctx, `"status" must be one of ${PROPOSAL_STATUSES.join(', ')}`, 400)
      }
      let modifiedAfterIso: string | undefined
      if (p.modified_after) {
        const ts = new Date(p.modified_after)
        if (Number.isNaN(ts.getTime())) {
          return ApiResponder.error(ctx, '"modified_after" must be a valid ISO 8601 datetime', 400)
        }
        modifiedAfterIso = ts.toISOString()
      }

      const blockHeight = getBlockHeight(ctx)
      const pendingVoter = p.pending_voter?.trim() || undefined

      // All four pending_voter predicates run in SQL so the cursors and limit bound the scan.
      const pendingVoterNowIso =
        pendingVoter && blockHeight !== undefined
          ? (await getBlockChainTimeAsOf(blockHeight, { logger: this.logger })).toISOString()
          : new Date().toISOString()

      let candidates: Array<Record<string, unknown>>
      if (blockHeight !== undefined) {
        let query = this.latestProposalHistoryQuery(blockHeight).select('ph.proposal_id', 'ph.snapshot')
        if (p.corporation_id !== undefined) {
          query = query.whereRaw("(ph.snapshot->>'corporation_id')::bigint = ?", [p.corporation_id])
        }
        if (p.status) query = query.whereRaw("ph.snapshot->>'status' = ?", [p.status])
        if (p.proposer) query = query.whereRaw("ph.snapshot->'proposers' @> ?::jsonb", [JSON.stringify([p.proposer])])
        if (modifiedAfterIso) {
          query = query.whereRaw("(ph.snapshot->>'modified')::timestamptz > ?::timestamptz", [modifiedAfterIso])
        }
        if (pendingVoter) {
          query = query
            .whereRaw("ph.snapshot->>'status' = ?", ['SUBMITTED'])
            .whereRaw("(ph.snapshot->>'voting_period_end')::timestamptz > ?::timestamptz", [pendingVoterNowIso])
            .whereNotExists((qb: Knex.QueryBuilder) =>
              qb
                .select(knex.raw('1'))
                .from('group_vote as gv')
                .whereRaw('gv.proposal_id = ph.proposal_id')
                .where('gv.voter', pendingVoter)
                .where('gv.height', '<=', blockHeight)
            )
            .whereExists((qb: Knex.QueryBuilder) =>
              qb
                .select(knex.raw('1'))
                .from(
                  knex.raw(
                    '(select distinct on (corporation_id) corporation_id, members from corporation_group_history where height <= ? order by corporation_id asc, height desc) as gh',
                    [blockHeight]
                  )
                )
                .whereRaw("gh.corporation_id = (ph.snapshot->>'corporation_id')::bigint")
                .whereRaw('gh.members @> ?::jsonb', [JSON.stringify([{ address: pendingVoter }])])
            )
        }
        if (minId !== undefined) query = query.where('ph.proposal_id', '>=', minId)
        if (maxId !== undefined) query = query.where('ph.proposal_id', '<', maxId)
        query = query.orderBy('ph.proposal_id', direction).limit(limit)
        const rows = await query
        candidates = rows.map((row: Record<string, unknown>) => ({
          ...(row.snapshot as Record<string, unknown>),
          id: Number(row.proposal_id),
        }))
      } else {
        let query = knex('group_proposal').select('*')
        if (p.corporation_id !== undefined) query = query.where('corporation_id', p.corporation_id)
        if (p.status) query = query.where('status', p.status)
        if (p.proposer) query = query.whereRaw('proposers @> ?::jsonb', [JSON.stringify([p.proposer])])
        if (modifiedAfterIso) query = query.where('modified', '>', modifiedAfterIso)
        if (pendingVoter) {
          query = query
            .where('status', 'SUBMITTED')
            .whereNotNull('voting_period_end')
            .where('voting_period_end', '>', pendingVoterNowIso)
            .whereNotExists((qb: Knex.QueryBuilder) =>
              qb
                .select(knex.raw('1'))
                .from('group_vote as gv')
                .whereRaw('gv.proposal_id = group_proposal.id')
                .where('gv.voter', pendingVoter)
            )
            .whereExists((qb: Knex.QueryBuilder) =>
              qb
                .select(knex.raw('1'))
                .from('corporation_member as cm')
                .whereRaw('cm.corporation_id = group_proposal.corporation_id')
                .where('cm.address', pendingVoter)
            )
        }
        if (minId !== undefined) query = query.where('id', '>=', minId)
        if (maxId !== undefined) query = query.where('id', '<', maxId)
        query = query.orderBy('id', direction).limit(limit)
        candidates = await query
      }

      const tallies = await this.computeTallies(candidates, blockHeight)
      return ApiResponder.success(ctx, {
        proposals: candidates.map((proposal) =>
          serializeProposal(proposal, tallies.get(Number(proposal.id)) ?? tallyFromNothing())
        ),
      })
    } catch (err) {
      this.logger.error('Error in Group.listProposals:', err)
      return ApiResponder.error(ctx, 'Internal Server Error', 500)
    }
  }

  // IDX-GR-QRY-4 Get Proposal
  @Action({
    rest: 'GET proposal/:id',
    params: { id: { type: 'number', integer: true, positive: true, convert: true } },
  })
  public async getProposal(ctx: Context<{ id: number }>) {
    try {
      const { id } = ctx.params
      const blockHeight = getBlockHeight(ctx)

      let row: Record<string, unknown> | undefined
      if (blockHeight !== undefined) {
        const history = await this.latestProposalHistoryQuery(blockHeight)
          .select('ph.proposal_id', 'ph.snapshot')
          .where('ph.proposal_id', id)
          .first()
        if (history) row = { ...(history.snapshot as Record<string, unknown>), id: Number(history.proposal_id) }
      } else {
        row = await knex('group_proposal').where('id', id).first()
      }
      if (!row) return ApiResponder.error(ctx, `Proposal ${id} not found`, 404)

      const tallies = await this.computeTallies([row], blockHeight)
      return ApiResponder.success(ctx, {
        proposal: serializeProposal(row, tallies.get(Number(row.id)) ?? tallyFromNothing()),
      })
    } catch (err) {
      this.logger.error('Error in Group.getProposal:', err)
      return ApiResponder.error(ctx, 'Internal Server Error', 500)
    }
  }

  // IDX-GR-QRY-5 List Votes
  @Action({
    rest: 'GET votes',
    params: {
      proposal_id: { type: 'number', integer: true, positive: true, optional: true, convert: true },
      voter: { type: 'string', optional: true },
      limit: { type: 'string', optional: true, convert: true },
      min_id: { type: 'string', optional: true, convert: true },
      max_id: { type: 'string', optional: true, convert: true },
      sort: { type: 'string', optional: true },
    },
  })
  public async listVotes(
    ctx: Context<{
      proposal_id?: number
      voter?: string
      limit?: string
      min_id?: string
      max_id?: string
      sort?: string
    }>
  ) {
    try {
      const p = ctx.params
      const voter = p.voter?.trim() || undefined
      if (p.proposal_id === undefined && !voter) {
        return ApiResponder.error(ctx, 'At least one of "proposal_id" and "voter" must be provided', 400)
      }
      const pageParsed = parseCorporationListPagination(p)
      if (!pageParsed.ok) return ApiResponder.error(ctx, pageParsed.message, 400)
      const { limit, minId, maxId, direction } = pageParsed.value
      const blockHeight = getBlockHeight(ctx)

      let query = knex('group_vote').select('*')
      if (p.proposal_id !== undefined) query = query.where('proposal_id', p.proposal_id)
      if (voter) query = query.where('voter', voter)
      if (blockHeight !== undefined) query = query.where('height', '<=', blockHeight)
      if (minId !== undefined) query = query.where('id', '>=', minId)
      if (maxId !== undefined) query = query.where('id', '<', maxId)
      const rows = await query.orderBy('id', direction).limit(limit)

      return ApiResponder.success(ctx, { votes: rows.map(serializeVote) })
    } catch (err) {
      this.logger.error('Error in Group.listVotes:', err)
      return ApiResponder.error(ctx, 'Internal Server Error', 500)
    }
  }
}

function tallyFromNothing(): Record<string, string> {
  return { yes_count: '0', no_count: '0', abstain_count: '0', no_with_veto_count: '0' }
}
