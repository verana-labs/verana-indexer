import { fromBase64 } from '@cosmjs/encoding'
import { Registry } from '@cosmjs/proto-signing'
import { defaultRegistryTypes } from '@cosmjs/stargate'
import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended'
import { ServiceBroker } from 'moleculer'
import BullableService from '../../base/bullable.service'
import { BULL_JOB_NAME, SERVICE } from '../../common'
import { getLcdBaseUrl } from '../../common/utils/account_balance_utils'
import { CheckpointManager } from '../../common/utils/checkpoint_manager'
import knex from '../../common/utils/db_connection'
import { detectStartMode } from '../../common/utils/start_mode_detector'
import { veranaRegistry } from '../../common/utils/veranaChain.client'
import config from '../../config.json' with { type: 'json' }
import { BlockCheckpoint } from '../../models/block_checkpoint'
import { indexerStatusManager } from '../manager/indexer_status.manager'
import {
  type AnyMessage,
  anyTypeUrl,
  decideProposalOutcome,
  GROUP_EVENT_TYPES,
  GROUP_MSG_TYPES,
  isUndecodedAny,
  normalizeTallyResult,
  parseEventAttrJson,
  parseEventAttrValue,
  readPositiveInt,
  shortExecutorResult,
  shortProposalStatus,
  shortVoteOption,
  votingPeriodSecondsFromDecisionPolicy,
} from './group_helpers'

const LCD_TIMEOUT_MS = 8000

type MessageRow = {
  tx_id: number
  message_index: number
  message_type: string
  sender: string | null
  content: Record<string, unknown>
  block_height: number
  tx_hash: string
  timestamp: Date | string
}

type GroupEventRow = {
  event_id: number
  type: string
  tx_id: number | null
  tx_msg_index: number | null
  block_height: number
  attrs: Record<string, string>
}

@Service({
  name: SERVICE.V1.GroupProcessorService.key,
  version: 1,
})
export default class GroupProcessorService extends BullableService {
  private timer: NodeJS.Timeout | null = null
  private checkpointManager: CheckpointManager
  private isProcessing = false
  private decodeRegistry = new Registry([...defaultRegistryTypes, ...veranaRegistry] as ConstructorParameters<
    typeof Registry
  >[0])

  public constructor(public broker: ServiceBroker) {
    super(broker)
    this.checkpointManager = new CheckpointManager(this.logger)
  }

  public async started() {
    try {
      await detectStartMode(BULL_JOB_NAME.HANDLE_GROUP)
      const [startBlock] = await BlockCheckpoint.getCheckpoint(BULL_JOB_NAME.HANDLE_GROUP, [])
      await this.checkpointManager.ensureCheckpoint(BULL_JOB_NAME.HANDLE_GROUP, startBlock || 0)
      const crawlInterval = config.crawlGroup.millisecondCrawl

      this.processBlocks().catch((err) => {
        this.logger.error('[GroupProcessorService] Error in initial processBlocks:', err)
      })
      this.timer = setInterval(() => {
        if (!indexerStatusManager.isCrawlingActive()) return
        if (!this.isProcessing) {
          this.processBlocks().catch((err) => {
            this.logger.error('[GroupProcessorService] Error in scheduled processBlocks:', err)
          })
        }
      }, crawlInterval)
      this.logger.info(`[GroupProcessorService] Service started | Interval: ${crawlInterval}ms`)
    } catch (err) {
      this.logger.error('[GroupProcessorService] Failed to start', err)
    }
  }

  public async stopped() {
    if (this.timer) clearInterval(this.timer)
  }

  @Action({ name: 'processBlocks' })
  public async processBlocks() {
    if (this.isProcessing) return
    if (!indexerStatusManager.isCrawlingActive()) return
    this.isProcessing = true
    try {
      const jobName = BULL_JOB_NAME.HANDLE_GROUP
      const [startBlock, dependencyHeight] = await BlockCheckpoint.getCheckpoint(jobName, [
        BULL_JOB_NAME.HANDLE_TRANSACTION,
      ])
      const chunkSize = config.crawlGroup.chunkSize || 500
      const endBlock = Math.min(dependencyHeight, startBlock + chunkSize)
      if (endBlock > startBlock) {
        await this.processRange(startBlock, endBlock)
        await this.checkpointManager.updateCheckpoint(jobName, endBlock, { logger: this.logger })
      }
      await this.healMissingDecisionPolicies()
      await this.reconcileOpenProposals(endBlock > startBlock ? endBlock : startBlock)
    } catch (error) {
      this.logger.error('[GroupProcessorService] Fatal error in processBlocks:', error)
    } finally {
      this.isProcessing = false
    }
  }

  private async processRange(startBlock: number, endBlock: number) {
    const messages = (await knex('transaction_message as tm')
      .innerJoin('transaction as tx', 'tx.id', 'tm.tx_id')
      .where('tx.code', 0)
      .where('tx.height', '>', startBlock)
      .where('tx.height', '<=', endBlock)
      .whereIn('tm.type', [
        GROUP_MSG_TYPES.CreateCorporation,
        GROUP_MSG_TYPES.SubmitProposal,
        GROUP_MSG_TYPES.Vote,
        GROUP_MSG_TYPES.WithdrawProposal,
      ])
      .select(
        'tm.tx_id',
        'tm.index as message_index',
        'tm.type as message_type',
        'tm.sender',
        'tm.content',
        'tx.height as block_height',
        'tx.hash as tx_hash',
        'tx.timestamp'
      )
      .orderBy('tx.height', 'asc')
      .orderBy('tx.index', 'asc')
      .orderBy('tm.index', 'asc')) as MessageRow[]

    const events = await this.loadGroupEvents(startBlock, endBlock)
    if (messages.length === 0 && events.length === 0) return

    const heights = [...new Set([...messages.map((m) => m.block_height), ...events.map((e) => e.block_height)])]
    const blockTimes = new Map<number, string>()
    if (heights.length > 0) {
      const rows = await knex('block').select('height', 'time').whereIn('height', heights)
      for (const row of rows) blockTimes.set(Number(row.height), new Date(row.time).toISOString())
    }

    const byHeight = new Map<number, { messages: MessageRow[]; events: GroupEventRow[] }>()
    for (const height of heights.sort((a, b) => a - b)) byHeight.set(height, { messages: [], events: [] })
    for (const message of messages) byHeight.get(message.block_height)?.messages.push(message)
    for (const event of events) byHeight.get(event.block_height)?.events.push(event)

    for (const [height, bucket] of byHeight) {
      const blockTime = blockTimes.get(height) ?? new Date().toISOString()
      // Group/policy ids created in this block: their admin-handover update events are already covered by the create seed.
      const createdGroupIds = new Set<string>()
      const createTxIds = new Set(
        bucket.messages.filter((m) => m.message_type === GROUP_MSG_TYPES.CreateCorporation).map((m) => m.tx_id)
      )
      for (const event of bucket.events) {
        if (event.tx_id === null || !createTxIds.has(event.tx_id)) continue
        if (event.type === GROUP_EVENT_TYPES.CreateGroup) {
          const id = parseEventAttrValue(event.attrs.group_id)
          if (id) createdGroupIds.add(`g:${id}`)
        }
        if (event.type === GROUP_EVENT_TYPES.CreateGroupPolicy) {
          const address = parseEventAttrValue(event.attrs.address)
          if (address) createdGroupIds.add(`p:${address}`)
        }
      }
      for (const message of bucket.messages) {
        if (message.message_type === GROUP_MSG_TYPES.CreateCorporation) {
          await this.handleCreateCorporation(message, bucket.events, blockTime)
        }
      }
      for (const message of bucket.messages) {
        if (message.message_type === GROUP_MSG_TYPES.SubmitProposal) {
          await this.handleSubmitProposal(message, bucket.events, blockTime)
        }
      }
      for (const message of bucket.messages) {
        if (message.message_type === GROUP_MSG_TYPES.Vote) await this.handleVote(message, blockTime)
      }
      for (const message of bucket.messages) {
        if (message.message_type === GROUP_MSG_TYPES.WithdrawProposal) {
          await this.handleWithdraw(message, height, blockTime)
        }
      }
      for (const event of bucket.events) {
        if (event.type === GROUP_EVENT_TYPES.Exec) await this.handleExecEvent(event, height, blockTime)
      }
      for (const event of bucket.events) {
        if (event.type === GROUP_EVENT_TYPES.ProposalPruned) await this.handlePrunedEvent(event, height, blockTime)
      }
      for (const event of bucket.events) {
        // The create tx emits update events for the admin handover; those are covered by the create seed.
        if (event.type === GROUP_EVENT_TYPES.UpdateGroup) {
          if (createdGroupIds.has(`g:${parseEventAttrValue(event.attrs.group_id)}`)) continue
          await this.handleGroupUpdated(event, height, blockTime)
        }
        // A leave carries the same group_id and can never occur in the create tx.
        if (event.type === GROUP_EVENT_TYPES.LeaveGroup) {
          await this.handleGroupUpdated(event, height, blockTime)
        }
        if (event.type === GROUP_EVENT_TYPES.UpdateGroupPolicy) {
          if (createdGroupIds.has(`p:${parseEventAttrValue(event.attrs.address)}`)) continue
          await this.handlePolicyUpdated(event, height, blockTime)
        }
      }
    }
  }

  private async loadGroupEvents(startBlock: number, endBlock: number): Promise<GroupEventRow[]> {
    const rows = (await knex('event as e')
      .leftJoin('transaction as tx', 'tx.id', 'e.tx_id')
      .where('e.block_height', '>', startBlock)
      .where('e.block_height', '<=', endBlock)
      .whereIn('e.type', [
        GROUP_EVENT_TYPES.CreateGroup,
        GROUP_EVENT_TYPES.CreateGroupPolicy,
        GROUP_EVENT_TYPES.SubmitProposal,
        GROUP_EVENT_TYPES.Exec,
        GROUP_EVENT_TYPES.ProposalPruned,
        GROUP_EVENT_TYPES.UpdateGroup,
        GROUP_EVENT_TYPES.UpdateGroupPolicy,
        GROUP_EVENT_TYPES.LeaveGroup,
      ])
      .where(function () {
        this.whereNull('e.tx_id').orWhere('tx.code', 0)
      })
      .select('e.id as event_id', 'e.type', 'e.tx_id', 'e.tx_msg_index', 'e.block_height')
      .orderBy('e.id', 'asc')) as Array<Omit<GroupEventRow, 'attrs'>>

    if (rows.length === 0) return []
    const attributeRows = (await knex('event_attribute')
      .whereIn(
        'event_id',
        rows.map((row) => row.event_id)
      )
      .where('block_height', '>', startBlock)
      .where('block_height', '<=', endBlock)
      .select('event_id', 'key', 'value')) as Array<{ event_id: number; key: string; value: string }>

    const attrsByEvent = new Map<number, Record<string, string>>()
    for (const attribute of attributeRows) {
      const bucket = attrsByEvent.get(attribute.event_id) ?? {}
      bucket[attribute.key] = attribute.value
      attrsByEvent.set(attribute.event_id, bucket)
    }
    return rows.map((row) => ({ ...row, attrs: attrsByEvent.get(row.event_id) ?? {} }))
  }

  private async lcdGet(path: string, height?: number): Promise<Record<string, unknown> | null> {
    const baseUrl = getLcdBaseUrl()
    if (!baseUrl) return null
    const controller = new AbortController()
    const timeoutId = setTimeout(() => controller.abort(), LCD_TIMEOUT_MS)
    try {
      const headers: Record<string, string> = {}
      if (height !== undefined) headers['x-cosmos-block-height'] = String(height)
      const res = await fetch(`${baseUrl}${path}`, { headers, signal: controller.signal })
      if (!res.ok) return null
      return (await res.json().catch(() => null)) as Record<string, unknown> | null
    } catch {
      return null
    } finally {
      clearTimeout(timeoutId)
    }
  }

  // Historical state queries only answer on an archive node; on a pruning node fall back to current
  // state rather than losing the update entirely.
  private async lcdGetAtHeightOrLatest(path: string, height?: number): Promise<Record<string, unknown> | null> {
    if (height === undefined) return this.lcdGet(path)
    return (await this.lcdGet(path, height)) ?? (await this.lcdGet(path))
  }

  // null means the lookup failed; an empty array means the group genuinely has no members left.
  private async fetchGroupMembers(groupId: number, height?: number): Promise<Array<Record<string, unknown>> | null> {
    const data = await this.lcdGetAtHeightOrLatest(
      `/cosmos/group/v1/group_members/${groupId}?pagination.limit=500`,
      height
    )
    if (!data || !Array.isArray(data.members)) return null
    const members = data.members as Array<Record<string, unknown>>
    return members.map((entry) => {
      const member = (entry.member ?? {}) as Record<string, unknown>
      return {
        address: String(member.address ?? ''),
        weight: String(member.weight ?? '0'),
        metadata: String(member.metadata ?? ''),
        added_at: member.added_at ?? null,
      }
    })
  }

  private decodeProposalMessages(rawMessages: unknown): Array<Record<string, unknown>> {
    if (!Array.isArray(rawMessages)) return []
    return rawMessages.map((raw) => {
      const message = (raw ?? {}) as AnyMessage
      const typeUrl = anyTypeUrl(message)
      if (!typeUrl) return message as Record<string, unknown>
      if (!isUndecodedAny(message)) {
        const { type_url: _tu, typeUrl: _tuc, ...rest } = message
        return { '@type': typeUrl, ...rest }
      }
      try {
        const decoded = this.decodeRegistry.decode({ typeUrl, value: fromBase64(String(message.value)) })
        const generated = this.decodeRegistry.lookupType(typeUrl) as { toJSON?: (value: unknown) => unknown }
        const json = generated?.toJSON ? generated.toJSON(decoded) : decoded
        return { '@type': typeUrl, ...(json as Record<string, unknown>) }
      } catch {
        return { '@type': typeUrl, value: message.value } as Record<string, unknown>
      }
    })
  }

  private async handleCreateCorporation(message: MessageRow, events: GroupEventRow[], blockTime: string) {
    const content = message.content ?? {}
    const createGroupEvent = events.find(
      (event) =>
        event.type === GROUP_EVENT_TYPES.CreateGroup &&
        event.tx_id === message.tx_id &&
        (readPositiveInt(parseEventAttrValue(event.attrs.msg_index)) ?? 0) === message.message_index
    )
    const groupId = readPositiveInt(parseEventAttrValue(createGroupEvent?.attrs.group_id))
    if (!groupId) {
      this.logger.warn(`[GroupProcessorService] create at height=${message.block_height} without EventCreateGroup`)
      return
    }

    // Resolve by the tx's own policy address — corporation.height is the last-modified height, not the create height.
    const groupPolicyEvent = events.find(
      (event) =>
        event.type === GROUP_EVENT_TYPES.CreateGroupPolicy &&
        event.tx_id === message.tx_id &&
        (readPositiveInt(parseEventAttrValue(event.attrs.msg_index)) ?? 0) === message.message_index
    )
    const policyAddress = parseEventAttrValue(groupPolicyEvent?.attrs.address)
    if (!policyAddress) {
      this.logger.warn(
        `[GroupProcessorService] create at height=${message.block_height} without EventCreateGroupPolicy`
      )
      return
    }
    const corporation = await knex('corporation').select('id').where('policy_address', policyAddress).first()
    if (!corporation) {
      this.logger.warn(
        `[GroupProcessorService] create at height=${message.block_height}: no corporation for ${policyAddress}`
      )
      return
    }

    const policyInfo = await this.lcdGetAtHeightOrLatest(
      `/cosmos/group/v1/group_policy_info/${policyAddress}`,
      message.block_height
    )
    const info = (policyInfo?.info ?? {}) as Record<string, unknown>
    const groupInfoData = await this.lcdGetAtHeightOrLatest(
      `/cosmos/group/v1/group_info/${groupId}`,
      message.block_height
    )
    const groupInfo = (groupInfoData?.info ?? groupInfoData?.group ?? {}) as Record<string, unknown>

    const members = Array.isArray(content.members)
      ? (content.members as Array<Record<string, unknown>>).map((member) => ({
          address: String(member.address ?? ''),
          weight: String(member.weight ?? '0'),
          metadata: String(member.metadata ?? ''),
          added_at: blockTime,
        }))
      : ((await this.fetchGroupMembers(groupId, message.block_height)) ?? [])

    const row = {
      corporation_id: Number(corporation.id),
      group_id: groupId,
      policy_address: policyAddress,
      group_version: readPositiveInt(groupInfo.version) ?? 1,
      policy_version: readPositiveInt(info.version) ?? 1,
      total_weight: String(groupInfo.total_weight ?? members.reduce((sum, m) => sum + Number(m.weight || 0), 0)),
      decision_policy: (info.decision_policy as Record<string, unknown> | undefined) ?? null,
      created: blockTime,
      modified: blockTime,
      height: message.block_height,
    }
    await knex('corporation_group').insert(row).onConflict('corporation_id').merge()
    await this.snapshotGroup(row.corporation_id, message.block_height, members)
  }

  // LCD may have been unreachable when the group was first seen; decision_policy drives voting_period_end
  // and reconciliation, so re-fetch it on later passes until it lands.
  private async healMissingDecisionPolicies() {
    const pending = await knex('corporation_group').whereNull('decision_policy').select('*')
    for (const group of pending) {
      const policyInfo = await this.lcdGet(`/cosmos/group/v1/group_policy_info/${group.policy_address}`)
      const info = (policyInfo?.info ?? {}) as Record<string, unknown>
      if (!info.decision_policy) continue
      await knex('corporation_group')
        .where('corporation_id', group.corporation_id)
        .update({
          decision_policy: JSON.stringify(info.decision_policy),
          policy_version: readPositiveInt(info.version) ?? group.policy_version,
        })
      const seconds = votingPeriodSecondsFromDecisionPolicy(info.decision_policy)
      if (seconds === null) continue
      const open = await knex('group_proposal')
        .where('corporation_id', group.corporation_id)
        .whereNull('voting_period_end')
      for (const proposal of open) {
        await knex('group_proposal')
          .where('id', proposal.id)
          .update({
            voting_period_end: new Date(new Date(proposal.submit_time).getTime() + seconds * 1000).toISOString(),
          })
      }
    }
  }

  private async snapshotGroup(corporationId: number, height: number, membersOverride?: Array<Record<string, unknown>>) {
    const group = await knex('corporation_group').where('corporation_id', corporationId).first()
    if (!group) return
    const members =
      membersOverride ??
      (
        await knex('corporation_member')
          .select('address', 'weight', 'metadata', 'created')
          .where('corporation_id', corporationId)
          .orderBy('id', 'asc')
      ).map((member) => ({
        address: member.address,
        weight: String(member.weight),
        metadata: member.metadata ?? '',
        added_at: member.created,
      }))
    await knex('corporation_group_history')
      .insert({
        corporation_id: corporationId,
        group_id: group.group_id,
        group_version: group.group_version,
        policy_version: group.policy_version,
        total_weight: group.total_weight,
        decision_policy: group.decision_policy,
        members: JSON.stringify(members),
        height,
      })
      .onConflict(['corporation_id', 'height'])
      .merge()
  }

  private async handleSubmitProposal(message: MessageRow, events: GroupEventRow[], blockTime: string) {
    const content = message.content ?? {}
    const policyAddress = String(content.group_policy_address ?? '')
    if (!policyAddress) return
    const group = await knex('corporation_group').where('policy_address', policyAddress).first()
    if (!group) return

    const submitEvent = events.find(
      (event) =>
        event.type === GROUP_EVENT_TYPES.SubmitProposal &&
        event.tx_id === message.tx_id &&
        (readPositiveInt(parseEventAttrValue(event.attrs.msg_index)) ?? 0) === message.message_index
    )
    const proposalId = readPositiveInt(parseEventAttrValue(submitEvent?.attrs.proposal_id))
    if (!proposalId) {
      this.logger.warn(`[GroupProcessorService] submit at height=${message.block_height} without EventSubmitProposal`)
      return
    }

    const votingPeriodSeconds = votingPeriodSecondsFromDecisionPolicy(group.decision_policy)
    const votingPeriodEnd =
      votingPeriodSeconds !== null
        ? new Date(new Date(blockTime).getTime() + votingPeriodSeconds * 1000).toISOString()
        : null

    await knex('group_proposal')
      .insert({
        id: proposalId,
        corporation_id: Number(group.corporation_id),
        group_policy_address: policyAddress,
        metadata: content.metadata != null ? String(content.metadata) : null,
        proposers: JSON.stringify(Array.isArray(content.proposers) ? content.proposers : []),
        submit_time: blockTime,
        group_version: readPositiveInt(group.group_version),
        group_policy_version: readPositiveInt(group.policy_version),
        status: 'SUBMITTED',
        executor_result: 'NOT_RUN',
        voting_period_end: votingPeriodEnd,
        messages: JSON.stringify(this.decodeProposalMessages(content.messages)),
        final_tally_result: null,
        modified: blockTime,
        height: message.block_height,
      })
      .onConflict('id')
      .merge()

    // exec=EXEC_TRY makes x/group cast a YES vote per proposer inside this same tx, with no MsgVote message
    // and an EventVote that carries only proposal_id — record them here or they are lost.
    if (String(content.exec ?? '') === 'EXEC_TRY') {
      const proposers = Array.isArray(content.proposers) ? content.proposers : []
      for (const proposer of proposers) {
        await knex('group_vote')
          .insert({
            proposal_id: proposalId,
            voter: String(proposer),
            option: 'YES',
            metadata: null,
            submit_time: blockTime,
            height: message.block_height,
          })
          .onConflict(['proposal_id', 'voter'])
          .ignore()
      }
    }
    await this.snapshotProposal(proposalId, message.block_height)
  }

  private async snapshotProposal(proposalId: number, height: number) {
    const row = await knex('group_proposal').where('id', proposalId).first()
    if (!row) return
    await knex('group_proposal_history')
      .insert({ proposal_id: proposalId, snapshot: JSON.stringify(row), height })
      .onConflict(['proposal_id', 'height'])
      .merge()
  }

  private async handleVote(message: MessageRow, blockTime: string) {
    const content = message.content ?? {}
    const proposalId = readPositiveInt(content.proposal_id)
    const voter = String(content.voter ?? '')
    const option = shortVoteOption(content.option)
    if (!proposalId || !voter || !option) return
    const proposal = await knex('group_proposal').where('id', proposalId).first()
    if (!proposal) return

    await knex('group_vote')
      .insert({
        proposal_id: proposalId,
        voter,
        option,
        metadata: content.metadata != null ? String(content.metadata) : null,
        submit_time: blockTime,
        height: message.block_height,
      })
      .onConflict(['proposal_id', 'voter'])
      .ignore()
    await knex('group_proposal').where('id', proposalId).update({ modified: blockTime })
    await this.snapshotProposal(proposalId, message.block_height)
  }

  private async handleWithdraw(message: MessageRow, height: number, blockTime: string) {
    const proposalId = readPositiveInt((message.content ?? {}).proposal_id)
    if (!proposalId) return
    const updated = await knex('group_proposal')
      .where('id', proposalId)
      .update({ status: 'WITHDRAWN', modified: blockTime })
    if (updated > 0) await this.snapshotProposal(proposalId, height)
  }

  private async handleExecEvent(event: GroupEventRow, height: number, blockTime: string) {
    const proposalId = readPositiveInt(parseEventAttrValue(event.attrs.proposal_id))
    const result = shortExecutorResult(parseEventAttrValue(event.attrs.result))
    if (!proposalId || !result) return
    const patch: Record<string, unknown> = { executor_result: result, modified: blockTime }
    // x/group only executes an ACCEPTED proposal, so SUCCESS and FAILURE both imply ACCEPTED.
    if (result !== 'NOT_RUN') patch.status = 'ACCEPTED'
    const updated = await knex('group_proposal').where('id', proposalId).update(patch)
    if (updated === 0) return
    // NOT_RUN is ambiguous and FAILURE leaves a frozen final tally; both stay queryable.
    if (result !== 'SUCCESS') {
      const row = await knex('group_proposal').where('id', proposalId).first()
      if (row) await this.reconcileProposal(row, height, blockTime)
    }
    await this.snapshotProposal(proposalId, height)
  }

  private async handlePrunedEvent(event: GroupEventRow, height: number, blockTime: string) {
    const proposalId = readPositiveInt(parseEventAttrValue(event.attrs.proposal_id))
    if (!proposalId) return
    const status = shortProposalStatus(parseEventAttrValue(event.attrs.status))
    const tally = normalizeTallyResult(parseEventAttrJson(event.attrs.tally_result))
    const patch: Record<string, unknown> = { modified: blockTime }
    if (status) patch.status = status
    if (tally) patch.final_tally_result = JSON.stringify(tally)
    const updated = await knex('group_proposal').where('id', proposalId).update(patch)
    if (updated > 0) await this.snapshotProposal(proposalId, height)
  }

  private async handleGroupUpdated(event: GroupEventRow, height: number, blockTime: string) {
    const groupId = readPositiveInt(parseEventAttrValue(event.attrs.group_id))
    if (!groupId) return
    const group = await knex('corporation_group').where('group_id', groupId).first()
    if (!group) return

    const groupInfoData = await this.lcdGetAtHeightOrLatest(`/cosmos/group/v1/group_info/${groupId}`, height)
    const groupInfo = (groupInfoData?.info ?? groupInfoData?.group ?? {}) as Record<string, unknown>
    const members = await this.fetchGroupMembers(groupId, height)

    await knex('corporation_group')
      .where('group_id', groupId)
      .update({
        group_version: readPositiveInt(groupInfo.version) ?? group.group_version,
        total_weight: String(groupInfo.total_weight ?? group.total_weight),
        modified: blockTime,
        height,
      })
    // Only rewrite when the lookup succeeded; an empty result means the last member left.
    if (members !== null) {
      await knex.transaction(async (trx) => {
        await trx('corporation_member').where('corporation_id', group.corporation_id).delete()
        if (members.length > 0) {
          await trx('corporation_member').insert(
            members.map((member) => ({
              corporation_id: group.corporation_id,
              address: member.address,
              weight: member.weight,
              metadata: member.metadata,
              created: member.added_at ?? blockTime,
            }))
          )
        }
      })
    }
    await this.snapshotGroup(Number(group.corporation_id), height, members ?? undefined)
  }

  private async handlePolicyUpdated(event: GroupEventRow, height: number, blockTime: string) {
    const address = parseEventAttrValue(event.attrs.address)
    if (!address) return
    const group = await knex('corporation_group').where('policy_address', address).first()
    if (!group) return

    const policyInfo = await this.lcdGetAtHeightOrLatest(`/cosmos/group/v1/group_policy_info/${address}`, height)
    const info = (policyInfo?.info ?? {}) as Record<string, unknown>
    await knex('corporation_group')
      .where('policy_address', address)
      .update({
        policy_version: readPositiveInt(info.version) ?? group.policy_version,
        decision_policy: info.decision_policy ? JSON.stringify(info.decision_policy) : group.decision_policy,
        modified: blockTime,
        height,
      })
    await this.snapshotGroup(Number(group.corporation_id), height)
    // A decision-policy update aborts live proposals on-chain; reconcile them now.
    await this.reconcileProposalsForCorporation(Number(group.corporation_id), height)
  }

  private async chainTimeAt(height: number): Promise<string> {
    const block = await knex('block').select('time').where('height', height).first()
    return block ? new Date(block.time).toISOString() : new Date().toISOString()
  }

  private async reconcileProposalsForCorporation(corporationId: number, height: number) {
    const open = await knex('group_proposal').where('corporation_id', corporationId).where('status', 'SUBMITTED')
    const chainNow = await this.chainTimeAt(height)
    for (const proposal of open) await this.reconcileProposal(proposal, height, chainNow)
  }

  private async reconcileOpenProposals(height: number) {
    if (!height) return
    const chainNow = await this.chainTimeAt(height)
    const due = await knex('group_proposal')
      .where('status', 'SUBMITTED')
      .whereNotNull('voting_period_end')
      .where('voting_period_end', '<=', chainNow)
    for (const proposal of due) await this.reconcileProposal(proposal, height, chainNow)
  }

  private async reconcileProposal(proposal: Record<string, unknown>, height: number, chainNow: string) {
    const proposalId = Number(proposal.id)
    // Pinned to this block, falling back to current state on a node that no longer retains it.
    const data = await this.lcdGetAtHeightOrLatest(`/cosmos/group/v1/proposal/${proposalId}`, height)
    const chainProposal = (data?.proposal ?? null) as Record<string, unknown> | null

    const patch: Record<string, unknown> = {}
    if (chainProposal) {
      const status = shortProposalStatus(chainProposal.status)
      const executorResult = shortExecutorResult(chainProposal.executor_result)
      const tally = normalizeTallyResult(chainProposal.final_tally_result)
      if (status && status !== proposal.status) patch.status = status
      if (executorResult && executorResult !== proposal.executor_result) patch.executor_result = executorResult
      // The chain's window wins over the value derived from the stored policy.
      const chainVotingPeriodEnd = chainProposal.voting_period_end
      if (typeof chainVotingPeriodEnd === 'string' && chainVotingPeriodEnd) {
        const parsed = new Date(chainVotingPeriodEnd)
        const current = proposal.voting_period_end ? new Date(String(proposal.voting_period_end)) : null
        if (!Number.isNaN(parsed.getTime()) && parsed.getTime() !== current?.getTime()) {
          patch.voting_period_end = parsed.toISOString()
        }
      }
      const hasFinalTally = tally && Object.values(tally).some((count) => count !== '0')
      if (hasFinalTally && (patch.status ?? proposal.status) !== 'SUBMITTED') {
        patch.final_tally_result = JSON.stringify(tally)
      }
    } else {
      // A null read also means "LCD unreachable", so only settle a proposal that is still open here
      // AND whose voting window has closed. Inventing a terminal status is not recoverable.
      if (String(proposal.status) !== 'SUBMITTED') return
      const votingPeriodEnd = proposal.voting_period_end ? new Date(String(proposal.voting_period_end)) : null
      if (!votingPeriodEnd || Number.isNaN(votingPeriodEnd.getTime())) return
      if (new Date(chainNow).getTime() <= votingPeriodEnd.getTime()) return

      // Pruned without an indexed prune event: settle from the indexed votes and threshold.
      const votes = await knex('group_vote').select('voter', 'option').where('proposal_id', proposalId)
      const members = await knex('corporation_member')
        .select('address', 'weight')
        .where('corporation_id', Number(proposal.corporation_id))
      const weightByAddress = new Map(members.map((member) => [member.address, Number(member.weight) || 0]))
      const weightFor = (option: string) =>
        votes
          .filter((vote) => vote.option === option)
          .reduce((sum, vote) => sum + (weightByAddress.get(vote.voter) ?? 0), 0)
      const tally = {
        yes_count: String(weightFor('YES')),
        no_count: String(weightFor('NO')),
        abstain_count: String(weightFor('ABSTAIN')),
        no_with_veto_count: String(weightFor('NO_WITH_VETO')),
      }
      const groupRow = await knex('corporation_group').where('corporation_id', Number(proposal.corporation_id)).first()
      const totalWeight =
        Number(groupRow?.total_weight ?? 0) || members.reduce((sum, member) => sum + (Number(member.weight) || 0), 0)
      const outcome = decideProposalOutcome(groupRow?.decision_policy, Number(tally.yes_count), totalWeight)
      if (!outcome) {
        this.logger.warn(
          `[GroupProcessorService] proposal ${proposalId} is gone from state and its decision policy cannot be evaluated; leaving status unchanged`
        )
        return
      }
      patch.status = outcome
      patch.final_tally_result = JSON.stringify(tally)
    }

    if (Object.keys(patch).length > 0) {
      patch.modified = chainNow
      await knex('group_proposal').where('id', proposalId).update(patch)
      await this.snapshotProposal(proposalId, height)
    }
  }
}
