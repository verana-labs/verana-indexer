export const GROUP_MSG_TYPES = {
  CreateCorporation: '/verana.co.v1.MsgCreateCorporation',
  SubmitProposal: '/cosmos.group.v1.MsgSubmitProposal',
  Vote: '/cosmos.group.v1.MsgVote',
  Exec: '/cosmos.group.v1.MsgExec',
  WithdrawProposal: '/cosmos.group.v1.MsgWithdrawProposal',
  UpdateGroupMembers: '/cosmos.group.v1.MsgUpdateGroupMembers',
  UpdateGroupMetadata: '/cosmos.group.v1.MsgUpdateGroupMetadata',
  UpdateGroupAdmin: '/cosmos.group.v1.MsgUpdateGroupAdmin',
  UpdateGroupPolicyDecisionPolicy: '/cosmos.group.v1.MsgUpdateGroupPolicyDecisionPolicy',
  UpdateGroupPolicyMetadata: '/cosmos.group.v1.MsgUpdateGroupPolicyMetadata',
  UpdateGroupPolicyAdmin: '/cosmos.group.v1.MsgUpdateGroupPolicyAdmin',
  LeaveGroup: '/cosmos.group.v1.MsgLeaveGroup',
} as const

export const GROUP_EVENT_TYPES = {
  CreateGroup: 'cosmos.group.v1.EventCreateGroup',
  CreateGroupPolicy: 'cosmos.group.v1.EventCreateGroupPolicy',
  SubmitProposal: 'cosmos.group.v1.EventSubmitProposal',
  Vote: 'cosmos.group.v1.EventVote',
  Exec: 'cosmos.group.v1.EventExec',
  WithdrawProposal: 'cosmos.group.v1.EventWithdrawProposal',
  ProposalPruned: 'cosmos.group.v1.EventProposalPruned',
  UpdateGroup: 'cosmos.group.v1.EventUpdateGroup',
  UpdateGroupPolicy: 'cosmos.group.v1.EventUpdateGroupPolicy',
} as const

export const PROPOSAL_STATUSES = ['SUBMITTED', 'ACCEPTED', 'REJECTED', 'ABORTED', 'WITHDRAWN'] as const
export const EXECUTOR_RESULTS = ['NOT_RUN', 'SUCCESS', 'FAILURE'] as const
export const VOTE_OPTIONS = ['YES', 'NO', 'ABSTAIN', 'NO_WITH_VETO'] as const

// Maps x/group enum names (or their numeric wire values) to the spec's short forms.
export function shortProposalStatus(raw: unknown): string | null {
  const byNumber: Record<number, string> = {
    1: 'SUBMITTED',
    2: 'ACCEPTED',
    3: 'REJECTED',
    4: 'ABORTED',
    5: 'WITHDRAWN',
  }
  if (typeof raw === 'number') return byNumber[raw] ?? null
  if (typeof raw !== 'string') return null
  const name = raw.replace(/^PROPOSAL_STATUS_/, '')
  return (PROPOSAL_STATUSES as readonly string[]).includes(name) ? name : null
}

export function shortExecutorResult(raw: unknown): string | null {
  const byNumber: Record<number, string> = { 1: 'NOT_RUN', 2: 'SUCCESS', 3: 'FAILURE' }
  if (typeof raw === 'number') return byNumber[raw] ?? null
  if (typeof raw !== 'string') return null
  const name = raw.replace(/^PROPOSAL_EXECUTOR_RESULT_/, '')
  return (EXECUTOR_RESULTS as readonly string[]).includes(name) ? name : null
}

export function shortVoteOption(raw: unknown): string | null {
  const byNumber: Record<number, string> = { 1: 'YES', 2: 'ABSTAIN', 3: 'NO', 4: 'NO_WITH_VETO' }
  if (typeof raw === 'number') return byNumber[raw] ?? null
  if (typeof raw !== 'string') return null
  const name = raw.replace(/^VOTE_OPTION_/, '')
  return (VOTE_OPTIONS as readonly string[]).includes(name) ? name : null
}

// Typed-event attribute values are raw JSON: uint64s and strings arrive quoted ("5").
export function parseEventAttrValue(raw: unknown): string | null {
  if (raw === undefined || raw === null) return null
  const s = String(raw).trim()
  if (s.startsWith('"') && s.endsWith('"') && s.length >= 2) return s.slice(1, -1)
  return s
}

export function parseEventAttrJson(raw: unknown): Record<string, unknown> | null {
  if (typeof raw !== 'string') return null
  try {
    const parsed = JSON.parse(raw)
    return parsed && typeof parsed === 'object' ? (parsed as Record<string, unknown>) : null
  } catch {
    return null
  }
}

export function readPositiveInt(value: unknown): number | null {
  const n = Number(typeof value === 'string' ? value.trim() : value)
  return Number.isInteger(n) && n > 0 ? n : null
}

// Cosmos Duration JSON ("300s") -> seconds.
export function durationToSeconds(raw: unknown): number | null {
  if (typeof raw !== 'string') return null
  const match = raw.trim().match(/^(\d+(?:\.\d+)?)s$/)
  return match ? Number(match[1]) : null
}

export function votingPeriodSecondsFromDecisionPolicy(decisionPolicy: unknown): number | null {
  if (!decisionPolicy || typeof decisionPolicy !== 'object') return null
  const windows = (decisionPolicy as Record<string, unknown>).windows
  if (!windows || typeof windows !== 'object') return null
  return durationToSeconds((windows as Record<string, unknown>).voting_period)
}

// x/group TallyResult JSON field names -> the spec's tally counter names.
export function normalizeTallyResult(raw: unknown): Record<string, string> | null {
  if (!raw || typeof raw !== 'object') return null
  const source = raw as Record<string, unknown>
  const pick = (...keys: string[]) => {
    for (const key of keys) {
      const value = source[key]
      if (value !== undefined && value !== null && String(value).trim() !== '') return String(value)
    }
    return '0'
  }
  return {
    yes_count: pick('yes_count', 'yesCount'),
    no_count: pick('no_count', 'noCount'),
    abstain_count: pick('abstain_count', 'abstainCount'),
    no_with_veto_count: pick('no_with_veto_count', 'noWithVetoCount'),
  }
}

export function tallyFromVotes(votes: Array<{ option: string }>): Record<string, string> {
  const counts: Record<string, number> = { YES: 0, NO: 0, ABSTAIN: 0, NO_WITH_VETO: 0 }
  for (const vote of votes) {
    if (counts[vote.option] !== undefined) counts[vote.option] += 1
  }
  return {
    yes_count: String(counts.YES),
    no_count: String(counts.NO),
    abstain_count: String(counts.ABSTAIN),
    no_with_veto_count: String(counts.NO_WITH_VETO),
  }
}

// Final-tally outcome for the two x/group decision policies. Returns null when the policy cannot be
// evaluated, so callers leave the status alone instead of inventing a terminal state.
export function decideProposalOutcome(
  decisionPolicy: unknown,
  yesWeight: number,
  totalWeight: number
): 'ACCEPTED' | 'REJECTED' | null {
  if (!decisionPolicy || typeof decisionPolicy !== 'object') return null
  const policy = decisionPolicy as Record<string, unknown>
  const type = String(policy['@type'] ?? '')

  if (type.endsWith('ThresholdDecisionPolicy') || policy.threshold !== undefined) {
    const threshold = Number(policy.threshold)
    if (!Number.isFinite(threshold)) return null
    return yesWeight >= threshold ? 'ACCEPTED' : 'REJECTED'
  }

  if (type.endsWith('PercentageDecisionPolicy') || policy.percentage !== undefined) {
    const percentage = Number(policy.percentage)
    if (!Number.isFinite(percentage) || totalWeight <= 0) return null
    return yesWeight / totalWeight >= percentage ? 'ACCEPTED' : 'REJECTED'
  }

  return null
}

export type AnyMessage = {
  '@type'?: string
  type_url?: string
  typeUrl?: string
  value?: unknown
  [key: string]: unknown
}

export function anyTypeUrl(message: AnyMessage): string | null {
  const url = message['@type'] ?? message.type_url ?? message.typeUrl
  return typeof url === 'string' && url.length > 0 ? url : null
}

// True when the Any still carries an undecoded base64 payload rather than JSON fields.
export function isUndecodedAny(message: AnyMessage): boolean {
  if (typeof message.value !== 'string') return false
  const keys = Object.keys(message).filter((key) => !['@type', 'type_url', 'typeUrl', 'value'].includes(key))
  return keys.length === 0
}
