import {
  anyTypeUrl,
  decodeProposalMessages,
  GROUP_EVENT_TYPES,
  parseEventAttrValue,
  shortExecutorResult,
} from '../crawl-group/group_helpers'

export const AUTHZ_EXEC_TYPE = '/cosmos.authz.v1beta1.MsgExec'

type TxEventLike = { type?: string; attributes?: Array<{ key?: string; value?: string }> }

export interface ExecutedProposal {
  proposalId: number
  msgIndex: number
}

export function authzInnerMessages<T extends { type: string; content?: unknown }>(msg: T): T[] {
  if (msg.type !== AUTHZ_EXEC_TYPE) return []
  return decodeProposalMessages((msg.content as { msgs?: unknown } | null)?.msgs).flatMap((inner) => {
    const type = anyTypeUrl(inner)
    return type ? [{ ...msg, type, content: inner }] : []
  })
}

export function executedProposals(txEvents: TxEventLike[]): ExecutedProposal[] {
  return txEvents.flatMap((event) => {
    if (event.type !== GROUP_EVENT_TYPES.Exec) return []
    const attrs = new Map((event.attributes ?? []).map((attr) => [attr.key ?? '', parseEventAttrValue(attr.value)]))
    if (shortExecutorResult(attrs.get('result')) !== 'SUCCESS') return []
    const proposalId = Number(attrs.get('proposal_id'))
    if (!Number.isInteger(proposalId) || proposalId <= 0) return []
    return [{ proposalId, msgIndex: Number(attrs.get('msg_index') ?? 0) }]
  })
}
