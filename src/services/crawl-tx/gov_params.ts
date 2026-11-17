import { Buffer } from 'node:buffer'
import { getLcdBaseUrl } from '../../common/utils/account_balance_utils'
import knex from '../../common/utils/db_connection'
import { decodeProposalMessages, parseEventAttrValue } from '../crawl-group/group_helpers'

const LCD_TIMEOUT_MS = 8000

type BlockEvent = { type?: string; attributes?: Array<{ key?: string; value?: string }> }

const decodeBase64 = (value: string) => Buffer.from(value, 'base64').toString('utf8')

function readAttribute(event: BlockEvent, key: string): string | null {
  for (const attr of event.attributes ?? []) {
    if (attr.key === key) return parseEventAttrValue(attr.value)
    if (attr.key && decodeBase64(attr.key) === key) return parseEventAttrValue(decodeBase64(attr.value ?? ''))
  }
  return null
}

// Gov runs the messages of a passed proposal in EndBlock. Only the finalize events of the block record this.
export async function passedGovProposals(
  fromHeight: number,
  toHeight: number
): Promise<Array<{ height: number; proposalId: number }>> {
  if (toHeight < fromHeight) return []
  const rows = (await knex('block')
    .select('height', knex.raw("data->'block_result'->'finalize_block_events' as events"))
    .whereBetween('height', [fromHeight, toHeight])
    .whereRaw(`data->'block_result'->'finalize_block_events' @> '[{"type":"active_proposal"}]'::jsonb`)
    .orderBy('height', 'asc')) as Array<{ height: number | string; events: BlockEvent[] | null }>

  return rows.flatMap((row) =>
    (row.events ?? []).flatMap((event) => {
      if (event.type !== 'active_proposal' || readAttribute(event, 'proposal_result') !== 'proposal_passed') return []
      const proposalId = Number(readAttribute(event, 'proposal_id'))
      return Number.isInteger(proposalId) && proposalId > 0 ? [{ height: Number(row.height), proposalId }] : []
    })
  )
}

async function fetchGovProposal(proposalId: number, height?: number): Promise<{ messages?: unknown } | undefined> {
  const baseUrl = getLcdBaseUrl()
  if (!baseUrl) throw new Error('LCD_ENDPOINT is not configured')
  const res = await fetch(`${baseUrl}/cosmos/gov/v1/proposals/${proposalId}`, {
    headers: height ? { 'x-cosmos-block-height': String(height) } : {},
    signal: AbortSignal.timeout(LCD_TIMEOUT_MS),
  })
  if (!res.ok) return undefined
  return ((await res.json()) as { proposal?: { messages?: unknown } }).proposal
}

// The chain stores the proposal messages. Authz, group and v1beta1 submits keep no messages in the tx.
export async function loadGovProposalMessages(
  proposalId: number,
  height: number
): Promise<Array<Record<string, unknown>> | undefined> {
  const proposal =
    (await fetchGovProposal(proposalId, height).catch(() => undefined)) ?? (await fetchGovProposal(proposalId))
  return Array.isArray(proposal?.messages) ? decodeProposalMessages(proposal.messages) : undefined
}
