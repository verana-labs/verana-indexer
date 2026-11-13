import { Buffer } from 'node:buffer'
import knex from '../../common/utils/db_connection'
import { decodeProposalMessages, parseEventAttrValue } from '../crawl-group/group_helpers'

type BlockEvent = { type?: string; attributes?: Array<{ key?: string; value?: string }> }

const decodeBase64 = (value: string) => Buffer.from(value, 'base64').toString('utf8')

function readAttribute(event: BlockEvent, key: string): string | null {
  for (const attr of event.attributes ?? []) {
    if (attr.key === key) return parseEventAttrValue(attr.value)
    if (attr.key && decodeBase64(attr.key) === key) return parseEventAttrValue(decodeBase64(attr.value ?? ''))
  }
  return null
}

// Gov runs a passed proposal's Msgs in EndBlock; the block's finalize events are the only record of it.
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

export async function loadGovProposalMessages(proposalId: number): Promise<Array<Record<string, unknown>> | undefined> {
  const submit = await knex('event_attribute as ea')
    .innerJoin('event as e', 'e.id', 'ea.event_id')
    .where('e.type', 'submit_proposal')
    .where('ea.key', 'proposal_id')
    .whereIn('ea.value', [String(proposalId), JSON.stringify(String(proposalId))])
    .select('e.tx_id', 'e.tx_msg_index')
    .first()
  if (!submit?.tx_id) return undefined
  const message = await knex('transaction_message')
    .where('tx_id', submit.tx_id)
    .where('index', Number(submit.tx_msg_index ?? 0))
    .select('content')
    .first()
  const messages = (message?.content as { messages?: unknown } | undefined)?.messages
  return messages === undefined ? undefined : decodeProposalMessages(messages)
}
