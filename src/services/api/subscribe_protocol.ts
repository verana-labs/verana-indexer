import { matchesMembership, parseSubscribeMembership } from './api_shared'
import type { IndexerEventRecord } from './indexer_events_query'

export const CHAIN_BLOCK_INTERVAL_MS = 6000

export type SubscribeControl = {
  action: 'subscribe'
  dids: string[] | null
  corporationId: number | null
}

export type UnsubscribeControl = {
  action: 'unsubscribe'
}

export type ControlMessage = SubscribeControl | UnsubscribeControl

export type ReadyMessage = {
  type: 'ready'
  block: number
  blockTime: string
  blockIntervalMs: number
}

export type BlockEnvelope = {
  type: 'block'
  block: number
  blockTime: string
  events: IndexerEventRecord[]
}

export type ControlParseResult = { ok: true; message: ControlMessage } | { ok: false; error: string }

export function parseControlMessage(raw: string): ControlParseResult {
  let json: unknown
  try {
    json = JSON.parse(raw)
  } catch {
    return { ok: false, error: 'Invalid JSON' }
  }

  if (!json || typeof json !== 'object') {
    return { ok: false, error: 'Control message must be an object' }
  }

  const action = (json as Record<string, unknown>).action
  if (action !== 'subscribe' && action !== 'unsubscribe') {
    return { ok: false, error: "Unknown action. Expected 'subscribe' or 'unsubscribe'" }
  }

  if (action === 'unsubscribe') {
    return { ok: true, message: { action: 'unsubscribe' } }
  }

  const base = parseSubscribeMembership(json as Record<string, unknown>)
  if (!base.ok) {
    return { ok: false, error: base.error }
  }

  return {
    ok: true,
    message: { action: 'subscribe', dids: base.value.dids, corporationId: base.value.corporationId },
  }
}

export function buildReadyMessage(lastProcessedBlock: number, lastBlockTime: string): ReadyMessage {
  return {
    type: 'ready',
    block: lastProcessedBlock + 1,
    blockTime: lastBlockTime,
    blockIntervalMs: CHAIN_BLOCK_INTERVAL_MS,
  }
}

export function buildBlockEnvelope(
  block: number,
  blockTime: string,
  events: IndexerEventRecord[],
  didFilter: Set<string> | null,
  corporationId: number | null
): BlockEnvelope {
  if (didFilter === null && corporationId === null) {
    return { type: 'block', block, blockTime, events }
  }

  const filtered = events.filter((event) =>
    matchesMembership(didFilter, corporationId, {
      did: event.did,
      relatedDids: event.payload?.related_dids ?? [],
      corporationIds: eventCorporationIds(event),
    })
  )

  return { type: 'block', block, blockTime, events: filtered }
}

function eventCorporationIds(event: IndexerEventRecord): number[] {
  const ids: number[] = []
  if (typeof event.payload?.corporation_id === 'number') ids.push(event.payload.corporation_id)
  if (event.payload?.related_corporation_ids) ids.push(...event.payload.related_corporation_ids)
  return ids
}
