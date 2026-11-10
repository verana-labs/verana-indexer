import { Buffer } from 'node:buffer'
import type { ServiceBroker } from 'moleculer'
import { SERVICE } from '../../common'
import { fetchDigest, serializeLedgerDigest } from './di_height_sync_helpers'

export const DI_EVENT_TYPES = {
  STORE: 'store_digest',
} as const

const DI_EVENT_TYPE_SET = new Set<string>(Object.values(DI_EVENT_TYPES))

interface BlockEventAttribute {
  key?: string
  value?: string
}

interface BlockEvent {
  type?: string
  attributes?: BlockEventAttribute[]
}

function decodeBase64(value: string): string {
  return Buffer.from(value, 'base64').toString('utf8')
}

function getAttr(event: BlockEvent, key: string): string | undefined {
  for (const attr of event.attributes ?? []) {
    if (attr.key === undefined) continue
    if (attr.key === key) return attr.value
    if (decodeBase64(attr.key) === key) return attr.value === undefined ? undefined : decodeBase64(attr.value)
  }
  return undefined
}

export function extractStoredDigests(events: BlockEvent[]): string[] {
  const digests = new Set<string>()
  for (const event of events) {
    if (!event.type || !DI_EVENT_TYPE_SET.has(event.type)) continue
    const digest = getAttr(event, 'digest')
    if (digest) digests.add(digest)
  }
  return [...digests]
}

export function hasDigestEvents(events: BlockEvent[]): boolean {
  return events.some((event) => event.type !== undefined && DI_EVENT_TYPE_SET.has(event.type))
}

export async function runHeightSyncDI(
  broker: ServiceBroker,
  payload: { events: BlockEvent[] },
  blockHeight: number
): Promise<void> {
  const events = payload.events ?? []
  if (!hasDigestEvents(events) || typeof blockHeight !== 'number' || blockHeight <= 0) {
    return
  }

  const digests = extractStoredDigests(events)

  for (const digest of digests) {
    const ledgerDigest = await fetchDigest(digest, blockHeight)
    if (!ledgerDigest) throw new Error(`digest ${digest} not found on the ledger at block ${blockHeight}`)
    await broker.call(`${SERVICE.V1.DigestDatabaseService.path}.syncFromLedger`, {
      digest: serializeLedgerDigest(ledgerDigest),
      blockHeight,
    })
  }
}
