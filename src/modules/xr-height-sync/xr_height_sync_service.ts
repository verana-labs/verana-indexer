import { Buffer } from 'node:buffer'
import type { ServiceBroker } from 'moleculer'
import { SERVICE } from '../../common'
import { fetchExchangeRates, serializeLedgerExchangeRate } from './xr_height_sync_helpers'

export const XR_EVENT_TYPES = {
  CREATE: 'create_exchange_rate',
  UPDATE: 'update_exchange_rate',
  SET_STATE: 'set_exchange_rate_state',
} as const

const XR_EVENT_TYPE_SET = new Set<string>(Object.values(XR_EVENT_TYPES))

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

export function buildEventTypeResolverFromEvents(events: BlockEvent[]): (id: number) => string {
  const byId = new Map<number, string>()
  for (const event of events) {
    if (!event.type || !XR_EVENT_TYPE_SET.has(event.type)) continue
    const id = Number(getAttr(event, 'id'))
    if (Number.isInteger(id) && id > 0) {
      byId.set(id, event.type)
    }
  }
  return (id: number): string => byId.get(id) ?? 'SYNC_LEDGER'
}

export function hasExchangeRateEvents(events: BlockEvent[]): boolean {
  return events.some((event) => event.type !== undefined && XR_EVENT_TYPE_SET.has(event.type))
}

export async function runHeightSyncXR(
  broker: ServiceBroker,
  payload: { events: BlockEvent[] },
  blockHeight: number
): Promise<void> {
  const events = payload.events ?? []
  if (!hasExchangeRateEvents(events) || typeof blockHeight !== 'number' || blockHeight <= 0) {
    return
  }

  const ledgerRates = await fetchExchangeRates(blockHeight)
  const resolveEventType = buildEventTypeResolverFromEvents(events)

  for (const ledgerRate of ledgerRates) {
    await broker.call(`${SERVICE.V1.ExchangeRateDatabaseService.path}.syncFromLedger`, {
      exchangeRate: serializeLedgerExchangeRate(ledgerRate),
      blockHeight,
      eventType: resolveEventType(ledgerRate.id),
    })
  }
}
