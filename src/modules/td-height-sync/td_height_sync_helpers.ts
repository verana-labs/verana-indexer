import { Buffer } from 'node:buffer'
import {
  QueryGetTrustDepositRequest,
  QueryClientImpl as TdQueryClientImpl,
} from '@verana-labs/verana-types/codec/verana/td/v1/query'
import { TrustDepositEventType } from '../../common/constant'
import { dateToIsoOrNull } from '../../common/utils/date_utils'
import knex from '../../common/utils/db_connection'
import { isAbciNotFound, withAbciQueryClient } from '../../common/utils/grpc_query'
import { VeranaTrustDepositMessageTypes } from '../../common/verana-message-types'

async function resolveCorporation(idOrAddress: string): Promise<{ corporationId: number; address: string } | null> {
  const raw = String(idOrAddress).trim()
  if (!raw) return null
  if (/^\d+$/.test(raw)) {
    const row = await knex('corporation')
      .where({ id: Number(raw) })
      .select('id', 'corporation')
      .first()
    if (row?.id != null) return { corporationId: Number(row.id), address: String(row.corporation ?? raw) }
    return { corporationId: Number(raw), address: raw }
  }
  const row = await knex('corporation').where({ corporation: raw }).select('id', 'corporation').first()
  if (row?.id != null) return { corporationId: Number(row.id), address: raw }
  return null
}

const TD_MESSAGE_TYPES = new Set<string>([
  VeranaTrustDepositMessageTypes.UpdateParams,
  VeranaTrustDepositMessageTypes.AdjustTrustDeposit,
  VeranaTrustDepositMessageTypes.ReclaimYield,
  VeranaTrustDepositMessageTypes.RepaySlashed,
  VeranaTrustDepositMessageTypes.SlashTrustDeposit,
  VeranaTrustDepositMessageTypes.BurnEcosystemSlashedTrustDeposit,
])

export const TD_BLOCKCHAIN_EVENT_TYPES = new Set<string>(Object.values(TrustDepositEventType) as string[])

export interface NormalizedLedgerTrustDeposit {
  corporation: string
  deposit: number
  share: number
  claimable: number
  slashed_deposit: number
  repaid_deposit: number
  last_slashed: string | null
  last_repaid: string | null
  slash_count: number
}

export interface TdMessageLike {
  type: string
  content?: Record<string, unknown> | null
  txEvents?: TxEventLike[]
}

export interface TxEventLike {
  type?: string
  attributes?: Array<{ key?: string; value?: string }>
}

function decodeBase64(value: string): string {
  return Buffer.from(value, 'base64').toString('utf8')
}

function getAttr(event: TxEventLike, key: string): string | undefined {
  for (const attr of event.attributes ?? []) {
    if (attr.key === undefined) continue
    if (attr.key === key) return attr.value
    if (decodeBase64(attr.key) === key) return attr.value === undefined ? undefined : decodeBase64(attr.value)
  }
  return undefined
}

function getTrustDepositIdFromEvent(event: TxEventLike): string | undefined {
  if (!TD_BLOCKCHAIN_EVENT_TYPES.has((event.type ?? '').trim())) return undefined
  const id = (getAttr(event, 'account') ?? getAttr(event, 'corporation_id'))?.trim()
  return id || undefined
}

export function blockchainEventTypeToHistoryEventType(blockchainEventType: string): string {
  switch (blockchainEventType) {
    case TrustDepositEventType.SlashTrustDeposit:
      return 'SLASH_TRUST_DEPOSIT'
    case TrustDepositEventType.RepaySlashedTrustDeposit:
      return 'REPAY_SLASHED'
    case TrustDepositEventType.ReclaimTrustDepositYield:
      return 'RECLAIM_YIELD'
    case TrustDepositEventType.ReclaimTrustDeposit:
      return 'RECLAIM_DEPOSIT'
    case TrustDepositEventType.AdjustTrustDeposit:
      return 'ADJUST_TRUST_DEPOSIT'
    case TrustDepositEventType.YieldDistribution:
      return 'YIELD_DISTRIBUTION'
    case TrustDepositEventType.YieldTransfer:
      return 'YIELD_TRANSFER'
    case TrustDepositEventType.BurnEcosystemSlashedTrustDeposit:
      return 'SLASH_PARTICIPANT_TRUST_DEPOSIT'
    default:
      return 'SYNC_LEDGER'
  }
}

const LEGACY_DEC_FRACTIONAL_DIGITS = 18

export function legacyDecWireToNumber(value: string | number | undefined | null): number {
  if (value === null || value === undefined) return 0
  const str = String(value).trim()
  if (str === '') return 0
  if (str.includes('.')) return Number(str)
  if (!/^-?\d+$/.test(str)) return 0

  const negative = str.startsWith('-')
  const digits = negative ? str.slice(1) : str
  const padded = digits.padStart(LEGACY_DEC_FRACTIONAL_DIGITS + 1, '0')
  const integerPart = padded.slice(0, padded.length - LEGACY_DEC_FRACTIONAL_DIGITS)
  const fractionalPart = padded.slice(padded.length - LEGACY_DEC_FRACTIONAL_DIGITS).replace(/0+$/, '')
  const decimalString = fractionalPart ? `${integerPart}.${fractionalPart}` : integerPart
  return Number(negative ? `-${decimalString}` : decimalString)
}

export async function fetchTrustDeposit(
  id: string,
  blockHeight?: number
): Promise<NormalizedLedgerTrustDeposit | null> {
  try {
    const resolved = await resolveCorporation(id)
    if (!resolved) return null
    return await withAbciQueryClient(blockHeight, async (rpc) => {
      const query = new TdQueryClientImpl(rpc)
      const res = await query.GetTrustDeposit(
        QueryGetTrustDepositRequest.fromPartial({ corporationId: resolved.corporationId })
      )
      const td = res?.trustDeposit
      if (!td) return null
      return {
        corporation: resolved.address,
        deposit: Number(td.deposit ?? 0),
        share: legacyDecWireToNumber(td.share),
        claimable: Number(td.refunded ?? 0),
        slashed_deposit: Number(td.slashedDeposit ?? 0),
        repaid_deposit: Number(td.repaidDeposit ?? 0),
        last_slashed: dateToIsoOrNull(td.lastSlashed),
        last_repaid: dateToIsoOrNull(td.lastRepaid),
        slash_count: Number(td.slashCount ?? 0),
      }
    })
  } catch (error) {
    if (isAbciNotFound(error)) return null
    throw error
  }
}

export function isTdMessageType(type: string): boolean {
  return TD_MESSAGE_TYPES.has(type)
}

export function extractTrustDepositIdsFromMessageContent(
  content: Record<string, unknown> | null | undefined
): string[] {
  if (!content || typeof content !== 'object') return []
  const ids: string[] = []
  for (const raw of [content.corporation, content.corporationId, content.corporation_id]) {
    if (raw === null || raw === undefined) continue
    const s = String(raw).trim()
    if (s.length > 0) ids.push(s)
  }
  return [...new Set(ids)]
}

export function extractTrustDepositIdsFromEvents(events: TxEventLike[]): string[] {
  const ids: string[] = []
  for (const ev of events) {
    const id = getTrustDepositIdFromEvent(ev)
    if (id) ids.push(id)
  }
  return [...new Set(ids)]
}

export function extractImpactedTrustDepositIds(messages: TdMessageLike[], events?: TxEventLike[]): string[] {
  const fromMessages: string[] = []
  for (const msg of messages) {
    if (!isTdMessageType(msg.type)) continue
    if (msg.type === VeranaTrustDepositMessageTypes.UpdateParams) continue
    const fromContent = extractTrustDepositIdsFromMessageContent(msg.content ?? undefined)
    fromMessages.push(...fromContent)
    if (Array.isArray(msg.txEvents) && msg.txEvents.length > 0) {
      fromMessages.push(...extractTrustDepositIdsFromEvents(msg.txEvents))
    }
  }
  const fromEvents = events?.length ? extractTrustDepositIdsFromEvents(events) : []
  return [...new Set([...fromMessages, ...fromEvents])]
}

/**
 * Builds a map from trust deposit ID to the event type to store in history.
 * Each ID is assigned the event type from the first message that references it.
 * IDs only present in events (not in any message) are not in the map; callers should use "SYNC_LEDGER" for those.
 */
export function buildDepositIdToEventTypeMap(messages: TdMessageLike[]): Map<string, string> {
  const map = new Map<string, string>()
  for (const msg of messages) {
    if (!isTdMessageType(msg.type)) continue
    if (msg.type === VeranaTrustDepositMessageTypes.UpdateParams) continue
    const ids: string[] = []
    ids.push(...extractTrustDepositIdsFromMessageContent(msg.content ?? undefined))
    if (Array.isArray(msg.txEvents) && msg.txEvents.length > 0) {
      ids.push(...extractTrustDepositIdsFromEvents(msg.txEvents))
    }
    const eventType = messageTypeToEventType(msg.type)
    for (const id of [...new Set(ids)]) {
      if (!map.has(id)) map.set(id, eventType)
    }
  }
  return map
}

export function buildDepositIdToEventTypeMapFromEvents(events: TxEventLike[]): Map<string, string> {
  const map = new Map<string, string>()
  for (const ev of events) {
    const id = getTrustDepositIdFromEvent(ev)
    if (id && !map.has(id)) map.set(id, blockchainEventTypeToHistoryEventType(ev.type ?? ''))
  }
  return map
}

export function messageTypeToEventType(msgType: string): string {
  switch (msgType) {
    case VeranaTrustDepositMessageTypes.AdjustTrustDeposit:
      return 'ADJUST_TRUST_DEPOSIT'
    case VeranaTrustDepositMessageTypes.ReclaimYield:
      return 'RECLAIM_YIELD'
    case VeranaTrustDepositMessageTypes.RepaySlashed:
      return 'REPAY_SLASHED'
    case VeranaTrustDepositMessageTypes.SlashTrustDeposit:
      return 'SLASH_TRUST_DEPOSIT'
    case VeranaTrustDepositMessageTypes.BurnEcosystemSlashedTrustDeposit:
      return 'BURN_ECOSYSTEM_SLASHED_TRUST_DEPOSIT'
    case VeranaTrustDepositMessageTypes.UpdateParams:
      return 'UPDATE_PARAMS'
    default:
      return 'SYNC_LEDGER'
  }
}
