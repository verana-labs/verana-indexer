import { Buffer } from 'node:buffer'
import {
  QueryClientImpl as PpQueryClientImpl,
  QueryGetParticipantRequest,
  QueryGetParticipantSessionRequest,
} from '@verana-labs/verana-types/codec/verana/pp/v1/query'
import { Participant, ParticipantSession } from '@verana-labs/verana-types/codec/verana/pp/v1/types'
import { withAbciQueryClient } from '../../common/utils/grpc_query'

export type ParticipantMessagePayload = {
  type: string
  content: any
  timestamp?: string
  height?: number
  txHash?: string
  txCode?: number
  msgIndex?: number
  txEvents?: Array<{ type?: string; attributes?: Array<{ key?: string; value?: string }> }>
}

function decodeEventValue(raw: string | undefined): string {
  if (!raw) return ''
  try {
    if (/^[A-Za-z0-9+/=]+$/.test(raw) && raw.length % 4 === 0) {
      return Buffer.from(raw, 'base64').toString('utf-8')
    }
  } catch {
    //
  }
  return raw
}

export function extractIdsFromTxEvents(events: ParticipantMessagePayload['txEvents'], keyPatterns: string[]): number[] {
  if (!Array.isArray(events) || events.length === 0) return []
  const ids = new Set<number>()
  for (const event of events) {
    const attrs = event?.attributes || []
    for (const attr of attrs) {
      const key = decodeEventValue(attr?.key).toLowerCase()
      const value = decodeEventValue(attr?.value)
      if (!keyPatterns.some((pattern) => key.includes(pattern))) continue
      const id = Number(value)
      if (Number.isInteger(id) && id > 0) {
        ids.add(id)
      }
    }
  }
  return [...ids]
}

export function extractImpactedParticipantIds(msg: ParticipantMessagePayload): number[] {
  const ids = new Set<number>()
  const candidates = [
    msg?.content?.id,
    msg?.content?.participant_id,
    msg?.content?.participant_id,
    msg?.content?.validator_participant_id,
    msg?.content?.validatorParticipantId,
    msg?.content?.issuer_participant_id,
    msg?.content?.issuerParticipantId,
    msg?.content?.verifier_participant_id,
    msg?.content?.verifierParticipantId,
    msg?.content?.agent_participant_id,
    msg?.content?.agentParticipantId,
    msg?.content?.wallet_agent_participant_id,
    msg?.content?.walletAgentParticipantId,
  ]

  for (const value of candidates) {
    const id = Number(value)
    if (Number.isInteger(id) && id > 0) ids.add(id)
  }

  for (const id of extractIdsFromTxEvents(msg.txEvents, [
    'participant_id',
    'root_participant_id',
    'validator_participant_id',
    'issuer_participant_id',
    'verifier_participant_id',
    'agent_participant_id',
    'wallet_agent_participant_id',
  ])) {
    ids.add(id)
  }

  return [...ids]
}

export function extractStartParticipantOpNewParticipantId(msg: ParticipantMessagePayload): number | undefined {
  const validatorRaw = msg?.content?.validator_participant_id ?? msg?.content?.validatorParticipantId
  const validatorId = Number(validatorRaw)
  const hasValidator =
    validatorRaw !== undefined &&
    validatorRaw !== null &&
    String(validatorRaw).trim() !== '' &&
    Number.isInteger(validatorId) &&
    validatorId > 0

  const directCandidates = [msg?.content?.id, msg?.content?.participant_id, msg?.content?.participantId]
  for (const candidate of directCandidates) {
    const n = Number(candidate)
    if (!Number.isInteger(n) || n <= 0) continue
    if (hasValidator && n === validatorId) continue
    return n
  }

  const exactNewParticipantKeys = new Set([
    'participant_id',
    'participantid',
    'new_participant_id',
    'new_participantid',
    'created_participant_id',
    'created_participantid',
  ])
  const fromExactEvents: number[] = []
  if (Array.isArray(msg.txEvents)) {
    for (const event of msg.txEvents) {
      for (const attr of event?.attributes || []) {
        const key = decodeEventValue(attr?.key).toLowerCase()
        if (!exactNewParticipantKeys.has(key)) continue
        const id = Number(decodeEventValue(attr?.value))
        if (Number.isInteger(id) && id > 0) {
          fromExactEvents.push(id)
        }
      }
    }
  }
  const uniqueExact = [...new Set(fromExactEvents)]
  const filteredExact = hasValidator ? uniqueExact.filter((id) => id !== validatorId) : uniqueExact
  if (filteredExact.length === 1) {
    return filteredExact[0]
  }
  if (filteredExact.length > 1) {
    const notValidator = filteredExact.filter((id) => !hasValidator || id !== validatorId)
    if (notValidator.length === 1) return notValidator[0]
  }

  const looseFromEvents: number[] = []
  if (Array.isArray(msg.txEvents)) {
    for (const event of msg.txEvents) {
      for (const attr of event?.attributes || []) {
        const key = decodeEventValue(attr?.key).toLowerCase()
        if (!key.includes('participant_id') && !key.includes('participantid')) {
          continue
        }
        if (key.includes('validator')) continue
        const id = Number(decodeEventValue(attr?.value))
        if (Number.isInteger(id) && id > 0) {
          looseFromEvents.push(id)
        }
      }
    }
  }
  const uniqueLoose = [...new Set(looseFromEvents)]
  const filteredLoose = hasValidator ? uniqueLoose.filter((id) => id !== validatorId) : uniqueLoose
  if (filteredLoose.length === 1) {
    return filteredLoose[0]
  }

  const impacted = extractImpactedParticipantIds(msg)
  const remaining = hasValidator ? impacted.filter((id) => id !== validatorId) : impacted
  if (remaining.length === 1) {
    return remaining[0]
  }

  return undefined
}

export function extractImpactedSessionIds(msg: ParticipantMessagePayload): string[] {
  const ids = new Set<string>()
  const directCandidates = [msg?.content?.id, msg?.content?.session_id, msg?.content?.sessionId]
  for (const candidate of directCandidates) {
    if (candidate === null || candidate === undefined) continue
    const value = String(candidate).trim()
    if (value.length > 0) ids.add(value)
  }

  if (Array.isArray(msg?.txEvents)) {
    for (const event of msg.txEvents) {
      for (const attr of event?.attributes || []) {
        const key = decodeEventValue(attr?.key).toLowerCase()
        if (!key.includes('session_id') && !key.includes('sessionid') && !key.includes('session')) continue
        const value = decodeEventValue(attr?.value).trim()
        if (value) ids.add(value)
      }
    }
  }

  return [...ids]
}

export async function fetchParticipant(
  participantId: number,
  blockHeight?: number
): Promise<{ participant: Record<string, unknown> } | null> {
  try {
    return await withAbciQueryClient(blockHeight, async (rpc) => {
      const query = new PpQueryClientImpl(rpc)
      const res = await query.GetParticipant(QueryGetParticipantRequest.fromPartial({ id: participantId }))
      if (!res?.participant) return null
      return {
        participant: Participant.toJSON(res.participant) as Record<string, unknown>,
      }
    })
  } catch {
    return null
  }
}

export async function fetchParticipantSession(
  sessionId: string,
  blockHeight?: number
): Promise<{ session: Record<string, unknown> } | null> {
  try {
    return await withAbciQueryClient(blockHeight, async (rpc) => {
      const query = new PpQueryClientImpl(rpc)
      const res = await query.GetParticipantSession(QueryGetParticipantSessionRequest.fromPartial({ id: sessionId }))
      if (!res?.session) return null
      return {
        session: ParticipantSession.toJSON(res.session) as Record<string, unknown>,
      }
    })
  } catch {
    return null
  }
}
