import { Buffer } from 'node:buffer'
import {
  QueryClientImpl as EcQueryClientImpl,
  QueryGetEcosystemRequest,
} from '@verana-labs/verana-types/codec/verana/ec/v1/query'
import type { EcosystemWithVersions } from '@verana-labs/verana-types/codec/verana/ec/v1/types'
import { dateToIsoOrNull } from '../../common/utils/date_utils'
import { withAbciQueryClient } from '../../common/utils/grpc_query'
import { VeranaEcosystemMessageTypes } from '../../common/verana-message-types'

export interface LedgerEcosystemVersion {
  id?: number | string
  version?: number | string
  created?: string
  active_since?: string | null
  activeSince?: string | null
  documents?: Array<{
    id?: number | string
    created?: string
    language?: string | null
    url?: string | null
    digest_sri?: string | null
    digestSri?: string | null
  }>
  [key: string]: unknown
}

export interface LedgerEcosystem {
  id?: number | string
  ecosystem_id?: number | string
  did?: string
  corporation_id?: number
  created?: string
  modified?: string
  archived?: string | null
  deposit?: number | string
  aka?: string | null
  language?: string | null
  active_version?: number | string
  activeVersion?: number | string
  versions?: LedgerEcosystemVersion[]
  [key: string]: unknown
}

export interface LedgerEcosystemResponse {
  ecosystem?: LedgerEcosystem
  ec?: LedgerEcosystem
  [key: string]: unknown
}

export interface TrMessageLike {
  type: string
  content?: Record<string, unknown> | null
}

const TR_MESSAGE_TYPES = new Set<string>([
  VeranaEcosystemMessageTypes.CreateEcosystem,
  VeranaEcosystemMessageTypes.UpdateEcosystem,
  VeranaEcosystemMessageTypes.ArchiveEcosystem,
  VeranaEcosystemMessageTypes.AddGovernanceFrameworkDoc,
  VeranaEcosystemMessageTypes.IncreaseGovernanceFrameworkVersion,
  VeranaEcosystemMessageTypes.UpdateParams,
])

export function mapEcosystemToLedgerEcosystem(eco: EcosystemWithVersions): LedgerEcosystem {
  const versions: LedgerEcosystemVersion[] = (eco.versions ?? []).map((v) => ({
    id: v.id,
    version: v.version,
    created: dateToIsoOrNull(v.created) ?? undefined,
    active_since: dateToIsoOrNull(v.activeSince),
    documents: (v.documents ?? []).map((d) => ({
      id: d.id,
      created: dateToIsoOrNull(d.created) ?? undefined,
      language: d.language ?? null,
      url: d.url ?? null,
      digest_sri: d.digestSri ?? null,
    })),
  }))

  return {
    id: eco.id,
    did: eco.did,
    corporation_id: Number(eco.corporationId ?? 0) || 0,
    created: dateToIsoOrNull(eco.created) ?? undefined,
    modified: dateToIsoOrNull(eco.modified) ?? undefined,
    archived: eco.archived ? (dateToIsoOrNull(eco.modified) ?? new Date().toISOString()) : null,
    language: eco.language ?? null,
    active_version: eco.activeVersion,
    versions,
  } as LedgerEcosystem
}

export async function getEcosystem(ecosystemId: number, blockHeight?: number): Promise<LedgerEcosystemResponse | null> {
  return withAbciQueryClient(blockHeight, async (rpc) => {
    const ecQuery = new EcQueryClientImpl(rpc)
    const res = await ecQuery.GetEcosystem(
      QueryGetEcosystemRequest.fromPartial({
        id: ecosystemId,
        activeGfOnly: false,
        preferredLanguage: '',
      })
    )
    if (!res?.ecosystem) return null
    return { ecosystem: mapEcosystemToLedgerEcosystem(res.ecosystem) }
  })
}

export function isTrMessageType(type: string): boolean {
  return TR_MESSAGE_TYPES.has(type)
}

export function extractEcosystemIdFromContent(content: Record<string, unknown> | null | undefined): number | null {
  if (!content || typeof content !== 'object') return null
  const candidates = [
    content.ecosystem_id,
    content.ecosystemId,
    content.ecosystem_id,
    content.ecosystemId,
    content.id,
    (content.ecosystem as Record<string, unknown> | undefined)?.id,
    (content.ecosystem as Record<string, unknown> | undefined)?.ecosystem_id,
    (content.ecosystem as Record<string, unknown> | undefined)?.ecosystemId,
    (content.ecosystem as Record<string, unknown> | undefined)?.id,
    (content.ecosystem as Record<string, unknown> | undefined)?.ecosystem_id,
    (content.ecosystem as Record<string, unknown> | undefined)?.ecosystemId,
  ]
  for (const raw of candidates) {
    const n = Number(raw)
    if (Number.isInteger(n) && n > 0) return n
  }
  return null
}

export interface TxEventLike {
  type?: string
  attributes?: Array<{ key?: string; value?: string }>
}

function decodeAttributeValue(value: string | undefined): string {
  if (value == null || value === '') return ''
  try {
    if (/^[A-Za-z0-9+/=]+$/.test(value) && value.length % 4 === 0) {
      return Buffer.from(value, 'base64').toString('utf-8')
    }
  } catch {
    //
  }
  return value
}

function decodeAttributeKey(key: string | undefined): string {
  if (key == null || key === '') return ''
  try {
    if (/^[A-Za-z0-9+/=]+$/.test(key) && key.length % 4 === 0) {
      return Buffer.from(key, 'base64').toString('utf-8')
    }
  } catch {
    //
  }
  return key
}

function getDecodedEventAttributes(
  event: TxEventLike,
  decodeAttributes?: boolean
): Array<{ key: string; value: string }> {
  return (event.attributes ?? []).map((attr) => ({
    key: decodeAttributes ? decodeAttributeKey(attr.key) : (attr.key ?? ''),
    value: decodeAttributes ? decodeAttributeValue(attr.value) : (attr.value ?? ''),
  }))
}

const TR_EVENT_TYPES = new Set<string>([
  'create_ecosystem',
  'create_governance_framework_version',
  'create_governance_framework_document',
  'add_governance_framework_document',
  'increase_active_gf_version',
  'update_ecosystem',
  'archive_ecosystem',
])

export function extractEcosystemIdsFromEvents(events: TxEventLike[], decodeAttributes?: boolean): number[] {
  const ids: number[] = []
  for (const ev of events) {
    const eventType = (ev.type ?? '').toLowerCase()
    if (!TR_EVENT_TYPES.has(eventType)) continue

    const attrs = getDecodedEventAttributes(ev, decodeAttributes)
    if (attrs.length === 0) continue

    for (const attr of attrs) {
      const keyLower = attr.key.toLowerCase()
      if (keyLower === 'ecosystem_id') {
        const n = Number(attr.value)
        if (Number.isInteger(n) && n > 0) {
          ids.push(n)
        }
      }
    }
  }
  return [...new Set(ids)]
}
