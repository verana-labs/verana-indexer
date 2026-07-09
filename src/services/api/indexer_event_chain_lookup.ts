import {
  QueryClientImpl as CoQueryClientImpl,
  QueryGetCorporationRequest,
  QueryListCorporationsRequest,
} from '@verana-labs/verana-types/codec/verana/co/v1/query'
import { withAbciQueryClient } from '../../common/utils/grpc_query'
import { fetchParticipant } from '../../modules/pp-height-sync/pp_height_sync_helpers'
import { normalizeDid } from './indexer_event_utils'

export type ChainCorporation = {
  corporationId?: number
  did?: string
}

function toCorporationId(value: unknown): number | undefined {
  const id = Number(value)
  return Number.isInteger(id) && id > 0 ? id : undefined
}

export async function fetchCorporationById(corporationId: number, blockHeight?: number): Promise<ChainCorporation> {
  try {
    return await withAbciQueryClient(blockHeight, async (rpc) => {
      const query = new CoQueryClientImpl(rpc)
      const response = await query.GetCorporation(
        QueryGetCorporationRequest.fromPartial({ corporationId, activeGfOnly: false, preferredLanguage: '' })
      )
      const corporation = response?.corporation
      if (!corporation) return {}
      return { corporationId: toCorporationId(corporation.id), did: normalizeDid(corporation.did) }
    })
  } catch {
    return {}
  }
}

let corporationsByAddressCache: { blockHeight?: number; corporations: Map<string, ChainCorporation> } | null = null

async function listCorporationsByAddress(blockHeight?: number): Promise<Map<string, ChainCorporation>> {
  const cached = corporationsByAddressCache
  if (cached && cached.blockHeight === blockHeight) return cached.corporations

  const corporations = new Map<string, ChainCorporation>()
  try {
    await withAbciQueryClient(blockHeight, async (rpc) => {
      const query = new CoQueryClientImpl(rpc)
      const response = await query.ListCorporations(
        QueryListCorporationsRequest.fromPartial({ activeGfOnly: false, preferredLanguage: '', responseMaxSize: 0 })
      )
      for (const corporation of response?.corporations ?? []) {
        corporations.set(corporation.policyAddress, {
          corporationId: toCorporationId(corporation.id),
          did: normalizeDid(corporation.did),
        })
      }
    })
  } catch {
    return corporations
  }

  corporationsByAddressCache = { blockHeight, corporations }
  return corporations
}

export async function fetchCorporationByAddress(address: string, blockHeight?: number): Promise<ChainCorporation> {
  const corporations = await listCorporationsByAddress(blockHeight)
  return corporations.get(address) ?? {}
}

export async function fetchParticipantDid(participantId: number, blockHeight?: number): Promise<string | undefined> {
  const result = await fetchParticipant(participantId, blockHeight)
  return normalizeDid(result?.participant?.did)
}
