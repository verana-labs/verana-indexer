import {
  QueryClientImpl as CoQueryClientImpl,
  QueryGetCorporationRequest,
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

export async function fetchParticipantDid(participantId: number, blockHeight?: number): Promise<string | undefined> {
  const result = await fetchParticipant(participantId, blockHeight)
  return normalizeDid(result?.participant?.did)
}
