import {
  QueryGetDigestRequest,
  QueryClientImpl as DiQueryClientImpl,
} from '@verana-labs/verana-types/codec/verana/di/v1/query'
import type { DigestInfo as LedgerDigest } from '@verana-labs/verana-types/codec/verana/di/v1/query'
import { dateToIsoOrNull } from '../../common/utils/date_utils'
import { withAbciQueryClient } from '../../common/utils/grpc_query'

export interface DigestRow {
  digest: string
  created: string | null
}

export function serializeLedgerDigest(digest: LedgerDigest): DigestRow {
  return {
    digest: digest.digest,
    created: dateToIsoOrNull(digest.created),
  }
}

export async function fetchDigest(
  digest: string,
  blockHeight: number | undefined
): Promise<LedgerDigest | undefined> {
  return withAbciQueryClient(blockHeight, async (rpc) => {
    const query = new DiQueryClientImpl(rpc)
    const res = await query.GetDigest(QueryGetDigestRequest.fromPartial({ digest }))
    return res?.digest ?? undefined
  })
}
