import { QueryClient } from '@cosmjs/stargate'
import { Tendermint37Client } from '@cosmjs/tendermint-rpc'
import { Network } from '../../network'

/**
 * Minimal protobuf RPC client shape expected by the generated
 * `QueryClientImpl` classes from `@verana-labs/verana-types`.
 */
export interface ProtobufRpcClient {
  request(service: string, method: string, data: Uint8Array): Promise<Uint8Array>
}

/**
 * Resolves the Tendermint RPC base URL used for gRPC (ABCI) queries.
 * Prefers the `RPC_ENDPOINT` env var, falling back to `Network.RPC`.
 */
export function getRpcBaseUrl(): string {
  const envRpc = (typeof process !== 'undefined' && process.env?.RPC_ENDPOINT?.trim()) || ''
  const base = envRpc || Network?.RPC || ''
  return base.replace(/\/$/, '')
}

const FUTURE_HEIGHT_RETRY_DELAYS_MS = [500, 1000, 2000, 4000, 4000, 4000, 4000]
const MAX_WAITED_HEIGHT_LAG = 3

export function isAbciNotFound(error: unknown): boolean {
  return /code = NotFound/.test((error as Error)?.message ?? '')
}

function isFutureHeightError(error: unknown): boolean {
  return /cannot query with height in the future/.test((error as Error)?.message ?? '')
}

/**
 * Opens a Tendermint RPC connection, builds an ABCI-backed protobuf RPC client
 * pinned to `blockHeight` (when provided), runs `fn`, and always disconnects.
 *
 * This lets us query the ledger state for the exact block height currently
 * being processed via gRPC instead of the REST/LCD API.
 */
export async function withAbciQueryClient<T>(
  blockHeight: number | undefined,
  fn: (rpc: ProtobufRpcClient) => Promise<T>
): Promise<T> {
  const rpcUrl = getRpcBaseUrl()
  if (!rpcUrl) {
    throw new Error('Missing RPC base URL for gRPC query. Set RPC_ENDPOINT or Network.RPC.')
  }

  const tmClient = await Tendermint37Client.connect(rpcUrl)
  try {
    const queryClient = new QueryClient(tmClient as any)
    const withHeight = typeof blockHeight === 'number' && blockHeight > 0
    const rpc: ProtobufRpcClient = {
      request: async (service: string, method: string, data: Uint8Array): Promise<Uint8Array> => {
        const path = `/${service}/${method}`
        const response = await queryClient.queryAbci(path, data, withHeight ? blockHeight : undefined)
        return response.value
      },
    }
    const isHeightImminent = async (): Promise<boolean> => {
      const status = await tmClient.status().catch(() => null)
      return status !== null && status.syncInfo.latestBlockHeight + MAX_WAITED_HEIGHT_LAG >= Number(blockHeight)
    }
    for (const delayMs of FUTURE_HEIGHT_RETRY_DELAYS_MS) {
      try {
        return await fn(rpc)
      } catch (error) {
        if (!withHeight || !isFutureHeightError(error) || !(await isHeightImminent())) throw error
        await new Promise((resolve) => setTimeout(resolve, delayMs))
      }
    }
    return await fn(rpc)
  } finally {
    try {
      tmClient.disconnect()
    } catch {
      // ignore disconnect errors
    }
  }
}
