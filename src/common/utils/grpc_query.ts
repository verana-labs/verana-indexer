import { Tendermint37Client } from "@cosmjs/tendermint-rpc";
import { QueryClient } from "@cosmjs/stargate";
import { Network } from "../../network";

/**
 * Minimal protobuf RPC client shape expected by the generated
 * `QueryClientImpl` classes from `@verana-labs/verana-types`.
 */
export interface ProtobufRpcClient {
  request(service: string, method: string, data: Uint8Array): Promise<Uint8Array>;
}

/**
 * Resolves the Tendermint RPC base URL used for gRPC (ABCI) queries.
 * Prefers the `RPC_ENDPOINT` env var, falling back to `Network.RPC`.
 */
export function getRpcBaseUrl(): string {
  const envRpc =
    (typeof process !== "undefined" && process.env?.RPC_ENDPOINT?.trim()) || "";
  const base = envRpc || Network?.RPC || "";
  return base.replace(/\/$/, "");
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
  const rpcUrl = getRpcBaseUrl();
  if (!rpcUrl) {
    throw new Error(
      "Missing RPC base URL for gRPC query. Set RPC_ENDPOINT or Network.RPC."
    );
  }

  const tmClient = await Tendermint37Client.connect(rpcUrl);
  try {
    const queryClient = new QueryClient(tmClient as any);
    const withHeight = typeof blockHeight === "number" && blockHeight > 0;
    const rpc: ProtobufRpcClient = {
      request: async (
        service: string,
        method: string,
        data: Uint8Array
      ): Promise<Uint8Array> => {
        const path = `/${service}/${method}`;
        const response = await queryClient.queryAbci(
          path,
          data,
          withHeight ? blockHeight : undefined
        );
        return response.value;
      },
    };
    return await fn(rpc);
  } finally {
    try {
      tmClient.disconnect();
    } catch {
      // ignore disconnect errors
    }
  }
}
