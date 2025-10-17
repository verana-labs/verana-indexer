import { PublicClient, createPublicClient, http } from 'viem';
import config from '../../../config.json' with { type: 'json' };
import '../../../fetch-polyfill.js';
import { Network } from '../../../network';

let viemClient!: PublicClient;

export function getViemClient(): PublicClient {
  if (!viemClient) {
    const EVMJSONRPC=Network?.EVMJSONRPC
    if (!EVMJSONRPC) {
      throw new Error(`EVMJSONRPC not found.`);
    }
    viemClient = createPublicClient({
      batch: {
        multicall: {
          batchSize: config.viemConfig.multicall.batchSize,
          wait: config.viemConfig.multicall.waitMilisecond,
        },
      },
      transport: http((EVMJSONRPC as any), {
        batch: {
          batchSize: config.viemConfig.transport.batchSize,
          wait: config.viemConfig.transport.waitMilisecond,
        },
      }),
    });
  }
  return viemClient;
}
