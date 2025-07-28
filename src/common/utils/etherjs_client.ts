import { PublicClient, createPublicClient, http } from 'viem';
import config from '../../../config.json' assert { type: 'json' };
import '../../../fetch-polyfill.js';
import networks from '../../../network.json' assert { type: 'json' };

let viemClient!: PublicClient;

export function getViemClient(): PublicClient {
  if (!viemClient) {
    const selectedChain = networks.find(
      (network) => network.chainId === config.chainId
    );
    if (!selectedChain?.EVMJSONRPC) {
      throw new Error(`EVMJSONRPC not found with chainId: ${config.chainId}`);
    }
    viemClient = createPublicClient({
      batch: {
        multicall: {
          batchSize: config.viemConfig.multicall.batchSize,
          wait: config.viemConfig.multicall.waitMilisecond,
        },
      },
      transport: http(selectedChain.EVMJSONRPC[0], {
        batch: {
          batchSize: config.viemConfig.transport.batchSize,
          wait: config.viemConfig.transport.waitMilisecond,
        },
      }),
    });
  }
  return viemClient;
}
