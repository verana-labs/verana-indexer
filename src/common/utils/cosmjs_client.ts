/* eslint-disable import/no-extraneous-dependencies */
import { HttpBatchClient } from '@cosmjs/tendermint-rpc';
import config from '../../../config.json' with { type: 'json' };
import { Network } from '../../../network';

export default class CosmjsClient {
  public httpBatchClient: HttpBatchClient;

  public constructor() {
    const rpc = Network?.RPC || '';
    this.httpBatchClient = new HttpBatchClient((rpc as any), {
      batchSizeLimit: config.httpBatchRequest.batchSizeLimit ?? 20,
      dispatchInterval: config.httpBatchRequest.dispatchMilisecond ?? 20,
    });
  }
}

const client = new CosmjsClient();

export function getHttpBatchClient(): HttpBatchClient {
  return client.httpBatchClient;
}
