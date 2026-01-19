import { Service } from '@ourparentcenter/moleculer-decorators-extended';
import { ServiceBroker } from 'moleculer';
import {
  QueryTallyResultRequest,
  QueryTallyResultResponse,
} from '@aura-nw/aurajs/types/codegen/cosmos/gov/v1/query';
import Long from 'long';
import { fromBase64, toHex } from '@cosmjs/encoding';
import { cosmos } from '@aura-nw/aurajs';
import { createJsonRpcRequest } from '@cosmjs/tendermint-rpc/build/jsonrpc';
import { JsonRpcSuccessResponse } from '@cosmjs/json-rpc';
import { HttpBatchClient } from '@cosmjs/tendermint-rpc';
import config from '../../config.json' with { type: 'json' };
import { Proposal } from '../../models';
import BullableService, { QueueHandler } from '../../base/bullable.service';
import {
  ABCI_QUERY_PATH,
  BULL_JOB_NAME,
  getHttpBatchClient,
  getLcdClient,
  IProviderJSClientFactory,
  SERVICE,
} from '../../common';
import Utils from '../../common/utils/utils';
import knex from '../../common/utils/db_connection';

@Service({
  name: SERVICE.V1.CrawlTallyProposalService.key,
  version: 1,
})
export default class CrawlTallyProposalService extends BullableService {
  private _lcdClient!: IProviderJSClientFactory;

  private _httpBatchClient: HttpBatchClient;

  public constructor(public broker: ServiceBroker) {
    super(broker);
    this._httpBatchClient = getHttpBatchClient();
  }

  private async retryRpcCall<T>(
    rpcCall: () => Promise<T>,
    operationName: string
  ): Promise<T> {
    const attempts = config.crawlTallyProposal.rpcRetryAttempts || 3;
    const delay = config.crawlTallyProposal.rpcRetryDelay || 2000;
    const timeout = config.crawlTallyProposal.rpcTimeout || 30000;
    let lastError: Error | unknown;

    for (let attempt = 1; attempt <= attempts; attempt++) {
      try {
        return await Promise.race([
          rpcCall(),
          new Promise<never>((_, reject) => {
            setTimeout(() => reject(new Error('RPC call timeout')), timeout);
          }),
        ]);
      } catch (error: unknown) {
        lastError = error;
        const isLastAttempt = attempt === attempts;
        const err = error as NodeJS.ErrnoException;
        
        const errorMessage = err?.message || String(error);
        const isNetworkError = err?.code === 'EACCES' || err?.code === 'ECONNREFUSED' || 
                              err?.code === 'ETIMEDOUT' || err?.code === 'ENOTFOUND' ||
                              err?.code === 'ECONNABORTED' || errorMessage.toLowerCase().includes('timeout');
        
        if (isLastAttempt) {
          if (isNetworkError) {
            this.logger.warn(
              `⚠️ RPC call failed due to network/timeout error (${err.code || 'timeout'}): ${operationName}. ${err.message || errorMessage}. This is non-critical. Will retry on next job execution.`
            );
          } else {
            this.logger.error(
              `❌ RPC call failed after ${attempts} attempts: ${operationName}. Error: ${errorMessage}`
            );
          }
          throw error;
        }

        const backoffDelay = delay * (2 ** (attempt - 1));
        this.logger.warn(
          `⚠️ RPC call failed (attempt ${attempt}/${attempts}): ${operationName}. Retrying in ${backoffDelay}ms...`
        );
        await new Promise<void>((resolve) => {
          setTimeout(() => {
            resolve();
          }, backoffDelay);
        });
      }
    }

    throw lastError instanceof Error 
      ? lastError 
      : new Error(`Failed to execute ${operationName} after ${attempts} attempts`);
  }

  @QueueHandler({
    queueName: BULL_JOB_NAME.CRAWL_TALLY_PROPOSAL,
    jobName: BULL_JOB_NAME.CRAWL_TALLY_PROPOSAL,
    // prefix: `horoscope-v2-${config.chainId}`,
  })
  public async handleJob(_payload: object): Promise<void> {
    try {
      this.logger.info('Update proposal tally');
      const lcdClient = await getLcdClient();
      if (!lcdClient?.provider) {
        this.logger.warn(' LCD client not available, skipping proposal tally update. Will retry on next job execution.');
        return;
      }
      this._lcdClient = lcdClient;

      const batchQueries: any[] = [];
      const patchQueries: any[] = [];

      const now = new Date(Date.now() - 10);
      const prev = new Date(Date.now() - 30);
      // Query proposals that match the conditions to update tally and turnout
      const votingProposals = await Proposal.query()
        // Proposals that are still in the voting period
        .where('status', Proposal.STATUS.PROPOSAL_STATUS_VOTING_PERIOD)
        // Proposals that had just completed
        .orWhere((builder) =>
          builder
            .whereIn('status', [
              Proposal.STATUS.PROPOSAL_STATUS_FAILED,
              Proposal.STATUS.PROPOSAL_STATUS_PASSED,
              Proposal.STATUS.PROPOSAL_STATUS_REJECTED,
            ])
            .andWhere('voting_end_time', '<=', now)
            .andWhere('voting_end_time', '>', prev)
        )
        // Old proposals that finished a long time ago but just got crawled recently so its tally and turnout are missing
        .orWhere('turnout', null)
        .select('*');

      votingProposals.forEach((proposal: Proposal) => {
        const request: QueryTallyResultRequest = {
          proposalId: Long.fromInt(proposal.proposal_id),
        };
        const data = toHex(
          cosmos.gov.v1.QueryTallyResultRequest.encode(request).finish()
        );

        batchQueries.push(
          this._httpBatchClient.execute(
            createJsonRpcRequest('abci_query', {
              path: ABCI_QUERY_PATH.TALLY_RESULT,
              data,
            })
          )
        );
      });

      const pool = await this.retryRpcCall(
        () => this._lcdClient.provider.cosmos.staking.v1beta1.pool(),
        'getStakingPool'
      ) as any;
      
      const result: JsonRpcSuccessResponse[] = await Promise.all(batchQueries);
      const proposalTally: QueryTallyResultResponse[] = result.map(
        (res: JsonRpcSuccessResponse) =>
          cosmos.gov.v1.QueryTallyResultResponse.decode(
            fromBase64(res.result.response.value)
          )
      );

      proposalTally.forEach((pro, index) => {
        if (pro.tally) {
          const tally = {
            yes: pro.tally.yesCount,
            no: pro.tally.noCount,
            abstain: pro.tally.abstainCount,
            noWithVeto: pro.tally.noWithVetoCount,
          };
          let turnout = 0;
          if (pool && pool.pool && tally) {
            turnout =
              Number(
                ((BigInt(tally.yes) +
                  BigInt(tally.no) +
                  BigInt(tally.abstain) +
                  BigInt(tally.noWithVeto)) *
                  BigInt(100000000)) /
                BigInt(pool.pool.bonded_tokens)
              ) / 1000000;
          }

          patchQueries.push(
            Proposal.query()
              .where('proposal_id', votingProposals[index].proposal_id)
              .patch({
                tally: Utils.camelizeKeys(tally),
                turnout,
              })
          );
        }
      });

      if (patchQueries.length > 0) {
        await knex.transaction(async (trx) => {
          await Promise.all(
            patchQueries.map(query => query.transacting(trx))
          );
        }).catch((error) => {
          this.logger.error(
            `Error update proposals tally: ${JSON.stringify(votingProposals)}`
          );
          this.logger.error(error);
        });
      }
    } catch (error: unknown) {
      const err = error as NodeJS.ErrnoException;
      const errorMessage = err?.message || String(error);
      const isNetworkError = err?.code === 'EACCES' || err?.code === 'ECONNREFUSED' || 
                            err?.code === 'ETIMEDOUT' || err?.code === 'ENOTFOUND' ||
                            err?.code === 'ECONNABORTED' || errorMessage.toLowerCase().includes('timeout');
      
      if (isNetworkError) {
        this.logger.warn(
          `⚠️ Network/timeout error in proposal tally update (${err.code || 'timeout'}): ${errorMessage}. This is non-critical. Will retry on next job execution.`
        );
      } else {
        this.logger.error(
          `❌ Error in proposal tally update: ${errorMessage}`
        );
        throw error;
      }
    }
  }

  public async _start() {
    this.createJob(
      BULL_JOB_NAME.CRAWL_TALLY_PROPOSAL,
      BULL_JOB_NAME.CRAWL_TALLY_PROPOSAL,
      {},
      {
        removeOnComplete: true,
        removeOnFail: {
          count: 3,
        },
        repeat: {
          every: config.crawlTallyProposal.millisecondCrawl,
        },
      }
    );

    return super._start();
  }
}
