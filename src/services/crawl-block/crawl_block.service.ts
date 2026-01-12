/* eslint-disable import/no-extraneous-dependencies */
import { ServiceBroker } from 'moleculer';
import { Service } from '@ourparentcenter/moleculer-decorators-extended';
import {
  GetLatestBlockResponseSDKType,
  GetNodeInfoResponseSDKType,
} from '@aura-nw/aurajs/types/codegen/cosmos/base/tendermint/v1beta1/query';
import { CommitSigSDKType } from '@aura-nw/aurajs/types/codegen/tendermint/types/types';
import { HttpBatchClient } from '@cosmjs/tendermint-rpc';
import { createJsonRpcRequest } from '@cosmjs/tendermint-rpc/build/jsonrpc';
import { JsonRpcRequest, JsonRpcSuccessResponse } from '@cosmjs/json-rpc';
import { Queue } from 'bullmq';
import Redis from 'ioredis';
import WebSocket from 'ws';
import {
  BULL_JOB_NAME,
  getHttpBatchClient,
  getLcdClient,
  IProviderJSClientFactory,
  SERVICE,
  Config,
} from '../../common';
import { indexerStatusManager } from '../manager/indexer_status.service';
import { handleErrorGracefully, checkCrawlingStatus } from '../../common/utils/error_handler';
import { Block, BlockCheckpoint, Event, EventAttribute } from '../../models';
import BullableService, { QueueHandler, DEFAULT_PREFIX } from '../../base/bullable.service';
import config from '../../config.json' with { type: 'json' };
import knex from '../../common/utils/db_connection';
import ChainRegistry from '../../common/utils/chain.registry';
import { getProviderRegistry } from '../../common/utils/provider.registry';
import { Network } from '../../network';

@Service({
  name: SERVICE.V1.CrawlBlock.key,
  version: 1,
})
export default class CrawlBlockService extends BullableService {
  private _currentBlock = 0;

  private _httpBatchClient: HttpBatchClient;

  private _lcdClient!: IProviderJSClientFactory;

  private _registry!: ChainRegistry;

  private _isCaughtUp: boolean = false;

  private _currentInterval: number = config.crawlBlock.millisecondCrawl;

  private _updatingInterval: boolean = false;

  private _processingLock: boolean = false;

  private _lastCheckedHeight: number = 0;

  private _websocket: WebSocket | null = null;

  private _websocketConnected: boolean = false;

  private _websocketReconnectTimer: NodeJS.Timeout | null = null;

  private _initialSyncComplete: boolean = false;

  private _lastLatestBlockHeight: number = 0;

  private _blocksProcessedThisCycle: number = 0;

  private _lastWebSocketBlockHeight: number = 0;

  private _websocketFailureCount: number = 0;

  private _lastRpcBackupCheck: number = 0;

  public constructor(public broker: ServiceBroker) {
    super(broker);
    this._httpBatchClient = getHttpBatchClient();
  }

  @QueueHandler({
    queueName: BULL_JOB_NAME.CRAWL_BLOCK,
    jobName: BULL_JOB_NAME.CRAWL_BLOCK,
  })
  private async jobHandler(_payload: any): Promise<void> {
    try {
      await this.initEnv();
      await this.handleJobCrawlBlock();
    } catch (error: any) {
      if (error?.code === 'EACCES' || error?.code === 'ECONNREFUSED' || error?.code === 'ETIMEDOUT' || error?.code === 'ENOTFOUND') {
        this.logger.error(`❌ Network connection error (${error.code}): ${error.message}. Will retry on next job execution.`);
        this.logger.info('⏭️ Skipping this cycle due to network issues');
        return;
      }
      await handleErrorGracefully(
        error,
        SERVICE.V1.CrawlBlock.key,
        'Unexpected error in job handler'
      );
    }
  }

  private async initEnv() {
    try {
      this._lcdClient = await getLcdClient();
    } catch (error: any) {
      const wasStopped = await handleErrorGracefully(
        error,
        SERVICE.V1.CrawlBlock.key,
        'Failed to initialize LCD client'
      );
      
      if (wasStopped) {
        throw error; // Re-throw to stop the job handler
      } else {
        // Non-critical error, but still throw to be safe
        throw error;
      }
    }

    try {
      const nodeInfo: GetNodeInfoResponseSDKType = await this.retryRpcCall(
        () => this._lcdClient.provider.cosmos.base.tendermint.v1beta1.getNodeInfo(),
        'getNodeInfo'
      );
      const cosmosSdkVersion = nodeInfo?.application_version?.cosmos_sdk_version;
      if (cosmosSdkVersion) {
        this._registry.setCosmosSdkVersionByString(cosmosSdkVersion);
      }
    } catch (error: any) {
      if (error?.code === 'EACCES' || error?.code === 'ECONNREFUSED' || error?.code === 'ETIMEDOUT' || error?.code === 'ENOTFOUND') {
        this.logger.warn(`⚠️ Failed to get node info due to network error (${error.code}): ${error.message}. Continuing without SDK version update.`);
      } else {
        this.logger.warn(`⚠️ Failed to get node info (non-critical): ${error}. Continuing without SDK version update.`);
      }
    }

    let blockHeightCrawled = await BlockCheckpoint.query().findOne({
      job_name: BULL_JOB_NAME.CRAWL_BLOCK,
    });

    if (!blockHeightCrawled) {
      blockHeightCrawled = await BlockCheckpoint.query().insert({
        job_name: BULL_JOB_NAME.CRAWL_BLOCK,
        height: config.crawlBlock.startBlock,
      });
    }

    this._currentBlock = blockHeightCrawled ? blockHeightCrawled.height : 0;
    
    try {
      const latestBlockResponse: GetLatestBlockResponseSDKType = await this.retryRpcCall(
        () => this._lcdClient.provider.cosmos.base.tendermint.v1beta1.getLatestBlock(),
        'getLatestBlock'
      );
      const latestBlockNetwork = latestBlockResponse?.block?.header?.height
        ? parseInt(latestBlockResponse.block.header.height.toString(), 10)
        : 0;
      
      const blocksBehind = latestBlockNetwork - this._currentBlock;
      
      if (blocksBehind > 0) {
        this._initialSyncComplete = false;
        this.logger.info(
          `📊 Initial Sync Status: NOT COMPLETE | Current Block: ${this._currentBlock} | Latest Block: ${latestBlockNetwork} | Blocks Behind: ${blocksBehind} | Status: Catching up with existing blocks...`
        );
      } else {
        this._initialSyncComplete = true;
        this.logger.info(
          `✅ Initial Sync Status: COMPLETE | Current Block: ${this._currentBlock} | Latest Block: ${latestBlockNetwork} | Blocks Behind: ${blocksBehind} | Status: Ready for new blocks`
        );
      }
    } catch (error) {
      this.logger.warn(`⚠️ Could not check initial sync status: ${error}. Will check on first crawl cycle.`);
      this._initialSyncComplete = false;
    }
  }

  async handleJobCrawlBlock() {
    try {
      checkCrawlingStatus();
    } catch {
      this.logger.warn('⏸️ Crawling is stopped, skipping block crawl');
      return;
    }

    if (this._processingLock) {
      return;
    }

    const isCaughtUp = this._isCaughtUp && this._initialSyncComplete;
    const isWebSocketActive = config.crawlBlock.enableWebSocketSubscription &&
      this._websocketConnected &&
      isCaughtUp;
    
    if (isWebSocketActive) {
      const now = Date.now();
      const backupInterval = 10000;
      if (this._lastRpcBackupCheck > 0 && (now - this._lastRpcBackupCheck) < backupInterval) {
        return;
      }
      this._lastRpcBackupCheck = now;
    }

    this._processingLock = true;

    try {
      if (
        config.crawlBlock.enableOptimizedPolling &&
        config.crawlBlock.heightCheckOptimization &&
        this._isCaughtUp
      ) {
        const latestHeight = await this.getLatestBlockHeight();
        
        if (latestHeight === 0) {
          this.logger.warn('⚠️ Failed to get latest block height, skipping height-check optimization');
        } else if (latestHeight <= this._currentBlock && latestHeight === this._lastCheckedHeight) {
          this.logger.debug(`⏭️ No new blocks (latest: ${latestHeight}, current: ${this._currentBlock}), skipping fetch`);
          await this.adjustPollingInterval(latestHeight);
          return;
        } else {
          this._lastCheckedHeight = latestHeight;
        }
      }

      const responseGetLatestBlock: GetLatestBlockResponseSDKType = await this.retryRpcCall(
        () => this._lcdClient.provider.cosmos.base.tendermint.v1beta1.getLatestBlock(),
        'getLatestBlock'
      );

      if (!responseGetLatestBlock?.block?.header?.height) {
        this.logger.error('❌ Failed to get latest block after retries. Skipping this cycle.');
        return;
      }

      const latestBlockNetwork = parseInt(
        responseGetLatestBlock.block.header.height.toString(),
        10
      );

      if (latestBlockNetwork <= 0) {
        this.logger.error(`❌ Invalid latest block height: ${latestBlockNetwork}`);
      }

      if (this._lastLatestBlockHeight > 0 && latestBlockNetwork < this._lastLatestBlockHeight - 1000) {
        this.logger.error(`❌ Block height decreased: ${this._lastLatestBlockHeight} -> ${latestBlockNetwork}`);
      }

      this._lastLatestBlockHeight = latestBlockNetwork;
      const blocksBehind = latestBlockNetwork - this._currentBlock;
      
      if (isCaughtUp && blocksBehind > 1) {
        this.logger.warn(`⚠️ Block gap detected: ${blocksBehind} blocks behind`);
      }

      const startBlock = this._currentBlock + 1;

      let endBlock = startBlock + config.crawlBlock.blocksPerCall - 1;
      if (endBlock > latestBlockNetwork) {
        endBlock = latestBlockNetwork;
      }

      if (startBlock > latestBlockNetwork) {
        this.logger.info(
          `✅ Already at latest block | Current: ${this._currentBlock} | Latest: ${latestBlockNetwork} | Status: Waiting for new blocks`
        );
        await this.adjustPollingInterval(latestBlockNetwork);
        return;
      }

      this._blocksProcessedThisCycle = endBlock - startBlock + 1;
      const blockQueries = [];
      for (let i = startBlock; i <= endBlock; i += 1) {
        try {
          const heightStr = i.toString();

          let blockReq: JsonRpcRequest | null = null;
          let blockResultsReq: JsonRpcRequest | null = null;
          try {
            blockReq = createJsonRpcRequest('block', { height: heightStr });
            blockResultsReq = createJsonRpcRequest('block_results', { height: heightStr });
          } catch (err) {
            this.logger.error(`❌ Failed to create JSON-RPC request at height ${heightStr}: ${err}`);
          }

          if (blockReq && blockResultsReq) {
            blockQueries.push(
              this.executeRpcWithRetry(() => this._httpBatchClient.execute(blockReq!), `block-${heightStr}`),
              this.executeRpcWithRetry(() => this._httpBatchClient.execute(blockResultsReq!), `block_results-${heightStr}`)
            );
          }

        } catch (err) {
          this.logger.error(`❌ Unexpected error preparing request at block ${i}: ${err}`);
        }

      }

      const blockResponsesResults = await Promise.allSettled(blockQueries);
      
      const blockResponses: JsonRpcSuccessResponse[] = [];
      let failedBlocks = 0;
      
      for (let i = 0; i < blockResponsesResults.length; i += 2) {
        const blockResult = blockResponsesResults[i];
        const blockResultsResult = blockResponsesResults[i + 1];
        const blockHeight = startBlock + Math.floor(i / 2);
        
        if (blockResult.status === 'fulfilled' && blockResultsResult.status === 'fulfilled') {
          blockResponses.push(blockResult.value, blockResultsResult.value);
        } else {
          failedBlocks++;
          if (blockResult.status === 'rejected') {
            this.logger.error(`❌ Failed to fetch block ${blockHeight}: ${blockResult.reason}`);
          }
          if (blockResultsResult.status === 'rejected') {
            this.logger.error(`❌ Failed to fetch block_results ${blockHeight}: ${blockResultsResult.reason}`);
          }
        }
      }

      if (blockResponses.length === 0) {
        this.logger.error('❌ All block RPC calls failed. Skipping this cycle.');
        return;
      }

      if (failedBlocks > 0) {
        this.logger.warn(`⚠️ ${failedBlocks} block(s) failed to fetch, but processing ${blockResponses.length / 2} successful blocks`);
      }

      const mergeBlockResponses: any[] = [];

      for (let i = 0; i < blockResponses?.length; i += 2) {
        const blockData = blockResponses[i]?.result;
        const blockResultData = blockResponses[i + 1]?.result;
        
        if (!blockData || !blockResultData) {
          const blockHeight = startBlock + Math.floor(i / 2);
          this.logger.warn(`⚠️ Skipping block ${blockHeight} due to missing data`);
          continue;
        }
        
        mergeBlockResponses.push({
          ...blockData,
          block_result: blockResultData,
        });
      }

      if (mergeBlockResponses.length === 0) {
        this.logger.warn('⚠️ No valid blocks to process. Skipping this cycle.');
        return;
      }

      await this.handleListBlock(mergeBlockResponses);

      let highestSavedBlock = this._currentBlock;
      mergeBlockResponses.forEach((block) => {
        const height = parseInt(block?.block?.header?.height ?? '0', 10);
        if (height > highestSavedBlock) {
          highestSavedBlock = height;
        }
      });

      if (highestSavedBlock > this._currentBlock) {
        await BlockCheckpoint.query()
          .update(
            BlockCheckpoint.fromJson({
              job_name: BULL_JOB_NAME.CRAWL_BLOCK,
              height: highestSavedBlock,
            })
          )
          .where({
            job_name: BULL_JOB_NAME.CRAWL_BLOCK,
          });
        this._currentBlock = highestSavedBlock;
        
        if (highestSavedBlock < endBlock) {
          this.logger.warn(
            `⚠️ Some blocks failed to save. Checkpoint updated to ${highestSavedBlock} instead of ${endBlock}. ` +
            `Failed blocks: ${endBlock - highestSavedBlock} (will retry on next cycle)`
          );
        }
        
        const remainingBlocks = latestBlockNetwork - this._currentBlock;
        if (!this._initialSyncComplete && remainingBlocks <= 0) {
          this._initialSyncComplete = true;
          this.logger.info(`✅ Initial sync complete. Current: ${this._currentBlock}, Latest: ${latestBlockNetwork}`);
        }
      }

      await this.adjustPollingInterval(latestBlockNetwork);
    } catch (error: any) {
      if (error?.code === 'EACCES' || error?.code === 'ECONNREFUSED' || error?.code === 'ETIMEDOUT' || error?.code === 'ENOTFOUND') {
        this.logger.error(`❌ Network connection error in block crawling (${error.code}): ${error.message}`);
        this.logger.info('⏭️ Skipping this cycle, will retry on next polling interval');
      } else {
        await handleErrorGracefully(
          error,
          SERVICE.V1.CrawlBlock.key,
          'Critical error in block crawling'
        );
      }
    } finally {
      this._processingLock = false;
    }
  }

  private async getLatestBlockHeight(): Promise<number> {
    try {
      const responseGetLatestBlock: GetLatestBlockResponseSDKType = await this.retryRpcCall(
        () => this._lcdClient.provider.cosmos.base.tendermint.v1beta1.getLatestBlock(),
        'getLatestBlockHeight'
      );
      return parseInt(
        responseGetLatestBlock?.block?.header?.height
          ? responseGetLatestBlock.block.header.height.toString()
          : '0',
        10
      );
    } catch (error: any) {
      if (error?.code === 'EACCES' || error?.code === 'ECONNREFUSED' || error?.code === 'ETIMEDOUT' || error?.code === 'ENOTFOUND') {
        this.logger.error(`❌ Failed to get latest block height due to network error (${error.code}): ${error.message}`);
      } else {
        this.logger.error(`❌ Failed to get latest block height after retries: ${error}`);
      }
      return 0;
    }
  }

  private async retryRpcCall<T>(
    rpcCall: () => Promise<T>,
    operationName: string,
    maxAttempts?: number
  ): Promise<T> {
    const attempts = maxAttempts || config.crawlBlock.rpcRetryAttempts || 3;
    const delay = config.crawlBlock.rpcRetryDelay || 1000;
    const timeout = config.crawlBlock.rpcTimeout || 30000;
    let lastError: Error | unknown;

    for (let attempt = 1; attempt <= attempts; attempt++) {
      try {
        return await Promise.race([
          rpcCall(),
          new Promise<never>((_, reject) => {
            setTimeout(() => reject(new Error('RPC call timeout')), timeout);
          }),
        ]);
      } catch (error: any) {
        lastError = error;
        const isLastAttempt = attempt === attempts;
        
        const isNetworkError = error?.code === 'EACCES' || error?.code === 'ECONNREFUSED' || 
                              error?.code === 'ETIMEDOUT' || error?.code === 'ENOTFOUND';
        
        if (isLastAttempt || isNetworkError) {
          if (isNetworkError) {
            this.logger.error(
              `❌ RPC call failed due to network error (${error.code}): ${operationName}. ${error.message}`
            );
          } else {
            this.logger.error(
              `❌ RPC call failed after ${attempts} attempts: ${operationName}. Error: ${error}`
            );
          }
          throw error;
        }

        const backoffDelay = delay * (2 ** (attempt - 1));
        this.logger.warn(
          `⚠️ RPC call failed (attempt ${attempt}/${attempts}): ${operationName}. Retrying in ${backoffDelay}ms...`
        );
        await new Promise((resolve) => {
          setTimeout(resolve, backoffDelay);
        });
      }
    }

    throw lastError instanceof Error 
      ? lastError 
      : new Error(`Failed to execute ${operationName} after ${attempts} attempts`);
  }

  private async executeRpcWithRetry<T>(
    rpcCall: () => Promise<T>,
    operationName: string
  ): Promise<T> {
    const attempts = config.crawlBlock.rpcRetryAttempts || 3;
    const delay = config.crawlBlock.rpcRetryDelay || 1000;
    const timeout = config.crawlBlock.rpcTimeout || 30000;
    let lastError: Error | unknown;

    for (let attempt = 1; attempt <= attempts; attempt++) {
      try {
        return await Promise.race([
          rpcCall(),
          new Promise<never>((_, reject) => {
            setTimeout(() => reject(new Error('RPC call timeout')), timeout);
          }),
        ]);
      } catch (error: any) {
        lastError = error;
        const isLastAttempt = attempt === attempts;
        
        const isNetworkError = error?.code === 'EACCES' || error?.code === 'ECONNREFUSED' || 
                              error?.code === 'ETIMEDOUT' || error?.code === 'ENOTFOUND';
        
        if (isLastAttempt || isNetworkError) {
          if (isNetworkError) {
            this.logger.error(
              `❌ RPC batch call failed due to network error (${error.code}): ${operationName}. ${error.message}`
            );
          } else {
            this.logger.error(
              `❌ RPC batch call failed after ${attempts} attempts: ${operationName}`
            );
          }
          throw error;
        }

        const backoffDelay = delay * (2 ** (attempt - 1));
        await new Promise((resolve) => {
          setTimeout(resolve, backoffDelay);
        });
      }
    }

    throw lastError instanceof Error 
      ? lastError 
      : new Error(`Failed to execute ${operationName} after ${attempts} attempts`);
  }

  private async adjustPollingInterval(latestBlockNetwork: number): Promise<void> {
    if (this._updatingInterval) {
      return;
    }

    const blocksBehind = latestBlockNetwork - this._currentBlock;
    const threshold = config.crawlBlock.caughtUpThreshold || 3;
    const wasCaughtUp = this._isCaughtUp;
    const isCaughtUp = blocksBehind <= threshold;

    this._isCaughtUp = isCaughtUp;

    if (config.crawlBlock.enableWebSocketSubscription) {
      if (isCaughtUp && !wasCaughtUp && this._initialSyncComplete) {
        this.logger.info(
          `🔄 Sync Status: CAUGHT UP | Blocks Behind: ${blocksBehind} (threshold: ${threshold}) | Initial Sync: COMPLETE | Action: Starting WebSocket subscription for instant block notifications`
        );
        await this.startWebSocketSubscription();
      } else if (isCaughtUp && !wasCaughtUp && !this._initialSyncComplete) {
        this.logger.info(
          `⏳ Sync Status: Within threshold but initial sync NOT COMPLETE | Blocks Behind: ${blocksBehind} | Current Block: ${this._currentBlock} | Latest Block: ${latestBlockNetwork} | Action: Continuing to crawl existing blocks before starting WebSocket`
        );
      } else if (!isCaughtUp && wasCaughtUp) {
        this.logger.info(
          `🔄 Sync Status: FELL BEHIND | Blocks Behind: ${blocksBehind} (threshold: ${threshold}) | Action: Stopping WebSocket subscription, using polling instead`
        );
        await this.stopWebSocketSubscription();
      } else if (isCaughtUp && this._initialSyncComplete && !this._websocketConnected) {
        this.logger.info(
          `🔄 Sync Status: CAUGHT UP | Initial Sync: COMPLETE | WebSocket: NOT CONNECTED | Action: Attempting to start WebSocket subscription`
        );
        await this.startWebSocketSubscription();
      }
    }

    const targetInterval = isCaughtUp
      ? (config.crawlBlock.millisecondCrawlCaughtUp || 500)
      : config.crawlBlock.millisecondCrawl;

    if (targetInterval !== this._currentInterval) {
      this.logger.info(
        `🔄 Sync status changed: ${wasCaughtUp ? 'caught up' : 'catching up'} -> ${isCaughtUp ? 'caught up' : 'catching up'} ` +
        `(blocks behind: ${blocksBehind}, threshold: ${threshold}). ` +
        `Updating polling interval: ${this._currentInterval}ms -> ${targetInterval}ms`
      );

      this._updatingInterval = true;
      try {
        await this.updateJobInterval(targetInterval);
        this._currentInterval = targetInterval;
      } catch (error) {
        await handleErrorGracefully(
          error,
          SERVICE.V1.CrawlBlock.key,
          'Critical error updating job interval'
        );
      } finally {
        this._updatingInterval = false;
      }
    }
  }

  private async updateJobInterval(newInterval: number): Promise<void> {
    if (!Config.QUEUE_JOB_REDIS) {
      this.logger.warn('QUEUE_JOB_REDIS not configured, skipping interval update');
      return;
    }

    const redisClient = new Redis(Config.QUEUE_JOB_REDIS);
    const jobQueue = new Queue(BULL_JOB_NAME.CRAWL_BLOCK, {
      prefix: DEFAULT_PREFIX,
      connection: redisClient,
    });

    try {
      const repeatableJobs = await jobQueue.getRepeatableJobs();
      for (const job of repeatableJobs) {
        if (job.name === BULL_JOB_NAME.CRAWL_BLOCK) {
          await jobQueue.removeRepeatableByKey(job.key);
        }
      }

      await this.createJob(
        BULL_JOB_NAME.CRAWL_BLOCK,
        BULL_JOB_NAME.CRAWL_BLOCK,
        {},
        {
          removeOnComplete: true,
          removeOnFail: {
            count: 3,
          },
          repeat: {
            every: newInterval,
          },
        }
      );

      this.logger.info(`✅ Successfully updated job interval to ${newInterval}ms`);
    } catch (error) {
      this.logger.error(`❌ Critical error updating job interval: ${error}`);
      this.logger.error('💀 Fatal error, exiting process...');
      
    } finally {
      await redisClient.quit();
    }
  }

  async handleListBlock(listBlock: any[]) {
    try {
      const listBlockHeight: number[] = [];
      const mapExistedBlock = new Map<number, boolean>();
      listBlock.forEach((block) => {
        if (block.block?.header?.height) {
          listBlockHeight.push(parseInt(block.block?.header?.height, 10));
        }
      });
      if (listBlockHeight.length) {
        const listExistedBlock = await Block.query().whereIn(
          'height',
          listBlockHeight
        );
        listExistedBlock?.forEach((block) => {
          if (block?.height != null) {
            mapExistedBlock.set(block.height, true);
          }
        });
      }
      const listBlockModel: any[] = [];

      listBlock.forEach((block) => {
        const height = parseInt(block?.block?.header?.height ?? '0', 10);

        if (!mapExistedBlock.get(height)
        ) {
          const events: Event[] = [];
          if (block.block_result.begin_block_events?.length > 0) {
            block.block_result.begin_block_events.forEach((event: any) => {
              events.push({
                ...event,
                source: Event.SOURCE.BEGIN_BLOCK_EVENT,
              });
            });
          }
          if (block.block_result.end_block_events?.length > 0) {
            block.block_result.end_block_events.forEach((event: any) => {
              if (event.type === Event.EVENT_TYPE.BLOCK_BLOOM) {
                const attrBloom = event.attributes.filter(
                  (attr: any) => attr.key === EventAttribute.ATTRIBUTE_KEY.BLOOM
                );
                if (attrBloom.length > 0) {
                  attrBloom[0].value = '';
                }
              }
              events.push({
                ...event,
                source: Event.SOURCE.END_BLOCK_EVENT,
              });
            });
          }
          listBlockModel.push({
            ...Block.fromJson({
              height: block?.block?.header?.height,
              hash: block?.block_id?.hash,
              time: block?.block?.header?.time,
              proposer_address: block?.block?.header?.proposer_address,
              data: config.crawlBlock.saveRawLog ? block : null,
              tx_count: block?.block?.data?.txs?.length ?? 0,
            }),
            signatures: block?.block?.last_commit?.signatures.map(
              (signature: CommitSigSDKType) => ({
                block_id_flag: signature.block_id_flag,
                validator_address: signature.validator_address,
                timestamp: signature.timestamp,
              })
            ),
            events: events.map((event: any) => ({
              type: event.type,
              attributes: event.attributes.map(
                (attribute: any, index: number) => ({
                  block_height: block?.block?.header?.height,
                  index,
                  composite_key: attribute?.key
                    ? `${event.type}.${this._registry.decodeAttribute(
                      attribute?.key
                    )}`
                    : null,
                  key: attribute?.key
                    ? this._registry.decodeAttribute(attribute?.key)
                    : null,
                  value: attribute?.value
                    ? this._registry.decodeAttribute(attribute?.value)
                    : null,
                })
              ),
              source: event.source,
            })),
          });
        }
      });


      if (listBlockModel.length) {
        await knex.transaction(async (trx) => {
          await Block.query()
            .insertGraph(listBlockModel)
            .transacting(trx);
          await this.broker.call(
            SERVICE.V1.CrawlTransaction.TriggerHandleTxJob.path
          );
        });
      }
    } catch (error) {
      await handleErrorGracefully(
        error,
        SERVICE.V1.CrawlBlock.key,
        'Critical error in handleListBlock'
      );
    }
  }

  public async _start() {
    try {
      const providerRegistry = await getProviderRegistry();
      this._registry = new ChainRegistry(this.logger, providerRegistry);

      await this.waitForServices(SERVICE.V1.CrawlTransaction.name);
      
      this.logger.info(
        `🚀 CrawlBlock Service Starting | Initial Polling Interval: ${config.crawlBlock.millisecondCrawl}ms | ` +
        `WebSocket Subscription: ${config.crawlBlock.enableWebSocketSubscription ? 'ENABLED' : 'DISABLED'} | ` +
        `Caught Up Threshold: ${config.crawlBlock.caughtUpThreshold} blocks`
      );
      
      // Only create job if indexer is running
      if (indexerStatusManager.isIndexerRunning()) {
        this.createJob(
          `${BULL_JOB_NAME.CRAWL_BLOCK}`,
          `${BULL_JOB_NAME.CRAWL_BLOCK}`,
          {},
          {
            removeOnComplete: true,
            removeOnFail: {
              count: 3,
            },
            repeat: {
              every: config.crawlBlock.millisecondCrawl,
            },
          }
        );
      } else {
        this.logger.warn('⚠️ Indexer is stopped, not creating crawl block job. APIs will return error status.');
      }
    } catch (error: any) {
      const wasStopped = await handleErrorGracefully(
        error,
        SERVICE.V1.CrawlBlock.key,
        'CrawlBlock service startup error'
      );
      
      if (wasStopped) {
        this.logger.warn('⚠️ Service will start but indexer is stopped. APIs will return error status.');
      }
    }
    return super._start();
  }

  public setRegistry(registry: ChainRegistry) {
    this._registry = registry;
  }

  private async getWebSocketUrl(): Promise<string> {
    const rpcUrl = Network?.RPC || '';
    if (!rpcUrl) {
      const error = new Error('RPC_ENDPOINT is not configured');
      await handleErrorGracefully(
        error,
        SERVICE.V1.CrawlBlock.key,
        'RPC_ENDPOINT configuration error'
      );
      throw error;
    }

    let wsUrl = rpcUrl.replace(/^http:/, 'ws:').replace(/^https:/, 'wss:');
    
    wsUrl = wsUrl.replace(/\/$/, '');
    
    if (!wsUrl.endsWith('/websocket')) {
      wsUrl += '/websocket';
    }

    return wsUrl;
  }

  private async startWebSocketSubscription(): Promise<void> {
    if (this._websocket && this._websocketConnected) {
      this.logger.debug('WebSocket already connected, skipping subscription');
      return;
    }

    if (this._websocketReconnectTimer) {
      clearTimeout(this._websocketReconnectTimer);
      this._websocketReconnectTimer = null;
    }

    try {
      const wsUrl = await this.getWebSocketUrl();
      this.logger.info(`🔌 Connecting to Verana RPC WebSocket: ${wsUrl}`);

      this._websocket = new WebSocket(wsUrl);

      this._websocket.on('open', () => {
        this.logger.info(
          `✅ WebSocket Status: CONNECTED | URL: ${wsUrl} | Initial Sync: ${this._initialSyncComplete ? 'COMPLETE' : 'IN PROGRESS'} | Ready to receive new block events`
        );
        this._websocketConnected = true;

        if (this._websocket && this._websocket.readyState === WebSocket.OPEN) {
          this._websocket.send(
            JSON.stringify({
              jsonrpc: '2.0',
              method: 'subscribe',
              id: 'new-block-sub',
              params: {
                query: "tm.event = 'NewBlock'"
              }
            })
          );
          this.logger.info('📡 WebSocket Subscription: Subscribed to NewBlock events | Status: ACTIVE');
        }
      });

      this._websocket.on('message', async (data: WebSocket.Data) => {
        try {
          const msg = JSON.parse(data.toString());
          if (!msg.result?.data?.value?.block) {
            return;
          }

          if (!this._isCaughtUp || !this._initialSyncComplete) {
            return;
          }

          const blockHeight = Number(msg.result.data.value.block.header.height);
          if (blockHeight <= this._currentBlock) {
            return;
          }

          this.logger.info(`🔔 WebSocket: New block ${blockHeight} detected, fetching immediately`);
          this._lastWebSocketBlockHeight = blockHeight;

          setImmediate(async () => {
            try {
              await this.handleJobCrawlBlock();
            } catch (error: any) {
              if (error?.code === 'EACCES' || error?.code === 'ECONNREFUSED' || error?.code === 'ETIMEDOUT' || error?.code === 'ENOTFOUND') {
                this.logger.warn(`⚠️ WebSocket fetch failed for block ${blockHeight}: ${error.message}`);
              } else {
                this.logger.error(`❌ WebSocket fetch failed for block ${blockHeight}: ${error}`);
              }
            }
          });
        } catch (err) {
          this.logger.error(`❌ WebSocket message error: ${err}`);
        }
      });

      this._websocket.on('error', (err: Error) => {
        this._websocketFailureCount++;
        this.logger.error(`❌ WebSocket error: ${err.message} (failures: ${this._websocketFailureCount})`);
        this._websocketConnected = false;
        this._lastRpcBackupCheck = 0;
      });

      this._websocket.on('close', () => {
        this.logger.warn(`⚠️ WebSocket closed. RPC polling will continue as backup`);
        this._websocketConnected = false;
        this._websocket = null;
        this._lastRpcBackupCheck = 0;

        if (this._isCaughtUp && this._initialSyncComplete && config.crawlBlock.enableWebSocketSubscription) {
          const reconnectDelay = config.crawlBlock.websocketReconnectDelay || 2000;
          this._websocketReconnectTimer = setTimeout(() => {
            this.startWebSocketSubscription().catch((err) => {
              this.logger.error(`❌ WebSocket reconnection failed: ${err}`);
            });
          }, reconnectDelay);
        }
      });
    } catch (error) {
      this._websocketConnected = false;
      await handleErrorGracefully(
        error,
        SERVICE.V1.CrawlBlock.key,
        'Critical error starting WebSocket subscription'
      );
    }
  }


  private async stopWebSocketSubscription(): Promise<void> {
    if (this._websocketReconnectTimer) {
      clearTimeout(this._websocketReconnectTimer);
      this._websocketReconnectTimer = null;
      this.logger.debug('🔄 WebSocket: Cancelled pending reconnection timer');
    }

    if (this._websocket) {
      try {
        this._websocket.close();
        this.logger.info(
          `🔌 WebSocket Status: STOPPED | Reason: Fell behind or service stopping | Initial Sync: ${this._initialSyncComplete ? 'COMPLETE' : 'IN PROGRESS'}`
        );
      } catch (error) {
        this.logger.error(`❌ WebSocket Stop Error: ${error}`);
      } finally {
        this._websocket = null;
        this._websocketConnected = false;
      }
    } else {
      this.logger.debug('ℹ️ WebSocket: Already stopped, no action needed');
    }
  }

 
  public async stopped() {
    await this.stopWebSocketSubscription();
    return super.stopped();
  }
}
