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
import {
  BULL_JOB_NAME,
  getHttpBatchClient,
  getLcdClient,
  IProviderJSClientFactory,
  SERVICE,
} from '../../common';
import { Block, BlockCheckpoint, Event, EventAttribute } from '../../models';
import BullableService, { QueueHandler } from '../../base/bullable.service';
import config from '../../../config.json' with { type: 'json' };
import knex from '../../common/utils/db_connection';
import ChainRegistry from '../../common/utils/chain.registry';
import { getProviderRegistry } from '../../common/utils/provider.registry';

@Service({
  name: SERVICE.V1.CrawlBlock.key,
  version: 1,
})
export default class CrawlBlockService extends BullableService {
  private _currentBlock = 0;

  private _httpBatchClient: HttpBatchClient;

  private _lcdClient!: IProviderJSClientFactory;

  private _registry!: ChainRegistry;

  public constructor(public broker: ServiceBroker) {
    super(broker);
    this._httpBatchClient = getHttpBatchClient();
  }

  @QueueHandler({
    queueName: BULL_JOB_NAME.CRAWL_BLOCK,
    jobName: BULL_JOB_NAME.CRAWL_BLOCK,
    // // prefix: `horoscope-v2-${config.chainId}`,
  })
  private async jobHandler(_payload: any): Promise<void> {
    await this.initEnv();
    await this.handleJobCrawlBlock();
  }

  private async initEnv() {
    this._lcdClient = await getLcdClient();

    // set version cosmos sdk to registry
    const nodeInfo: GetNodeInfoResponseSDKType =
      await this._lcdClient.provider.cosmos.base.tendermint.v1beta1.getNodeInfo();
    const cosmosSdkVersion = nodeInfo.application_version?.cosmos_sdk_version;
    if (cosmosSdkVersion) {
      this._registry.setCosmosSdkVersionByString(cosmosSdkVersion);
    }

    // Get handled block from db
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
    this.logger.info(`_currentBlock: ${this._currentBlock}`);
  }

  async handleJobCrawlBlock() {
    // Get latest block in network
    const responseGetLatestBlock: GetLatestBlockResponseSDKType =
      await this._lcdClient.provider.cosmos.base.tendermint.v1beta1.getLatestBlock();
    const latestBlockNetwork = parseInt(
      responseGetLatestBlock.block?.header?.height
        ? responseGetLatestBlock.block?.header?.height.toString()
        : '0',
      10
    );

    this.logger.info(`latestBlockNetwork: ${latestBlockNetwork}`);

    // crawl block from startBlock to endBlock
    const startBlock = this._currentBlock + 1;

    let endBlock = startBlock + config.crawlBlock.blocksPerCall - 1;
    if (endBlock > latestBlockNetwork) {
      endBlock = latestBlockNetwork;
    }
    this.logger.info(`startBlock: ${startBlock} endBlock: ${endBlock}`);
    try {
      const blockQueries = [];
      for (let i = startBlock; i <= endBlock; i += 1) {
        try {
          const heightStr = i.toString();

          let blockReq: JsonRpcRequest | null = null;
          let blockResultsReq: JsonRpcRequest | null = null;
          try {
            blockReq = createJsonRpcRequest('block', { height: heightStr });
            // this.logger.warn(`➡️ JSON-RPC Request [block]: ${JSON.stringify(blockReq)}`);
            blockResultsReq = createJsonRpcRequest('block_results', { height: heightStr });
            // this.logger.warn(`➡️ JSON-RPC Request [block_results]: ${JSON.stringify(blockResultsReq)}`);
          } catch (err) {
            this.logger.error(`❌ Failed to create JSON-RPC request at height ${heightStr}: ${err}`);
          }

          if (blockReq && blockResultsReq) {
            blockQueries.push(
              this._httpBatchClient.execute(blockReq),
              this._httpBatchClient.execute(blockResultsReq)
            );
          }

        } catch (err) {
          this.logger.error(`❌ Unexpected error preparing request at block ${i}: ${err}`);
        }

      }

      const blockResponses: JsonRpcSuccessResponse[] = await Promise.all(blockQueries);

      // this.logger.info(`blockResponses: ${JSON.stringify(blockResponses)}`);
      const mergeBlockResponses: any[] = [];

      for (let i = 0; i < blockResponses?.length; i += 2) {
        const blockHeight = startBlock + i / 2;

        const blockData = blockResponses[i]?.result;
        const blockResultData = blockResponses[i + 1]?.result;

        this.logger.info(`📦 Block [${blockHeight}] fetched`);
        // this.logger.debug(`🧱 Block Data: ${JSON.stringify(blockData, null, 2)}`);
        // this.logger.debug(`📑 Block Results: ${JSON.stringify(blockResultData, null, 2)}`);

        mergeBlockResponses.push({
          ...blockData,
          block_result: blockResultData,
        });
      }

      // insert data to DB
      await this.handleListBlock(mergeBlockResponses);

      // update crawled block to db
      if (this._currentBlock < endBlock) {
        await BlockCheckpoint.query()
          .update(
            BlockCheckpoint.fromJson({
              job_name: BULL_JOB_NAME.CRAWL_BLOCK,
              height: endBlock,
            })
          )
          .where({
            job_name: BULL_JOB_NAME.CRAWL_BLOCK,
          });
        this._currentBlock = endBlock;
      }
    } catch (error) {
      this.logger.error(error);
      throw new Error('cannot crawl block');
    }
  }

  async handleListBlock(listBlock: any[]) {
    try {
      // query list existed block and mark to a map
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
      // insert list block to DB
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
        // this.logger.warn('listBlockModel: ', listBlockModel);
        await knex.transaction(async (trx) => {
          const result: any = await Block.query()
            .insertGraph(listBlockModel)
            .transacting(trx);
          this.logger.warn('result insert list block: ', result);
          // trigger crawl transaction job
          await this.broker.call(
            SERVICE.V1.CrawlTransaction.TriggerHandleTxJob.path
          );
        });
      }
    } catch (error) {
      this.logger.error(error);
      throw error;
    }
  }

  public async _start() {
    const providerRegistry = await getProviderRegistry();
    this._registry = new ChainRegistry(this.logger, providerRegistry);

    await this.waitForServices(SERVICE.V1.CrawlTransaction.name);
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
    return super._start();
  }

  public setRegistry(registry: ChainRegistry) {
    this._registry = registry;
  }
}
