/* eslint-disable import/no-extraneous-dependencies */
import { ServiceBroker } from 'moleculer';
import {
  Action,
  Service,
} from '@ourparentcenter/moleculer-decorators-extended';
import { HttpBatchClient } from '@cosmjs/tendermint-rpc';
import { createJsonRpcRequest } from '@cosmjs/tendermint-rpc/build/jsonrpc';
import { decodeTxRaw } from '@cosmjs/proto-signing';
import { toBase64, fromBase64 } from '@cosmjs/encoding';
import { Knex } from 'knex';
import { Queue } from 'bullmq';
import { GetNodeInfoResponseSDKType } from '@aura-nw/aurajs/types/codegen/cosmos/base/tendermint/v1beta1/query';
import _ from 'lodash';
import Utils from '../../common/utils/utils';
import {
  BULL_JOB_NAME,
  getHttpBatchClient,
  getLcdClient,
  SERVICE,
} from '../../common';
import {
  Block,
  BlockCheckpoint,
  Event,
  Transaction,
  TransactionMessage,
} from '../../models';
import BullableService, { QueueHandler } from '../../base/bullable.service';
import config from '../../../config.json' assert { type: 'json' };
import knex from '../../common/utils/db_connection';
import ChainRegistry from '../../common/utils/chain.registry';
import { getProviderRegistry } from '../../common/utils/provider.registry';

@Service({
  name: SERVICE.V1.CrawlTransaction.key,
  version: 1,
})
export default class CrawlTxService extends BullableService {
  private _httpBatchClient: HttpBatchClient;

  private _registry!: ChainRegistry;

  public constructor(public broker: ServiceBroker) {
    super(broker);
    this._httpBatchClient = getHttpBatchClient();
  }

  @QueueHandler({
    queueName: BULL_JOB_NAME.CRAWL_TRANSACTION,
    jobName: BULL_JOB_NAME.CRAWL_TRANSACTION,
  })
  public async jobCrawlTx(): Promise<void> {
    const [startBlock, endBlock, blockCheckpoint] =
      await BlockCheckpoint.getCheckpoint(
        BULL_JOB_NAME.CRAWL_TRANSACTION,
        [BULL_JOB_NAME.CRAWL_BLOCK],
        config.crawlTransaction.key
      );

    this.logger.info(
      `Crawl transaction from block ${startBlock} to ${endBlock}`
    );
    if (startBlock >= endBlock) {
      return;
    }
    const listTxRaw = await this.getListRawTx(startBlock, endBlock);
    const listdecodedTx = await this.decodeListRawTx(listTxRaw);
    await knex.transaction(async (trx) => {
      await this.insertTxDecoded(listdecodedTx, trx);
      if (blockCheckpoint) {
        blockCheckpoint.height = endBlock;
        await BlockCheckpoint.query()
          .insert(blockCheckpoint)
          .onConflict('job_name')
          .merge()
          .returning('id')
          .transacting(trx);
      }
    });
  }

  @QueueHandler({
    queueName: BULL_JOB_NAME.HANDLE_TRANSACTION,
    jobName: BULL_JOB_NAME.HANDLE_TRANSACTION,
  })
  public async jobHandlerCrawlTx(): Promise<void> {
    const [startBlock, endBlock, blockCheckpoint] =
      await BlockCheckpoint.getCheckpoint(
        BULL_JOB_NAME.HANDLE_TRANSACTION,
        [BULL_JOB_NAME.CRAWL_TRANSACTION],
        config.handleTransaction.key
      );

    this.logger.info(
      `Handle transaction from block ${startBlock} to ${endBlock}`
    );

    if (startBlock >= endBlock) {
      return;
    }
    const listTxRaw = await Transaction.query()
      .where('height', '>', startBlock)
      .andWhere('height', '<=', endBlock)
      .orderBy('height', 'asc')
      .orderBy('index', 'asc');
    await knex.transaction(async (trx) => {
      await this.insertRelatedTx(listTxRaw, trx);
      if (blockCheckpoint) {
        blockCheckpoint.height = endBlock;
        await BlockCheckpoint.query()
          .insert(blockCheckpoint)
          .onConflict('job_name')
          .merge()
          .returning('id')
          .transacting(trx);
      }
    });
  }

  // get list raw tx from block to block
  async getListRawTx(
    startBlock: number,
    endBlock: number
  ): Promise<{ listTx: any; height: number; timestamp: string }[]> {
    const blocks: any[] = await Block.query()
      .select('height', 'time', 'tx_count')
      .where('height', '>', startBlock)
      .andWhere('height', '<=', endBlock)
      .orderBy('height', 'asc');
    this.logger.debug(blocks);
    const promises: any[] = [];

    const getBlockInfo = async (
      height: number,
      timestamp: Date,
      page: string,
      perPage: string
    ) => {
      const blockInfo = await this._httpBatchClient.execute(
        createJsonRpcRequest('tx_search', {
          query: `tx.height=${height}`,
          page,
          per_page: perPage,
        })
      );
      return {
        txs: blockInfo.result.txs,
        tx_count: Number(blockInfo.result.total_count),
        height,
        timestamp,
      };
    };

    blocks.forEach((block) => {
      if (block.tx_count > 0) {
        this.logger.info('crawl tx by height: ', block.height);
        const totalPages = Math.ceil(
          block.tx_count / config.handleTransaction.txsPerCall
        );

        [...Array(totalPages)].forEach((e, i) => {
          const pageIndex = (i + 1).toString();
          promises.push(
            getBlockInfo(
              block.height,
              block.timestamp,
              pageIndex,
              config.handleTransaction.txsPerCall.toString()
            )
          );
        });
      }
    });
    const resultPromises: any[] = await Promise.all(promises);

    const listRawTxs: any[] = [];
    blocks.forEach((block) => {
      if (block.tx_count > 0) {
        const listTxs: any[] = [];
        resultPromises
          .filter(
            (result) => result.height.toString() === block.height.toString()
          )
          .forEach((resultPromise) => {
            listTxs.push(...resultPromise.txs);
          });
        if (listTxs.length !== block.tx_count) {
          const error = `Error in block ${block.height}: ${listTxs.length} txs found, ${block.tx_count} txs expected`;
          this.logger.error(error);
          throw new Error(error);
        }
        listRawTxs.push({
          listTx: {
            txs: listTxs,
            total_count: block.tx_count,
          },
          height: block.height,
          timestamp: block.time,
        });
      }
    });
    return listRawTxs;
  }

  // decode list raw tx
  async decodeListRawTx(
    listRawTx: { listTx: any; height: number; timestamp: string }[]
  ): Promise<{ listTx: any; height: number; timestamp: string }[]> {
    const listDecodedTx = await Promise.all(
      listRawTx.map(async (payloadBlock) => {
        const { listTx, timestamp, height } = payloadBlock;
        const listHandleTx: any[] = [];
        try {
          // check if tx existed
          const mapExistedTx: Map<string, boolean> = new Map();
          const listHash = listTx.txs.map((tx: any) => tx.hash);
          const listTxExisted = await Transaction.query().whereIn(
            'hash',
            listHash
          );
          listTxExisted.forEach((tx) => {
            mapExistedTx.set(tx.hash, true);
          });

          // parse tx to format LCD return
          listTx.txs.forEach((tx: any) => {
            this.logger.debug(`Handle txhash ${tx.hash}`);
            if (mapExistedTx.get(tx.hash)) {
              return;
            }
            // decode tx to readable
            const decodedTx = decodeTxRaw(fromBase64(tx.tx));

            const parsedTx: any = {};
            parsedTx.tx = decodedTx;
            parsedTx.tx.signatures = decodedTx.signatures.map(
              (signature: Uint8Array) => toBase64(signature)
            );

            const decodedMsgs = decodedTx.body.messages.map((msg) => {
              const decodedMsg = Utils.camelizeKeys(
                this._registry.decodeMsg(msg)
              );
              decodedMsg['@type'] = msg.typeUrl;
              return decodedMsg;
            });

            parsedTx.tx = {
              body: {
                messages: decodedMsgs,
                memo: decodedTx.body?.memo,
                timeout_height: decodedTx.body?.timeoutHeight,
                extension_options: decodedTx.body?.extensionOptions,
                non_critical_extension_options:
                  decodedTx.body?.nonCriticalExtensionOptions,
              },
              auth_info: {
                fee: {
                  amount: decodedTx.authInfo.fee?.amount,
                  gas_limit: decodedTx.authInfo.fee?.gasLimit,
                  granter: decodedTx.authInfo.fee?.granter,
                  payer: decodedTx.authInfo.fee?.payer,
                },
                signer_infos: decodedTx.authInfo.signerInfos.map(
                  (signerInfo) => {
                    const pubkey = signerInfo.publicKey?.value;

                    if (pubkey instanceof Uint8Array) {
                      return {
                        mode_info: signerInfo.modeInfo,
                        public_key: {
                          '@type': signerInfo.publicKey?.typeUrl,
                          key: toBase64(pubkey.slice(2)),
                        },
                        sequence: signerInfo.sequence.toString(),
                      };
                    }
                    return {
                      mode_info: signerInfo.modeInfo,
                      sequence: signerInfo.sequence.toString(),
                    };
                  }
                ),
              },
              signatures: decodedTx.signatures,
            };

            parsedTx.tx_response = {
              height: tx.height,
              txhash: tx.hash,
              codespace: tx.tx_result.codespace,
              code: tx.tx_result.code,
              data: tx.tx_result.data,
              raw_log: tx.tx_result.log,
              info: tx.tx_result.info,
              gas_wanted: tx.tx_result.gas_wanted,
              gas_used: tx.tx_result.gas_used,
              tx: tx.tx,
              index: tx.index,
              events: tx.tx_result.events,
              timestamp,
            };
            try {
              parsedTx.tx_response.logs = JSON.parse(tx.tx_result.log);
            } catch (error) {
              this.logger.debug('tx fail');
            }
            listHandleTx.push(parsedTx);
          });

          return { listTx: listHandleTx, timestamp, height };
        } catch (error) {
          this.logger.error(error);
          throw error;
        }
      })
    );
    return listDecodedTx;
  }

  async insertTxDecoded(
    listTxDecoded: { listTx: any; height: number; timestamp: string }[],
    transactionDB: Knex.Transaction
  ) {
    this.logger.debug(listTxDecoded);
    const listTxModel: any[] = [];
    listTxDecoded.forEach((payloadBlock) => {
      const { listTx, height, timestamp } = payloadBlock;
      listTx.forEach((tx: any) => {
        this.logger.debug(tx, timestamp);

        const txInsert = {
          ...Transaction.fromJson({
            index: tx.tx_response.index,
            height,
            hash: tx.tx_response.txhash,
            codespace: tx.tx_response.codespace ?? '',
            code: parseInt(tx.tx_response.code ?? '0', 10),
            gas_used: tx.tx_response.gas_used?.toString() ?? '0',
            gas_wanted: tx.tx_response.gas_wanted?.toString() ?? '0',
            gas_limit: tx.tx.auth_info.fee.gas_limit?.toString() ?? '0',
            fee: JSON.stringify(tx.tx.auth_info.fee.amount),
            timestamp,
            data: config.handleTransaction.saveRawLog ? tx : null,
            memo: tx.tx.body.memo,
          }),
        };
        listTxModel.push(txInsert);
      });
    });

    if (listTxModel.length) {
      const resultInsert = await transactionDB.batchInsert(
        Transaction.tableName,
        listTxModel,
        config.crawlTransaction.chunkSize
      );
      this.logger.debug('result insert tx', resultInsert);
    }
  }

  // insert related table (event, event_attribute, message)
  async insertRelatedTx(
    listDecodedTx: Transaction[],
    transactionDB: Knex.Transaction
  ) {
    this.logger.debug(listDecodedTx);
    const listEventModel: any[] = [];
    const listMsgModel: any[] = [];
    listDecodedTx.forEach((tx) => {
      const rawLogTx = tx.data;
      let sender = '';
      try {
        sender = this._registry.decodeAttribute(
          this._findAttribute(
            rawLogTx.tx_response.events,
            'message',
            this._registry.encodeAttribute('sender')
          )
        );
      } catch (error) {
        this.logger.debug(
          'txhash not has sender event: ',
          rawLogTx.tx_response.txhash
        );
        // this.logger.warn(error);
      }

      // create list event with msg index
      const listEventWithMsgIndex = this.createListEventWithMsgIndex(rawLogTx);

      const eventInsert =
        listEventWithMsgIndex?.map((event: any) => ({
          tx_id: tx.id,
          tx_msg_index: event.msg_index ?? undefined,
          type: event.type,
          attributes: event.attributes?.map(
            (attribute: any, index: number) => ({
              tx_id: tx.id,
              block_height: tx.height,
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
          block_height: tx.height,
          source: Event.SOURCE.TX_EVENT,
        })) ?? [];
      const msgInsert =
        rawLogTx.tx.body.messages.map((message: any, index: any) => ({
          tx_id: tx.id,
          sender,
          index,
          type: message['@type'],
          content: message,
        })) ?? [];
      listEventModel.push(...eventInsert);
      listMsgModel.push(...msgInsert);
    });

    if (listEventModel.length) {
      const resultInsertEvents = await Event.query()
        .insertGraph(listEventModel, { allowRefs: true })
        .transacting(transactionDB);
      this.logger.debug('result insert events:', resultInsertEvents);
    }
    if (listMsgModel.length) {
      const resultInsertMsgs = await TransactionMessage.query()
        .insert(listMsgModel)
        .transacting(transactionDB);
      this.logger.debug('result insert messages:', resultInsertMsgs);
    }
  }

  private checkMappingEventToLog(tx: any) {
    this.logger.info('checking mapping log in tx :', tx.tx_response.txhash);
    let flattenLog: string[] = [];
    let flattenEventEncoded: string[] = [];

    tx?.tx_response?.logs?.forEach((log: any, index: number) => {
      log.events.forEach((event: any) => {
        event.attributes?.forEach((attr: any) => {
          if (attr.value === undefined) {
            flattenLog.push(`${index}-${event.type}-${attr.key}-null`);
          } else {
            flattenLog.push(`${index}-${event.type}-${attr.key}-${attr.value}`);
          }
        });
      });
    });

    tx?.tx_response?.events?.forEach((event: any) => {
      event.attributes?.forEach((attr: any) => {
        if (event.msg_index !== undefined) {
          const key = attr.key
            ? this._registry.decodeAttribute(attr.key)
            : null;
          const value = attr.value
            ? this._registry.decodeAttribute(attr.value)
            : null;
          flattenEventEncoded.push(
            `${event.msg_index}-${event.type}-${key}-${value}`
          );
        }
      });
    });
    // compare 2 array
    if (flattenLog.length !== flattenEventEncoded.length) {
      this.logger.warn(
        'Length between 2 flatten array is not equal',
        tx.tx_response.txhash
      );
    }
    flattenLog = flattenLog.sort();
    flattenEventEncoded = flattenEventEncoded.sort();
    const checkResult = flattenLog.every(
      (item: string, index: number) => item === flattenEventEncoded[index]
    );
    if (checkResult === false) {
      this.logger.warn(
        'Mapping event to log is wrong: ',
        tx.tx_response.txhash
      );
    }
  }

  public createListEventWithMsgIndex(tx: any): any[] {
    const returnEvents: any[] = [];
    // if this is failed tx, then no need to set index msg
    if (!tx.tx_response.logs) {
      this.logger.debug('Failed tx, no need to set index msg');
      return [];
    }
    let reachLastEventTypeTx = false;
    // last event type in event field which belongs to tx event
    const listTxEventType = config.handleTransaction.lastEventsTypeTx;
    for (let i = 0; i < tx?.tx_response?.events?.length; i += 1) {
      if (listTxEventType.includes(tx.tx_response.events[i].type)) {
        reachLastEventTypeTx = true;
      }
      if (
        reachLastEventTypeTx &&
        !listTxEventType.includes(tx.tx_response.events[i].type)
      ) {
        break;
      }
      returnEvents.push(tx.tx_response.events[i]);
    }
    // get messages log and append to list event
    tx.tx_response.logs.forEach((log: any, index: number) => {
      log.events.forEach((event: any) => {
        returnEvents.push({
          ...event,
          msg_index: index,
        });
      });
    });

    // check mapping event log ok
    const cloneTx = _.clone(tx);
    cloneTx.tx_response.events = returnEvents;
    this.checkMappingEventToLog(cloneTx);

    return returnEvents;
  }

  private _findAttribute(
    events: any,
    eventType: string,
    attributeKey: string
  ): string {
    let result = '';
    const foundEvent = events.find(
      (event: any) =>
        event.type === eventType &&
        event.attributes.some(
          (attribute: any) => attribute.key === attributeKey
        )
    );
    if (foundEvent) {
      const foundAttribute = foundEvent.attributes.find(
        (attribute: any) => attribute.key === attributeKey
      );
      result = foundAttribute.value;
    }
    if (!result.length) {
      throw new Error(
        `Could not find attribute ${attributeKey} in event type ${eventType}`
      );
    }
    return result;
  }

  @Action({
    name: SERVICE.V1.CrawlTransaction.TriggerHandleTxJob.key,
  })
  async triggerHandleTxJob() {
    try {
      const queue: Queue = this.getQueueManager().getQueue(
        BULL_JOB_NAME.CRAWL_TRANSACTION
      );
      const jobInDelayed = await queue.getDelayed();
      if (jobInDelayed?.length > 0) {
        await jobInDelayed[0].promote();
      }
    } catch (error) {
      this.logger.error('No job can be promoted');
      this.logger.error(error);
    }
  }

  public async _start() {
    const providerRegistry = await getProviderRegistry();
    this._registry = new ChainRegistry(this.logger, providerRegistry);

    const lcdClient = await getLcdClient();
    // set version cosmos sdk to registry
    const nodeInfo: GetNodeInfoResponseSDKType =
      await lcdClient.provider.cosmos.base.tendermint.v1beta1.getNodeInfo();
    const cosmosSdkVersion = nodeInfo.application_version?.cosmos_sdk_version;
    if (cosmosSdkVersion) {
      this._registry.setCosmosSdkVersionByString(cosmosSdkVersion);
    }

    this.createJob(
      BULL_JOB_NAME.CRAWL_TRANSACTION,
      BULL_JOB_NAME.CRAWL_TRANSACTION,
      {},
      {
        removeOnComplete: true,
        removeOnFail: {
          count: 3,
        },
        repeat: {
          every: config.crawlTransaction.millisecondCrawl,
        },
      }
    );
    this.createJob(
      BULL_JOB_NAME.HANDLE_TRANSACTION,
      BULL_JOB_NAME.HANDLE_TRANSACTION,
      {},
      {
        removeOnComplete: true,
        removeOnFail: {
          count: 3,
        },
        repeat: {
          every: config.handleTransaction.millisecondCrawl,
        },
      }
    );
    return super._start();
  }

  public setRegistry(registry: ChainRegistry) {
    this._registry = registry;
  }
}
