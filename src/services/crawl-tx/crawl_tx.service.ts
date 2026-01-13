/* eslint-disable import/no-extraneous-dependencies */
import { GetNodeInfoResponseSDKType } from '@aura-nw/aurajs/types/codegen/cosmos/base/tendermint/v1beta1/query';
import { fromBase64, toBase64 } from '@cosmjs/encoding';
import { decodeTxRaw } from '@cosmjs/proto-signing';
import { HttpBatchClient } from '@cosmjs/tendermint-rpc';
import { createJsonRpcRequest } from '@cosmjs/tendermint-rpc/build/jsonrpc';
import {
  Action,
  Service,
} from '@ourparentcenter/moleculer-decorators-extended';
import { Knex } from 'knex';
import _ from 'lodash';
import { ServiceBroker } from 'moleculer';
import config from '../../config.json' with { type: 'json' };
import BullableService, { QueueHandler } from '../../base/bullable.service';
import {
  BULL_JOB_NAME,
  CredentialSchemaMessageType,
  DidMessages,
  getHttpBatchClient,
  getLcdClient,
  PermissionMessageTypes,
  SERVICE,
  TrustDepositMessageTypes,
  TrustRegistryMessageTypes,
  UpdateParamsMessageTypes
} from '../../common';
import ChainRegistry from '../../common/utils/chain.registry';
import knex from '../../common/utils/db_connection';
import { getProviderRegistry } from '../../common/utils/provider.registry';
import Utils from '../../common/utils/utils';
import { extractController } from '../../common/utils/extract_controller';
import {
  Block,
  BlockCheckpoint,
  Event,
  Transaction,
  TransactionMessage,
} from '../../models';

@Service({
  name: SERVICE.V1.CrawlTransaction.key,
  version: 1,

})
export default class CrawlTxService extends BullableService {
  private _httpBatchClient: HttpBatchClient;

  private _registry!: ChainRegistry;

  private _processingLock: boolean = false;

  private _isFreshStart: boolean = false;

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

    let actualEndBlock = endBlock;
    if (this._isFreshStart && config.crawlTransaction.freshStart) {
      const maxBlocks = config.crawlTransaction.freshStart.blocksPerCall || config.crawlTransaction.blocksPerCall;
      actualEndBlock = Math.min(endBlock, startBlock + maxBlocks);
    }

    const listTxRaw = await this.getListRawTx(startBlock, actualEndBlock);
    const listdecodedTx = await this.decodeListRawTx(listTxRaw);
    await knex.transaction(async (trx) => {
      await this.insertTxDecoded(listdecodedTx, trx);
      if (blockCheckpoint) {
        blockCheckpoint.height = actualEndBlock;
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
    if (this._processingLock) {
      this.logger.debug(' [HANDLE_TRANSACTION] Already in progress, skipping...');
      return;
    }

    this._processingLock = true;

    try {
      const [startBlock, endBlock, blockCheckpoint] =
        await BlockCheckpoint.getCheckpoint(
          BULL_JOB_NAME.HANDLE_TRANSACTION,
          [BULL_JOB_NAME.CRAWL_TRANSACTION],
          config.handleTransaction.key
        );

      this.logger.info(
        ` [HANDLE_TRANSACTION] Handle transaction from block ${startBlock} to ${endBlock}`
      );

      if (startBlock >= endBlock) {
        this.logger.debug(` [HANDLE_TRANSACTION] No new blocks to process (${startBlock} >= ${endBlock})`);
        return;
      }

      let actualEndBlock = endBlock;
      if (this._isFreshStart && config.handleTransaction.freshStart) {
        const maxBlocks = config.handleTransaction.freshStart.blocksPerCall || config.handleTransaction.blocksPerCall;
        actualEndBlock = Math.min(endBlock, startBlock + maxBlocks);
      }

      const listTxRaw = await Transaction.query()
        .where('height', '>', startBlock)
        .andWhere('height', '<=', actualEndBlock)
        .orderBy('height', 'asc')
        .orderBy('index', 'asc');

      this.logger.info(` [HANDLE_TRANSACTION] Found ${listTxRaw.length} transactions to process`);

      if (listTxRaw.length === 0) {

        if (blockCheckpoint) {
          await knex.transaction(async (trx) => {
            try {
              blockCheckpoint.height = actualEndBlock;
              await BlockCheckpoint.query()
                .insert(blockCheckpoint)
                .onConflict('job_name')
                .merge()
                .returning('id')
                .transacting(trx);
            } catch (error) {
              this.logger.error(`❌ [HANDLE_TRANSACTION] Error updating checkpoint:`, error);
              throw error;
            }
          });
        }
        return;
      }

        await knex.transaction(async (trx) => {
          try {
            await this.insertRelatedTx(listTxRaw, trx);
            if (blockCheckpoint) {
              blockCheckpoint.height = actualEndBlock;
              await BlockCheckpoint.query()
                .insert(blockCheckpoint)
                .onConflict('job_name')
                .merge()
                .returning('id')
                .transacting(trx);
            }
          } catch (error) {
            this.logger.error(`❌ [HANDLE_TRANSACTION] Transaction failed, rolling back:`, error);
            throw error;
          }
        });

      this.logger.info(`✅ [HANDLE_TRANSACTION] Completed processing up to block ${actualEndBlock}`);

      try {
        await this.broker.call(
          `${SERVICE.V1.IndexerEventsService.path}.broadcastBlockProcessed`,
          {
            height: actualEndBlock,
            timestamp: new Date().toISOString(),
          }
        );
        this.logger.info(
          ` [HANDLE_TRANSACTION] Emitted block-processed event for height ${actualEndBlock}`
        );
      } catch (error) {
        this.logger.warn(
          `⚠️ [HANDLE_TRANSACTION] Failed to broadcast block-processed event for height ${endBlock}:`,
          error
        );
      }
    } catch (error) {
      this.logger.error(`❌ [HANDLE_TRANSACTION] Error processing transactions:`, error);
      throw error;
    } finally {
      this._processingLock = false;
    }
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
    // this.logger.warn(blocks);
    const promises: any[] = [];

    const getBlockInfo = async (
      height: number,
      timestamp: Date,
      page: string,
      perPage: string
    ) => {
      try {
        const blockInfo = await this.retryRpcCall(
          () => this._httpBatchClient.execute(
            createJsonRpcRequest('tx_search', {
              query: `tx.height=${height}`,
              page,
              per_page: perPage,
            })
          ),
          `tx_search-height-${height}-page-${page}`
        );
        return {
          txs: blockInfo.result?.txs || [],
          tx_count: Number(blockInfo.result?.total_count || 0),
          height,
          timestamp,
        };
      } catch (error) {
        this.logger.error(`❌ Failed to fetch tx_search for height ${height}, page ${page}: ${error}`);
        return {
          txs: [],
          tx_count: 0,
          height,
          timestamp,
        };
      }
    };

    blocks.forEach((block) => {
      if (block.tx_count > 0) {
        this.logger.info('crawl tx by height: ', block.height);
        const txsPerCall = (this._isFreshStart && config.handleTransaction.freshStart)
          ? (config.handleTransaction.freshStart.txsPerCall || config.handleTransaction.txsPerCall)
          : config.handleTransaction.txsPerCall;

        const totalPages = Math.ceil(
          block.tx_count / txsPerCall
        );

        [...Array(totalPages)].forEach((e, i) => {
          const pageIndex = (i + 1).toString();
          promises.push(
            getBlockInfo(
              block.height,
              block.timestamp,
              pageIndex,
              txsPerCall.toString()
            )
          );
        });
      }
    });
    const resultPromisesResults = await Promise.allSettled(promises);
    const resultPromises: any[] = resultPromisesResults
      .filter((result) => result.status === 'fulfilled')
      .map((result) => (result as PromiseFulfilledResult<any>).value);

    const failures = resultPromisesResults.filter((result) => result.status === 'rejected');
    if (failures.length > 0) {
      this.logger.warn(`⚠️ ${failures.length} tx_search RPC call(s) failed, but processing ${resultPromises.length} successful results`);
    }

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
            this.logger.warn(`Handle txhash ${tx.hash}`);
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
              this.logger.warn('tx fail');
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
    this.logger.warn(listTxDecoded);
    const listTxModel: any[] = [];
    listTxDecoded.forEach((payloadBlock) => {
      const { listTx, height, timestamp } = payloadBlock;
      listTx.forEach((tx: any) => {
        this.logger.warn(tx, timestamp, "dataArray");

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
      const effectiveConfig = this._isFreshStart && config.crawlTransaction.freshStart
        ? config.crawlTransaction.freshStart
        : config.crawlTransaction;
      const chunkSize = effectiveConfig.chunkSize || config.crawlTransaction.chunkSize;
      const resultInsert = await transactionDB.batchInsert(
        Transaction.tableName,
        listTxModel,
        chunkSize
      );
      this.logger.warn('result insert tx', resultInsert);
    }
  }



  // insert related table (event, event_attribute, message)
  async insertRelatedTx(
    listDecodedTx: Transaction[],
    transactionDB: Knex.Transaction
  ) {
    this.logger.warn(listDecodedTx);
    const listEventModel: any[] = [];
    const listMsgModel: any[] = [];
    listDecodedTx.forEach((tx) => {
      const rawLogTx = tx.data;

      let sender = '';
      try {
        if (this._registry.decodeAttribute && this._registry.encodeAttribute) {
          sender = this._registry.decodeAttribute(
            this._findAttribute(
              rawLogTx.tx_response.events,
              'message',
              this._registry.encodeAttribute('sender')
            )
          );
        }
      } catch (error) {
        this.logger.warn(
          'txhash not has sender event: ',
          rawLogTx.tx_response.txhash
        );
      }

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
              composite_key: attribute?.key && this._registry.decodeAttribute
                ? `${event.type}.${this._registry.decodeAttribute(
                  attribute?.key
                )}`
                : null,
              key: attribute?.key && this._registry.decodeAttribute
                ? this._registry.decodeAttribute(attribute?.key)
                : null,
              value: attribute?.value && this._registry.decodeAttribute
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
      const effectiveConfig = this._isFreshStart && config.handleTransaction.freshStart
        ? config.handleTransaction.freshStart
        : config.handleTransaction;
      const chunkSize = effectiveConfig.chunkSize || config.handleTransaction.chunkSize || 10000;
      this.logger.info(`📝 [insertRelatedTx] Inserting ${listEventModel.length} events in chunks of ${chunkSize}`);
      for (let i = 0; i < listEventModel.length; i += chunkSize) {
        const chunk = listEventModel.slice(i, i + chunkSize);
        await Event.query()
          .insertGraph(chunk, { allowRefs: true })
          .transacting(transactionDB);
      }
      this.logger.info(`✅ [insertRelatedTx] Inserted ${listEventModel.length} events`);
    }

    if (listMsgModel?.length) {
      const effectiveConfig = this._isFreshStart && config.handleTransaction.freshStart
        ? config.handleTransaction.freshStart
        : config.handleTransaction;
      const chunkSize = effectiveConfig.chunkSize || config.handleTransaction.chunkSize || 10000;
      this.logger.info(`📝 [insertRelatedTx] Inserting ${listMsgModel.length} messages in chunks of ${chunkSize}`);
      const allInsertedMsgs: any[] = [];
      for (let i = 0; i < listMsgModel.length; i += chunkSize) {
        const chunk = listMsgModel.slice(i, i + chunkSize);
        const resultInsertMsgs = await TransactionMessage.query()
          .insert(chunk)
          .transacting(transactionDB);
        const messagesArray = (Array.isArray(resultInsertMsgs) ? resultInsertMsgs : [resultInsertMsgs]) as any[];
        allInsertedMsgs.push(...messagesArray);
      }
      this.logger.info(`✅ [insertRelatedTx] Inserted ${listMsgModel.length} messages`);
      await this.processMessageTypes(allInsertedMsgs, listDecodedTx);
    }
  }

  private async processMessageTypes(resultInsertMsgs: any[], listDecodedTx: Transaction[]): Promise<void> {
    const successfulMsgs = resultInsertMsgs.filter((msg: any) => {
      const parentTx = listDecodedTx.find((tx) => tx.id === msg.tx_id);
      return parentTx?.code === 0;
    });

    this.logger.info(`📋 [insertRelatedTx] Total messages: ${resultInsertMsgs.length}, Successful: ${successfulMsgs.length}`);

    const DIDfiltered = successfulMsgs
      .filter((msg: any) => Object.values(DidMessages).includes(msg.type))
      .map((msg: any) => {
        const parentTx = listDecodedTx.find((tx) => tx.id === msg.tx_id);
        const controller = extractController(msg.content || {});
        return {
          type: msg.type,
          did: msg.content?.did ?? null,
          controller: controller ?? null,
          years: msg.content?.years ?? null,
          timestamp: parentTx?.timestamp ?? null,
          height: parentTx?.height ?? null,
          id: msg?.tx_id ?? null,
        };
      });

    this.logger.info(`📋 [insertRelatedTx] DID messages: ${DIDfiltered.length}`);
    if (DIDfiltered?.length) {
      this.logger.info(`🚀 [insertRelatedTx] Sending ${DIDfiltered.length} DID messages to processor`);
      try {
        await this.broker.call(
          `${SERVICE.V1.DidMessageProcessorService.path}.handleDidMessages`,
          { messages: DIDfiltered },
        );
        this.logger.info(`✅ [insertRelatedTx] DID messages processed successfully`);
      } catch (err) {
        this.logger.error(`❌ [insertRelatedTx] Failed to process DID messages:`, err);
        console.error("FATAL CRAWL_TX DID ERROR:", err);
      }
    }

    const trustRegistryList = successfulMsgs
      .filter((msg: any) =>
        Object.values(TrustRegistryMessageTypes).includes(msg.type as TrustRegistryMessageTypes)
      )
      .map((msg: any) => {
        const parentTx = listDecodedTx.find((tx) => tx.id === msg.tx_id);
        return {
          type: msg.type,
          content: msg.content ?? null,
          timestamp: parentTx?.timestamp ?? null,
          height: parentTx?.height ?? null,
          id: msg?.tx_id ?? null,
        };
      });

    this.logger.info(`📋 [insertRelatedTx] TrustRegistry messages: ${trustRegistryList.length}`);
    if (trustRegistryList?.length) {
      this.logger.info(`🚀 [insertRelatedTx] Sending ${trustRegistryList.length} TR messages to processor`);
      try {
        await this.broker.call(
          `${SERVICE.V1.TrustRegistryMessageProcessorService.path}.handleTrustRegistryMessages`,
          { trustRegistryList },
        );
        this.logger.info(`✅ [insertRelatedTx] TR messages processed successfully`);
      } catch (err) {
        this.logger.error(`❌ [insertRelatedTx] Failed to process TR messages:`, err);
        console.error("FATAL CRAWL_TX TR ERROR:", err);
      }
    }

    const credentialSchemaMessages = successfulMsgs
      .filter((msg: any) =>
        Object.values(CredentialSchemaMessageType).includes(msg.type as CredentialSchemaMessageType)
      )
      .map((msg: any) => {
        const parentTx = listDecodedTx.find((tx) => tx.id === msg.tx_id);
        return {
          type: msg.type,
          content: msg.content ?? null,
          timestamp: parentTx?.timestamp ?? null,
          height: parentTx?.height ?? null,
        };
      });

    this.logger.info(`📋 [insertRelatedTx] CredentialSchema messages: ${credentialSchemaMessages.length}`);

    if (credentialSchemaMessages?.length) {
      this.logger.info(`🚀 [insertRelatedTx] Sending ${credentialSchemaMessages.length} CS messages to processor`);
      try {
        await this.broker.call(
          `${SERVICE.V1.ProcessCredentialSchemaService.path}.handleCredentialSchemas`,
          { credentialSchemaMessages },
        );
        this.logger.info(`✅ [insertRelatedTx] CS messages processed successfully`);
      } catch (err) {
        this.logger.error(`❌ [insertRelatedTx] Failed to process CS messages:`, err);
        console.error("FATAL CRAWL_TX CS ERROR:", err);
      }
    }

    const permissionMessages = successfulMsgs
      .filter((msg: any) => Object.values(PermissionMessageTypes).includes(msg.type))
      .map((msg: any) => {
        const parentTx = listDecodedTx.find((tx) => tx.id === msg.tx_id);
        return {
          type: msg.type,
          content: msg.content ?? null,
          timestamp: parentTx?.timestamp ?? null,
          height: parentTx?.height ?? null,
        };
      });

    this.logger.info(`📋 [insertRelatedTx] Permission messages: ${permissionMessages.length}`);
    if (permissionMessages?.length) {
      this.logger.info(` [insertRelatedTx] Waiting 1s for schema transactions to commit before processing permissions`);
      await new Promise<void>(resolve => { setTimeout(resolve, 1000); });
      const permissionBatchSize = 50;
      for (let i = 0; i < permissionMessages.length; i += permissionBatchSize) {
        const batch = permissionMessages.slice(i, i + permissionBatchSize);
        this.logger.info(`🚀 [insertRelatedTx] Processing permission batch ${Math.floor(i / permissionBatchSize) + 1}/${Math.ceil(permissionMessages.length / permissionBatchSize)} (${batch.length} messages)`);

        try {
          await this.broker.call(
            `${SERVICE.V1.PermProcessorService.path}.handlePermissionMessages`,
            { permissionMessages: batch },
          );
          this.logger.info(`✅ [insertRelatedTx] Permission batch processed successfully`);
        } catch (err) {
          this.logger.error(`❌ [insertRelatedTx] Failed to process permission batch:`, err);
          console.error("FATAL CRAWL_TX PERMISSION ERROR:", err);
        }

        if (i + permissionBatchSize < permissionMessages.length) {
          await new Promise<void>(resolve => { setTimeout(resolve, 200); });
        }
      }

      this.logger.info(`✅ [insertRelatedTx] All permission batches processed`);
    }

    const trustDepositList = resultInsertMsgs
      .filter((msg: any) =>
        Object.values(TrustDepositMessageTypes).includes(msg.type as TrustDepositMessageTypes),
      )
      .map((msg: any) => {
        const parentTx = listDecodedTx.find((tx) => tx.id === msg.tx_id);
        return {
          type: msg.type,
          content: msg.content ?? null,
          timestamp: parentTx?.timestamp ?? null,
          height: parentTx?.height ?? null,
        };
      });

    if (trustDepositList?.length) {
      await this.broker.call(
        `${SERVICE.V1.TrustDepositMessageProcessorService.path}.handleTrustDepositMessages`,
        { trustDepositList },
      );
    }

    const updateParamsList = successfulMsgs
      .filter((msg: any) =>
        Object.values(UpdateParamsMessageTypes).includes(msg.type as UpdateParamsMessageTypes),
      )
      .map((msg: any) => {
        const parentTx = listDecodedTx.find((tx) => tx.id === msg.tx_id);
        return {
          message: msg,
          height: parentTx?.height,
          txHash: parentTx?.hash,
        };
      });

    for (const updateMsg of updateParamsList) {
      this.logger.info(`[insertRelatedTx] Processing UpdateParams message at height ${updateMsg.height}`);
      try {
        await this.broker.call(
          `${SERVICE.V1.GenesisParamsService.path}.handleUpdateParams`,
          updateMsg,
        );
        this.logger.info(`[insertRelatedTx] UpdateParams message processed successfully`);
      } catch (err) {
        this.logger.error(`❌ [insertRelatedTx] Failed to process UpdateParams message:`, err);
        console.error("FATAL CRAWL_TX UPDATE_PARAMS ERROR:", err);
      }
    }

    this.logger.info(`✅ [insertRelatedTx] Completed processing all messages`);
  }

  private checkMappingEventToLog(tx: any) {
    this.logger.info('checking mapping log in tx :', tx.tx_response.txhash);
    let flattenLog: string[] = [];
    let flattenEventEncoded: string[] = [];

    tx?.tx_response?.logs?.forEach((log: any, index: number) => {
      log.events.forEach((event: any) => {
        event.attributes?.forEach((attr: any) => {
          if (attr.value === undefined) {
            flattenLog.push(`${index} - ${event.type} - ${attr.key} - null`);
          } else {
            flattenLog.push(`${index} - ${event.type} - ${attr.key} - ${attr.value}`);
          }
        });
      });
    });

    tx?.tx_response?.events?.forEach((event: any) => {
      event.attributes?.forEach((attr: any) => {
        if (event.msg_index !== undefined) {
          const key = attr.key && this._registry.decodeAttribute
            ? this._registry.decodeAttribute(attr.key)
            : null;
          const value = attr.value && this._registry.decodeAttribute
            ? this._registry.decodeAttribute(attr.value)
            : null;
          flattenEventEncoded.push(
            `${event.msg_index} - ${event.type} - ${key} - ${value}`
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
      this.logger.warn('Failed tx, no need to set index msg');
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
      const queue = this.getQueueManager().getQueue(
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
    await this.waitForServices(SERVICE.V1.CrawlBlock.name);
    const providerRegistry = await getProviderRegistry();
    this._registry = new ChainRegistry(this.logger, providerRegistry);

    try {
      const blockCountResult = await knex('block').count('* as count').first();
      const totalBlocks = blockCountResult ? parseInt(String((blockCountResult as { count: string | number }).count), 10) : 0;
      const checkpoint = await BlockCheckpoint.query().findOne({
        job_name: BULL_JOB_NAME.CRAWL_TRANSACTION,
      });
      const currentBlock = checkpoint ? checkpoint.height : 0;
      this._isFreshStart = totalBlocks < 100 && currentBlock < 1000;
      this.logger.info(` Start mode detection: totalBlocks=${totalBlocks}, currentBlock=${currentBlock}, isFreshStart=${this._isFreshStart}`);
    } catch (error) {
      this.logger.warn(` Could not determine start mode: ${error}. Defaulting to reindexing mode.`);
      this._isFreshStart = false;
    }

    try {
      const lcdClient = await getLcdClient();
      if (!lcdClient?.provider) {
        this.logger.warn(`LCD client not available, skipping node info fetch. Will retry on next operation.`);
      } else {
        try {
          const nodeInfo: GetNodeInfoResponseSDKType = await this.retryRpcCall(
            () => lcdClient.provider.cosmos.base.tendermint.v1beta1.getNodeInfo(),
            'getNodeInfo'
          );
          const cosmosSdkVersion = nodeInfo?.application_version?.cosmos_sdk_version;
          if (cosmosSdkVersion) {
            this._registry.setCosmosSdkVersionByString(cosmosSdkVersion);
          }
        } catch (error: any) {
          const errorMessage = error?.message || String(error);
          if (errorMessage.includes('timeout') || error?.code === 'ECONNABORTED') {
            this.logger.warn(`⚠️ Failed to get node info due to timeout (non-critical): ${errorMessage}. Continuing without SDK version update.`);
          } else {
            this.logger.warn(`⚠️ Failed to get node info (non-critical): ${errorMessage}. Continuing without SDK version update.`);
          }
        }
      }
    } catch (error: any) {
      const errorMessage = error?.message || String(error);
      if (errorMessage.includes('timeout') || error?.code === 'ECONNABORTED') {
        this.logger.warn(`⚠️ LCD client initialization timeout (non-critical): ${errorMessage}. Service will continue and retry later.`);
      } else {
        this.logger.warn(`⚠️ Failed to initialize LCD client (non-critical): ${errorMessage}. Service will continue and retry later.`);
      }
    }

    const crawlTxInterval = (this._isFreshStart && config.crawlTransaction.freshStart)
      ? (config.crawlTransaction.freshStart.millisecondCrawl || config.crawlTransaction.millisecondCrawl)
      : config.crawlTransaction.millisecondCrawl;

    const handleTxInterval = (this._isFreshStart && config.handleTransaction.freshStart)
      ? (config.handleTransaction.freshStart.millisecondCrawl || config.handleTransaction.millisecondCrawl)
      : config.handleTransaction.millisecondCrawl;

    this.logger.info(
      `🚀 CrawlTx Service Starting | Mode: ${this._isFreshStart ? 'Fresh Start' : 'Reindexing'} | ` +
      `CrawlTx Interval: ${crawlTxInterval}ms | HandleTx Interval: ${handleTxInterval}ms`
    );

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
          every: crawlTxInterval,
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
          every: handleTxInterval,
        },
      }
    );
    return super._start();
  }


  private async retryRpcCall<T>(
    rpcCall: () => Promise<T>,
    operationName: string,
    maxAttempts?: number
  ): Promise<T> {
    const attempts = maxAttempts || config.handleTransaction.rpcRetryAttempts || 3;
    const delay = config.handleTransaction.rpcRetryDelay || 1000;
    const timeout = config.handleTransaction.rpcTimeout || 30000;
    let lastError: Error | unknown;

    for (let attempt = 1; attempt <= attempts; attempt++) {
      try {
        return await Promise.race([
          rpcCall(),
          new Promise<never>((_, reject) => {
            setTimeout(() => reject(new Error('RPC call timeout')), timeout);
          }),
        ]);
      } catch (error) {
        lastError = error;
        const isLastAttempt = attempt === attempts;

        if (isLastAttempt) {
          this.logger.error(
            `❌ RPC call failed after ${attempts} attempts: ${operationName}. Error: ${error}`
          );
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

  public setRegistry(registry: ChainRegistry) {
    this._registry = registry;
  }
}
