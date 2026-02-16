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
  getHttpBatchClient,
  getLcdClient,
  SERVICE,
} from '../../common';
import { indexerStatusManager } from '../manager/indexer_status.manager';
import { handleErrorGracefully, checkCrawlingStatus } from '../../common/utils/error_handler';
import { getDbQueryTimeoutMs } from '../../common/utils/db_query_helper';
import { triggerGC } from '../../common/utils/health_check';
import { applySpeedToDelay, applySpeedToBatchSize, getCrawlSpeedMultiplier } from '../../common/utils/crawl_speed_config';
import {
  isVeranaMessageType,
  shouldSkipUnknownMessages,
  isUpdateParamsMessageType,
  isCredentialSchemaMessageType,
  isPermissionMessageType,
  isTrustDepositMessageType,
  isTrustRegistryMessageType,
  isDidMessageType,
  isKnownVeranaMessageType,
} from '../../common/verana-message-types';
import ChainRegistry from '../../common/utils/chain.registry';
import knex from '../../common/utils/db_connection';
import { getProviderRegistry } from '../../common/utils/provider.registry';
import Utils from '../../common/utils/utils';
import { extractController } from '../../common/utils/extract_controller';
import { detectStartMode } from '../../common/utils/start_mode_detector';
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

  private _hasUniqueConstraintCache: boolean | null = null;

  public constructor(public broker: ServiceBroker) {
    super(broker);
    this._httpBatchClient = getHttpBatchClient();
  }

  @QueueHandler({
    queueName: BULL_JOB_NAME.CRAWL_TRANSACTION,
    jobName: BULL_JOB_NAME.CRAWL_TRANSACTION,
  })
  public async jobCrawlTx(): Promise<void> {
    try {
      checkCrawlingStatus();
    } catch {
      this.logger.warn('⏸️ Crawling is stopped, skipping transaction crawl');
      return;
    }

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
      const baseMaxBlocks = config.crawlTransaction.freshStart.blocksPerCall || config.crawlTransaction.blocksPerCall;
      const maxBlocks = applySpeedToBatchSize(baseMaxBlocks, false);
      actualEndBlock = Math.min(endBlock, startBlock + maxBlocks);
    } else if (!this._isFreshStart) {
      const baseMaxBlocks = config.crawlTransaction.blocksPerCall || 200;
      const maxBlocks = applySpeedToBatchSize(baseMaxBlocks, true);
      actualEndBlock = Math.min(endBlock, startBlock + maxBlocks);
    }

    const listTxRaw = await this.getListRawTx(startBlock, actualEndBlock);
    const listdecodedTx = await this.decodeListRawTx(listTxRaw);

    listTxRaw.length = 0;

    await knex.transaction(async (trx) => {
      await this.insertTxDecoded(listdecodedTx, trx);
      if (blockCheckpoint) {
        blockCheckpoint.height = actualEndBlock;
        blockCheckpoint.updated_at = new Date();

        await BlockCheckpoint.query()
          .insert(blockCheckpoint)
          .onConflict('job_name')
          .merge({
            height: actualEndBlock,
            updated_at: blockCheckpoint.updated_at,
          })
          .returning('id')
          .timeout(getDbQueryTimeoutMs())
          .transacting(trx);
        }
      });

    const txCount = listdecodedTx.length;
    if (txCount > 25) {
      triggerGC();
    }

    listdecodedTx.length = 0;
  }

  @QueueHandler({
    queueName: BULL_JOB_NAME.HANDLE_TRANSACTION,
    jobName: BULL_JOB_NAME.HANDLE_TRANSACTION,
  })
  public async jobHandlerCrawlTx(): Promise<void> {
    try {
      checkCrawlingStatus();
    } catch {
      this.logger.warn('⏸️ [HANDLE_TRANSACTION] Crawling is stopped, skipping transaction handling');
      return;
    }

    if (this._processingLock) {
      this.logger.debug(' [HANDLE_TRANSACTION] Already in progress, skipping...');
      return;
    }

    this._processingLock = true;

    try {
      // Check if unique constraint exists ONCE before processing (cache the result)
      if (this._hasUniqueConstraintCache === null) {
        try {
          const constraintCheck = await knex.raw(`
            SELECT 1 FROM pg_indexes 
            WHERE schemaname = 'public' 
              AND tablename LIKE 'transaction_message%'
              AND indexdef LIKE '%UNIQUE%'
              AND indexdef LIKE '%tx_id%'
              AND indexdef LIKE '%index%'
            LIMIT 1
          `);
          this._hasUniqueConstraintCache = constraintCheck.rows.length > 0;
          this.logger.info(` [HANDLE_TRANSACTION] Unique constraint check: ${this._hasUniqueConstraintCache ? 'EXISTS' : 'DOES NOT EXIST'}`);
        } catch (error) {
          // If check fails, assume constraint doesn't exist
          this._hasUniqueConstraintCache = false;
          this.logger.warn(` [HANDLE_TRANSACTION] Constraint check failed, assuming no constraint exists:`, error);
        }
      }

      // Get checkpoint - use height for compatibility but track by transaction ID
      const [startBlock, endBlock, blockCheckpoint] =
        await BlockCheckpoint.getCheckpoint(
          BULL_JOB_NAME.HANDLE_TRANSACTION,
          [BULL_JOB_NAME.CRAWL_TRANSACTION],
          config.handleTransaction.key
        );

      // Get last processed transaction ID from checkpoint height
      // Checkpoint now stores actual block height, so we need to find the last transaction ID at that height
      let lastProcessedTxId = 0;
      if (blockCheckpoint && blockCheckpoint.height > 0) {
        // Find the last transaction ID at or before the checkpoint height
        const txAtHeight = await Transaction.query()
          .where('height', '<=', blockCheckpoint.height)
          .orderBy('id', 'desc')
          .first();
        lastProcessedTxId = txAtHeight?.id || 0;
      }

      // Get max transaction ID from crawl:transaction checkpoint
      const crawlTxCheckpoint = await BlockCheckpoint.query()
        .select('height')
        .where('job_name', BULL_JOB_NAME.CRAWL_TRANSACTION)
        .first();
      
      const maxTxId = await Transaction.query()
        .where('height', '<=', crawlTxCheckpoint?.height || endBlock)
        .max('id as max_id')
        .first() as any;
      
      const maxAvailableTxId = maxTxId?.max_id || 0;

      if (lastProcessedTxId >= maxAvailableTxId) {
        // No new transactions, but still advance checkpoint to CRAWL_TRANSACTION height
        // so downstream services (e.g. stats) know all blocks have been checked
        const crawlTxHeight = crawlTxCheckpoint?.height || 0;
        if (blockCheckpoint && crawlTxHeight > blockCheckpoint.height) {
          const previousHeight = blockCheckpoint.height;
          blockCheckpoint.height = crawlTxHeight;
          blockCheckpoint.updated_at = new Date();
          await knex.transaction(async (trx) => {
            await BlockCheckpoint.query()
              .insert(blockCheckpoint)
              .onConflict('job_name')
              .merge({
                height: crawlTxHeight,
                updated_at: blockCheckpoint.updated_at,
              })
              .timeout(getDbQueryTimeoutMs())
              .transacting(trx);
          });
          this.logger.info(` [HANDLE_TRANSACTION] No new transactions, advanced checkpoint from ${previousHeight} to ${crawlTxHeight}`);
          try {
            await this.broker.call(
              `${SERVICE.V1.IndexerEventsService.path}.broadcastBlockProcessed`,
              {
                height: crawlTxHeight,
                timestamp: blockCheckpoint.updated_at.toISOString(),
              }
            );
            this.logger.debug(` [HANDLE_TRANSACTION] Emitted block-processed event for height ${crawlTxHeight} (no-tx advance)`);
          } catch (error) {
            this.logger.warn(
              `⚠️ [HANDLE_TRANSACTION] Failed to broadcast block-processed for height ${crawlTxHeight}:`,
              error
            );
          }
        } else {
          this.logger.debug(` [HANDLE_TRANSACTION] No new transactions to process (${lastProcessedTxId} >= ${maxAvailableTxId})`);
        }
        return;
      }

      this.logger.info(
        ` [HANDLE_TRANSACTION] Processing transactions from ID ${lastProcessedTxId + 1} to ${maxAvailableTxId}`
      );

      // Process transactions sequentially by ID
      const maxTxsPerCall = this._isFreshStart && config.handleTransaction.freshStart
        ? applySpeedToBatchSize(
            config.handleTransaction.freshStart.txsPerCall || 100,
            false
          )
        : applySpeedToBatchSize(
            config.handleTransaction.txsPerCall || 100,
            true
          );

      let currentTxId = lastProcessedTxId;
      let txsProcessed = 0;
      let lastProcessedHeight = startBlock;

      while (currentTxId < maxAvailableTxId && txsProcessed < maxTxsPerCall) {
        // Fetch transactions in batches by ID (much faster than by height)
        const batchSize = Math.min(50, maxTxsPerCall - txsProcessed);
        const transactions = await Transaction.query()
          .where('id', '>', currentTxId)
          .orderBy('id', 'asc')
          .limit(batchSize);

        if (transactions.length === 0) {
          break;
        }

        // Group transactions by block height for batch processing
        const transactionsByBlock = new Map<number, Transaction[]>();
        for (const tx of transactions) {
          if (!transactionsByBlock.has(tx.height)) {
            transactionsByBlock.set(tx.height, []);
          }
          transactionsByBlock.get(tx.height)!.push(tx);
        }

        // Process each block's transactions
        for (const [blockHeight, blockTxs] of transactionsByBlock.entries()) {
          try {
            // Process all transactions for this block
            const processingPayloads = await this.processTransactionsForBlock(blockTxs, blockCheckpoint);

            // Process payloads after successful commit
            if (processingPayloads && !(config.handleTransaction && (config.handleTransaction as any).processMessagesInsideTransaction)) {
              try {
                await this.processPayloads(processingPayloads);
              } catch (err: any) {
                this.logger.error(` [HANDLE_TRANSACTION] Error processing message batches:`, err);
                throw err;
              }
            }

            // Update tracking
            lastProcessedHeight = blockHeight;
            currentTxId = Math.max(...blockTxs.map(tx => tx.id));
            txsProcessed += blockTxs.length;

            if (blockCheckpoint) {
              await knex.transaction(async (trx) => {
                blockCheckpoint.height = blockHeight; 
                blockCheckpoint.updated_at = new Date();
                await BlockCheckpoint.query()
                  .insert(blockCheckpoint)
                  .onConflict('job_name')
                  .merge({
                    height: blockHeight,
                    updated_at: blockCheckpoint.updated_at,
                  })
                  .transacting(trx);
              });
            }

            // Broadcast websocket event for processed block
            try {
              await this.broker.call(
                `${SERVICE.V1.IndexerEventsService.path}.broadcastBlockProcessed`,
                {
                  height: blockHeight,
                  timestamp: new Date().toISOString(),
                }
              );
              this.logger.debug(
                ` [HANDLE_TRANSACTION] Emitted block-processed event for height ${blockHeight}`
              );
            } catch (error) {
              this.logger.warn(
                `⚠️ [HANDLE_TRANSACTION] Failed to broadcast block-processed event for height ${blockHeight}:`,
                error
              );
            }

            this.logger.info(`✅ [HANDLE_TRANSACTION] Successfully processed block ${blockHeight} with ${blockTxs.length} transaction(s)`);
          } catch (error) {
            this.logger.error(`❌ [HANDLE_TRANSACTION] Failed to process block ${blockHeight}:`, error);
            throw error;
          }
        }
      }

      // Checkpoint is updated per block above, just log summary
      if (txsProcessed > 0) {
        this.logger.info(`✅ [HANDLE_TRANSACTION] Completed processing ${txsProcessed} transaction(s), checkpoint at height ${lastProcessedHeight}, last tx ID ${currentTxId}`);
      }
    } catch (error) {
      await handleErrorGracefully(
        error,
        SERVICE.V1.CrawlTransaction.key,
        '[HANDLE_TRANSACTION] Error processing transactions'
      );
    } finally {
      this._processingLock = false;
    }
  }

  /**
   * Process transactions for a block sequentially.
   * Processes transactions one by one in a single DB transaction.
   */
  private async processTransactionsForBlock(
    blockTransactions: Transaction[],
    blockCheckpoint: BlockCheckpoint | null
  ): Promise<any> {
    if (blockTransactions.length === 0) {
      return null;
    }

    const blockHeight = blockTransactions[0].height;
    this.logger.info(` [HANDLE_TRANSACTION] Processing ${blockTransactions.length} transaction(s) for block ${blockHeight}`);

    // Sort transactions by index to maintain order
    blockTransactions.sort((a, b) => a.index - b.index);

    // Wrap entire block processing in a single DB transaction
    return await knex.transaction(async (trx) => {
      try {
        const allPayloads: any = {
          DIDfiltered: [],
          trustRegistryList: [],
          credentialSchemaMessages: [],
          permissionMessages: [],
          trustDepositList: [],
          updateParamsList: [],
        };

        // Process transactions sequentially using a for loop
        for (let i = 0; i < blockTransactions.length; i++) {
          const tx = blockTransactions[i];
          this.logger.debug(` [HANDLE_TRANSACTION] Processing transaction ${i + 1}/${blockTransactions.length} (id: ${tx.id}, hash: ${tx.hash}) for block ${blockHeight}`);

          const txPayloads = await this.processSingleTransaction(tx, trx);
          
          // Merge payloads
          if (txPayloads) {
            if (txPayloads.DIDfiltered?.length) {
              allPayloads.DIDfiltered.push(...txPayloads.DIDfiltered);
            }
            if (txPayloads.trustRegistryList?.length) {
              allPayloads.trustRegistryList.push(...txPayloads.trustRegistryList);
            }
            if (txPayloads.credentialSchemaMessages?.length) {
              allPayloads.credentialSchemaMessages.push(...txPayloads.credentialSchemaMessages);
            }
            if (txPayloads.permissionMessages?.length) {
              allPayloads.permissionMessages.push(...txPayloads.permissionMessages);
            }
            if (txPayloads.trustDepositList?.length) {
              allPayloads.trustDepositList.push(...txPayloads.trustDepositList);
            }
            if (txPayloads.updateParamsList?.length) {
              allPayloads.updateParamsList.push(...txPayloads.updateParamsList);
            }
          }
        }

        this.logger.info(`✅ [HANDLE_TRANSACTION] Successfully committed block ${blockHeight} with ${blockTransactions.length} transaction(s)`);
        return allPayloads;
      } catch (error) {
        this.logger.error(`❌ [HANDLE_TRANSACTION] Transaction failed for block ${blockHeight}, rolling back:`, error);
        throw error;
      }
    });
  }

  /**
   * Process a single transaction sequentially.
   * Inserts events and messages with idempotent operations.
   */
  private async processSingleTransaction(
    tx: Transaction,
    trx: Knex.Transaction
  ): Promise<any> {
    const rawLogTx = tx.data;

    if (!rawLogTx || !rawLogTx.tx_response) {
      this.logger.warn(` [HANDLE_TRANSACTION] Transaction ${tx.hash} has no raw log data, skipping`);
      return null;
    }

    // Extract sender
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
        ` [HANDLE_TRANSACTION] Transaction ${tx.hash} has no sender event`
      );
    }

    // Create events with message index
    const listEventWithMsgIndex = this.createListEventWithMsgIndex(rawLogTx);

    // Prepare events for insertion (idempotent)
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

    // Prepare messages for insertion (idempotent)
    const msgInsert =
      rawLogTx.tx.body.messages.map((message: any, index: any) => ({
        tx_id: tx.id,
        sender,
        index,
        type: message['@type'],
        content: message,
      })) ?? [];

    // Insert events idempotently (chunked for performance)
    // Note: insertGraph doesn't support onConflict directly, so we use try-catch for idempotency
    if (eventInsert.length > 0) {
      const effectiveConfig = this._isFreshStart && config.handleTransaction.freshStart
        ? config.handleTransaction.freshStart
        : config.handleTransaction;
      const baseChunkSize = effectiveConfig.chunkSize || config.handleTransaction.chunkSize || 10000;
      const chunkSize = applySpeedToBatchSize(baseChunkSize, !this._isFreshStart);

      for (let i = 0; i < eventInsert.length; i += chunkSize) {
        const chunk = eventInsert.slice(i, i + chunkSize);
        try {
          await Event.query()
            .insertGraph(chunk, { allowRefs: true })
            .transacting(trx);
        } catch (error: any) {
          // If unique constraint violation, ignore (idempotent replay)
          // PostgreSQL: 23505 = unique_violation, SQLite: SQLITE_CONSTRAINT
          const isConflictError = error?.nativeError?.code === '23505' 
            || error?.code === 'SQLITE_CONSTRAINT'
            || error?.message?.includes('duplicate key')
            || error?.message?.includes('UNIQUE constraint');
          
          if (isConflictError) {
            this.logger.debug(` [HANDLE_TRANSACTION] Event already exists (idempotent replay), skipping`);
          } else {
            throw error;
          }
        }
      }
    }

    // Insert messages idempotently (chunked for performance)
    // Check if unique constraint exists first to avoid transaction abort
    if (msgInsert.length > 0) {
      const effectiveConfig = this._isFreshStart && config.handleTransaction.freshStart
        ? config.handleTransaction.freshStart
        : config.handleTransaction;
      const baseChunkSizeMsg = effectiveConfig.chunkSize || config.handleTransaction.chunkSize || 10000;
      const chunkSizeMsg = applySpeedToBatchSize(baseChunkSizeMsg, !this._isFreshStart);

      // Always use regular insert with duplicate handling to avoid transaction abort issues
      // ON CONFLICT can abort the transaction if constraint doesn't exist, so we avoid it
      for (let i = 0; i < msgInsert.length; i += chunkSizeMsg) {
        const chunk = msgInsert.slice(i, i + chunkSizeMsg);
        
        try {
          await TransactionMessage.query()
            .insert(chunk)
            .timeout(60000)
            .transacting(trx);
        } catch (insertError: any) {
          // If unique constraint violation, ignore (idempotent replay)
          const isConflictError = insertError?.nativeError?.code === '23505' 
            || insertError?.code === 'SQLITE_CONSTRAINT'
            || insertError?.message?.includes('duplicate key')
            || insertError?.message?.includes('UNIQUE constraint');
          
          if (isConflictError) {
            this.logger.debug(` [HANDLE_TRANSACTION] Message already exists (idempotent replay), skipping`);
          } else {
            throw insertError;
          }
        }
      }

      // Process message types for payload generation
      const payload = await this.processMessageTypes(msgInsert, [tx]);
      
      // Process payloads inside transaction if configured
      if (config.handleTransaction && (config.handleTransaction as any).processMessagesInsideTransaction) {
        try {
          await this.processPayloads(payload);
        } catch (err: any) {
          this.logger.error(`[processSingleTransaction] Error processing payloads inside transaction:`, err);
          throw err;
        }
      }

      return payload;
    }

    return null;
  }

  /**
   * Process TrustDeposit events for a single block sequentially.
   */
  private async processTrustDepositEventsForSingleBlock(blockHeight: number): Promise<void> {
    const block = await Block.query()
      .where('height', '=', blockHeight)
      .first();

    if (!block) {
      this.logger.warn(`[HANDLE_TRANSACTION] Block ${blockHeight} not found for TrustDeposit processing`);
      return;
    }

    try {
      await this.broker.call(
        `${SERVICE.V1.CrawlTrustDepositService.path}.processBlockEvents`,
        { block },
        { timeout: 30000 }
      );
    } catch (err: any) {
      this.logger.warn(`[HANDLE_TRANSACTION] Failed to process TrustDeposit events for block ${blockHeight}:`, err?.message || err);
      throw err;
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
              order_by: 'asc',
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

    const baseTxsPerCall = (this._isFreshStart && config.handleTransaction.freshStart)
      ? (config.handleTransaction.freshStart.txsPerCall || config.handleTransaction.txsPerCall)
      : config.handleTransaction.txsPerCall;
    const rawTxsPerCall = applySpeedToBatchSize(baseTxsPerCall, !this._isFreshStart);
    const maxPerPage = this.getTxSearchMaxPerPage();
    const txsPerCall = Math.max(1, Math.min(rawTxsPerCall, maxPerPage));

    if (rawTxsPerCall > maxPerPage) {
      this.logger.warn(
        `[crawl_tx] txsPerCall ${rawTxsPerCall} exceeds tx_search max ${maxPerPage}; clamping per_page to ${txsPerCall} to avoid missing transactions`
      );
    }

    blocks.forEach((block) => {
      if (block.tx_count > 0) {
        this.logger.info('crawl tx by height: ', block.height);
        const totalPages = Math.ceil(
          block.tx_count / txsPerCall
        );

        [...Array(totalPages)].forEach((e, i) => {
          const pageIndex = (i + 1).toString();
          promises.push(
            getBlockInfo(
              block.height,
              block.time,
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
          promises.length = 0;
          resultPromisesResults.length = 0;
          resultPromises.length = 0;
          blocks.length = 0;
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

    promises.length = 0;
    resultPromisesResults.length = 0;
    resultPromises.length = 0;
    blocks.length = 0;

    return listRawTxs;
  }

  private getTxSearchMaxPerPage(): number {
    const envValue = Number(process.env.TX_SEARCH_MAX_PER_PAGE);
    if (Number.isFinite(envValue) && envValue > 0) {
      return Math.floor(envValue);
    }
    return 100;
  }

  // decode list raw tx
  async decodeListRawTx(
    listRawTx: { listTx: any; height: number; timestamp: string }[]
  ): Promise<{ listTx: any; height: number; timestamp: string }[]> {
    const listDecodedTx = await Promise.all(
      listRawTx.map(async (payloadBlock) => {
        const { listTx, timestamp, height } = payloadBlock;
        const listHandleTx: any[] = [];
        const mapExistedTx: Map<string, boolean> = new Map();
        let listHash: string[] = [];
        try {
          // check if tx existed
          listHash = listTx.txs.map((tx: any) => tx.hash);
          const listTxExisted = await Transaction.query()
            .whereIn('hash', listHash)
            .timeout(getDbQueryTimeoutMs(120000));
          listTxExisted.forEach((tx) => {
            mapExistedTx.set(tx.hash, true);
          });
          listTxExisted.length = 0;

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

          mapExistedTx.clear();
          listHash.length = 0;

          return { listTx: listHandleTx, timestamp, height };
        } catch (error) {
          this.logger.error(error);
          mapExistedTx.clear();
          listHash.length = 0;
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
    const totalTxs = listTxDecoded.reduce((sum, block) => {
      const txList = Array.isArray(block.listTx) ? block.listTx : (block.listTx?.txs || []);
      return sum + txList.length;
    }, 0);
    if (totalTxs > 0) {
      this.logger.info(`[insertTxDecoded] Processing ${totalTxs} transactions across ${listTxDecoded.length} blocks`);
    }
    const listTxModel: any[] = [];
    listTxDecoded.forEach((payloadBlock) => {
      const { listTx, height, timestamp } = payloadBlock;
      const txArray = Array.isArray(listTx) ? listTx : (listTx?.txs || []);
      if (txArray.length === 0) {
        this.logger.debug(`[insertTxDecoded] No transactions found for block ${height}`);
        return;
      }
      txArray.forEach((tx: any) => {
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
      const baseChunkSize = effectiveConfig.chunkSize || config.crawlTransaction.chunkSize;
      const chunkSize = applySpeedToBatchSize(baseChunkSize, !this._isFreshStart);
      const resultInsert = await transactionDB.batchInsert(
        Transaction.tableName,
        listTxModel,
        chunkSize
      );
      this.logger.info(`[insertTxDecoded] Successfully inserted ${listTxModel.length} transactions`);
      listTxModel.length = 0;
    }
  }



  // insert related table (event, event_attribute, message)
  async insertRelatedTx(
    listDecodedTx: Transaction[],
    transactionDB: Knex.Transaction
  ) {
    this.logger.info(`[insertRelatedTx] Processing ${listDecodedTx.length} decoded transactions`);
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
      const baseChunkSize = effectiveConfig.chunkSize || config.handleTransaction.chunkSize || 10000;
      const chunkSize = applySpeedToBatchSize(baseChunkSize, !this._isFreshStart);
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
      const baseChunkSizeMsg = effectiveConfig.chunkSize || config.handleTransaction.chunkSize || 10000;
      const chunkSizeMsg = applySpeedToBatchSize(baseChunkSizeMsg, !this._isFreshStart);
      this.logger.info(`[insertRelatedTx] Inserting ${listMsgModel.length} messages in chunks of ${chunkSizeMsg}`);

      const messagesForProcessing = [...listMsgModel];

      for (let i = 0; i < listMsgModel.length; i += chunkSizeMsg) {
        const chunk = listMsgModel.slice(i, i + chunkSizeMsg);
        await TransactionMessage.query()
          .insert(chunk)
          .timeout(getDbQueryTimeoutMs(60000))
          .transacting(transactionDB);
      }
      this.logger.info(`[insertRelatedTx] Inserted ${listMsgModel.length} messages`);
      const payload = await this.processMessageTypes(messagesForProcessing, listDecodedTx);
      if (config.handleTransaction && (config.handleTransaction as any).processMessagesInsideTransaction) {
        try {
          await this.processPayloads(payload);
        } catch (err: any) {
          this.logger.error(`[insertRelatedTx] Error processing payloads inside transaction:`, err);
          throw err;
        }
      }
      listEventModel.length = 0;
      listMsgModel.length = 0;
      messagesForProcessing.length = 0;
      return payload;
    }
    return {
      DIDfiltered: [],
      trustRegistryList: [],
      credentialSchemaMessages: [],
      permissionMessages: [],
      trustDepositList: [],
      updateParamsList: [],
    };
  }

  private async processMessageTypes(resultInsertMsgs: any[], listDecodedTx: Transaction[]): Promise<any> {
    const successfulMsgs = resultInsertMsgs.filter((msg: any) => {
      const parentTx = listDecodedTx.find((tx) => tx.id === msg.tx_id);
      return parentTx?.code === 0;
    });

    this.logger.info(`📋 [insertRelatedTx] Total messages: ${resultInsertMsgs.length}, Successful: ${successfulMsgs.length}`);

    const DIDfiltered = successfulMsgs
      .filter((msg: any) => isDidMessageType(msg.type))
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

    const trustRegistryList = successfulMsgs
      .filter((msg: any) => isTrustRegistryMessageType(msg.type))
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

    const credentialSchemaMessages = successfulMsgs
      .filter((msg: any) => isCredentialSchemaMessageType(msg.type))
      .map((msg: any) => {
        const parentTx = listDecodedTx.find((tx) => tx.id === msg.tx_id);
        return {
          type: msg.type,
          content: msg.content ?? null,
          timestamp: parentTx?.timestamp ?? null,
          height: parentTx?.height ?? null,
        };
      });

    const permissionMessages = successfulMsgs
      .filter((msg: any) => isPermissionMessageType(msg.type))
      .map((msg: any) => {
        const parentTx = listDecodedTx.find((tx) => tx.id === msg.tx_id);
        return {
          type: msg.type,
          content: msg.content ?? null,
          timestamp: parentTx?.timestamp ?? null,
          height: parentTx?.height ?? null,
        };
      });

    const trustDepositList = resultInsertMsgs
      .filter((msg: any) => isTrustDepositMessageType(msg.type))
      .map((msg: any) => {
        const parentTx = listDecodedTx.find((tx) => tx.id === msg.tx_id);
        return {
          type: msg.type,
          content: msg.content ?? null,
          timestamp: parentTx?.timestamp ?? null,
          height: parentTx?.height ?? null,
        };
      });

    const updateParamsList = successfulMsgs
      .filter((msg: any) => isUpdateParamsMessageType(msg.type))
      .map((msg: any) => {
        const parentTx = listDecodedTx.find((tx) => tx.id === msg.tx_id);
        return {
          message: msg,
          height: parentTx?.height,
          txHash: parentTx?.hash,
        };
      });

    await this.validateAllMessagesProcessed(successfulMsgs, listDecodedTx);

    this.logger.info(`[insertRelatedTx] Completed processing all messages (payload prepared)`);

    return {
      DIDfiltered,
      trustRegistryList,
      credentialSchemaMessages,
      permissionMessages,
      trustDepositList,
      updateParamsList,
    };
  }

  private async validateAllMessagesProcessed(successfulMsgs: any[], listDecodedTx: Transaction[]): Promise<void> {
    const unknownMessages: any[] = [];

    successfulMsgs.forEach((msg: any) => {
      const isVeranaMessage = isVeranaMessageType(msg.type);
      if (!isVeranaMessage) {
        return;
      }

      if (!isKnownVeranaMessageType(msg.type)) {
        unknownMessages.push(msg);
      }
    });

    if (unknownMessages.length > 0) {
      const unknownTypes = [...new Set(unknownMessages.map((msg: any) => msg.type))];
      const skipUnknown = shouldSkipUnknownMessages();

      this.logger.error(`INDEXER COLLISION RISK: Unknown Verana message types detected: ${unknownTypes.join(', ')}`);
      console.error('='.repeat(80));
      console.error('CRITICAL: UNKNOWN VERANA MESSAGE TYPES DETECTED');
      console.error('='.repeat(80));
      console.error(`Unknown Verana message types: ${unknownTypes.join(', ')}`);
      console.error(`This indicates a protocol change or new feature that requires indexer updates.`);
      console.error(`Affected transactions: ${unknownMessages.length}`);
      console.error(`Skip mode: ${skipUnknown ? 'ENABLED (TESTING)' : 'DISABLED (PRODUCTION)'}`);
      console.error('');
      console.error('Sample affected transactions:');
      unknownMessages.slice(0, 3).forEach((msg: any, index: number) => {
        const parentTx = listDecodedTx.find((tx) => tx.id === msg.tx_id);
        const height = parentTx?.height ?? 'unknown';
        console.error(`  ${index + 1}. TX ${msg.tx_id} at height ${height}: ${msg.type}`);
      });
      console.error('');
      console.error(
        skipUnknown
          ? 'Continuing in test mode - monitor for protocol changes'
          : 'Indexer stopped - update message handlers for new message types'
      );
      console.error('='.repeat(80));

      if (!skipUnknown) {
        console.error(`STOPPING CRAWLING: Unknown Verana message types: ${unknownTypes.join(', ')}`);
        const error = new Error(`Unknown Verana message types: ${unknownTypes.join(', ')}`);
        await handleErrorGracefully(
          error,
          SERVICE.V1.CrawlTransaction.key,
          'Unknown Verana message types',
          true
        );
      } else {
        this.logger.warn(
          `TEST MODE: Skipping validation for unknown Verana message types: ${unknownTypes.join(', ')}`
        );
      }
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
        const job = jobInDelayed[0];
        try {
          const jobState = await job.getState();
          if (jobState === 'delayed') {
            await job.promote();
            this.logger.debug(`Promoted delayed job ${job.id}`);
          } else {
            this.logger.debug(`Job ${job.id} is not in delayed state (current: ${jobState}), skipping promotion`);
          }
        } catch (promoteError: any) {
          const errorMessage = promoteError?.message || String(promoteError);
          if (errorMessage.includes('not in the delayed state') ||
            errorMessage.includes('is not in the delayed state') ||
            errorMessage.includes('Job is not in delayed state')) {
            this.logger.debug(`Job ${job.id} cannot be promoted (not in delayed state), this is normal if job was already processed`);
          } else {
            this.logger.warn(`Failed to promote job ${job.id}:`, promoteError);
          }
        }
      }
    } catch (error: any) {
      if (error?.message?.includes('not in the delayed state') || error?.message?.includes('is not in the delayed state')) {
        this.logger.debug('Job promotion skipped (job not in delayed state), this is normal');
      } else {
        this.logger.warn('Error checking/promoting delayed jobs:', error);
      }
    }
  }

  public async _start() {
    try {
      await this.broker.waitForServices([SERVICE.V1.CrawlBlock.name]);
      const providerRegistry = await getProviderRegistry();
      this._registry = new ChainRegistry(this.logger, providerRegistry);

      const startMode = await detectStartMode(BULL_JOB_NAME.CRAWL_TRANSACTION, this.logger);
      this._isFreshStart = startMode.isFreshStart;
      this.logger.info(`Start mode: blocks=${startMode.totalBlocks}, checkpoint=${startMode.currentBlock}, freshStart=${this._isFreshStart}, cacheCleared=${startMode.cacheCleared || false}`);

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
            const wasStopped = await handleErrorGracefully(
              error,
              SERVICE.V1.CrawlTransaction.key,
              'Failed to get node info'
            );

            if (!wasStopped) {
              if (errorMessage.includes('timeout') || error?.code === 'ECONNABORTED') {
                this.logger.warn(`⚠️ Failed to get node info due to timeout (non-critical): ${errorMessage}. Continuing without SDK version update.`);
              } else {
                this.logger.warn(`⚠️ Failed to get node info (non-critical): ${errorMessage}. Continuing without SDK version update.`);
              }
            } else {
              this.logger.warn('⚠️ Service will start but indexer is stopped. APIs will return error status.');
            }
          }
        }
      } catch (error: any) {
        const errorMessage = error?.message || String(error);
        if (!indexerStatusManager.isIndexerRunning()) {
          this.logger.error(`❌ Service startup encountered error (indexer already stopped): ${errorMessage}`);
          this.logger.warn('⚠️ Service will start but indexer is stopped. APIs will return error status.');
        } else {
          const wasStopped = await handleErrorGracefully(
            error,
            SERVICE.V1.CrawlTransaction.key,
            'Service startup error'
          );

          if (wasStopped) {
            this.logger.warn('⚠️ Service will start but indexer is stopped. APIs will return error status.');
          } else if (errorMessage.includes('timeout') || error?.code === 'ECONNABORTED') {
            this.logger.warn(`⚠️ LCD client initialization timeout (non-critical): ${errorMessage}. Service will continue and retry later.`);
          } else {
            this.logger.warn(`⚠️ Failed to initialize LCD client (non-critical): ${errorMessage}. Service will continue and retry later.`);
          }
        }
      }
    } catch (error: any) {
      const wasStopped = await handleErrorGracefully(
        error,
        SERVICE.V1.CrawlTransaction.key,
        'Service startup error'
      );

      if (wasStopped) {
        this.logger.warn('⚠️ Service will start but indexer is stopped. APIs will return error status.');
      }
    }

    const baseCrawlTxInterval = (this._isFreshStart && config.crawlTransaction.freshStart)
      ? (config.crawlTransaction.freshStart.millisecondCrawl || config.crawlTransaction.millisecondCrawl)
      : config.crawlTransaction.millisecondCrawl;
    const crawlTxInterval = applySpeedToDelay(baseCrawlTxInterval, !this._isFreshStart);

    const baseHandleTxInterval = (this._isFreshStart && config.handleTransaction.freshStart)
      ? (config.handleTransaction.freshStart.millisecondCrawl || config.handleTransaction.millisecondCrawl)
      : config.handleTransaction.millisecondCrawl;
    const handleTxInterval = applySpeedToDelay(baseHandleTxInterval, !this._isFreshStart);

    const speedMultiplier = getCrawlSpeedMultiplier();
    this.logger.info(
      `🚀 CrawlTx Service Starting | Mode: ${this._isFreshStart ? 'Fresh Start' : 'Reindexing'} | ` +
      `CrawlTx Interval: ${crawlTxInterval}ms | HandleTx Interval: ${handleTxInterval}ms | ` +
      `Speed Multiplier: ${speedMultiplier}x ${speedMultiplier !== 1.0 ? `(${this._isFreshStart ? 'slower/conservative' : 'faster'})` : '(default)'}`
    );

    if (process.env.NODE_ENV !== 'test') {
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
    }
    return super._start();
  }



  private async processTrustDepositEventsForBlocks(startBlock: number, endBlock: number): Promise<void> {
    try {
      const blocks = await Block.query()
        .where('height', '>', startBlock)
        .andWhere('height', '<=', endBlock)
        .orderBy('height', 'asc');

      if (!blocks.length) {
        return;
      }

      this.logger.info(`[HANDLE_TRANSACTION] Processing TrustDeposit events for ${blocks.length} blocks (${startBlock} to ${endBlock})`);

      for (const block of blocks) {
        try {
          await this.broker.call(
            `${SERVICE.V1.CrawlTrustDepositService.path}.processBlockEvents`,
            { block },
            { timeout: 30000 }
          );
        } catch (err: any) {
          this.logger.warn(`[HANDLE_TRANSACTION] Failed to process TrustDeposit events for block ${block.height}:`, err?.message || err);
        }
      }
    } catch (error) {
      this.logger.error(`[HANDLE_TRANSACTION] Error in processTrustDepositEventsForBlocks:`, error);
      throw error;
    }
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
        const { delay: delayUtil } = await import('../../common/utils/db_query_helper');
        await delayUtil(backoffDelay);
      }
    }

    throw lastError instanceof Error
      ? lastError
      : new Error(`Failed to execute ${operationName} after ${attempts} attempts`);
  }

  public setRegistry(registry: ChainRegistry) {
    this._registry = registry;
  }
  private async processPayloads(payload: any) {
    if (!payload) return;
    if (payload.DIDfiltered?.length) {
      await this.broker.call(
        `${SERVICE.V1.DidMessageProcessorService.path}.handleDidMessages`,
        { messages: payload.DIDfiltered },
      );
    }
    if (payload.trustRegistryList?.length) {
      await this.broker.call(
        `${SERVICE.V1.TrustRegistryMessageProcessorService.path}.handleTrustRegistryMessages`,
        { trustRegistryList: payload.trustRegistryList },
      );
    }

    if (payload.credentialSchemaMessages?.length) {
      await this.broker.call(
        `${SERVICE.V1.ProcessCredentialSchemaService.path}.handleCredentialSchemas`,
        { credentialSchemaMessages: payload.credentialSchemaMessages },
      );
    }

    if (payload.permissionMessages?.length) {
      const permissionMessages = payload.permissionMessages;
      const permissionBatchSize = 50;
      for (let i = 0; i < permissionMessages.length; i += permissionBatchSize) {
        const batch = permissionMessages.slice(i, i + permissionBatchSize);
        await this.broker.call(
          `${SERVICE.V1.PermProcessorService.path}.handlePermissionMessages`,
          { permissionMessages: batch },
        );
        if (i + permissionBatchSize < permissionMessages.length) {
          const { delay } = await import('../../common/utils/db_query_helper');
          await delay(200);
        }
      }
    }

    if (payload.trustDepositList?.length) {
      await this.broker.call(
        `${SERVICE.V1.TrustDepositMessageProcessorService.path}.handleTrustDepositMessages`,
        { trustDepositList: payload.trustDepositList },
      );
    }

    if (payload.updateParamsList?.length) {
      for (const updateMsg of payload.updateParamsList) {
        try {
          await this.broker.call(
            `${SERVICE.V1.GenesisParamsService.path}.handleUpdateParams`,
            updateMsg,
          );
        } catch (err) {
          this.logger.error(`[processPayloads] Failed to process UpdateParams message:`, err);
        }
      }
    }
  }
}
 
