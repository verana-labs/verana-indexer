import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended';
import { Context, ServiceBroker } from 'moleculer';
import config from '../../config.json' with { type: 'json' };
import BullableService from '../../base/bullable.service';
import { BULL_JOB_NAME, SERVICE, TrustDepositEventType } from '../../common';
import { Block } from '../../models';
import { BlockCheckpoint } from '../../models/block_checkpoint';
import { formatTimestamp } from '../../common/utils/date_utils';
import { detectStartMode } from '../../common/utils/start_mode_detector';
import { CheckpointManager } from '../../common/utils/checkpoint_manager';
import { BatchProcessor } from '../../common/utils/batch_processor';
import { queryWithAutoRetry, delay, isStatementTimeoutError } from '../../common/utils/db_query_helper';

interface TrustDepositAdjustPayload {
  account: string;
  augend?: bigint;
  adjustmentType?: string;
  newAmount: bigint | null;
  newShare: bigint | null;
  newClaimable: bigint | null;
  newSlashed?: bigint | null;
  newRepaid?: bigint | null;
}

@Service({
  name: SERVICE.V1.CrawlTrustDepositService.key,
  version: 1,
})
export default class CrawlTrustDepositService extends BullableService {
  private timer: NodeJS.Timeout | null = null;
  private checkpointColumn: 'job_name' | null = 'job_name';
  private checkpointManager: CheckpointManager;
  private batchProcessor: BatchProcessor;
  private isProcessing: boolean = false;

  public constructor(public broker: ServiceBroker) {
    super(broker);
    this.checkpointManager = new CheckpointManager(this.logger);
    this.batchProcessor = new BatchProcessor(this.logger);
  }

  public async started() {
    try {
      const startMode = await detectStartMode((BULL_JOB_NAME as any).HANDLE_TRUST_DEPOSIT);
      this._isFreshStart = startMode.isFreshStart;

      const checkpoint = await this.ensureCheckpoint();
      
      const crawlInterval = (this._isFreshStart && config.crawlTrustDeposit.freshStart)
        ? (config.crawlTrustDeposit.freshStart.millisecondCrawl || config.crawlTrustDeposit.millisecondCrawl)
        : config.crawlTrustDeposit.millisecondCrawl;

      this.processBlocks(checkpoint, true).catch((err) => {
        this.logger.error('[CrawlTrustDepositService] Error in initial processBlocks:', err);
      });

      this.timer = setInterval(
        () => {
          if (!this.isProcessing) {
            this.processBlocks(checkpoint, false).catch((err) => {
              this.logger.error('[CrawlTrustDepositService] Error in scheduled processBlocks:', err);
            });
          }
        },
        crawlInterval,
      );
      this.logger.info(
        `[CrawlTrustDepositService] Service started | Mode: ${this._isFreshStart ? 'Fresh Start' : 'Reindexing'} | Interval: ${crawlInterval}ms`
      );
    } catch (err) {
      this.logger.error('[CrawlTrustDepositService] ❌ Failed to start', err);
    }
  }

  public async stopped() {
    if (this.timer) clearInterval(this.timer);
    this.logger.info('[CrawlTrustDepositService] ⏹️ Service stopped');
  }

  private async ensureCheckpoint() {
    const jobName = (BULL_JOB_NAME as any).HANDLE_TRUST_DEPOSIT;
    const [startBlock] = await BlockCheckpoint.getCheckpoint(jobName, []);
    return await this.checkpointManager.ensureCheckpoint(jobName, startBlock || 0);
  }

  private async updateCheckpoint(jobKey: string, newHeight: number, blockCheckpointRow?: any) {
    await this.checkpointManager.updateCheckpoint(jobKey, newHeight, {
      logger: this.logger
    });
  }

  @Action({ name: 'processBlocks' })
  public async processBlocks(blockCheckpointRow?: any, runInfiniteLoop: boolean = false) {
    if (this.isProcessing) {
      this.logger.debug('[CrawlTrustDepositService] Already processing, skipping...');
      return;
    }

    this.isProcessing = true;
    try {
      const jobName = (BULL_JOB_NAME as any).HANDLE_TRUST_DEPOSIT;
      const [startBlock] = await BlockCheckpoint.getCheckpoint(jobName, []);
      let lastHeight = startBlock || 0;
      
      const maxBlockBatch = (this._isFreshStart && config.crawlTrustDeposit.freshStart)
        ? (config.crawlTrustDeposit.freshStart.chunkSize || config.crawlTrustDeposit.chunkSize || 100)
        : (config.crawlTrustDeposit.chunkSize || 100);

      do {
      let nextBlocks: Block[] = [];
      const currentLastHeight = lastHeight;
      try {
        nextBlocks = await queryWithAutoRetry(
          async () => {
            const result = await Block.query()
              .where('height', '>', currentLastHeight)
              .orderBy('height', 'asc')
              .limit(maxBlockBatch)
              .timeout(30000);
            return result;
          },
          { timeout: 30000, retries: 3 },
          this.logger
        );
      } catch (queryError: any) {
        if (isStatementTimeoutError(queryError)) {
          this.logger.warn(`[CrawlTrustDepositService] Query timeout, waiting before retry...`);
          await delay(5000);
          continue;
        }
        this.logger.error(`[CrawlTrustDepositService] Error fetching blocks:`, queryError);
        break;
      }
      
      if (!nextBlocks.length) break;

      const processDelay = this._isFreshStart ? 2000 : 500;
      
      try {
        for (const block of nextBlocks) {
          await this.processBlockEventsInternal(block);
          
          if (this._isFreshStart) {
          await delay(processDelay);
          }
        }

        const newHeight = nextBlocks[nextBlocks.length - 1].height;
        if (newHeight > lastHeight) {
          await this.updateCheckpoint(jobName, newHeight, blockCheckpointRow);
          lastHeight = newHeight;
          this.logger.info(`[CrawlTrustDepositService] Processed blocks up to height ${newHeight}, checkpoint updated`);
        }

        if (nextBlocks.length < maxBlockBatch) break;
        
        if (!this._isFreshStart) {
          await delay(100);
        }
      } catch (err: any) {
        const errorCode = err?.code;
        const errorMessage = err?.message || String(err);
        
        if (isStatementTimeoutError(err)) {
          this.logger.warn(`[CrawlTrustDepositService] Query timeout, waiting before retry...`);
          await delay(5000);
          continue;
        }
        
        this.logger.error(`[CrawlTrustDepositService] Error processing blocks:`, err);
        await delay(5000);
        break;
      }
      } while (runInfiniteLoop);
      
      this.isProcessing = false;
    } catch (error) {
      this.isProcessing = false;
      this.logger.error('[CrawlTrustDepositService] Fatal error in processBlocks:', error);
      throw error;
    }
  }

  @Action({ name: 'processBlockEvents' })
  public async processBlockEvents(ctx: Context<{ block: Block }>) {
    const { block } = ctx.params;
    return await this.processBlockEventsInternal(block);
  }

  private async processBlockEventsInternal(block: Block) {
    const blockResult = block.data?.block_result;
    if (!blockResult) return;

    const finalizeBlockEvents = blockResult?.finalize_block_events || [];
    const endBlockEvents = blockResult?.end_block_events || [];
    const txEvents = blockResult?.txs_results?.flatMap((tx: any) => tx?.events || []) || [];
    const events = [...finalizeBlockEvents, ...txEvents, ...endBlockEvents];
    if (!events.length) return;

    const eventTypes = [TrustDepositEventType.ADJUST, TrustDepositEventType.SLASH];
    const filteredEvents = events.filter((e: any) => eventTypes.includes(e.type));

    const adjustEvents = filteredEvents.filter((e: any) => e.type === TrustDepositEventType.ADJUST);
    const slashEvents = filteredEvents.filter((e: any) => e.type === TrustDepositEventType.SLASH);

    if (adjustEvents.length) {
      this.logger.info(`[CrawlTrustDepositService] Found ${adjustEvents.length} adjust events`);
      
      await this.batchProcessor.processInBatches(
        adjustEvents,
        async (event: any) => {
          await this.handleAdjustTrustDepositEvent(block.height, event);
        },
        {
          maxConcurrent: this._isFreshStart ? 2 : 3,
          batchSize: this._isFreshStart ? 3 : 5,
          delayBetweenBatches: this._isFreshStart ? 2000 : 1000,
          logger: this.logger
        }
      );
    }

    if (slashEvents.length) {
      this.logger.info(`[CrawlTrustDepositService] Found ${slashEvents.length} slash events`);
      
      await this.batchProcessor.processInBatches(
        slashEvents,
        async (event: any) => {
          await this.handleSlashTrustDepositEvent(block.height, event);
        },
        {
          maxConcurrent: this._isFreshStart ? 1 : 2,
          batchSize: this._isFreshStart ? 2 : 3,
          delayBetweenBatches: this._isFreshStart ? 2000 : 1000,
          logger: this.logger
        }
      );
    }
  }

  private async handleAdjustTrustDepositEvent(height: number, event: any) {
    try {
      const attrs: Record<string, string> = {};
      for (const attr of event.attributes) attrs[attr.key] = attr.value;

      const account = attrs.account;
      if (!account) {
        this.logger.warn(`[AdjustEvent] Missing account attribute at height ${height}`);
        return;
      }

      const payload: TrustDepositAdjustPayload & { height: number } = {
        account,
        newAmount: attrs.new_amount ? BigInt(attrs.new_amount.split('.')[0]) : null,
        newShare: attrs.new_share ? BigInt(attrs.new_share.split('.')[0]) : null,
        newClaimable: attrs.new_claimable ? BigInt(attrs.new_claimable.split('.')[0]) : null,
        height,
      };

      const result = await this.broker.call(
        `${SERVICE.V1.TrustDepositDatabaseService.path}.adjustTrustDeposit`,
        payload
      );

      if (result) {
        this.logger.info(`[AdjustEvent] Processed adjust event for account ${account} at height ${height}`);
      } else {
        this.logger.warn(`[AdjustEvent] Failed processing for account ${account}: ${result || 'Unknown error'}`);
      }
    } catch (err) {
      this.logger.error('[AdjustEvent] Error processing adjust event:', err);
    }
  }

  private async handleSlashTrustDepositEvent(height: number, event: any) {
    try {
      const attrs: Record<string, string> = {};
      for (const attr of event.attributes) attrs[attr.key] = attr.value;

      const account = attrs.account;
      const lastSlashed = formatTimestamp(attrs.timestamp);
      const slashCount = attrs.slash_count ? parseInt(attrs.slash_count) : 0;
      const slashed = BigInt(attrs.amount || '0');

      this.logger.warn(`[SlashEvent] Account ${account} was slashed ${slashed} at height ${height}`);

      const result = await this.broker.call(
        `${SERVICE.V1.TrustDepositDatabaseService.path}.slash_trust_deposit`,
        { account, slashed: slashed.toString(), lastSlashed, slashCount, height }
      );

      if (result) {
        this.logger.info(`[SlashEvent] ✅ Slash recorded for ${account}`);
      } else {
        this.logger.warn(`[SlashEvent] ⚠️ Slash failed for ${account}: ${result || 'Unknown error'}`);
      }
    } catch (err) {
      this.logger.error('[SlashEvent] ❌ Error processing slash event:', err);
    }
  }
}
