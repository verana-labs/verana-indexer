import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended';
import { ServiceBroker } from 'moleculer';
import config from '../../config.json' with { type: 'json' };
import BullableService from '../../base/bullable.service';
import { BULL_JOB_NAME, SERVICE, TrustDepositEventType } from '../../common';
import knex from '../../common/utils/db_connection';
import { Block } from '../../models';
import { BlockCheckpoint } from '../../models/block_checkpoint';
import { formatTimestamp } from '../../common/utils/date_utils';

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

  public constructor(public broker: ServiceBroker) {
    super(broker);
  }

  public async started() {
    try {
      try {
        const blockCountResult = await knex('block').count('* as count').first();
        const totalBlocks = blockCountResult ? parseInt(String((blockCountResult as { count: string | number }).count), 10) : 0;
        const checkpoint = await BlockCheckpoint.query().findOne({
          job_name: (BULL_JOB_NAME as any).HANDLE_TRUST_DEPOSIT,
        });
        const currentBlock = checkpoint ? checkpoint.height : 0;
        this._isFreshStart = totalBlocks < 100 && currentBlock < 1000;
      } catch (error) {
        this.logger.warn(` Could not determine start mode: ${error}. Defaulting to reindexing mode.`);
        this._isFreshStart = false;
      }

      const checkpoint = await this.ensureCheckpoint();
      await this.processBlocks(checkpoint);

      const crawlInterval = (this._isFreshStart && config.crawlTrustDeposit.freshStart)
        ? (config.crawlTrustDeposit.freshStart.millisecondCrawl || config.crawlTrustDeposit.millisecondCrawl)
        : config.crawlTrustDeposit.millisecondCrawl;

      this.timer = setInterval(
        () => this.processBlocks(checkpoint),
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
    let checkpoint = await BlockCheckpoint.query(knex).findOne({ job_name: jobName });

    if (!checkpoint) {
      checkpoint = await BlockCheckpoint.query(knex).insertAndFetch({
        job_name: jobName,
        height: 0,
      });
      this.logger.info(`[CrawlTrustDepositService] 🆕 Created checkpoint for ${jobName}`);
    }
    return checkpoint;
  }

  private async updateCheckpoint(jobKey: string, newHeight: number, blockCheckpointRow?: any) {
    const patchObj: any = { height: newHeight, updated_at: new Date().toISOString() };
    try {
      const where: any = {};
      where[this.checkpointColumn!] = jobKey;

      const updated = await BlockCheckpoint.query(knex).patch(patchObj).where(where);
      if (updated) {
      } else if (blockCheckpointRow?.id) {
        await BlockCheckpoint.query(knex).patch(patchObj).where('id', blockCheckpointRow.id);
        this.logger.info(`[CrawlTrustDepositService] ✅ Patched checkpoint by id ${blockCheckpointRow.id}`);
      } else {
        this.logger.warn(`[CrawlTrustDepositService] ⚠️ No checkpoint row matched for ${jobKey}`);
      }
    } catch (err) {
      this.logger.error('[CrawlTrustDepositService] ❌ Error updating checkpoint:', err);
    }
  }

  @Action({ name: 'processBlocks' })
  public async processBlocks(blockCheckpointRow?: any) {
    const jobName = (BULL_JOB_NAME as any).HANDLE_TRUST_DEPOSIT;
    const [startBlock] = await BlockCheckpoint.getCheckpoint(jobName, []);
    let lastHeight = startBlock || 0;
    
    const maxBlockBatch = (this._isFreshStart && config.crawlTrustDeposit.freshStart)
      ? (config.crawlTrustDeposit.freshStart.chunkSize || config.crawlTrustDeposit.chunkSize || 100)
      : (config.crawlTrustDeposit.chunkSize || 100);

    while (true) {
      const nextBlocks = await Block.query()
        .where('height', '>', lastHeight)
        .orderBy('height', 'asc')
        .limit(maxBlockBatch);
      if (!nextBlocks.length) break;

      try {
        for (const block of nextBlocks) {
          await this.processBlockEvents(block);
        }

        const newHeight = nextBlocks[nextBlocks.length - 1].height;
        await this.updateCheckpoint(jobName, newHeight, blockCheckpointRow);
        lastHeight = newHeight;

        if (nextBlocks.length < maxBlockBatch) break;
      } catch (err) {
        this.logger.error(`[CrawlTrustDepositService] ❌ Error processing blocks:`, err);
        break;
      }
    }
  }

  private async processBlockEvents(block: Block) {
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
      this.logger.info(`[CrawlTrustDepositService] 📈 Found ${adjustEvents.length} adjust events`);
      for (const event of adjustEvents) {
        await this.handleAdjustTrustDepositEvent(block.height, event);
      }
    }

    if (slashEvents.length) {
      this.logger.info(`[CrawlTrustDepositService] ⚠️ Found ${slashEvents.length} slash events`);
      for (const event of slashEvents) {
        await this.handleSlashTrustDepositEvent(block.height, event);
      }
    }
  }

  private async handleAdjustTrustDepositEvent(height: number, event: any) {
    try {
      const attrs: Record<string, string> = {};
      for (const attr of event.attributes) attrs[attr.key] = attr.value;

      const account = attrs.account;
      if (!account) {
        this.logger.warn(`[AdjustEvent] ⚠️ Missing account attribute at height ${height}`);
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
        this.logger.info(`[AdjustEvent] ✅ Processed adjust event for account ${account} at height ${height}`);
      } else {
        this.logger.warn(`[AdjustEvent] ⚠️ Failed processing for account ${account}: ${result || 'Unknown error'}`);
      }
    } catch (err) {
      this.logger.error('[AdjustEvent] ❌ Error processing adjust event:', err);
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

      this.logger.warn(`[SlashEvent] ⚔️ Account ${account} was slashed ${slashed} at height ${height}`);

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
