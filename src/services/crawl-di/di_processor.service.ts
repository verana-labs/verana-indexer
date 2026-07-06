import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended'
import { Context, ServiceBroker } from 'moleculer'
import BullableService from '../../base/bullable.service'
import { BULL_JOB_NAME, SERVICE } from '../../common'
import { CheckpointManager } from '../../common/utils/checkpoint_manager'
import knex from '../../common/utils/db_connection'
import {
  delay,
  getDbQueryTimeoutMs,
  isStatementTimeoutError,
  queryWithAutoRetry,
} from '../../common/utils/db_query_helper'
import { detectStartMode } from '../../common/utils/start_mode_detector'
import config from '../../config.json' with { type: 'json' }
import { Block } from '../../models'
import { BlockCheckpoint } from '../../models/block_checkpoint'
import { hasDigestEvents, runHeightSyncDI } from '../../modules/di-height-sync/di_height_sync_service'
import { indexerStatusManager } from '../manager/indexer_status.manager'

@Service({
  name: SERVICE.V1.DigestProcessorService.key,
  version: 1,
})
export default class DigestProcessorService extends BullableService {
  private timer: NodeJS.Timeout | null = null
  private checkpointManager: CheckpointManager
  private isProcessing = false

  public constructor(public broker: ServiceBroker) {
    super(broker)
    this.checkpointManager = new CheckpointManager(this.logger)
  }

  public async started() {
    try {
      await detectStartMode(BULL_JOB_NAME.HANDLE_DIGEST)
      await this.ensureCheckpoint()
      const crawlInterval = config.crawlDigest.millisecondCrawl

      this.processBlocks().catch((err) => {
        this.logger.error('[DigestProcessorService] Error in initial processBlocks:', err)
      })

      this.timer = setInterval(() => {
        if (!indexerStatusManager.isCrawlingActive()) return
        if (!this.isProcessing) {
          this.processBlocks().catch((err) => {
            this.logger.error('[DigestProcessorService] Error in scheduled processBlocks:', err)
          })
        }
      }, crawlInterval)

      this.logger.info(`[DigestProcessorService] Service started | Interval: ${crawlInterval}ms`)
    } catch (err) {
      this.logger.error('[DigestProcessorService] Failed to start', err)
    }
  }

  public async stopped() {
    if (this.timer) clearInterval(this.timer)
    this.logger.info('[DigestProcessorService] Service stopped')
  }

  private async ensureCheckpoint() {
    const jobName = BULL_JOB_NAME.HANDLE_DIGEST
    const [startBlock] = await BlockCheckpoint.getCheckpoint(jobName, [])
    return await this.checkpointManager.ensureCheckpoint(jobName, startBlock || 0)
  }

  @Action({ name: 'processBlocks' })
  public async processBlocks() {
    if (this.isProcessing) return
    if (!indexerStatusManager.isCrawlingActive()) return

    this.isProcessing = true
    try {
      const jobName = BULL_JOB_NAME.HANDLE_DIGEST
      const [startBlock] = await BlockCheckpoint.getCheckpoint(jobName, [])
      let lastHeight = startBlock || 0
      const maxBlockBatch = config.crawlDigest.chunkSize || 500
      const queryTimeoutMs = getDbQueryTimeoutMs()

      let hasMore = true
      while (hasMore) {
        if (!indexerStatusManager.isCrawlingActive()) break

        let nextBlocks: { height: number; block_result: any }[] = []
        const currentLastHeight = lastHeight
        try {
          nextBlocks = await queryWithAutoRetry(
            async () =>
              knex('block')
                .select('height', knex.raw("data->'block_result' as block_result"))
                .where('height', '>', currentLastHeight)
                .orderBy('height', 'asc')
                .limit(maxBlockBatch)
                .timeout(queryTimeoutMs),
            { timeout: queryTimeoutMs, retries: 3 },
            this.logger
          )
        } catch (queryError: any) {
          if (isStatementTimeoutError(queryError)) {
            await delay(5000)
            continue
          }
          this.logger.error('[DigestProcessorService] Error fetching blocks:', queryError)
          break
        }

        if (!nextBlocks.length) break

        for (const block of nextBlocks) {
          if (!indexerStatusManager.isCrawlingActive()) break
          await this.processBlockEventsInternal(block)
        }

        const newHeight = nextBlocks[nextBlocks.length - 1].height
        if (newHeight > lastHeight) {
          await this.checkpointManager.updateCheckpoint(jobName, newHeight, { logger: this.logger })
          lastHeight = newHeight
        }

        if (nextBlocks.length < maxBlockBatch) hasMore = false
      }
    } catch (error) {
      this.logger.error('[DigestProcessorService] Fatal error in processBlocks:', error)
    } finally {
      this.isProcessing = false
    }
  }

  @Action({ name: 'processBlockEvents' })
  public async processBlockEvents(ctx: Context<{ block: Block }>) {
    return this.processBlockEventsInternal(ctx.params.block)
  }

  private async processBlockEventsInternal(block: Block | { height: number; block_result: any }) {
    const blockResult = (block as any).block_result ?? (block as any).data?.block_result
    if (!blockResult) return

    const finalizeBlockEvents = blockResult?.finalize_block_events || []
    const endBlockEvents = blockResult?.end_block_events || []
    const txEvents = blockResult?.txs_results?.flatMap((tx: any) => tx?.events || []) || []
    const events = [...finalizeBlockEvents, ...txEvents, ...endBlockEvents]

    if (!hasDigestEvents(events)) return

    await runHeightSyncDI(this.broker, { events }, block.height)
  }
}
