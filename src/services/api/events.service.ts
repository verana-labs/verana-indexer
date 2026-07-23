import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended'
import { Context, Errors, ServiceBroker } from 'moleculer'
import BaseService from '../../base/base.service'
import { SERVICE } from '../../common'
import knex from '../../common/utils/db_connection'
import { parseCsvList } from './indexer_event_utils'
import { listIndexerEvents, persistIndexerEventsForBlock } from './indexer_events_query'
import { subscribeBroadcaster } from './subscribe_broadcaster'

@Service({
  name: SERVICE.V1.IndexerEventsService.key,
  version: 1,
})
export default class IndexerEventsService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker)
  }

  @Action({
    name: 'broadcastBlockIndexed',
    params: {
      height: { type: 'number', integer: true, positive: true, convert: true },
      timestamp: { type: 'string', optional: true, convert: true },
    },
  })
  public async broadcastBlockIndexed(ctx: Context<{ height: number; timestamp?: string }>) {
    const { height, timestamp } = ctx.params
    const eventTimestamp = timestamp ? new Date(timestamp) : new Date()
    let events: Awaited<ReturnType<typeof persistIndexerEventsForBlock>> = []

    try {
      events = await persistIndexerEventsForBlock(height)
    } catch (error) {
      this.logger.error('[IndexerEventsService] Error persisting indexer events:', error)
    }

    try {
      subscribeBroadcaster.broadcastBlockEnvelope({
        block: height,
        blockTime: eventTimestamp.toISOString(),
        events,
      })
    } catch (error) {
      this.logger.error('[IndexerEventsService] Error broadcasting block-indexed:', error)
      throw error
    }

    return {
      success: true,
      clientsNotified: subscribeBroadcaster.getClientCount(),
      eventsNotified: events.length,
      height,
      timestamp: eventTimestamp.toISOString(),
    }
  }

  @Action({
    name: 'broadcastEmptyBlocks',
    params: {
      fromHeight: { type: 'number', integer: true, positive: true, convert: true },
      toHeight: { type: 'number', integer: true, positive: true, convert: true },
    },
  })
  public async broadcastEmptyBlocks(ctx: Context<{ fromHeight: number; toHeight: number }>) {
    const { fromHeight, toHeight } = ctx.params
    if (toHeight < fromHeight) return { success: true, blocksNotified: 0 }
    if (subscribeBroadcaster.getClientCount() === 0) return { success: true, blocksNotified: 0 }

    let blockTimeByHeight = new Map<number, string>()
    try {
      const rows = (await knex('block')
        .select('height', 'time')
        .where('height', '>=', fromHeight)
        .andWhere('height', '<=', toHeight)) as Array<{ height: number; time: Date | string }>
      blockTimeByHeight = new Map(
        rows.map((row) => [Number(row.height), new Date(row.time).toISOString()] as [number, string])
      )
    } catch (error) {
      this.logger.warn('[IndexerEventsService] Could not load block times for empty-block range:', error)
    }

    const fallbackTime = new Date().toISOString()
    let blocksNotified = 0
    for (let height = fromHeight; height <= toHeight; height++) {
      subscribeBroadcaster.broadcastBlockEnvelope({
        block: height,
        blockTime: blockTimeByHeight.get(height) ?? fallbackTime,
        events: [],
      })
      blocksNotified++
    }

    return { success: true, blocksNotified }
  }

  @Action({
    name: 'listEvents',
    params: {
      dids: { type: 'string', trim: true, optional: true },
      corporation_id: { type: 'number', integer: true, positive: true, optional: true, convert: true },
      after_block_height: { type: 'number', integer: true, min: 0, optional: true, convert: true },
      limit: { type: 'number', integer: true, min: 1, max: 500, optional: true, convert: true },
    },
  })
  public async listEvents(
    ctx: Context<{
      dids?: string
      corporation_id?: number
      after_block_height?: number
      limit?: number
    }>
  ) {
    const afterBlockHeight = ctx.params.after_block_height ?? 0
    try {
      const events = await listIndexerEvents({
        afterBlockHeight,
        dids: parseCsvList(ctx.params.dids),
        corporationId: ctx.params.corporation_id,
        limit: ctx.params.limit,
      })
      return {
        events,
        count: events.length,
        after_block_height: afterBlockHeight,
      }
    } catch (error) {
      this.logger.error('[IndexerEventsService] Error listing indexer events:', error)
      throw new Errors.MoleculerError('Failed to list indexer events', 500, 'INDEXER_EVENTS_QUERY_FAILED')
    }
  }
}
