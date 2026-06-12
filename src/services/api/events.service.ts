import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, Errors, ServiceBroker } from "moleculer";
import BaseService from "../../base/base.service";
import { SERVICE } from "../../common";
import { subscribeBroadcaster } from "./subscribe_broadcaster";
import { listIndexerEvents, persistIndexerEventsForBlock } from "./indexer_events_query";

@Service({
  name: SERVICE.V1.IndexerEventsService.key,
  version: 1,
})
export default class IndexerEventsService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker);
  }

  @Action({
    name: "broadcastBlockIndexed",
    params: {
      height: { type: "number", integer: true, positive: true, convert: true },
      timestamp: { type: "string", optional: true, convert: true },
    },
  })
  public async broadcastBlockIndexed(ctx: Context<{ height: number; timestamp?: string }>) {
    const { height, timestamp } = ctx.params;
    const eventTimestamp = timestamp ? new Date(timestamp) : new Date();
    let events: Awaited<ReturnType<typeof persistIndexerEventsForBlock>> = [];

    try {
      events = await persistIndexerEventsForBlock(height);
    } catch (error) {
      this.logger.error("[IndexerEventsService] Error persisting indexer events:", error);
    }

    try {
      subscribeBroadcaster.broadcastBlockEnvelope({
        block: height,
        blockTime: eventTimestamp.toISOString(),
        events,
      });
    } catch (error) {
      this.logger.error("[IndexerEventsService] Error broadcasting block-indexed:", error);
      throw error;
    }

    return {
      success: true,
      clientsNotified: subscribeBroadcaster.getClientCount(),
      eventsNotified: events.length,
      height,
      timestamp: eventTimestamp.toISOString(),
    };
  }

  @Action({
    name: "listEvents",
    params: {
      did: { type: "string", trim: true, pattern: /^did:[a-z0-9]+:.+/i },
      after_block_height: { type: "number", integer: true, min: 0, optional: true, convert: true },
      limit: { type: "number", integer: true, min: 1, max: 500, optional: true, convert: true },
    },
  })
  public async listEvents(ctx: Context<{
    did: string;
    after_block_height?: number;
    limit?: number;
  }>) {
    const afterBlockHeight = ctx.params.after_block_height ?? 0;
    try {
      const events = await listIndexerEvents({
        afterBlockHeight,
        did: ctx.params.did,
        limit: ctx.params.limit,
      });
      return {
        events,
        count: events.length,
        after_block_height: afterBlockHeight,
      };
    } catch (error) {
      this.logger.error("[IndexerEventsService] Error listing indexer events:", error);
      throw new Errors.MoleculerError(
        "Failed to list indexer events",
        500,
        "INDEXER_EVENTS_QUERY_FAILED"
      );
    }
  }
}
