import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, Errors, ServiceBroker } from "moleculer";
import BaseService from "../../base/base.service";
import { SERVICE } from "../../common";
import { eventsBroadcaster, isValidDid } from "./events_broadcaster";
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
    name: "broadcastBlockProcessed",
    params: {
      height: { type: "number", integer: true, positive: true, convert: true },
      timestamp: { type: "string", optional: true, convert: true },
    },
  })
  public async broadcastBlockProcessed(ctx: Context<{ height: number; timestamp?: string }>) {
    const { height, timestamp } = ctx.params;
    const eventTimestamp = timestamp ? new Date(timestamp) : new Date();
    
    try {
      const events = await persistIndexerEventsForBlock(height);
      events.forEach((event) => eventsBroadcaster.broadcastIndexerEvent(event));
      eventsBroadcaster.broadcastBlockProcessed(height, eventTimestamp);
      
      return {
        success: true,
        clientsNotified: eventsBroadcaster.getWSClientCount(),
        eventsNotified: events.length,
        height,
        timestamp: eventTimestamp.toISOString(),
      };
    } catch (error) {
      this.logger.error("[IndexerEventsService] Error broadcasting block processed:", error);
      throw error;
    }
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
    if (!isValidDid(ctx.params.did)) {
      throw new Errors.MoleculerClientError("Invalid did query parameter", 400, "INVALID_DID");
    }

    const afterBlockHeight = ctx.params.after_block_height ?? 0;
    const events = await listIndexerEvents({
      afterBlockHeight,
      did: ctx.params.did,
      limit: ctx.params.limit,
    });
    return {
      events,
      count: events.length,
      afterBlockHeight,
    };
  }
}
