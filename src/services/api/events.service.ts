import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BaseService from "../../base/base.service";
import { SERVICE } from "../../common";
import { eventsBroadcaster } from "./events_broadcaster";

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
      eventsBroadcaster.broadcastBlockProcessed(height, eventTimestamp);
      
      return {
        success: true,
        clientsNotified: eventsBroadcaster.getWSClientCount(),
        height,
        timestamp: eventTimestamp.toISOString(),
      };
    } catch (error) {
      this.logger.error("[IndexerEventsService] Error broadcasting block processed:", error);
      throw error;
    }
  }
}