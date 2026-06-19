import type { IndexerEventRecord } from "./indexer_events_query";
import { BaseSubscribeServer, type ControlParseResult } from "./subscribe_ws_server";
import {
  buildBlockEnvelope,
  parseControlMessage,
  type ControlMessage,
} from "./subscribe_protocol";

type ClientState = {
  established: boolean;
  dids: Set<string> | null;
  corporationId: number | null;
};

export class SubscribeBroadcaster extends BaseSubscribeServer<ControlMessage, ClientState> {
  protected readonly path = "/v4/indexer/subscribe";

  protected createInitialState(): ClientState {
    return { established: false, dids: null, corporationId: null };
  }

  protected parseControl(raw: string): ControlParseResult<ControlMessage> {
    return parseControlMessage(raw);
  }

  protected applyControl(_state: ClientState, message: ControlMessage): ClientState {
    if (message.action === "unsubscribe") {
      return { established: false, dids: null, corporationId: null };
    }

    return {
      established: true,
      dids: message.dids === null ? null : new Set(message.dids),
      corporationId: message.corporationId,
    };
  }

  broadcastBlockEnvelope(args: {
    block: number;
    blockTime: string;
    events: IndexerEventRecord[];
  }): void {
    if (this.clients.size === 0) return;

    let sent = 0;
    this.clients.forEach((state, ws) => {
      if (!state.established) return;
      const envelope = buildBlockEnvelope(
        args.block,
        args.blockTime,
        args.events,
        state.dids,
        state.corporationId
      );
      if (this.sendJson(ws, envelope as unknown as Record<string, unknown>)) sent++;
    });

    if (sent > 0) {
      this.logger.info(
        `[SubscribeBroadcaster] Broadcasted block ${args.block} to ${sent} subscriber(s)`
      );
    }
  }
}

export const subscribeBroadcaster = new SubscribeBroadcaster();
