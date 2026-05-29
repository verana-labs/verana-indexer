import { Server } from "http";
import { WebSocket, WebSocketServer } from "ws";
import { indexerStatusManager } from "../manager/indexer_status.manager";
import type { IndexerEventRecord } from "./indexer_events_query";
import { createLogger, toIsoSeconds, type LoggerLike } from "./api_shared";
import {
  buildBlockEnvelope,
  buildReadyMessage,
  parseControlMessage,
} from "./subscribe_protocol";

type ClientState = {
  established: boolean;
  dids: Set<string> | null;
};

export class SubscribeBroadcaster {
  private wss: WebSocketServer | null = null;
  private clients: Map<WebSocket, ClientState> = new Map();
  private clientAlive: Map<WebSocket, boolean> = new Map();
  private pingInterval: NodeJS.Timeout | null = null;
  private readonly MAX_CLIENTS = 10000; // TODO: Review this values
  private readonly PING_INTERVAL = 30000;
  private readonly MAX_CONTROL_MESSAGE_BYTES = 64 * 1024;
  private logger = createLogger(console);

  setLogger(logger: LoggerLike): void {
    this.logger = createLogger(logger);
  }

  initialize(server: Server): void {
    if (this.wss) {
      this.logger.warn("[SubscribeBroadcaster] WebSocket server already initialized");
      return;
    }

    this.wss = new WebSocketServer({
      server,
      path: "/verana/indexer/v1/subscribe",
      perMessageDeflate: false,
      maxPayload: this.MAX_CONTROL_MESSAGE_BYTES,
      verifyClient: () => {
        if (this.clients.size >= this.MAX_CLIENTS) {
          this.logger.warn(
            `[SubscribeBroadcaster] Max clients reached (${this.MAX_CLIENTS}), rejecting connection`
          );
          return false;
        }
        return true;
      },
    });

    this.wss.on("connection", async (ws: WebSocket) => {
      if (this.clients.size >= this.MAX_CLIENTS) {
        ws.close(1013, "Server overloaded");
        return;
      }

      this.clients.set(ws, { established: false, dids: null });
      this.clientAlive.set(ws, true);

      this.logger.info(
        `[SubscribeBroadcaster] New client connected. Total clients: ${this.clients.size}`
      );

      ws.on("close", () => this.cleanupClient(ws));
      ws.on("error", (error) => {
        this.logger.error("[SubscribeBroadcaster] WebSocket error:", error);
        this.cleanupClient(ws);
      });
      ws.on("pong", () => {
        this.clientAlive.set(ws, true);
      });
      ws.on("message", (raw) => this.handleControlMessage(ws, raw.toString()));

      try {
        await this.sendReady(ws);
      } catch (error) {
        this.logger.error("[SubscribeBroadcaster] Error sending ready message:", error);
        this.cleanupClient(ws);
      }
    });

    this.wss.on("error", (error) => {
      this.logger.error("[SubscribeBroadcaster] WebSocketServer error:", error);
    });

    this.pingInterval = setInterval(() => {
      this.pingClients();
    }, this.PING_INTERVAL);

    this.logger.info(
      "[SubscribeBroadcaster] WebSocket server initialized on /verana/indexer/v1/subscribe"
    );
  }

  private async sendReady(ws: WebSocket): Promise<void> {
    const status = await indexerStatusManager.getDetailedStatus();
    const lastProcessedBlock = status.lastProcessedBlock ?? 0;
    const lastBlockTime = status.lastBlockTime ?? toIsoSeconds();
    const ready = buildReadyMessage(lastProcessedBlock, lastBlockTime);
    this.sendJson(ws, ready as unknown as Record<string, unknown>);
  }

  private handleControlMessage(ws: WebSocket, raw: string): void {
    const result = parseControlMessage(raw);
    if (!result.ok) {
      this.logger.warn(`[SubscribeBroadcaster] Invalid control message: ${result.error}`);
      try {
        ws.close(1008, result.error);
      } catch {
        // best effort close
      }
      this.cleanupClient(ws);
      return;
    }

    const state = this.clients.get(ws);
    if (!state) return;

    if (result.message.action === "unsubscribe") {
      state.established = false;
      state.dids = null;
      return;
    }

    state.established = true;
    state.dids = result.message.dids === null ? null : new Set(result.message.dids);
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
      const envelope = buildBlockEnvelope(args.block, args.blockTime, args.events, state.dids);
      if (this.sendJson(ws, envelope as unknown as Record<string, unknown>)) sent++;
    });

    if (sent > 0) {
      this.logger.info(
        `[SubscribeBroadcaster] Broadcasted block ${args.block} to ${sent} subscriber(s)`
      );
    }
  }

  private sendJson(ws: WebSocket, payload: Record<string, unknown>): boolean {
    if (ws.readyState !== WebSocket.OPEN) {
      this.cleanupClient(ws);
      return false;
    }
    try {
      ws.send(JSON.stringify(payload), { compress: false });
      return true;
    } catch {
      this.cleanupClient(ws);
      return false;
    }
  }

  private cleanupClient(ws: WebSocket): void {
    const had = this.clients.delete(ws);
    this.clientAlive.delete(ws);
    if (had) {
      this.logger.info(
        `[SubscribeBroadcaster] Client disconnected. Total clients: ${this.clients.size}`
      );
    }
  }

  private pingClients(): void {
    const stale: WebSocket[] = [];
    this.clients.forEach((_, ws) => {
      const alive = this.clientAlive.get(ws);
      if (alive === false) {
        stale.push(ws);
        return;
      }
      this.clientAlive.set(ws, false);
      try {
        ws.ping();
      } catch {
        stale.push(ws);
      }
    });
    stale.forEach((ws) => {
      try {
        ws.terminate();
      } catch {
        // best effort
      }
      this.cleanupClient(ws);
    });
  }

  getClientCount(): number {
    return this.clients.size;
  }

  close(): void {
    if (this.pingInterval) {
      clearInterval(this.pingInterval);
      this.pingInterval = null;
    }
    this.clients.forEach((_, ws) => {
      try {
        if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
          ws.close();
        } else {
          ws.terminate();
        }
      } catch {
        // best effort
      }
    });
    this.clients.clear();
    this.clientAlive.clear();

    if (this.wss) {
      this.wss.close(() => {
        this.logger.info("[SubscribeBroadcaster] WebSocket server closed");
      });
      this.wss = null;
    }
  }
}

export const subscribeBroadcaster = new SubscribeBroadcaster();
