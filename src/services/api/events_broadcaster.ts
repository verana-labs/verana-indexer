import { IncomingMessage, Server } from "http";
import { WebSocket, WebSocketServer } from "ws";
import { indexerStatusManager } from "../manager/indexer_status.manager";
import type { IndexerEventRecord } from "./indexer_events_query";

type Logger = {
  info?: (...args: any[]) => void;
  warn?: (...args: any[]) => void;
  error?: (...args: any[]) => void;
};

type ClientDidParseResult = {
  did?: string;
  invalidDid?: string;
};

type IndexerStatusMessage = {
  indexerStatus: "running" | "stopped";
  crawlingStatus: "active" | "stopped";
  stoppedAt?: string;
  stoppedReason?: string;
  lastError?: {
    message: string;
    timestamp: string;
    service?: string;
  };
};

function isUnknownMessageError(errorMessage: string): boolean {
  if (!errorMessage) return false;
  return errorMessage.includes("Unknown Verana message types") ||
    errorMessage.includes("UNKNOWN VERANA MESSAGE TYPES");
}

export function isValidDid(value: unknown): value is string {
  return typeof value === "string" && /^did:[a-z0-9]+:.+/i.test(value.trim());
}

function toIsoSeconds(value: Date = new Date()): string {
  return value.toISOString().replace(/\.\d{3}Z$/, "Z");
}

export class EventsBroadcaster {
  private wss: WebSocketServer | null = null;
  private wsClients: Set<WebSocket> = new Set();
  private clientAlive: Map<WebSocket, boolean> = new Map();
  private clientDids: Map<WebSocket, string> = new Map();
  private didRooms: Map<string, Set<WebSocket>> = new Map();
  private pingInterval: NodeJS.Timeout | null = null;
  private readonly MAX_CLIENTS = 10000;
  private readonly PING_INTERVAL = 30000;
  private logger: Logger = console;

  setLogger(logger: Logger): void {
    this.logger = logger;
  }

  private logInfo(...args: any[]): void {
    if (this.logger.info) this.logger.info(...args);
    else console.log(...args);
  }

  private logWarn(...args: any[]): void {
    if (this.logger.warn) this.logger.warn(...args);
    else console.warn(...args);
  }

  private logError(...args: any[]): void {
    if (this.logger.error) this.logger.error(...args);
    else console.error(...args);
  }

  private parseClientDid(req: IncomingMessage): ClientDidParseResult {
    const host = req.headers.host || "localhost";
    const url = new URL(req.url || "/verana/indexer/v1/events", `http://${host}`);
    const did = (url.searchParams.get("did") || "").trim();
    if (!did) return {};
    return isValidDid(did) ? { did } : { invalidDid: did };
  }

  private addToRoom(ws: WebSocket, did: string): void {
    let room = this.didRooms.get(did);
    if (!room) {
      room = new Set<WebSocket>();
      this.didRooms.set(did, room);
    }
    room.add(ws);
  }

  private removeFromRoom(ws: WebSocket, did: string): void {
    const room = this.didRooms.get(did);
    if (!room) return;
    room.delete(ws);
    if (room.size === 0) this.didRooms.delete(did);
  }

  private cleanupClient(ws: WebSocket): void {
    const did = this.clientDids.get(ws);
    if (did) this.removeFromRoom(ws, did);

    const hadClient = this.wsClients.delete(ws);
    this.clientAlive.delete(ws);
    this.clientDids.delete(ws);

    if (hadClient) {
      this.logInfo(`[EventsBroadcaster] WebSocket client disconnected. Total clients: ${this.wsClients.size}`);
    }
  }

  private sendJson(ws: WebSocket, eventData: Record<string, unknown>): boolean {
    if (ws.readyState !== WebSocket.OPEN) {
      this.cleanupClient(ws);
      return false;
    }

    try {
      ws.send(JSON.stringify(eventData), { compress: false });
      return true;
    } catch {
      this.cleanupClient(ws);
      return false;
    }
  }

  private getCurrentIndexerStatus(): IndexerStatusMessage {
    const status = indexerStatusManager.getStatus();
    return {
      indexerStatus: status.isRunning ? "running" : "stopped",
      crawlingStatus: status.isCrawling ? "active" : "stopped",
      stoppedAt: status.stoppedAt,
      stoppedReason: status.stoppedReason,
      lastError: status.lastError,
    };
  }

  initialize(server: Server): void {
    indexerStatusManager.setStatusChangeCallback((status) => {
      this.broadcastIndexerStatus(status);
    });

    if (this.wss) {
      this.logWarn("[EventsBroadcaster] WebSocket server already initialized");
      return;
    }

    this.wss = new WebSocketServer({
      server,
      path: "/verana/indexer/v1/events",
      perMessageDeflate: false,
      maxPayload: 1024,
      verifyClient: () => {
        if (this.wsClients.size >= this.MAX_CLIENTS) {
          this.logWarn(`[EventsBroadcaster] Max clients reached (${this.MAX_CLIENTS}), rejecting connection`);
          return false;
        }
        return true;
      },
    });

    this.wss.on("connection", async (ws: WebSocket, req: IncomingMessage) => {
      if (this.wsClients.size >= this.MAX_CLIENTS) {
        ws.close(1013, "Server overloaded");
        return;
      }

      const { did, invalidDid } = this.parseClientDid(req);
      if (invalidDid) {
        ws.close(1008, "Invalid did query parameter");
        return;
      }

      this.wsClients.add(ws);
      this.clientAlive.set(ws, true);
      if (did) {
        this.clientDids.set(ws, did);
        this.addToRoom(ws, did);
      }

      this.logInfo(`[EventsBroadcaster] New WebSocket client connected. Total clients: ${this.wsClients.size}`);

      ws.on("close", () => this.cleanupClient(ws));
      ws.on("error", (error) => {
        this.logError("[EventsBroadcaster] WebSocket error:", error);
        this.cleanupClient(ws);
      });
      ws.on("pong", () => {
        this.clientAlive.set(ws, true);
      });

      try {
        const [status, detailedStatus] = await Promise.all([
          Promise.resolve(this.getCurrentIndexerStatus()),
          indexerStatusManager.getDetailedStatus().catch(() => null),
        ]);

        const connectionMessage: Record<string, unknown> = {
          type: "connected",
          message: "Connected to Verana Indexer Events",
          indexerStatus: status.indexerStatus,
          crawlingStatus: status.crawlingStatus,
          blockHeight: detailedStatus?.lastProcessedBlock ?? null,
          timestamp: toIsoSeconds(),
        };
        if (did) connectionMessage.did = did;

        if (status.crawlingStatus === "stopped") {
          if (status.stoppedAt) connectionMessage.stoppedAt = status.stoppedAt;

          const errorMessage = status.lastError?.message || status.stoppedReason || "";
          if (isUnknownMessageError(errorMessage)) {
            if (status.stoppedReason) connectionMessage.stoppedReason = status.stoppedReason;
            if (status.lastError) {
              connectionMessage.lastError = {
                message: status.lastError.message,
                timestamp: status.lastError.timestamp,
                service: status.lastError.service,
              };
            }
          }
        }

        this.sendJson(ws, connectionMessage);
      } catch (error) {
        this.logError("[EventsBroadcaster] Error sending welcome message:", error);
        this.cleanupClient(ws);
      }
    });

    this.wss.on("error", (error) => {
      this.logError("[EventsBroadcaster] WebSocketServer error:", error);
    });

    this.pingInterval = setInterval(() => {
      this.pingClients();
    }, this.PING_INTERVAL);

    this.logInfo("[EventsBroadcaster] WebSocket server initialized on /verana/indexer/v1/events");
  }

  private pingClients(): void {
    const staleClients: WebSocket[] = [];

    this.wsClients.forEach((ws) => {
      const isAlive = this.clientAlive.get(ws);
      if (isAlive === false) {
        staleClients.push(ws);
        return;
      }

      this.clientAlive.set(ws, false);
      try {
        ws.ping();
      } catch {
        staleClients.push(ws);
      }
    });

    staleClients.forEach((ws) => {
      try {
        ws.terminate();
      } catch {
        // Best effort cleanup.
      }
      this.cleanupClient(ws);
    });
  }

  broadcastIndexerEvent(event: IndexerEventRecord): void {
    this.broadcastToDid(event.did, event as unknown as Record<string, unknown>);
  }

  broadcastBlockProcessed(height: number, timestamp: Date | string): void {
    const eventTimestamp = timestamp instanceof Date ? timestamp : new Date(timestamp);
    this.broadcastToGlobalClients({
      type: "block-processed",
      height,
      timestamp: toIsoSeconds(eventTimestamp),
    });
  }

  broadcastIndexerStatus(status: IndexerStatusMessage): void {
    if (this.wsClients.size === 0) return;

    const errorMessage = status.lastError?.message || status.stoppedReason || "";
    const eventData: Record<string, unknown> = {
      type: "indexer-status",
      indexerStatus: status.indexerStatus,
      crawlingStatus: status.crawlingStatus,
      timestamp: toIsoSeconds(),
    };

    if (status.stoppedAt) eventData.stoppedAt = status.stoppedAt;
    if (isUnknownMessageError(errorMessage)) {
      if (status.stoppedReason) eventData.stoppedReason = status.stoppedReason;
      if (status.lastError) {
        eventData.lastError = {
          message: status.lastError.message,
          timestamp: status.lastError.timestamp,
          service: status.lastError.service,
        };
      }
    }

    this.broadcastMessage(eventData);
  }

  private broadcastMessage(eventData: Record<string, unknown>): void {
    let sentCount = 0;
    this.wsClients.forEach((ws) => {
      if (this.sendJson(ws, eventData)) sentCount++;
    });

    if (sentCount > 0) {
      this.logInfo(`[EventsBroadcaster] Broadcasted ${eventData.type} to ${sentCount} WebSocket client(s)`);
    }
  }

  private broadcastToGlobalClients(eventData: Record<string, unknown>): void {
    let sentCount = 0;
    this.wsClients.forEach((ws) => {
      if (this.clientDids.has(ws)) return;
      if (this.sendJson(ws, eventData)) sentCount++;
    });

    if (sentCount > 0) {
      this.logInfo(`[EventsBroadcaster] Broadcasted ${eventData.type} to ${sentCount} global WebSocket client(s)`);
    }
  }

  private broadcastToDid(did: string, eventData: Record<string, unknown>): void {
    const room = this.didRooms.get(did);
    if (!room || room.size === 0) return;

    let sentCount = 0;
    Array.from(room).forEach((ws) => {
      if (this.sendJson(ws, eventData)) sentCount++;
    });

    if (sentCount > 0) {
      this.logInfo(`[EventsBroadcaster] Broadcasted ${eventData.eventType || eventData.type} to ${sentCount} DID subscriber(s)`);
    }
  }

  getWSClientCount(): number {
    return this.wsClients.size;
  }

  close(): void {
    if (this.pingInterval) {
      clearInterval(this.pingInterval);
      this.pingInterval = null;
    }

    this.wsClients.forEach((ws) => {
      try {
        if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
          ws.close();
        } else {
          ws.terminate();
        }
      } catch {
        // Best effort close.
      }
    });

    this.wsClients.clear();
    this.clientAlive.clear();
    this.clientDids.clear();
    this.didRooms.clear();

    if (this.wss) {
      this.wss.close(() => {
        this.logInfo("[EventsBroadcaster] WebSocket server closed");
      });
      this.wss = null;
    }
  }
}

export const eventsBroadcaster = new EventsBroadcaster();
