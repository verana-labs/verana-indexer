import { WebSocket, WebSocketServer } from "ws";
import { Server } from "http";

export class EventsBroadcaster {
  private wss: WebSocketServer | null = null;
  private wsClients: Set<WebSocket> = new Set();
  private clientAlive: Map<WebSocket, boolean> = new Map();
  private pingInterval: NodeJS.Timeout | null = null;
  private readonly MAX_CLIENTS = 10000;
  private readonly PING_INTERVAL = 30000;
  private logger: any = console;

  setLogger(logger: any): void {
    this.logger = logger;
  }

  initialize(server: Server): void {
    if (this.wss) {
      this.logger.warn("[EventsBroadcaster] WebSocket server already initialized");
      return;
    }

    this.wss = new WebSocketServer({ 
      server,
      path: "/verana/indexer/v1/events",
      perMessageDeflate: false,
      maxPayload: 1024,
      verifyClient: () => {
        if (this.wsClients.size >= this.MAX_CLIENTS) {
          this.logger.warn(`[EventsBroadcaster] Max clients reached (${this.MAX_CLIENTS}), rejecting connection`);
          return false;
        }
        return true;
      }
    });

    this.wss.on("connection", (ws: WebSocket) => {
      if (this.wsClients.size >= this.MAX_CLIENTS) {
        ws.close(1013, "Server overloaded");
        return;
      }

      this.wsClients.add(ws);
      this.clientAlive.set(ws, true);

      if (this.logger.info) {
        this.logger.info(`[EventsBroadcaster] New WebSocket client connected. Total clients: ${this.wsClients.size}`);
      } else {
        console.log(`[EventsBroadcaster] New WebSocket client connected. Total clients: ${this.wsClients.size}`);
      }

      try {
        ws.send(JSON.stringify({
          type: "connected",
          message: "Connected to Verana Indexer Events"
        }), { compress: false });
      } catch (error) {
        this.logger.error("[EventsBroadcaster] Error sending welcome message:", error);
        this.wsClients.delete(ws);
        this.clientAlive.delete(ws);
        return;
      }

      const cleanup = () => {
        this.wsClients.delete(ws);
        this.clientAlive.delete(ws);
        if (this.logger.info) {
          this.logger.info(`[EventsBroadcaster] WebSocket client disconnected. Total clients: ${this.wsClients.size}`);
        }
      };

      ws.on("close", cleanup);
      ws.on("error", (error) => {
        this.logger.error("[EventsBroadcaster] WebSocket error:", error);
        cleanup();
      });

      ws.on("pong", () => {
        this.clientAlive.set(ws, true);
      });
    });

    this.wss.on("error", (error) => {
      this.logger.error("[EventsBroadcaster] WebSocketServer error:", error);
    });

    this.pingInterval = setInterval(() => {
      this.pingClients();
    }, this.PING_INTERVAL);

    if (this.logger.info) {
      this.logger.info("[EventsBroadcaster] WebSocket server initialized on /verana/indexer/v1/events");
    }
  }

  private pingClients(): void {
    const deadClients: WebSocket[] = [];
    
    this.wsClients.forEach((ws) => {
      const isAlive = this.clientAlive.get(ws);
      if (isAlive === false) {
        deadClients.push(ws);
        return;
      }
      this.clientAlive.set(ws, false);
      try {
        ws.ping();
      } catch (error) {
        deadClients.push(ws);
      }
    });

    deadClients.forEach((ws) => {
      try {
        ws.terminate();
      } catch (error) {
        // Ignore termination errors
      }
      this.wsClients.delete(ws);
      this.clientAlive.delete(ws);
    });
  }

  broadcastBlockProcessed(height: number, timestamp: Date | string): void {
    if (this.wsClients.size === 0) {
      return;
    }

    const eventTimestamp = timestamp instanceof Date ? timestamp : new Date(timestamp);
    // Format timestamp as ISO 8601 with 'Z' designator (without milliseconds)
    const isoString = eventTimestamp.toISOString();
    const timestampFormatted = isoString.replace(/\.\d{3}Z$/, 'Z');
    
    const eventData = {
      type: "block-processed",
      height,
      timestamp: timestampFormatted
    };
    const message = JSON.stringify(eventData);

    const deadClients: WebSocket[] = [];
    let sentCount = 0;
    
    this.wsClients.forEach((ws) => {
      if (ws.readyState === WebSocket.OPEN) {
        try {
          ws.send(message, { compress: false });
          sentCount++;
        } catch (error) {
          deadClients.push(ws);
        }
      } else {
        deadClients.push(ws);
      }
    });

    if (deadClients.length > 0) {
      deadClients.forEach((ws) => {
        this.wsClients.delete(ws);
        this.clientAlive.delete(ws);
      });
    }

    if (sentCount > 0 && this.logger.info) {
      this.logger.info(`[EventsBroadcaster] Broadcasted block ${height} to ${sentCount} WebSocket client(s)`);
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
      } catch (error) {
        // Ignore close errors
      }
    });
    
    this.wsClients.clear();
    this.clientAlive.clear();

    if (this.wss) {
      this.wss.close(() => {
        if (this.logger.info) {
          this.logger.info("[EventsBroadcaster] WebSocket server closed");
        }
      });
      this.wss = null;
    }
  }
}

export const eventsBroadcaster = new EventsBroadcaster();
