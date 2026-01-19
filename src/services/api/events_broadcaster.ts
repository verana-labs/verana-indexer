import { WebSocket, WebSocketServer } from "ws";
import { Server } from "http";
import { indexerStatusManager } from "../manager/indexer_status.manager";

function isTemporaryError(errorMessage: string): boolean {
  if (!errorMessage) return false;
  const lowerMessage = errorMessage.toLowerCase();
  return lowerMessage.includes('timeout') ||
         lowerMessage.includes('exceeded') ||
         lowerMessage.includes('timed out') ||
         lowerMessage.includes('econnrefused') ||
         lowerMessage.includes('etimedout') ||
         lowerMessage.includes('econaborted') ||
         lowerMessage.includes('network') ||
         lowerMessage.includes('connection') ||
         lowerMessage.includes('non-critical') ||
         lowerMessage.includes('service will continue');
}

function isUnknownMessageError(errorMessage: string): boolean {
  if (!errorMessage) return false;
  return errorMessage.includes('Unknown Verana message types') ||
         errorMessage.includes('UNKNOWN VERANA MESSAGE TYPES');
}

export class EventsBroadcaster {
  private wss: WebSocketServer | null = null;
  private wsClients: Set<WebSocket> = new Set();
  private clientAlive: Map<WebSocket, boolean> = new Map();
  private pingInterval: NodeJS.Timeout | null = null;
  private readonly MAX_CLIENTS = 10000;
  private readonly PING_INTERVAL = 30000;
  private logger: {
    info?: (...args: any[]) => void;
    warn?: (...args: any[]) => void;
    error?: (...args: any[]) => void;
  } = console;

  setLogger(logger: {
    info?: (...args: any[]) => void;
    warn?: (...args: any[]) => void;
    error?: (...args: any[]) => void;
  }): void {
    this.logger = logger;
  }

  private logInfo(...args: any[]): void {
    if (this.logger.info) {
      this.logger.info(...args);
    } else {
      console.log(...args);
    }
  }

  private logWarn(...args: any[]): void {
    if (this.logger.warn) {
      this.logger.warn(...args);
    } else {
      console.warn(...args);
    }
  }

  private logError(...args: any[]): void {
    if (this.logger.error) {
      this.logger.error(...args);
    } else {
      console.error(...args);
    }
  }

 
  private getCurrentIndexerStatus(): {
    indexerStatus: "running" | "stopped";
    crawlingStatus: "active" | "stopped";
    stoppedAt?: string;
    stoppedReason?: string;
    lastError?: {
      message: string;
      timestamp: string;
      service?: string;
    };
  } {
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
      }
    });

    this.wss.on("connection", (ws: WebSocket) => {
      if (this.wsClients.size >= this.MAX_CLIENTS) {
        ws.close(1013, "Server overloaded");
        return;
      }

      this.wsClients.add(ws);
      this.clientAlive.set(ws, true);

      this.logInfo(`[EventsBroadcaster] New WebSocket client connected. Total clients: ${this.wsClients.size}`);

      try {
        const status = this.getCurrentIndexerStatus();
        const connectionMessage: any = {
          type: "connected",
          message: "Connected to Verana Indexer Events",
          indexerStatus: status.indexerStatus,
          crawlingStatus: status.crawlingStatus,
          timestamp: new Date().toISOString().replace(/\.\d{3}Z$/, 'Z')
        };
        
        if (status.crawlingStatus === "stopped") {
          if (status.stoppedAt) {
            connectionMessage.stoppedAt = status.stoppedAt;
          }
          const errorMessage = status.lastError?.message || status.stoppedReason || '';
          const isUnknown = isUnknownMessageError(errorMessage);
          
          if (isUnknown) {
            if (status.stoppedReason) {
              connectionMessage.stoppedReason = status.stoppedReason;
            }
            if (status.lastError) {
              connectionMessage.lastError = {
                message: status.lastError.message,
                timestamp: status.lastError.timestamp,
                service: status.lastError.service
              };
            }
          }
        }
        
        ws.send(JSON.stringify(connectionMessage), { compress: false });
      } catch (error) {
        this.logError("[EventsBroadcaster] Error sending welcome message:", error);
        this.wsClients.delete(ws);
        this.clientAlive.delete(ws);
        return;
      }

      const cleanup = () => {
        this.wsClients.delete(ws);
        this.clientAlive.delete(ws);
        this.logInfo(`[EventsBroadcaster] WebSocket client disconnected. Total clients: ${this.wsClients.size}`);
      };

      ws.on("close", cleanup);
      ws.on("error", (error) => {
        this.logError("[EventsBroadcaster] WebSocket error:", error);
        cleanup();
      });

      ws.on("pong", () => {
        this.clientAlive.set(ws, true);
      });
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
    this.broadcastMessage(eventData);
  }


  broadcastIndexerStatus(status: {
    indexerStatus: "running" | "stopped";
    crawlingStatus: "active" | "stopped";
    stoppedAt?: string;
    stoppedReason?: string;
    lastError?: {
      message: string;
      timestamp: string;
      service?: string;
    };
  }): void {
    if (this.wsClients.size === 0) {
      return;
    }

    const errorMessage = status.lastError?.message || status.stoppedReason || '';
    const isUnknown = isUnknownMessageError(errorMessage);

    const eventData: any = {
      type: "indexer-status",
      indexerStatus: status.indexerStatus,
      crawlingStatus: status.crawlingStatus,
      timestamp: new Date().toISOString().replace(/\.\d{3}Z$/, 'Z')
    };

    if (status.stoppedAt) {
      eventData.stoppedAt = status.stoppedAt;
    }

    if (isUnknown) {
      if (status.stoppedReason) {
        eventData.stoppedReason = status.stoppedReason;
      }
      if (status.lastError) {
        eventData.lastError = {
          message: status.lastError.message,
          timestamp: status.lastError.timestamp,
          service: status.lastError.service
        };
      }
    }

    this.broadcastMessage(eventData);
  }

 
  private broadcastMessage(eventData: Record<string, any>): void {
    if (this.wsClients.size === 0) {
      return;
    }

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

    if (sentCount > 0) {
      this.logInfo(`[EventsBroadcaster] Broadcasted ${eventData.type} to ${sentCount} WebSocket client(s)`);
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
        this.logInfo("[EventsBroadcaster] WebSocket server closed");
      });
      this.wss = null;
    }
  }
}

export const eventsBroadcaster = new EventsBroadcaster();
