import WebSocket from 'ws';
import config from '../../../config.json' with { type: 'json' };

type LogFn = {
  (obj: unknown, msg?: string, ...args: unknown[]): void;
  (msg: string, ...args: unknown[]): void;
};

export interface Logger {
  info: LogFn;
  warn: LogFn;
  error: LogFn;
  debug?: LogFn;
}

interface WebSocketOptions {
  endpoint?: string;
  subscriptionQuery: string;
  onMessage: (data: string) => void | Promise<void>;
  logger?: Logger; 
}

export class ReusableWebSocketClient {
  private ws: WebSocket | null = null;
  private reconnectAttempts = 0;
  private readonly maxReconnectAttempts = config.websocket?.maxReconnectAttempts || 5;
  private readonly reconnectDelay = config.websocket?.reconnectDelay || 3000;
  private options: WebSocketOptions;

  constructor(options: WebSocketOptions) {
    this.options = options;
  }

  public connect() {
    const endpoint = this.options.endpoint || config.websocket?.endpoint || 'ws://node1.testnet.verana.network:26657/websocket';
    this.options.logger?.info?.(`🔌 Connecting to WebSocket at ${endpoint}`);
    this.ws = new WebSocket(endpoint);

    this.ws.on('open', () => {
      this.reconnectAttempts = 0;
      this.options.logger?.info?.('✅ WebSocket connected');
      this.subscribe();
    });

    this.ws.on('message', async (data: string) => {
      try {
        await this.options.onMessage(data);
      } catch (err) {
        this.options.logger?.error?.('❌ Error in message handler:', err);
      }
    });

    this.ws.on('close', () => {
      this.options.logger?.warn?.('🔌 WebSocket disconnected');
      this.scheduleReconnect();
    });

    this.ws.on('error', (err: Error) => {
      this.options.logger?.error?.('❌ WebSocket error:', err);
    });
  }

  private subscribe() {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      this.options.logger?.warn?.('⚠️ WebSocket not ready for subscription');
      return;
    }

    const subscription = {
      jsonrpc: "2.0",
      id: Date.now(),
      method: "subscribe",
      params: {
        query: this.options.subscriptionQuery
      }
    };

    this.ws.send(JSON.stringify(subscription), (err) => {
      if (err) {
        this.options.logger?.error?.('❌ Failed to subscribe:', err);
      } else {
        this.options.logger?.info?.('✅ Successfully subscribed to events');
      }
    });
  }

  private scheduleReconnect() {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      this.options.logger?.error?.('💥 Max reconnect attempts reached');
      return;
    }

    this.reconnectAttempts++;
    const delay = this.reconnectDelay * this.reconnectAttempts;
    this.options.logger?.info?.(`⏳ Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})...`);

    setTimeout(() => this.connect(), delay);
  }

  public close() {
    this.ws?.close();
    this.ws?.removeAllListeners();
    this.ws = null;
  }
}
