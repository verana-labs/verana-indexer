// test/unit/services/utils/ws-client.spec.ts

// 1) Mock 'ws' BEFORE importing the module under test.
jest.mock('ws', () => {
  type Listener = (...args: any[]) => void;
  type ListenerMap = Record<string, Listener[]>;

  const instances: any[] = [];

  class MockWS {
    // Mimic the real constants
    static OPEN = 1;
    static instances = instances;

    url: string;
    readyState = 0;
    listeners: ListenerMap = {};
    sent: string[] = [];
    closed = false;
    removedAll = false;

    constructor(url: string) {
      this.url = url;
      instances.push(this);
    }

    on(event: string, cb: Listener) {
      (this.listeners[event] ||= []).push(cb);
    }

    emit(event: string, ...args: any[]) {
      for (const cb of this.listeners[event] || []) cb(...args);
    }

    send(data: string, cb?: (err?: Error) => void) {
      this.sent.push(data);
      cb?.();
    }

    close() {
      this.closed = true;
      this.emit('close');
    }

    removeAllListeners() {
      this.removedAll = true;
      this.listeners = {};
    }

    // Helpers for tests
    openNow() {
      this.readyState = MockWS.OPEN;
      this.emit('open');
    }

    messageNow(payload: string) {
      this.emit('message', payload);
    }

    errorNow(err: Error) {
      this.emit('error', err);
    }
  }

  return { __esModule: true, default: MockWS };
});

// 2) Import the client AFTER the mock is registered.
import { ReusableWebSocketClient } from '../../../../src/common/utils/websocket-client';

// Get access to the mock class for assertions
const MockWS: any = require('ws').default;

type LogFn = (...args: any[]) => void;
const logger = {
  info: jest.fn<LogFn, any[]>(),
  warn: jest.fn<LogFn, any[]>(),
  error: jest.fn<LogFn, any[]>(),
};

beforeEach(() => {
  jest.useFakeTimers();
  (MockWS.instances as any[]).length = 0;
  jest.clearAllMocks();
});

afterEach(() => {
  jest.runOnlyPendingTimers();
  jest.useRealTimers();
});

describe('ReusableWebSocketClient', () => {
  test('connects, subscribes on open, and forwards messages', async () => {
    const onMessage = jest.fn();

    const client = new ReusableWebSocketClient({
      endpoint: 'ws://example.verana/websocket',
      subscriptionQuery: 'tm.event="NewBlock"',
      onMessage,
      logger,
    });

    client.connect();

    // First socket created
    const ws1 = MockWS.instances[0];
    expect(ws1).toBeDefined();
    expect(ws1.url).toBe('ws://example.verana/websocket');

    // Simulate successful open
    ws1.openNow();

    // Should have sent a subscription request
    expect(ws1.sent.length).toBe(1);
    const frame = JSON.parse(ws1.sent[0]);
    expect(frame.method).toBe('subscribe');
    expect(frame.params?.query).toBe('tm.event="NewBlock"');

    // Forward a message
    ws1.messageNow('{"hello":"world"}');
    expect(onMessage).toHaveBeenCalledWith('{"hello":"world"}');
  });

  test('reconnects with incremental backoff after close', () => {
    const client = new ReusableWebSocketClient({
      endpoint: 'ws://example.verana/websocket',
      subscriptionQuery: 'tm.event="NewBlock"',
      onMessage: () => {},
      logger,
    });

    client.connect();

    // Open first connection
    let ws = MockWS.instances[0];
    ws.openNow();

    // Close → attempt 1 (default reconnectDelay=3000)
    ws.close();
    jest.advanceTimersByTime(3000);
    expect(MockWS.instances.length).toBe(2);

    // Close → attempt 2 (6000)
    ws = MockWS.instances[1];
    ws.close();
    jest.advanceTimersByTime(6000);
    expect(MockWS.instances.length).toBe(3);

    // Close → attempt 3 (9000)
    ws = MockWS.instances[2];
    ws.close();
    jest.advanceTimersByTime(9000);
    expect(MockWS.instances.length).toBe(4);

    // We’ve clearly attempted to reconnect multiple times
    expect(logger.info).toHaveBeenCalled();
  });

  test('close() closes socket and removes listeners', () => {
    const client = new ReusableWebSocketClient({
      endpoint: 'ws://example.verana/websocket',
      subscriptionQuery: 'tm.event="NewBlock"',
      onMessage: () => {},
      logger,
    });

    client.connect();
    const ws1 = MockWS.instances[0];
    ws1.openNow();

    client.close();

    expect(ws1.closed).toBe(true);
    expect(ws1.removedAll).toBe(true);
  });
});
