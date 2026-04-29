import { createServer, Server } from "http";
import { WebSocket } from "ws";
import { EventsBroadcaster } from "../../../../src/services/api/events_broadcaster";
import type { IndexerEventRecord } from "../../../../src/services/api/indexer_events_query";

function waitForMessage(ws: WebSocket, predicate: (message: any) => boolean, timeoutMs = 1500): Promise<any> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      ws.off("message", onMessage);
      reject(new Error("Timed out waiting for WebSocket message"));
    }, timeoutMs);

    const onMessage = (data: WebSocket.RawData) => {
      const message = JSON.parse(data.toString());
      if (!predicate(message)) return;
      clearTimeout(timeout);
      ws.off("message", onMessage);
      resolve(message);
    };

    ws.on("message", onMessage);
  });
}

function waitForOpen(ws: WebSocket): Promise<void> {
  return new Promise((resolve, reject) => {
    ws.once("open", () => resolve());
    ws.once("error", reject);
  });
}

function waitForClose(ws: WebSocket): Promise<{ code: number; reason: string }> {
  return new Promise((resolve) => {
    ws.once("close", (code, reason) => {
      resolve({ code, reason: reason.toString() });
    });
  });
}

function waitForCondition(predicate: () => boolean, timeoutMs = 1500): Promise<void> {
  return new Promise((resolve, reject) => {
    const startedAt = Date.now();
    const check = () => {
      if (predicate()) {
        resolve();
        return;
      }
      if (Date.now() - startedAt >= timeoutMs) {
        reject(new Error("Timed out waiting for condition"));
        return;
      }
      setTimeout(check, 25);
    };
    check();
  });
}

function closeSocket(ws: WebSocket): void {
  if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
    ws.close();
  }
}

function makeEvent(did: string, overrides: Partial<IndexerEventRecord> = {}): IndexerEventRecord {
  return {
    type: "indexer-event",
    event_type: "StartPermissionVP",
    did,
    block_height: 123456,
    tx_hash: "ABC123",
    timestamp: "2025-01-15T10:30:00Z",
    payload: {
      module: "permission",
      action: "StartPermissionVP",
      message_type: "/verana.perm.v1.MsgStartPermissionVP",
      tx_index: 0,
      message_index: 0,
      sender: did,
      related_dids: [did],
      entity_type: "Permission",
      entity_id: "42",
    },
    ...overrides,
  };
}

describe("EventsBroadcaster", () => {
  let broadcaster: EventsBroadcaster;
  let httpServer: Server;
  let TEST_PORT = 0;
  let WS_URL = "";

  beforeAll((done) => {
    httpServer = createServer();
    httpServer.listen(0, () => {
      const addr = httpServer.address();
      const port = typeof addr === "object" && addr ? addr.port : 0;
      TEST_PORT = port;
      WS_URL = `ws://localhost:${TEST_PORT}/verana/indexer/v1/events`;
      done();
    });
  });

  afterAll((done) => {
    broadcaster?.close();
    httpServer.close(() => done());
  });

  beforeEach(() => {
    broadcaster?.close();
    broadcaster = new EventsBroadcaster();
    broadcaster.setLogger({ info: () => {}, warn: () => {}, error: () => {} });
    broadcaster.initialize(httpServer);
  });

  afterEach(() => {
    broadcaster.close();
  });

  it("accepts WebSocket connections and sends a connected message", async () => {
    const ws = new WebSocket(WS_URL);
    await waitForOpen(ws);

    const message = await waitForMessage(ws, (msg) => msg.type === "connected");
    expect(message).toMatchObject({
      type: "connected",
      message: "Connected to Verana Indexer Events",
    });
    expect(Object.prototype.hasOwnProperty.call(message, "block_height")).toBe(true);
    expect(message.did).toBeUndefined();
    expect(broadcaster.getWSClientCount()).toBe(1);

    closeSocket(ws);
    await waitForClose(ws);
    await waitForCondition(() => broadcaster.getWSClientCount() === 0);
    expect(broadcaster.getWSClientCount()).toBe(0);
  });

  it("rejects an invalid DID query", async () => {
    const ws = new WebSocket(`${WS_URL}?did=${encodeURIComponent("not-a-did")}`);
    const close = await waitForClose(ws);

    expect(close.code).toBe(1008);
    expect(close.reason).toBe("Invalid did query parameter");
    expect(broadcaster.getWSClientCount()).toBe(0);
  });

  describe("Block indexed events", () => {
    it("should broadcast block-indexed events to all connected clients", (done) => {
      const ws1 = new WebSocket(WS_URL);
      const ws2 = new WebSocket(WS_URL);
      const receivedMessages: any[] = [];
      let bothConnected = false;

      const checkDone = () => {
        if (bothConnected && receivedMessages.length === 2) {
          receivedMessages.forEach((msg) => {
            expect(msg.type).toBe("block-indexed");
            expect(msg.height).toBe(123456);
            expect(msg.timestamp).toBeDefined();
          });
          ws1.close();
          ws2.close();
          done();
        }
      };

      ws1.on("open", () => {
        ws2.on("open", () => {
          bothConnected = true;
          broadcaster.broadcastBlockIndexed(123456, new Date());
        });
      });

      ws1.on("message", (data) => {
        const message = JSON.parse(data.toString());
        if (message.type === "block-indexed") {
          receivedMessages.push(message);
          checkDone();
        }
      });

      ws2.on("message", (data) => {
        const message = JSON.parse(data.toString());
        if (message.type === "block-indexed") {
          receivedMessages.push(message);
          checkDone();
        }
      });

      ws1.on("error", (error) => done(error));
      ws2.on("error", (error) => done(error));
    }, 10000);

    it("should format timestamp correctly", (done) => {
      const ws = new WebSocket(WS_URL);
      const testTimestamp = new Date("2025-01-15T10:30:00.000Z");
      const expectedFormat = "2025-01-15T10:30:00Z";
      let blockIndexedReceived = false;

      ws.on("open", () => {
        broadcaster.broadcastBlockIndexed(789012, testTimestamp);
      });

      ws.on("message", (data) => {
        const message = JSON.parse(data.toString());
        if (message.type === "block-indexed") {
          expect(message.timestamp).toBe(expectedFormat);
          expect(new Date(message.timestamp).getTime()).toBe(testTimestamp.getTime());
          blockIndexedReceived = true;
          ws.close();
        }
      });

      ws.on("close", () => {
        if (blockIndexedReceived) {
          done();
        } else {
          done(new Error("Block indexed message not received"));
        }
      });

      ws.on("error", (error) => {
        done(error);
      });
    }, 10000);

    it("should handle string timestamps", (done) => {
      const ws = new WebSocket(WS_URL);
      const testTimestamp = "2025-01-15T10:30:00.000Z";
      const expectedFormat = "2025-01-15T10:30:00Z";
      let blockIndexedReceived = false;

      ws.on("open", () => {
        broadcaster.broadcastBlockIndexed(345678, testTimestamp);
      });

      ws.on("message", (data) => {
        const message = JSON.parse(data.toString());
        if (message.type === "block-indexed") {
          expect(message.timestamp).toBe(expectedFormat);
          blockIndexedReceived = true;
          ws.close();
        }
      });

      ws.on("close", () => {
        if (blockIndexedReceived) {
          done();
        } else {
          done(new Error("Block indexed message not received"));
        }
      });

      ws.on("error", (error) => {
        done(error);
      });
    }, 10000);

    it("should broadcast block-resolved events", (done) => {
      const ws = new WebSocket(WS_URL);
      let received = false;

      ws.on("open", () => {
        broadcaster.broadcastBlockResolved(999001, new Date("2025-06-01T12:00:00.000Z"));
      });

      ws.on("message", (data) => {
        const message = JSON.parse(data.toString());
        if (message.type === "block-resolved") {
          expect(message.height).toBe(999001);
          expect(message.timestamp).toBeDefined();
          received = true;
          ws.close();
        }
      });

      ws.on("close", () => {
        if (received) {
          done();
        } else {
          done(new Error("block-resolved not received"));
        }
      });

      ws.on("error", (error) => done(error));
    }, 10000);
  });

  it("includes did and block_height in DID room connected messages", async () => {
    const did = "did:web:agent.example";
    const ws = new WebSocket(`${WS_URL}?did=${encodeURIComponent(did)}`);
    await waitForOpen(ws);

    const message = await waitForMessage(ws, (msg) => msg.type === "connected");
    expect(message.did).toBe(did);
    expect(Object.prototype.hasOwnProperty.call(message, "block_height")).toBe(true);

    closeSocket(ws);
  });

  it("delivers persisted indexer events only to matching DID subscribers", async () => {
    const did = "did:web:agent.example";
    const otherDid = "did:web:other.example";
    const wsMatch = new WebSocket(`${WS_URL}?did=${encodeURIComponent(did)}`);
    const wsOther = new WebSocket(`${WS_URL}?did=${encodeURIComponent(otherDid)}`);
    await Promise.all([waitForOpen(wsMatch), waitForOpen(wsOther)]);
    await Promise.all([
      waitForMessage(wsMatch, (msg) => msg.type === "connected"),
      waitForMessage(wsOther, (msg) => msg.type === "connected"),
    ]);

    const matchPromise = waitForMessage(wsMatch, (msg) => msg.type === "indexer-event");
    let otherReceived = false;
    wsOther.on("message", (data) => {
      const message = JSON.parse(data.toString());
      if (message.type === "indexer-event") otherReceived = true;
    });

    broadcaster.broadcastIndexerEvent(makeEvent(did));
    const received = await matchPromise;
    await new Promise((resolve) => {
      setTimeout(resolve, 200);
    });

    expect(received.did).toBe(did);
    expect(received.event_type).toBe("StartPermissionVP");
    expect(otherReceived).toBe(false);

    closeSocket(wsMatch);
    closeSocket(wsOther);
  });

  describe("Real-time Event Flow", () => {
    it("should receive multiple block-indexed events in sequence", (done) => {
      const ws = new WebSocket(WS_URL);
      const receivedHeights: number[] = [];
      const expectedHeights = [100, 101, 102];

      ws.on("open", () => {
        setTimeout(() => broadcaster.broadcastBlockIndexed(100, new Date()), 100);
        setTimeout(() => broadcaster.broadcastBlockIndexed(101, new Date()), 200);
        setTimeout(() => broadcaster.broadcastBlockIndexed(102, new Date()), 300);
      });

      ws.on("message", (data) => {
        const message = JSON.parse(data.toString());
        if (message.type === "block-indexed") {
          receivedHeights.push(message.height);
          if (receivedHeights.length === expectedHeights.length) {
            expect(receivedHeights).toEqual(expectedHeights);
            ws.close();
          }
        }
      });

      ws.on("close", () => {
        if (receivedHeights.length === expectedHeights.length) {
          done();
        } else {
          done(new Error(`Expected ${expectedHeights.length} events, got ${receivedHeights.length}`));
        }
      });

      ws.on("error", (error) => {
        done(error);
      });
    }, 10000);
  });

  it("delivers multi-DID events to each relevant DID subscriber", async () => {
    const didA = "did:web:agent-a.example";
    const didB = "did:web:agent-b.example";
    const wsA = new WebSocket(`${WS_URL}?did=${encodeURIComponent(didA)}`);
    const wsB = new WebSocket(`${WS_URL}?did=${encodeURIComponent(didB)}`);
    await Promise.all([waitForOpen(wsA), waitForOpen(wsB)]);
    await Promise.all([
      waitForMessage(wsA, (msg) => msg.type === "connected"),
      waitForMessage(wsB, (msg) => msg.type === "connected"),
    ]);

    const eventA = makeEvent(didA, {
      payload: { ...makeEvent(didA).payload, related_dids: [didA, didB] },
    });
    const eventB = makeEvent(didB, {
      tx_hash: eventA.tx_hash,
      payload: { ...eventA.payload, related_dids: [didA, didB] },
    });

    const messageA = waitForMessage(wsA, (msg) => msg.type === "indexer-event");
    const messageB = waitForMessage(wsB, (msg) => msg.type === "indexer-event");
    broadcaster.broadcastIndexerEvent(eventA);
    broadcaster.broadcastIndexerEvent(eventB);

    await expect(messageA).resolves.toMatchObject({ did: didA, payload: { related_dids: [didA, didB] } });
    await expect(messageB).resolves.toMatchObject({ did: didB, payload: { related_dids: [didA, didB] } });

    closeSocket(wsA);
    closeSocket(wsB);
  });

  it("broadcasts legacy block-indexed events to global subscribers only", async () => {
    const ws = new WebSocket(WS_URL);
    const didWs = new WebSocket(`${WS_URL}?did=${encodeURIComponent("did:web:agent.example")}`);
    await waitForOpen(ws);
    await waitForOpen(didWs);
    await waitForMessage(ws, (msg) => msg.type === "connected");
    await waitForMessage(didWs, (msg) => msg.type === "connected");

    const blockProcessed = waitForMessage(ws, (msg) => msg.type === "block-indexed");
    let didRoomReceivedBlockProcessed = false;
    didWs.on("message", (data) => {
      const message = JSON.parse(data.toString());
      if (message.type === "block-indexed") didRoomReceivedBlockProcessed = true;
    });

    broadcaster.broadcastBlockProcessed(123456, new Date("2025-01-15T10:30:00.000Z"));
    const message = await blockProcessed;
    expect(Object.keys(message)).toEqual(["type", "height", "timestamp"]);
    expect(message).toMatchObject({
      type: "block-indexed",
      height: 123456,
      timestamp: "2025-01-15T10:30:00Z",
    });

    await new Promise((resolve) => {
      setTimeout(resolve, 200);
    });
    expect(didRoomReceivedBlockProcessed).toBe(false);

    closeSocket(ws);
    closeSocket(didWs);
  });

  it("does not send block-indexed to DID room subscribers", async () => {
    const globalWs = new WebSocket(WS_URL);
    const didWs = new WebSocket(`${WS_URL}?did=${encodeURIComponent("did:web:agent.example")}`);
    await Promise.all([waitForOpen(globalWs), waitForOpen(didWs)]);
    await Promise.all([
      waitForMessage(globalWs, (msg) => msg.type === "connected"),
      waitForMessage(didWs, (msg) => msg.type === "connected"),
    ]);

    const globalPromise = waitForMessage(globalWs, (msg) => msg.type === "block-indexed");
    let didRoomReceived = false;
    didWs.on("message", (data) => {
      const message = JSON.parse(data.toString());
      if (message.type === "block-indexed") didRoomReceived = true;
    });

    broadcaster.broadcastBlockIndexed(123456, new Date("2025-01-15T10:30:00.000Z"));
    const message = await globalPromise;
    expect(message.type).toBe("block-indexed");

    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(didRoomReceived).toBe(false);

    closeSocket(globalWs);
    closeSocket(didWs);
  });

  it("does not send block-resolved to DID room subscribers", async () => {
    const globalWs = new WebSocket(WS_URL);
    const didWs = new WebSocket(`${WS_URL}?did=${encodeURIComponent("did:web:agent.example")}`);
    await Promise.all([waitForOpen(globalWs), waitForOpen(didWs)]);
    await Promise.all([
      waitForMessage(globalWs, (msg) => msg.type === "connected"),
      waitForMessage(didWs, (msg) => msg.type === "connected"),
    ]);

    const globalPromise = waitForMessage(globalWs, (msg) => msg.type === "block-resolved");
    let didRoomReceived = false;
    didWs.on("message", (data) => {
      const message = JSON.parse(data.toString());
      if (message.type === "block-resolved") didRoomReceived = true;
    });

    broadcaster.broadcastBlockResolved(123456, new Date("2025-01-15T10:30:00.000Z"));
    const message = await globalPromise;
    expect(message.type).toBe("block-resolved");

    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(didRoomReceived).toBe(false);

    closeSocket(globalWs);
    closeSocket(didWs);
  });

  it("does not send DID indexer events to global subscribers", async () => {
    const ws = new WebSocket(WS_URL);
    await waitForOpen(ws);
    await waitForMessage(ws, (msg) => msg.type === "connected");

    let receivedIndexerEvent = false;
    ws.on("message", (data) => {
      const message = JSON.parse(data.toString());
      if (message.type === "indexer-event") receivedIndexerEvent = true;
    });

    broadcaster.broadcastIndexerEvent(makeEvent("did:web:agent.example"));
    await new Promise((resolve) => {
      setTimeout(resolve, 200);
    });

    expect(receivedIndexerEvent).toBe(false);
    closeSocket(ws);
  });

  it("cleans up clients on close and error/terminate", async () => {
    const wsClose = new WebSocket(`${WS_URL}?did=${encodeURIComponent("did:web:close.example")}`);
    await waitForOpen(wsClose);
    expect(broadcaster.getWSClientCount()).toBe(1);
    closeSocket(wsClose);
    await waitForClose(wsClose);
    await waitForCondition(() => broadcaster.getWSClientCount() === 0);
    expect(broadcaster.getWSClientCount()).toBe(0);

    const wsTerminate = new WebSocket(`${WS_URL}?did=${encodeURIComponent("did:web:error.example")}`);
    await waitForOpen(wsTerminate);
    expect(broadcaster.getWSClientCount()).toBe(1);
    wsTerminate.terminate();
    await waitForClose(wsTerminate);
    await waitForCondition(() => broadcaster.getWSClientCount() === 0);
    expect(broadcaster.getWSClientCount()).toBe(0);
  });
});
