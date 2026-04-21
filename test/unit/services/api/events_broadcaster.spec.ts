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
    eventType: "StartPermissionVP",
    did,
    blockHeight: 123456,
    txHash: "ABC123",
    timestamp: "2025-01-15T10:30:00Z",
    payload: {
      module: "permission",
      action: "StartPermissionVP",
      messageType: "/verana.perm.v1.MsgStartPermissionVP",
      txIndex: 0,
      messageIndex: 0,
      sender: did,
      relatedDids: [did],
      entityType: "Permission",
      entityId: "42",
    },
    ...overrides,
  };
}

describe("EventsBroadcaster", () => {
  let broadcaster: EventsBroadcaster;
  let httpServer: Server;
  const TEST_PORT = 9999;
  const WS_URL = `ws://localhost:${TEST_PORT}/verana/indexer/v1/events`;

  beforeAll((done) => {
    httpServer = createServer();
    httpServer.listen(TEST_PORT, done);
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
    expect(Object.prototype.hasOwnProperty.call(message, "blockHeight")).toBe(true);
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

  it("includes did and blockHeight in DID room connected messages", async () => {
    const did = "did:web:agent.example";
    const ws = new WebSocket(`${WS_URL}?did=${encodeURIComponent(did)}`);
    await waitForOpen(ws);

    const message = await waitForMessage(ws, (msg) => msg.type === "connected");
    expect(message.did).toBe(did);
    expect(Object.prototype.hasOwnProperty.call(message, "blockHeight")).toBe(true);

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
    expect(received.eventType).toBe("StartPermissionVP");
    expect(otherReceived).toBe(false);

    closeSocket(wsMatch);
    closeSocket(wsOther);
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
      payload: { ...makeEvent(didA).payload, relatedDids: [didA, didB] },
    });
    const eventB = makeEvent(didB, {
      txHash: eventA.txHash,
      payload: { ...eventA.payload, relatedDids: [didA, didB] },
    });

    const messageA = waitForMessage(wsA, (msg) => msg.type === "indexer-event");
    const messageB = waitForMessage(wsB, (msg) => msg.type === "indexer-event");
    broadcaster.broadcastIndexerEvent(eventA);
    broadcaster.broadcastIndexerEvent(eventB);

    await expect(messageA).resolves.toMatchObject({ did: didA, payload: { relatedDids: [didA, didB] } });
    await expect(messageB).resolves.toMatchObject({ did: didB, payload: { relatedDids: [didA, didB] } });

    closeSocket(wsA);
    closeSocket(wsB);
  });

  it("broadcasts legacy block-processed events to global subscribers only", async () => {
    const ws = new WebSocket(WS_URL);
    const didWs = new WebSocket(`${WS_URL}?did=${encodeURIComponent("did:web:agent.example")}`);
    await waitForOpen(ws);
    await waitForOpen(didWs);
    await waitForMessage(ws, (msg) => msg.type === "connected");
    await waitForMessage(didWs, (msg) => msg.type === "connected");

    const blockProcessed = waitForMessage(ws, (msg) => msg.type === "block-processed");
    let didRoomReceivedBlockProcessed = false;
    didWs.on("message", (data) => {
      const message = JSON.parse(data.toString());
      if (message.type === "block-processed") didRoomReceivedBlockProcessed = true;
    });

    broadcaster.broadcastBlockProcessed(123456, new Date("2025-01-15T10:30:00.000Z"));
    const message = await blockProcessed;
    expect(Object.keys(message)).toEqual(["type", "height", "timestamp"]);
    expect(message).toMatchObject({
      type: "block-processed",
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
