import { createServer, Server } from "http";
import { WebSocket } from "ws";
import { SubscribeBroadcaster } from "../../../../src/services/api/subscribe_broadcaster";
import type { IndexerEventRecord } from "../../../../src/services/api/indexer_events_query";

jest.setTimeout(15000);

function waitForMessage(
  ws: WebSocket,
  predicate: (message: any) => boolean,
  timeoutMs = 5000
): Promise<any> {
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

function broadcastBlockIndexed(
  b: SubscribeBroadcaster,
  height: number,
  ts: Date | string
): void {
  const blockTime = ts instanceof Date ? ts.toISOString() : ts;
  b.broadcastBlockEnvelope({ block: height, blockTime, events: [] });
}

function broadcastIndexerEvent(b: SubscribeBroadcaster, event: IndexerEventRecord): void {
  b.broadcastBlockEnvelope({
    block: event.block_height,
    blockTime: event.timestamp,
    events: [event],
  });
}

async function openSubscribed(
  url: string,
  dids: string[] | null
): Promise<WebSocket> {
  const ws = new WebSocket(url);
  await waitForOpen(ws);
  await waitForMessage(ws, (msg) => msg.type === "ready");
  ws.send(JSON.stringify({ action: "subscribe", dids }));
  await new Promise((resolve) => setTimeout(resolve, 20));
  return ws;
}

function makeEvent(did: string, overrides: Partial<IndexerEventRecord> = {}): IndexerEventRecord {
  return {
    type: "indexer-event",
    event_type: "StartParticipantOP",
    did,
    block_height: 123456,
    tx_hash: "ABC123",
    timestamp: "2025-01-15T10:30:00Z",
    payload: {
      module: "participant",
      action: "StartParticipantOP",
      message_type: "/verana.pp.v1.MsgStartParticipantOP",
      tx_index: 0,
      message_index: 0,
      sender: did,
      related_dids: [did],
      entity_type: "Participant",
      entity_id: "42",
    },
    ...overrides,
  };
}

describe("SubscribeBroadcaster", () => {
  let broadcaster: SubscribeBroadcaster;
  let httpServer: Server;
  let TEST_PORT = 0;
  let WS_URL = "";

  beforeAll((done) => {
    httpServer = createServer();
    httpServer.listen(0, () => {
      const addr = httpServer.address();
      const port = typeof addr === "object" && addr ? addr.port : 0;
      TEST_PORT = port;
      WS_URL = `ws://localhost:${TEST_PORT}/v4/indexer/subscribe`;
      done();
    });
  });

  afterAll((done) => {
    broadcaster?.close();
    httpServer.close(() => done());
  });

  beforeEach(() => {
    broadcaster?.close();
    broadcaster = new SubscribeBroadcaster();
    broadcaster.setLogger({ info: () => {}, warn: () => {}, error: () => {} });
    broadcaster.initialize(httpServer);
  });

  afterEach(() => {
    broadcaster.close();
  });

  it("accepts WebSocket connections and sends a ready message", async () => {
    const ws = new WebSocket(WS_URL);
    await waitForOpen(ws);

    const message = await waitForMessage(ws, (msg) => msg.type === "ready");
    expect(message).toMatchObject({ type: "ready" });
    expect(Object.prototype.hasOwnProperty.call(message, "block")).toBe(true);
    expect(Object.prototype.hasOwnProperty.call(message, "blockTime")).toBe(true);
    expect(typeof message.blockIntervalMs).toBe("number");
    expect(broadcaster.getClientCount()).toBe(1);

    closeSocket(ws);
    await waitForClose(ws);
    await waitForCondition(() => broadcaster.getClientCount() === 0);
    expect(broadcaster.getClientCount()).toBe(0);
  });

  it("rejects an invalid DID in a subscribe message", async () => {
    const ws = new WebSocket(WS_URL);
    await waitForOpen(ws);
    await waitForMessage(ws, (msg) => msg.type === "ready");

    ws.send(JSON.stringify({ action: "subscribe", dids: ["not-a-did"] }));
    const close = await waitForClose(ws);

    expect(close.code).toBe(1008);
    expect(broadcaster.getClientCount()).toBe(0);
  });

  describe("Block envelopes", () => {
    it("should broadcast block envelopes to all wildcard subscribers", async () => {
      const ws1 = await openSubscribed(WS_URL, null);
      const ws2 = await openSubscribed(WS_URL, null);

      const p1 = waitForMessage(ws1, (msg) => msg.type === "block", 10000);
      const p2 = waitForMessage(ws2, (msg) => msg.type === "block", 10000);

      broadcastBlockIndexed(broadcaster, 123456, new Date());
      const [m1, m2] = await Promise.all([p1, p2]);

      [m1, m2].forEach((msg) => {
        expect(msg.type).toBe("block");
        expect(msg.block).toBe(123456);
        expect(msg.blockTime).toBeDefined();
      });

      closeSocket(ws1);
      closeSocket(ws2);
    }, 15000);

    it("should format blockTime correctly from a Date", async () => {
      const ws = await openSubscribed(WS_URL, null);
      const testTimestamp = new Date("2025-01-15T10:30:00.000Z");

      const envelope = waitForMessage(ws, (msg) => msg.type === "block");
      broadcastBlockIndexed(broadcaster, 789012, testTimestamp);
      const message = await envelope;

      expect(new Date(message.blockTime).getTime()).toBe(testTimestamp.getTime());
      closeSocket(ws);
    }, 10000);

    it("should handle string blockTime", async () => {
      const ws = await openSubscribed(WS_URL, null);
      const testTimestamp = "2025-01-15T10:30:00.000Z";

      const envelope = waitForMessage(ws, (msg) => msg.type === "block");
      broadcastBlockIndexed(broadcaster, 345678, testTimestamp);
      const message = await envelope;

      expect(message.blockTime).toBe(testTimestamp);
      closeSocket(ws);
    }, 10000);
  });

  it("includes block in the ready message of a DID-filtered subscription", async () => {
    const did = "did:web:agent.example";
    const ws = new WebSocket(WS_URL);
    await waitForOpen(ws);

    const message = await waitForMessage(ws, (msg) => msg.type === "ready");
    expect(Object.prototype.hasOwnProperty.call(message, "block")).toBe(true);
    ws.send(JSON.stringify({ action: "subscribe", dids: [did] }));

    closeSocket(ws);
  });

  it("delivers persisted indexer events only to matching DID subscribers", async () => {
    const did = "did:web:agent.example";
    const otherDid = "did:web:other.example";
    const wsMatch = await openSubscribed(WS_URL, [did]);
    const wsOther = await openSubscribed(WS_URL, [otherDid]);

    const matchPromise = waitForMessage(
      wsMatch,
      (msg) => msg.type === "block" && msg.events.length > 0
    );
    let otherReceivedEvent = false;
    wsOther.on("message", (data) => {
      const message = JSON.parse(data.toString());
      if (message.type === "block" && message.events.length > 0) otherReceivedEvent = true;
    });

    broadcastIndexerEvent(broadcaster, makeEvent(did));
    const envelope = await matchPromise;
    await new Promise((resolve) => {
      setTimeout(resolve, 200);
    });

    expect(envelope.events).toHaveLength(1);
    expect(envelope.events[0].did).toBe(did);
    expect(envelope.events[0].event_type).toBe("StartParticipantOP");
    expect(otherReceivedEvent).toBe(false);

    closeSocket(wsMatch);
    closeSocket(wsOther);
  });

  describe("Real-time Event Flow", () => {
    it("should receive multiple block envelopes in sequence", (done) => {
      const receivedBlocks: number[] = [];
      const expectedBlocks = [100, 101, 102];

      openSubscribed(WS_URL, null)
        .then((ws) => {
          ws.on("message", (data) => {
            const message = JSON.parse(data.toString());
            if (message.type !== "block") return;
            receivedBlocks.push(message.block);
            if (receivedBlocks.length === expectedBlocks.length) {
              try {
                expect(receivedBlocks).toEqual(expectedBlocks);
                ws.close();
              } catch (err) {
                done(err);
              }
            }
          });
          ws.on("close", () => {
            if (receivedBlocks.length === expectedBlocks.length) done();
            else done(new Error(`Expected ${expectedBlocks.length} envelopes, got ${receivedBlocks.length}`));
          });
          ws.on("error", (error) => done(error));

          setTimeout(() => broadcastBlockIndexed(broadcaster, 100, new Date()), 100);
          setTimeout(() => broadcastBlockIndexed(broadcaster, 101, new Date()), 200);
          setTimeout(() => broadcastBlockIndexed(broadcaster, 102, new Date()), 300);
        })
        .catch(done);
    }, 10000);
  });

  it("delivers multi-DID events to each relevant DID subscriber", async () => {
    const didA = "did:web:agent-a.example";
    const didB = "did:web:agent-b.example";
    const wsA = await openSubscribed(WS_URL, [didA]);
    const wsB = await openSubscribed(WS_URL, [didB]);

    const eventA = makeEvent(didA, {
      payload: { ...makeEvent(didA).payload, related_dids: [didA, didB] },
    });
    const messageA = waitForMessage(wsA, (msg) => msg.type === "block" && msg.events.length > 0);
    const messageB = waitForMessage(wsB, (msg) => msg.type === "block" && msg.events.length > 0);
    broadcastIndexerEvent(broadcaster, eventA);

    await expect(messageA).resolves.toMatchObject({
      events: [{ did: didA, payload: { related_dids: [didA, didB] } }],
    });
    await expect(messageB).resolves.toMatchObject({
      events: [{ did: didA, payload: { related_dids: [didA, didB] } }],
    });

    closeSocket(wsA);
    closeSocket(wsB);
  });

  it("does not emit duplicate events when did is also related", async () => {
    const didA = "did:web:agent-duplicate.example";
    const wsA = await openSubscribed(WS_URL, [didA]);

    let receivedEvents = 0;
    wsA.on("message", (data) => {
      const message = JSON.parse(data.toString());
      if (message.type === "block") receivedEvents += message.events.length;
    });

    broadcastIndexerEvent(
      broadcaster,
      makeEvent(didA, {
        payload: { ...makeEvent(didA).payload, related_dids: [didA, didA] },
      })
    );
    await new Promise((resolve) => setTimeout(resolve, 250));

    expect(receivedEvents).toBe(1);
    closeSocket(wsA);
  });

  it("delivers empty envelopes as heartbeats to DID-filtered subscribers", async () => {
    const wildcardWs = await openSubscribed(WS_URL, null);
    const didWs = await openSubscribed(WS_URL, ["did:web:agent.example"]);

    const wildcardPromise = waitForMessage(wildcardWs, (msg) => msg.type === "block");
    const didPromise = waitForMessage(didWs, (msg) => msg.type === "block");

    broadcastBlockIndexed(broadcaster, 123456, new Date("2025-01-15T10:30:00.000Z"));
    const [wildcardMsg, didMsg] = await Promise.all([wildcardPromise, didPromise]);

    expect(wildcardMsg).toMatchObject({ type: "block", block: 123456, events: [] });
    expect(didMsg).toMatchObject({ type: "block", block: 123456, events: [] });

    closeSocket(wildcardWs);
    closeSocket(didWs);
  });

  it("does not deliver envelopes to clients that did not send subscribe", async () => {
    const ws = new WebSocket(WS_URL);
    await waitForOpen(ws);
    await waitForMessage(ws, (msg) => msg.type === "ready");

    let receivedBlock = false;
    ws.on("message", (data) => {
      const message = JSON.parse(data.toString());
      if (message.type === "block") receivedBlock = true;
    });

    broadcastIndexerEvent(broadcaster, makeEvent("did:web:agent.example"));
    await new Promise((resolve) => {
      setTimeout(resolve, 200);
    });

    expect(receivedBlock).toBe(false);
    closeSocket(ws);
  });

  it("cleans up clients on close and error/terminate", async () => {
    const wsClose = await openSubscribed(WS_URL, ["did:web:close.example"]);
    expect(broadcaster.getClientCount()).toBe(1);
    closeSocket(wsClose);
    await waitForClose(wsClose);
    await waitForCondition(() => broadcaster.getClientCount() === 0);
    expect(broadcaster.getClientCount()).toBe(0);

    const wsTerminate = await openSubscribed(WS_URL, ["did:web:error.example"]);
    expect(broadcaster.getClientCount()).toBe(1);
    wsTerminate.terminate();
    await waitForClose(wsTerminate);
    await waitForCondition(() => broadcaster.getClientCount() === 0);
    expect(broadcaster.getClientCount()).toBe(0);
  });
});
