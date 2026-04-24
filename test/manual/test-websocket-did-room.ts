/* eslint-disable no-console */

import WebSocket from "ws";

type IndexerEventMessage = {
  type: "indexer-event";
  eventType: string;
  did: string;
  blockHeight: number;
  txHash: string;
  timestamp: string;
  payload: Record<string, unknown>;
};

type ConnectedMessage = {
  type: "connected";
  did?: string;
  blockHeight?: number | null;
  timestamp: string;
};

type Message = ConnectedMessage | IndexerEventMessage | Record<string, unknown>;

const baseUrl = process.env.INDEXER_WS_URL || "ws://localhost:3001/verana/indexer/v1/events";
const did = process.env.DID || "did:web:agent.example";
const afterBlockHeight = Number(process.env.AFTER_BLOCK_HEIGHT || 0);
const wsUrl = `${baseUrl}?did=${encodeURIComponent(did)}`;

function httpReplayUrl(blockHeight: number): string {
  const httpBase = (process.env.INDEXER_HTTP_URL || "http://localhost:3001").replace(/\/$/, "");
  return `${httpBase}/verana/indexer/v1/events?did=${encodeURIComponent(did)}&after_block_height=${blockHeight}`;
}

async function replayMissedEvents(blockHeight: number): Promise<void> {
  const url = httpReplayUrl(blockHeight);
  const response = await fetch(url);
  const body = await response.json();
  console.log("Replay response:", JSON.stringify(body, null, 2));
}

const ws = new WebSocket(wsUrl);

ws.on("open", () => {
  console.log(`Connected to DID room: ${did}`);
});

ws.on("message", async (data: WebSocket.Data) => {
  const message = JSON.parse(data.toString()) as Message;
  console.log("Received:", JSON.stringify(message, null, 2));

  if (message.type === "connected") {
    const connectedAt = Number(message.blockHeight ?? afterBlockHeight);
    await replayMissedEvents(afterBlockHeight || connectedAt);
  }

  if (message.type === "indexer-event") {
    console.log(`DID event ${message.eventType} at block ${message.blockHeight}`);
  }
});

ws.on("error", (err: Error) => {
  console.error("WebSocket error:", err.message);
});

ws.on("close", () => {
  console.log("WebSocket connection closed");
});

process.on("SIGINT", () => {
  ws.close();
  process.exit(0);
});

