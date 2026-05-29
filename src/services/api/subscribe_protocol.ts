import { isValidDid } from "./api_shared";
import { uniqueNormalizedDids } from "./indexer_event_utils";
import type { IndexerEventRecord } from "./indexer_events_query";

export const CHAIN_BLOCK_INTERVAL_MS = 6000;

export type SubscribeControl = {
  action: "subscribe";
  dids: string[] | null;
  corporationId?: number; // TODO: Currently ignored, but must be implemented once we have update @verana-labs/verana-types
};

export type UnsubscribeControl = {
  action: "unsubscribe";
};

export type ControlMessage = SubscribeControl | UnsubscribeControl;

export type ReadyMessage = {
  type: "ready";
  block: number;
  blockTime: string;
  blockIntervalMs: number;
};

export type BlockEnvelope = {
  type: "block";
  block: number;
  blockTime: string;
  events: IndexerEventRecord[];
};

export type ControlParseResult =
  | { ok: true; message: ControlMessage }
  | { ok: false; error: string };

export function parseControlMessage(raw: string): ControlParseResult {
  let json: unknown;
  try {
    json = JSON.parse(raw);
  } catch {
    return { ok: false, error: "Invalid JSON" };
  }

  if (!json || typeof json !== "object") {
    return { ok: false, error: "Control message must be an object" };
  }

  const action = (json as Record<string, unknown>).action;
  if (action !== "subscribe" && action !== "unsubscribe") {
    return { ok: false, error: "Unknown action. Expected 'subscribe' or 'unsubscribe'" };
  }

  if (action === "unsubscribe") {
    return { ok: true, message: { action: "unsubscribe" } };
  }

  const rawDids = (json as Record<string, unknown>).dids;
  if (rawDids === undefined || rawDids === null) {
    return { ok: true, message: { action: "subscribe", dids: null } };
  }

  if (!Array.isArray(rawDids)) {
    return { ok: false, error: "'dids' must be an array of DID strings" };
  }

  if (rawDids.length === 0) {
    return { ok: true, message: { action: "subscribe", dids: null } };
  }

  for (const candidate of rawDids) {
    if (!isValidDid(candidate)) {
      return { ok: false, error: `Invalid DID in 'dids': ${String(candidate)}` };
    }
  }

  const normalized = uniqueNormalizedDids(rawDids);
  if (normalized.length === 0) {
    return { ok: false, error: "No valid DIDs after normalization" };
  }

  return { ok: true, message: { action: "subscribe", dids: normalized } };
}

export function buildReadyMessage(lastProcessedBlock: number, lastBlockTime: string): ReadyMessage {
  return {
    type: "ready",
    block: lastProcessedBlock + 1,
    blockTime: lastBlockTime,
    blockIntervalMs: CHAIN_BLOCK_INTERVAL_MS,
  };
}

export function buildBlockEnvelope(
  block: number,
  blockTime: string,
  events: IndexerEventRecord[],
  filter: Set<string> | null
): BlockEnvelope {
  if (filter === null) {
    return { type: "block", block, blockTime, events };
  }

  const filtered = events.filter((event) => {
    if (filter.has(event.did)) return true;
    const related = event.payload?.related_dids ?? [];
    for (const candidate of related) {
      if (filter.has(candidate)) return true;
    }
    return false;
  });

  return { type: "block", block, blockTime, events: filtered };
}
