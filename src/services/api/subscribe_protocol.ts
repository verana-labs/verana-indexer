import { isValidDid } from "./api_shared";
import { uniqueNormalizedDids } from "./indexer_event_utils";
import type { IndexerEventRecord } from "./indexer_events_query";

export const CHAIN_BLOCK_INTERVAL_MS = 6000;

export type SubscribeControl = {
  action: "subscribe";
  dids: string[] | null;
  corporationId: number | null;
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

  const corporationIdResult = parseCorporationId((json as Record<string, unknown>).corporationId);
  if (!corporationIdResult.ok) {
    return { ok: false, error: corporationIdResult.error };
  }
  const corporationId = corporationIdResult.value;

  const rawDids = (json as Record<string, unknown>).dids;
  if (rawDids === undefined || rawDids === null) {
    return { ok: true, message: { action: "subscribe", dids: null, corporationId } };
  }

  if (!Array.isArray(rawDids)) {
    return { ok: false, error: "'dids' must be an array of DID strings" };
  }

  if (rawDids.length === 0) {
    return { ok: true, message: { action: "subscribe", dids: null, corporationId } };
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

  return { ok: true, message: { action: "subscribe", dids: normalized, corporationId } };
}

function parseCorporationId(
  raw: unknown
): { ok: true; value: number | null } | { ok: false; error: string } {
  if (raw === undefined || raw === null) return { ok: true, value: null };
  const n = typeof raw === "number" ? raw : Number(raw);
  if (!Number.isInteger(n) || n <= 0) {
    return { ok: false, error: `'corporationId' must be a positive integer; got ${String(raw)}` };
  }
  return { ok: true, value: n };
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
  didFilter: Set<string> | null,
  corporationId: number | null
): BlockEnvelope {
  if (didFilter === null && corporationId === null) {
    return { type: "block", block, blockTime, events };
  }

  const filtered = events.filter((event) => {
    const matchesDids = didFilter === null || eventMatchesDids(event, didFilter);
    const matchesCorporation =
      corporationId === null || eventMatchesCorporation(event, corporationId);
    return matchesDids && matchesCorporation;
  });

  return { type: "block", block, blockTime, events: filtered };
}

function eventMatchesDids(event: IndexerEventRecord, didFilter: Set<string>): boolean {
  if (didFilter.has(event.did)) return true;
  const related = event.payload?.related_dids ?? [];
  for (const candidate of related) {
    if (didFilter.has(candidate)) return true;
  }
  return false;
}

function eventMatchesCorporation(event: IndexerEventRecord, corporationId: number): boolean {
  if (event.payload?.corporation_id === corporationId) return true;
  const related = event.payload?.related_corporation_ids ?? [];
  return related.includes(corporationId);
}
