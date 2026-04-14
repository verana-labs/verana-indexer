import { Network } from "../../network";
import { TrustDepositEventType } from "../../common/constant";
import { VeranaTrustDepositMessageTypes } from "../../common/verana-message-types";

const TD_GET_PATH = "/verana/td/v1/get";
const HEIGHT_HEADER = "x-cosmos-block-height";

const TD_MESSAGE_TYPES = new Set<string>([
  VeranaTrustDepositMessageTypes.UpdateParams,
  VeranaTrustDepositMessageTypes.AdjustTrustDeposit,
  VeranaTrustDepositMessageTypes.ReclaimYield,
  VeranaTrustDepositMessageTypes.RepaySlashed,
  VeranaTrustDepositMessageTypes.SlashTrustDeposit,
  VeranaTrustDepositMessageTypes.BurnEcosystemSlashedTrustDeposit,
]);

export const TD_BLOCKCHAIN_EVENT_TYPES = new Set<string>(
  Object.values(TrustDepositEventType) as string[]
);

export interface LedgerTrustDepositResponse {
  corporation?: string;
  account?: string;
  deposit?: number | string;
  amount?: number | string;
  share?: number | string;
  claimable?: number | string;
  slashed_deposit?: number | string;
  slashedDeposit?: number | string;
  repaid_deposit?: number | string;
  repaidDeposit?: number | string;
  last_slashed?: string | null;
  lastSlashed?: string | null;
  last_repaid?: string | null;
  lastRepaid?: string | null;
  slash_count?: number | string;
  slashCount?: number | string;
  [key: string]: unknown;
}

export interface NormalizedLedgerTrustDeposit {
  corporation: string;
  deposit: number;
  share: number;
  claimable: number;
  slashed_deposit: number;
  repaid_deposit: number;
  last_slashed: string | null;
  last_repaid: string | null;
  slash_count: number;
}

export interface TdMessageLike {
  type: string;
  content?: Record<string, unknown> | null;
  txEvents?: TxEventLike[];
}

export interface TxEventLike {
  type?: string;
  attributes?: Array<{ key?: string; value?: string }>;
}

function decodeAttributeValue(value: string | undefined): string {
  if (value == null || value === "") return "";
  try {
    if (/^[A-Za-z0-9+/=]+$/.test(value) && value.length % 4 === 0) {
      return Buffer.from(value, "base64").toString("utf-8");
    }
  } catch {
    //
  }
  return value;
}

function decodeAttributeKey(key: string | undefined): string {
  if (key == null || key === "") return "";
  try {
    if (/^[A-Za-z0-9+/=]+$/.test(key) && key.length % 4 === 0) {
      return Buffer.from(key, "base64").toString("utf-8");
    }
  } catch {
    //
  }
  return key;
}

function getDecodedEventAttributes(
  event: TxEventLike,
  decodeAttributes?: boolean
): Array<{ key: string; value: string }> {
  return (event.attributes ?? []).map((attr) => ({
    key: decodeAttributes ? decodeAttributeKey(attr.key) : (attr.key ?? ""),
    value: decodeAttributes ? decodeAttributeValue(attr.value) : (attr.value ?? ""),
  }));
}

const TD_EVENT_ATTR_KEYS = [
  "trust_deposit_id",
  "account",
  "owner",
  "deposit_account",
];

function isPotentialTrustDepositEvent(
  event: TxEventLike,
  decodeAttributes?: boolean
): boolean {
  const eventType = (event.type ?? "").trim();
  if (TD_BLOCKCHAIN_EVENT_TYPES.has(eventType)) return true;
  const eventTypeLower = eventType.toLowerCase();
  if (
    eventTypeLower.includes("trust_deposit") ||
    eventTypeLower.includes("trustdeposit") ||
    eventTypeLower.includes("verana.td")
  ) {
    return true;
  }
  const attrs = getDecodedEventAttributes(event, decodeAttributes);
  for (const attr of attrs) {
    const keyLower = attr.key.toLowerCase();
    if (TD_EVENT_ATTR_KEYS.some((k) => keyLower.includes(k.toLowerCase()))) {
      return true;
    }
  }
  return false;
}


export function blockchainEventTypeToHistoryEventType(blockchainEventType: string): string {
  switch (blockchainEventType) {
    case TrustDepositEventType.SlashTrustDeposit:
      return "SLASH_TRUST_DEPOSIT";
    case TrustDepositEventType.RepaySlashedTrustDeposit:
      return "REPAY_SLASHED";
    case TrustDepositEventType.ReclaimTrustDepositYield:
      return "RECLAIM_YIELD";
    case TrustDepositEventType.ReclaimTrustDeposit:
      return "RECLAIM_DEPOSIT";
    case TrustDepositEventType.AdjustTrustDeposit:
      return "ADJUST_TRUST_DEPOSIT";
    case TrustDepositEventType.YieldDistribution:
      return "YIELD_DISTRIBUTION";
    case TrustDepositEventType.YieldTransfer:
      return "YIELD_TRANSFER";
    default:
      return "SYNC_LEDGER";
  }
}

export function getTdLedgerBaseUrl(): string {
  const envLedger =
    (typeof process !== "undefined" && process.env?.LCD_ENDPOINT?.trim()) || "";
  const base = envLedger || Network?.LCD || "";
  return base.replace(/\/$/, "");
}

export function normalizeLedgerResponse(data: unknown): NormalizedLedgerTrustDeposit | null {
  if (!data || typeof data !== "object") return null;
  const obj = data as Record<string, unknown>;
  const raw =
    obj.trust_deposit ??
    obj.trustDeposit ??
    obj.deposit ??
    obj.data;
  if (!raw || typeof raw !== "object") return null;
  const r = raw as LedgerTrustDepositResponse;
  const corporation = String(r.corporation ?? r.account ?? "").trim();
  if (!corporation) return null;
  return {
    corporation,
    deposit: Number(r.deposit ?? r.amount ?? 0),
    share: Number(r.share ?? 0),
    claimable: Number(r.claimable ?? 0),
    slashed_deposit: Number(r.slashed_deposit ?? r.slashedDeposit ?? 0),
    repaid_deposit: Number(r.repaid_deposit ?? r.repaidDeposit ?? 0),
    last_slashed: r.last_slashed ?? r.lastSlashed ?? null,
    last_repaid: r.last_repaid ?? r.lastRepaid ?? null,
    slash_count: Number(r.slash_count ?? r.slashCount ?? 0),
  };
}

export async function fetchTrustDeposit(
  id: string,
  blockHeight?: number
): Promise<NormalizedLedgerTrustDeposit | null> {
  const baseUrl = getTdLedgerBaseUrl();
  if (!baseUrl) return null;
  const encodedId = encodeURIComponent(id);
  const url = `${baseUrl}${TD_GET_PATH}/${encodedId}`;
  const withHeight = typeof blockHeight === "number" && blockHeight > 0;
  const headers: Record<string, string> = {};
  if (withHeight) headers[HEIGHT_HEADER] = String(blockHeight);

  try {
    const res = await fetch(url, { headers });
    const data = res.ok ? await res.json().catch(() => null) : null;
    if (data) {
      const normalized = normalizeLedgerResponse(data);
      if (normalized) return normalized;
      return data as NormalizedLedgerTrustDeposit;
    }
    if (withHeight && (res.status >= 400 || res.status < 200)) {
      const fallback = await fetch(url);
      const fallbackData = fallback.ok ? await fallback.json().catch(() => null) : null;
      if (fallbackData) {
        const normalized = normalizeLedgerResponse(fallbackData);
        if (normalized) return normalized;
        return fallbackData as NormalizedLedgerTrustDeposit;
      }
    }
    return null;
  } catch {
    if (withHeight) {
      try {
        const fallback = await fetch(url);
        const fallbackData = fallback.ok ? await fallback.json().catch(() => null) : null;
        if (fallbackData) {
          const normalized = normalizeLedgerResponse(fallbackData);
          if (normalized) return normalized;
          return fallbackData as NormalizedLedgerTrustDeposit;
        }
      } catch {
        //
      }
    }
    return null;
  }
}

export function isTdMessageType(type: string): boolean {
  return TD_MESSAGE_TYPES.has(type);
}

export function extractTrustDepositIdsFromMessageContent(
  content: Record<string, unknown> | null | undefined
): string[] {
  if (!content || typeof content !== "object") return [];
  const ids: string[] = [];
  const candidates = [
    content.corporation,
    content.account,
    content.deposit_account,
    content.depositAccount,
    content.owner,
    content.trust_deposit_id,
    content.trustDepositId,
  ];
  for (const raw of candidates) {
    if (raw === null || raw === undefined) continue;
    const s = String(raw).trim();
    if (s.length > 0) ids.push(s);
  }
  return [...new Set(ids)];
}

export function extractTrustDepositIdsFromEvents(
  events: TxEventLike[],
  decodeAttributes?: boolean
): string[] {
  const ids: string[] = [];
  for (const ev of events) {
    const attrs = getDecodedEventAttributes(ev, decodeAttributes);
    if (attrs.length === 0) continue;
    if (!isPotentialTrustDepositEvent(ev, decodeAttributes)) continue;
    for (const attr of attrs) {
      const keyLower = attr.key.toLowerCase();
      const value = attr.value?.trim() ?? "";
      if (value.length === 0) continue;
      if (
        keyLower.includes("trust_deposit_id") ||
        keyLower.includes("account") ||
        keyLower.includes("owner") ||
        keyLower.includes("deposit_account")
      ) {
        const num = Number(value);
        if (Number.isInteger(num) && num > 0) {
          ids.push(String(num));
        } else {
          ids.push(value);
        }
      }
    }
  }
  return [...new Set(ids)];
}

export function extractImpactedTrustDepositIds(
  messages: TdMessageLike[],
  events?: TxEventLike[],
  decodeEventAttributes?: boolean
): string[] {
  const fromMessages: string[] = [];
  for (const msg of messages) {
    if (!isTdMessageType(msg.type)) continue;
    if (msg.type === VeranaTrustDepositMessageTypes.UpdateParams) continue;
    const fromContent = extractTrustDepositIdsFromMessageContent(msg.content ?? undefined);
    fromMessages.push(...fromContent);
    if (Array.isArray(msg.txEvents) && msg.txEvents.length > 0) {
      fromMessages.push(
        ...extractTrustDepositIdsFromEvents(msg.txEvents, decodeEventAttributes ?? true)
      );
    }
  }
  const fromEvents =
    events?.length ? extractTrustDepositIdsFromEvents(events, decodeEventAttributes ?? true) : [];
  return [...new Set([...fromMessages, ...fromEvents])];
}

/**
 * Builds a map from trust deposit ID to the event type to store in history.
 * Each ID is assigned the event type from the first message that references it.
 * IDs only present in events (not in any message) are not in the map; callers should use "SYNC_LEDGER" for those.
 */
export function buildDepositIdToEventTypeMap(
  messages: TdMessageLike[],
  decodeEventAttributes: boolean = true
): Map<string, string> {
  const map = new Map<string, string>();
  for (const msg of messages) {
    if (!isTdMessageType(msg.type)) continue;
    if (msg.type === VeranaTrustDepositMessageTypes.UpdateParams) continue;
    const ids: string[] = [];
    ids.push(...extractTrustDepositIdsFromMessageContent(msg.content ?? undefined));
    if (Array.isArray(msg.txEvents) && msg.txEvents.length > 0) {
      ids.push(...extractTrustDepositIdsFromEvents(msg.txEvents, decodeEventAttributes));
    }
    const eventType = messageTypeToEventType(msg.type);
    for (const id of [...new Set(ids)]) {
      if (!map.has(id)) map.set(id, eventType);
    }
  }
  return map;
}

export function buildDepositIdToEventTypeMapFromEvents(
  events: TxEventLike[],
  decodeEventAttributes: boolean = true
): Map<string, string> {
  const map = new Map<string, string>();
  for (const ev of events) {
    if (!isPotentialTrustDepositEvent(ev, decodeEventAttributes)) continue;
    const historyEventType = blockchainEventTypeToHistoryEventType(ev.type ?? "");
    const attrs = getDecodedEventAttributes(ev, decodeEventAttributes);
    const ids: string[] = [];
    for (const attr of attrs) {
      const keyLower = attr.key.toLowerCase();
      const value = attr.value?.trim() ?? "";
      if (value.length === 0) continue;
      if (
        keyLower.includes("trust_deposit_id") ||
        keyLower.includes("account") ||
        keyLower.includes("owner") ||
        keyLower.includes("deposit_account")
      ) {
        const num = Number(value);
        if (Number.isInteger(num) && num > 0) {
          ids.push(String(num));
        } else {
          ids.push(value);
        }
      }
    }
    for (const id of [...new Set(ids)]) {
      if (!map.has(id)) map.set(id, historyEventType);
    }
  }
  return map;
}

export function messageTypeToEventType(msgType: string): string {
  switch (msgType) {
    case VeranaTrustDepositMessageTypes.AdjustTrustDeposit:
      return "ADJUST_TRUST_DEPOSIT";
    case VeranaTrustDepositMessageTypes.ReclaimYield:
      return "RECLAIM_YIELD";
    case VeranaTrustDepositMessageTypes.RepaySlashed:
      return "REPAY_SLASHED";
    case VeranaTrustDepositMessageTypes.SlashTrustDeposit:
      return "SLASH_TRUST_DEPOSIT";
    case VeranaTrustDepositMessageTypes.BurnEcosystemSlashedTrustDeposit:
      return "BURN_ECOSYSTEM_SLASHED_TRUST_DEPOSIT";
    case VeranaTrustDepositMessageTypes.UpdateParams:
      return "UPDATE_PARAMS";
    default:
      return "SYNC_LEDGER";
  }
}
