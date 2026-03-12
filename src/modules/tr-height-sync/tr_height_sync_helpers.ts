import { VeranaTrustRegistryMessageTypes } from "../../common/verana-message-types";
import { Network } from "../../network";

const TR_GET_PATH = "/verana/tr/v1/get";
const HEIGHT_HEADER = "x-cosmos-block-height";

export interface LedgerTrustRegistryVersion {
  version?: number | string;
  created?: string;
  active_since?: string | null;
  activeSince?: string | null;
  documents?: Array<{
    created?: string;
    language?: string | null;
    url?: string | null;
    digest_sri?: string | null;
    digestSri?: string | null;
  }>;
  [key: string]: unknown;
}

export interface LedgerTrustRegistry {
  id?: number | string;
  tr_id?: number | string;
  did?: string;
  controller?: string;
  created?: string;
  modified?: string;
  archived?: string | null;
  deposit?: number | string;
  aka?: string | null;
  language?: string | null;
  active_version?: number | string;
  activeVersion?: number | string;
  versions?: LedgerTrustRegistryVersion[];
  [key: string]: unknown;
}

export interface LedgerTrustRegistryResponse {
  trust_registry?: LedgerTrustRegistry;
  trustRegistry?: LedgerTrustRegistry;
  tr?: LedgerTrustRegistry;
  [key: string]: unknown;
}

export interface TrMessageLike {
  type: string;
  content?: Record<string, unknown> | null;
}

const TR_MESSAGE_TYPES = new Set<string>([
  VeranaTrustRegistryMessageTypes.CreateTrustRegistry,
  VeranaTrustRegistryMessageTypes.CreateTrustRegistryLegacy,
  VeranaTrustRegistryMessageTypes.UpdateTrustRegistry,
  VeranaTrustRegistryMessageTypes.ArchiveTrustRegistry,
  VeranaTrustRegistryMessageTypes.AddGovernanceFrameworkDoc,
  VeranaTrustRegistryMessageTypes.IncreaseGovernanceFrameworkVersion,
  VeranaTrustRegistryMessageTypes.UpdateParams,
]);

function normalizeLedgerResponse(data: unknown): LedgerTrustRegistryResponse | null {
  if (!data || typeof data !== "object") return null;
  const obj = data as Record<string, unknown>;
  const tr =
    obj.trust_registry ??
    obj.trustRegistry ??
    obj.tr ??
    obj.trustRegistryState ??
    obj.data;
  if (tr && typeof tr === "object") {
    return { trust_registry: tr as LedgerTrustRegistry };
  }
  return null;
}

export function getLedgerBaseUrl(): string {
  const envLedger =
    (typeof process !== "undefined" && process.env?.LCD_ENDPOINT?.trim()) || "";
  const base = envLedger || Network?.LCD || "";
  return base.replace(/\/$/, "");
}

export async function getTrustRegistry(
  trId: number,
  blockHeight?: number
): Promise<LedgerTrustRegistryResponse | null> {
  const baseUrl = getLedgerBaseUrl();
  if (!baseUrl) return null;
  const url = `${baseUrl}${TR_GET_PATH}/${trId}`;
  const withHeight = typeof blockHeight === "number" && blockHeight > 0;
  const headers: Record<string, string> = {};
  if (withHeight) headers[HEIGHT_HEADER] = String(blockHeight);

  try {
    const res = await fetch(url, { headers });
    const data = res.ok ? await res.json().catch(() => null) : null;
    if (data) {
      return normalizeLedgerResponse(data) ?? (data as LedgerTrustRegistryResponse);
    }

    if (withHeight && (res.status >= 400 || res.status < 200)) {
      const fallback = await fetch(url);
      const fallbackData = fallback.ok ? await fallback.json().catch(() => null) : null;
      if (fallbackData) {
        return (
          normalizeLedgerResponse(fallbackData) ??
          (fallbackData as LedgerTrustRegistryResponse)
        );
      }
    }

    return null;
  } catch {
    if (withHeight) {
      try {
        const fallback = await fetch(url);
        const fallbackData = fallback.ok ? await fallback.json().catch(() => null) : null;
        if (fallbackData) {
          return (
            normalizeLedgerResponse(fallbackData) ??
            (fallbackData as LedgerTrustRegistryResponse)
          );
        }
      } catch {
        //
      }
    }
    return null;
  }
}

export function isTrMessageType(type: string): boolean {
  return TR_MESSAGE_TYPES.has(type);
}

export function extractTrustRegistryIdFromContent(
  content: Record<string, unknown> | null | undefined
): number | null {
  if (!content || typeof content !== "object") return null;
  const raw = content.trust_registry_id ?? content.id;
  if (raw === undefined || raw === null) return null;
  const n = Number(raw);
  return Number.isInteger(n) && n > 0 ? n : null;
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

const TR_EVENT_TYPES = new Set<string>([
  "create_trust_registry",
  "create_governance_framework_version",
  "create_governance_framework_document",
  "add_governance_framework_document",
  "increase_active_gf_version",
  "update_trust_registry",
  "archive_trust_registry",
]);

export function extractTrustRegistryIdsFromEvents(
  events: TxEventLike[],
  decodeAttributes?: boolean
): number[] {
  const ids: number[] = [];
  for (const ev of events) {
    const eventType = (ev.type ?? "").toLowerCase();
    if (!TR_EVENT_TYPES.has(eventType)) continue;

    const attrs = getDecodedEventAttributes(ev, decodeAttributes);
    if (attrs.length === 0) continue;

    for (const attr of attrs) {
      const keyLower = attr.key.toLowerCase();
      if (keyLower === "trust_registry_id") {
        const n = Number(attr.value);
        if (Number.isInteger(n) && n > 0) {
          ids.push(n);
        }
      }
    }
  }
  return [...new Set(ids)];
}

