import { VeranaTrustRegistryMessageTypes } from "../../common/verana-message-types";
import { Network } from "../../network";

const TR_GET_PATH = "/verana/tr/v1/get";
const HEIGHT_HEADER = "x-cosmos-block-height";

export interface LedgerTrustRegistryVersion {
  id?: number | string;
  version?: number | string;
  created?: string;
  active_since?: string | null;
  activeSince?: string | null;
  documents?: Array<{
    id?: number | string;
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
  if (!baseUrl) {
    throw new Error(
      `[TR Height-Sync] Missing LCD base URL. Please set LCD_ENDPOINT or Network.LCD.`
    );
  }
  const url = `${baseUrl}${TR_GET_PATH}/${trId}`;
  const withHeight = typeof blockHeight === "number" && blockHeight > 0;
  const headers: Record<string, string> = {};
  if (withHeight) headers[HEIGHT_HEADER] = String(blockHeight);

  const requestOnce = async (
    useHeightHeader: boolean
  ): Promise<{
    payload: LedgerTrustRegistryResponse | null;
    status: number | null;
    bodySnippet: string;
    errorMessage: string | null;
  }> => {
    const reqHeaders = useHeightHeader ? headers : {};
    try {
      const res = await fetch(url, { headers: reqHeaders });
      const bodyText = await res.text().catch(() => "");
      const bodySnippet = bodyText.slice(0, 300);
      if (!res.ok) {
        return {
          payload: null,
          status: res.status,
          bodySnippet,
          errorMessage: `HTTP ${res.status}`,
        };
      }
      const data = bodyText ? JSON.parse(bodyText) : null;
      if (!data) {
        return {
          payload: null,
          status: res.status,
          bodySnippet,
          errorMessage: "Empty JSON response body",
        };
      }
      return {
        payload: normalizeLedgerResponse(data) ?? (data as LedgerTrustRegistryResponse),
        status: res.status,
        bodySnippet,
        errorMessage: null,
      };
    } catch (err: any) {
      return {
        payload: null,
        status: null,
        bodySnippet: "",
        errorMessage: err?.message || String(err),
      };
    }
  };

  const firstAttempt = await requestOnce(withHeight);
  if (firstAttempt.payload) {
    return firstAttempt.payload;
  }

  if (withHeight) {
    const fallbackAttempt = await requestOnce(false);
    if (fallbackAttempt.payload) {
      return fallbackAttempt.payload;
    }
    throw new Error(
      `[TR Height-Sync] getTrustRegistry failed for trId=${trId}, height=${blockHeight}, ` +
      `with-height(${firstAttempt.errorMessage || "unknown"}, status=${String(firstAttempt.status)}), ` +
      `fallback(${fallbackAttempt.errorMessage || "unknown"}, status=${String(fallbackAttempt.status)}), ` +
      `with-height-body="${firstAttempt.bodySnippet}", fallback-body="${fallbackAttempt.bodySnippet}"`
    );
  }

  throw new Error(
    `[TR Height-Sync] getTrustRegistry failed for trId=${trId}, ` +
    `${firstAttempt.errorMessage || "unknown"} (status=${String(firstAttempt.status)}), ` +
    `body="${firstAttempt.bodySnippet}"`
  );
}

export function isTrMessageType(type: string): boolean {
  return TR_MESSAGE_TYPES.has(type);
}

export function extractTrustRegistryIdFromContent(
  content: Record<string, unknown> | null | undefined
): number | null {
  if (!content || typeof content !== "object") return null;
  const candidates = [
    content.trust_registry_id,
    content.trustRegistryId,
    content.tr_id,
    content.trId,
    content.id,
    (content.trust_registry as Record<string, unknown> | undefined)?.id,
    (content.trust_registry as Record<string, unknown> | undefined)?.tr_id,
    (content.trust_registry as Record<string, unknown> | undefined)?.trId,
    (content.trustRegistry as Record<string, unknown> | undefined)?.id,
    (content.trustRegistry as Record<string, unknown> | undefined)?.tr_id,
    (content.trustRegistry as Record<string, unknown> | undefined)?.trId,
  ];
  for (const raw of candidates) {
    const n = Number(raw);
    if (Number.isInteger(n) && n > 0) return n;
  }
  return null;
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

