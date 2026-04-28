import { VeranaCredentialSchemaMessageTypes } from "../../common/verana-message-types";
import { Network } from "../../network";
import { CS_EVENT_TYPES } from "../../services/crawl-cs/cs_event_mapper";

const CS_GET_PATH = "/verana/cs/v1/get";
const HEIGHT_HEADER = "x-cosmos-block-height";

const CS_MESSAGE_TYPES = new Set<string>([
  VeranaCredentialSchemaMessageTypes.CreateCredentialSchema,
  VeranaCredentialSchemaMessageTypes.UpdateCredentialSchema,
  VeranaCredentialSchemaMessageTypes.ArchiveCredentialSchema,
  VeranaCredentialSchemaMessageTypes.UpdateParams,
]);

export interface LedgerCredentialSchemaResponse {
  schema?: {
    id?: number;
    tr_id?: number;
    json_schema?: string | object;
    deposit?: number;
    issuer_grantor_validation_validity_period?: number;
    verifier_grantor_validation_validity_period?: number;
    issuer_validation_validity_period?: number;
    verifier_validation_validity_period?: number;
    holder_validation_validity_period?: number;
    issuer_onboarding_mode?: string;
    verifier_onboarding_mode?: string;
    holder_onboarding_mode?: string | null;
    pricing_asset_type?: string | null;
    pricing_asset?: string | null;
    digest_algorithm?: string | null;
    archived?: string | null;
    created?: string;
    modified?: string;
    title?: string | null;
    description?: string | null;
    [key: string]: unknown;
  };
  [key: string]: unknown;
}

export interface CsMessageLike {
  type: string;
  content?: Record<string, unknown> | null;
}

export interface TxEventLike {
  type?: string;
  attributes?: Array<{ key?: string; value?: string }>;
}


export function normalizeCredentialSchemaV4LedgerFields(
  schema: Record<string, unknown>
): Record<string, unknown> {
  const out = { ...schema };
  const pair = (snake: string, camel: string) => {
    const s = out[snake];
    const c = out[camel];
    if (s == null && c == null) return;
    const raw = s != null && s !== "" ? s : c;
    if (raw == null || raw === "") return;
    const str = String(raw);
    out[snake] = str;
    out[camel] = str;
  };
  pair("holder_onboarding_mode", "holderOnboardingMode");
  pair("pricing_asset_type", "pricingAssetType");
  pair("pricing_asset", "pricingAsset");
  pair("digest_algorithm", "digestAlgorithm");
  return out;
}

function normalizeLedgerResponse(data: unknown): LedgerCredentialSchemaResponse | null {
  if (!data || typeof data !== "object") return null;
  const obj = data as Record<string, unknown>;
  const schema =
    obj.schema ??
    obj.credential_schema ??
    obj.CredentialSchema ??
    obj.data;
  if (schema && typeof schema === "object") {
    const s = schema as Record<string, unknown>;
    const normalized = normalizeCredentialSchemaV4LedgerFields(s);
    return { schema: normalized as LedgerCredentialSchemaResponse["schema"] };
  }
  if (obj.id != null && (obj.json_schema != null || obj.jsonSchema != null)) {
    const normalized = normalizeCredentialSchemaV4LedgerFields(obj);
    return { schema: normalized as LedgerCredentialSchemaResponse["schema"] };
  }
  return null;
}

function applyLedgerV4Normalization(
  res: LedgerCredentialSchemaResponse | null | undefined
): LedgerCredentialSchemaResponse | null | undefined {
  if (!res?.schema || typeof res.schema !== "object") return res;
  res.schema = normalizeCredentialSchemaV4LedgerFields(
    res.schema as Record<string, unknown>
  ) as LedgerCredentialSchemaResponse["schema"];
  return res;
}

export function getLedgerBaseUrl(): string {
  const envLedger =
    (typeof process !== "undefined" && process.env?.LCD_ENDPOINT?.trim()) || "";
  const base = envLedger || Network?.LCD || "";
  return base.replace(/\/$/, "");
}

export async function getCredentialSchema(
  id: number,
  blockHeight?: number
): Promise<LedgerCredentialSchemaResponse | null> {
  const baseUrl = getLedgerBaseUrl();
  if (!baseUrl) return null;
  const url = `${baseUrl}${CS_GET_PATH}/${id}`;
  const withHeight = typeof blockHeight === "number" && blockHeight > 0;
  const headers: Record<string, string> = {};
  if (withHeight) headers[HEIGHT_HEADER] = String(blockHeight);

  try {
    const res = await fetch(url, { headers });
    const data = res.ok ? await res.json().catch(() => null) : null;
    if (data) {
      return applyLedgerV4Normalization(
        normalizeLedgerResponse(data) ?? (data as LedgerCredentialSchemaResponse)
      ) ?? null;
    }

    if (withHeight && (res.status >= 400 || res.status < 200)) {
      const fallback = await fetch(url);
      const fallbackData = fallback.ok ? await fallback.json().catch(() => null) : null;
      if (fallbackData) {
        return applyLedgerV4Normalization(
          normalizeLedgerResponse(fallbackData) ?? (fallbackData as LedgerCredentialSchemaResponse)
        ) ?? null;
      }
    }

    return null;
  } catch {
    if (withHeight) {
      try {
        const fallback = await fetch(url);
        const fallbackData = fallback.ok ? await fallback.json().catch(() => null) : null;
        if (fallbackData) {
          return applyLedgerV4Normalization(
            normalizeLedgerResponse(fallbackData) ?? (fallbackData as LedgerCredentialSchemaResponse)
          ) ?? null;
        }
      } catch {
        //
      }
    }
    return null;
  }
}

export function isCsMessageType(type: string): boolean {
  return CS_MESSAGE_TYPES.has(type);
}

export function extractCredentialSchemaIdFromContent(
  content: Record<string, unknown> | null | undefined
): number | null {
  if (!content || typeof content !== "object") return null;
  const raw = content.id ?? content.credential_schema_id;
  if (raw === undefined || raw === null) return null;
  const n = Number(raw);
  return Number.isInteger(n) && n > 0 ? n : null;
}

export function extractImpactedCredentialSchemaIdsFromMessages(messages: CsMessageLike[]): number[] {
  const ids: number[] = [];
  for (const msg of messages) {
    if (!isCsMessageType(msg.type)) continue;
    if (msg.type === VeranaCredentialSchemaMessageTypes.UpdateParams) continue;
    const id = extractCredentialSchemaIdFromContent(msg.content ?? undefined);
    if (id !== null) ids.push(id);
  }
  return [...new Set(ids)];
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

export function isPotentialCredentialSchemaEvent(
  event: TxEventLike,
  decodeAttributes?: boolean
): boolean {
  const eventType = (event.type ?? "").toLowerCase();
  if (
    eventType.includes("credential_schema") ||
    (CS_EVENT_TYPES as readonly string[]).includes(eventType)
  ) {
    return true;
  }

  const attrs = getDecodedEventAttributes(event, decodeAttributes);
  if (attrs.length === 0) return false;

  for (const attr of attrs) {
    const key = attr.key.toLowerCase();
    const value = attr.value.toLowerCase();

    if (key.includes("credential_schema")) {
      return true;
    }

    if (
      (key === "action" || key === "msg_type" || key === "module" || key === "message.module") &&
      (
        value.includes("credentialschema") ||
        value.includes("credential_schema") ||
        value.includes("verana.cs") ||
        value.includes("/verana.cs.v1.")
      )
    ) {
      return true;
    }
  }

  return false;
}

export function extractCredentialSchemaIdsFromEvents(
  events: TxEventLike[],
  decodeAttributes?: boolean
): number[] {
  const ids: number[] = [];
  for (const ev of events) {
    const attrs = getDecodedEventAttributes(ev, decodeAttributes);
    if (attrs.length === 0) continue;
    if (!isPotentialCredentialSchemaEvent(ev, decodeAttributes)) continue;
    for (const attr of attrs) {
      const keyLower = attr.key.toLowerCase();
      if (keyLower.includes("credential_schema_id") || keyLower === "id") {
        const n = Number(attr.value);
        if (Number.isInteger(n) && n > 0) ids.push(n);
      }
    }
  }
  return [...new Set(ids)];
}

export function extractImpactedCredentialSchemaIds(
  messages: CsMessageLike[],
  events?: TxEventLike[],
  decodeEventAttributes?: boolean
): number[] {
  const fromMessages = extractImpactedCredentialSchemaIdsFromMessages(messages);
  const fromEvents = events?.length
    ? extractCredentialSchemaIdsFromEvents(events, decodeEventAttributes)
    : [];
  return [...new Set([...fromEvents, ...fromMessages])];
}

function normalizeJsonSchema(js: unknown): object | null {
  if (js == null) return null;
  if (typeof js === "object") return js as object;
  if (typeof js === "string") {
    try {
      const parsed = JSON.parse(js);
      return typeof parsed === "object" && parsed !== null ? parsed : null;
    } catch {
      return null;
    }
  }
  return null;
}

export function extractTitleDescriptionFromJsonSchema(
  jsonSchema: unknown
): { title: string | null; description: string | null } {
  const obj = normalizeJsonSchema(jsonSchema);
  if (!obj || typeof obj !== "object") return { title: null, description: null };
  const o = obj as Record<string, unknown>;
  const title = typeof o.title === "string" ? o.title : null;
  const description = typeof o.description === "string" ? o.description : null;
  return { title, description };
}
