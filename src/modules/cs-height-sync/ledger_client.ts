import { Network } from "../../network";

const CS_GET_PATH = "/verana/cs/v1/get";
const HEIGHT_HEADER = "x-cosmos-block-height";

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
    issuer_perm_management_mode?: string;
    verifier_perm_management_mode?: string;
    archived?: string | null;
    created?: string;
    modified?: string;
    title?: string | null;
    description?: string | null;
    [key: string]: unknown;
  };
  [key: string]: unknown;
}

function normalizeLedgerResponse(data: unknown): LedgerCredentialSchemaResponse | null {
  if (!data || typeof data !== "object") return null;
  const obj = data as Record<string, unknown>;
  const schema =
    obj.schema ??
    obj.credential_schema ??
    obj.CredentialSchema ??
    obj.data;
  if (schema && typeof schema === "object") return { schema: schema as LedgerCredentialSchemaResponse["schema"] };
  return null;
}

export function getLedgerBaseUrl(): string {
  const envLedger = typeof process !== "undefined" && process.env?.LEDGER_LCD_URL;
  const base = (envLedger && String(envLedger).trim()) || Network?.LCD || "";
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
    if (data) return normalizeLedgerResponse(data) ?? (data as LedgerCredentialSchemaResponse);
    if (withHeight && (res.status >= 400 || res.status < 200)) {
      const fallback = await fetch(url);
      const fallbackData = fallback.ok ? await fallback.json().catch(() => null) : null;
      if (fallbackData) return normalizeLedgerResponse(fallbackData) ?? (fallbackData as LedgerCredentialSchemaResponse);
    }
    return null;
  } catch {
    if (withHeight) {
      try {
        const fallback = await fetch(url);
        const fallbackData = fallback.ok ? await fallback.json().catch(() => null) : null;
        if (fallbackData) return normalizeLedgerResponse(fallbackData) ?? (fallbackData as LedgerCredentialSchemaResponse);
      } catch {
        //
      }
    }
    return null;
  }
}
