export const CS_EVENT_TYPES = [
  "create_credential_schema",
  "update_credential_schema",
  "archive_credential_schema",
] as const;

export function getDeterministicEvent(
  events: any[] | undefined,
) {
  if (!Array.isArray(events) || events.length === 0) return null;

  const filtered = events.filter(
    (event) =>
      typeof event?.type === 'string' &&
      CS_EVENT_TYPES.includes(event.type)
  );

  if (filtered.length === 0) return null;

  if (filtered.length === 1) return filtered[0];

  const normalize = (event: any) =>
    JSON.stringify(
      Object.fromEntries(
        (event.attributes ?? []).map((a: any) => [a.key, a.value])
      )
    );

  const firstNormalized = normalize(filtered[0]);

  for (let i = 1; i < filtered.length; i++) {
    if (normalize(filtered[i]) !== firstNormalized) {
      return filtered[0]; 
    }
  }

  return filtered[0];
}

type CSType = "create" | "update" | "archive";

export interface CredentialSchemaEventData {
  type: CSType;
  id: number;
  tr_id: number;
  issuer_grantor_validation_validity_period: number;
  verifier_grantor_validation_validity_period: number;
  issuer_validation_validity_period: number;
  verifier_validation_validity_period: number;
  holder_validation_validity_period: number;
  issuer_onboarding_mode: string;
  verifier_onboarding_mode: string;
  archived: Date | null;
  modified: Date;
}

function getAttr(attributeMap: Record<string, string>, key: string): string {
  return attributeMap[key] ?? "";
}

function num(val: string): number {
  const n = Number(val);
  return Number.isFinite(n) ? n : 0;
}

function toDate(val: string): Date | null {
  if (!val) return null;
  const d = new Date(val);
  return Number.isFinite(d.getTime()) ? d : null;
}

export function parseCredentialSchemaEvent(
  txResponse: { events?: Array<{ type: string; attributes?: Array<{ key: string; value: string }> }>; logs?: Array<{ events?: Array<{ type: string; attributes?: Array<{ key: string; value: string }> }> }> } | null,
  messageIndex?: number
): CredentialSchemaEventData | null {
  if (!txResponse?.events && !txResponse?.logs?.length) return null;
  let events: Array<{ type: string; attributes?: Array<{ key: string; value: string }> }> = [];
  if (typeof messageIndex === "number" && txResponse.logs?.[messageIndex]?.events) {
    events = txResponse.logs[messageIndex].events ?? [];
  } else if (txResponse.events?.length) {
    events = txResponse.events;
  }
  const event = events.find((e) => CS_EVENT_TYPES.includes(e.type as any));
  if (!event?.attributes?.length) return null;
  const attributeMap: Record<string, string> = {};
  for (const a of event.attributes) {
    attributeMap[a.key] = a.value ?? "";
  }
  const typeMap: Record<string, CSType> = {
    create_credential_schema: "create",
    update_credential_schema: "update",
    archive_credential_schema: "archive",
  };
  const timestamp = getAttr(attributeMap, "timestamp");
  const archiveStatusRaw = getAttr(attributeMap, "archive_status");
  const modified = toDate(timestamp) ?? new Date(0);
  const isArchived = String(archiveStatusRaw).toLowerCase() === "archived";
  return {
    type: typeMap[event.type] ?? "update",
    id: num(getAttr(attributeMap, "credential_schema_id")),
    tr_id: num(getAttr(attributeMap, "trust_registry_id")),
    issuer_grantor_validation_validity_period: num(getAttr(attributeMap, "issuer_grantor_validation_validity_period")),
    verifier_grantor_validation_validity_period: num(getAttr(attributeMap, "verifier_grantor_validation_validity_period")),
    issuer_validation_validity_period: num(getAttr(attributeMap, "issuer_validation_validity_period")),
    verifier_validation_validity_period: num(getAttr(attributeMap, "verifier_validation_validity_period")),
    holder_validation_validity_period: num(getAttr(attributeMap, "holder_validation_validity_period")),
    issuer_onboarding_mode: getAttr(attributeMap, "issuer_onboarding_mode") || getAttr(attributeMap, "issuer_perm_management_mode"),
    verifier_onboarding_mode: getAttr(attributeMap, "verifier_onboarding_mode") || getAttr(attributeMap, "verifier_perm_management_mode"),
    archived: isArchived ? modified : null,
    modified,
  };
}
