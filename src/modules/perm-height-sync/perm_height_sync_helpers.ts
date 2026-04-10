import { Network } from "../../network";

export type PermissionMessagePayload = {
  type: string;
  content: any;
  timestamp?: string;
  height?: number;
  txHash?: string;
  txCode?: number;
  msgIndex?: number;
  txEvents?: Array<{ type?: string; attributes?: Array<{ key?: string; value?: string }> }>;
};

const HEIGHT_HEADER = "x-cosmos-block-height";

export function getPermLedgerBaseUrl(): string {
  const envLedger =
    (typeof process !== "undefined" && process.env?.LCD_ENDPOINT?.trim()) || "";
  const base = envLedger || Network?.LCD || "";
  return base.replace(/\/$/, "");
}

function decodeEventValue(raw: string | undefined): string {
  if (!raw) return "";
  try {
    if (/^[A-Za-z0-9+/=]+$/.test(raw) && raw.length % 4 === 0) {
      return Buffer.from(raw, "base64").toString("utf-8");
    }
  } catch {
    //
  }
  return raw;
}

export function extractIdsFromTxEvents(
  events: PermissionMessagePayload["txEvents"],
  keyPatterns: string[]
): number[] {
  if (!Array.isArray(events) || events.length === 0) return [];
  const ids = new Set<number>();
  for (const event of events) {
    const attrs = event?.attributes || [];
    for (const attr of attrs) {
      const key = decodeEventValue(attr?.key).toLowerCase();
      const value = decodeEventValue(attr?.value);
      if (!keyPatterns.some((pattern) => key.includes(pattern))) continue;
      const id = Number(value);
      if (Number.isInteger(id) && id > 0) {
        ids.add(id);
      }
    }
  }
  return [...ids];
}

export function extractImpactedPermissionIds(
  msg: PermissionMessagePayload
): number[] {
  const ids = new Set<number>();
  const candidates = [
    msg?.content?.id,
    msg?.content?.permission_id,
    msg?.content?.perm_id,
    msg?.content?.validator_perm_id,
    msg?.content?.validatorPermId,
    msg?.content?.issuer_perm_id,
    msg?.content?.issuerPermId,
    msg?.content?.verifier_perm_id,
    msg?.content?.verifierPermId,
    msg?.content?.agent_perm_id,
    msg?.content?.agentPermId,
    msg?.content?.wallet_agent_perm_id,
    msg?.content?.walletAgentPermId,
  ];

  for (const value of candidates) {
    const id = Number(value);
    if (Number.isInteger(id) && id > 0) ids.add(id);
  }

  for (const id of extractIdsFromTxEvents(msg.txEvents, [
    "permission_id",
    "root_permission_id",
    "validator_perm_id",
    "issuer_perm_id",
    "verifier_perm_id",
    "agent_perm_id",
    "wallet_agent_perm_id",
  ])) {
    ids.add(id);
  }

  return [...ids];
}

export function extractStartPermissionVpNewPermissionId(
  msg: PermissionMessagePayload
): number | undefined {
  const validatorRaw =
    msg?.content?.validator_perm_id ?? msg?.content?.validatorPermId;
  const validatorId = Number(validatorRaw);
  const hasValidator =
    validatorRaw !== undefined &&
    validatorRaw !== null &&
    String(validatorRaw).trim() !== "" &&
    Number.isInteger(validatorId) &&
    validatorId > 0;

  const directCandidates = [
    msg?.content?.id,
    msg?.content?.permission_id,
    msg?.content?.permissionId,
  ];
  for (const candidate of directCandidates) {
    const n = Number(candidate);
    if (!Number.isInteger(n) || n <= 0) continue;
    if (hasValidator && n === validatorId) continue;
    return n;
  }

  const exactNewPermKeys = new Set([
    "permission_id",
    "permissionid",
    "new_permission_id",
    "new_permissionid",
    "created_permission_id",
    "created_permissionid",
  ]);
  const fromExactEvents: number[] = [];
  if (Array.isArray(msg.txEvents)) {
    for (const event of msg.txEvents) {
      for (const attr of event?.attributes || []) {
        const key = decodeEventValue(attr?.key).toLowerCase();
        if (!exactNewPermKeys.has(key)) continue;
        const id = Number(decodeEventValue(attr?.value));
        if (Number.isInteger(id) && id > 0) {
          fromExactEvents.push(id);
        }
      }
    }
  }
  const uniqueExact = [...new Set(fromExactEvents)];
  const filteredExact = hasValidator
    ? uniqueExact.filter((id) => id !== validatorId)
    : uniqueExact;
  if (filteredExact.length === 1) {
    return filteredExact[0];
  }
  if (filteredExact.length > 1) {
    const notValidator = filteredExact.filter(
      (id) => !hasValidator || id !== validatorId
    );
    if (notValidator.length === 1) return notValidator[0];
  }

  // Fallback: attribute keys like `msg_permission_id` (contains `permission_id`, not `validator_*`)
  const looseFromEvents: number[] = [];
  if (Array.isArray(msg.txEvents)) {
    for (const event of msg.txEvents) {
      for (const attr of event?.attributes || []) {
        const key = decodeEventValue(attr?.key).toLowerCase();
        if (!key.includes("permission_id") && !key.includes("permissionid")) {
          continue;
        }
        if (key.includes("validator")) continue;
        const id = Number(decodeEventValue(attr?.value));
        if (Number.isInteger(id) && id > 0) {
          looseFromEvents.push(id);
        }
      }
    }
  }
  const uniqueLoose = [...new Set(looseFromEvents)];
  const filteredLoose = hasValidator
    ? uniqueLoose.filter((id) => id !== validatorId)
    : uniqueLoose;
  if (filteredLoose.length === 1) {
    return filteredLoose[0];
  }

  const impacted = extractImpactedPermissionIds(msg);
  const remaining = hasValidator
    ? impacted.filter((id) => id !== validatorId)
    : impacted;
  if (remaining.length === 1) {
    return remaining[0];
  }

  return undefined;
}

export function extractImpactedSessionIds(
  msg: PermissionMessagePayload
): string[] {
  const ids = new Set<string>();
  const directCandidates = [
    msg?.content?.id,
    msg?.content?.session_id,
    msg?.content?.sessionId,
  ];
  for (const candidate of directCandidates) {
    if (candidate === null || candidate === undefined) continue;
    const value = String(candidate).trim();
    if (value.length > 0) ids.add(value);
  }

  if (Array.isArray(msg?.txEvents)) {
    for (const event of msg.txEvents) {
      for (const attr of event?.attributes || []) {
        const key = decodeEventValue(attr?.key).toLowerCase();
        if (!key.includes("session_id") && !key.includes("sessionid") && !key.includes("session")) continue;
        const value = decodeEventValue(attr?.value).trim();
        if (value) ids.add(value);
      }
    }
  }

  return [...ids];
}

export async function fetchPermLedgerJson(
  path: string,
  blockHeight?: number
): Promise<any | null> {
  const base = getPermLedgerBaseUrl();
  if (!base) return null;
  const withHeight = typeof blockHeight === "number" && blockHeight > 0;
  const headers: Record<string, string> = {};
  if (withHeight) {
    headers[HEIGHT_HEADER] = String(blockHeight);
  }

  try {
    const res = await fetch(`${base}${path}`, { headers });
    if (res.ok) {
      return await res.json();
    }

    if (withHeight) {
      const fallback = await fetch(`${base}${path}`);
      if (!fallback.ok) return null;
      return await fallback.json();
    }

    return null;
  } catch {
    if (withHeight) {
      try {
        const fallback = await fetch(`${base}${path}`);
        if (!fallback.ok) return null;
        return await fallback.json();
      } catch {
        //
      }
    }
    return null;
  }
}

