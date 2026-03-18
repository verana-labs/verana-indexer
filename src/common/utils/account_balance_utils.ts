import { Network } from "../../network";

const ACCOUNT_REGEX = /^verana1[0-9a-z]{10,}$/;

export function isValidAccountAddress(address: string): boolean {
  if (!address || typeof address !== "string") return false;
  const s = address.trim();
  return s.length > 0 && ACCOUNT_REGEX.test(s);
}

const TD_ACCOUNT_KEYS = [
  "account",
  "owner",
  "deposit_account",
  "depositAccount",
  "creator",
  "sender",
  "validator",
];

export function extractAccountAddressesFromContent(
  content: Record<string, unknown> | null | undefined
): string[] {
  if (!content || typeof content !== "object") return [];
  const out: string[] = [];
  for (const key of TD_ACCOUNT_KEYS) {
    const raw = content[key];
    if (raw === null || raw === undefined) continue;
    const s = String(raw).trim();
    if (s.length > 0 && isValidAccountAddress(s)) out.push(s);
  }
  return [...new Set(out)];
}

export interface TxEventLike {
  type?: string;
  attributes?: Array<{ key?: string; value?: string }>;
}

function decodeAttr(v: string | undefined): string {
  if (v == null || v === "") return "";
  try {
    if (/^[A-Za-z0-9+/=]+$/.test(v) && v.length % 4 === 0) {
      return Buffer.from(v, "base64").toString("utf-8");
    }
  } catch {
    //
  }
  return v;
}

const TD_EVENT_ACCOUNT_KEYS = [
  "account",
  "owner",
  "deposit_account",
  "creator",
  "sender",
  "validator",
];

export function extractAccountAddressesFromEvents(
  events: TxEventLike[],
  decodeAttributes?: boolean
): string[] {
  const out: string[] = [];
  for (const ev of events ?? []) {
    const attrs = ev.attributes ?? [];
    for (const attr of attrs) {
      const key = (decodeAttributes ? decodeAttr(attr.key) : attr.key ?? "").toLowerCase();
      const value = (decodeAttributes ? decodeAttr(attr.value) : attr.value ?? "").trim();
      if (value.length === 0) continue;
      const matches = TD_EVENT_ACCOUNT_KEYS.some((k) => key.includes(k.toLowerCase()));
      if (matches && isValidAccountAddress(value)) out.push(value);
    }
  }
  return [...new Set(out)];
}

export function extractAccountAddressesFromTdSources(options: {
  messageContent?: Record<string, unknown> | null;
  events?: TxEventLike[];
  ledgerAccount?: string | null;
  decodeEventAttributes?: boolean;
}): string[] {
  const { messageContent, events, ledgerAccount, decodeEventAttributes = true } = options;
  const set = new Set<string>();
  extractAccountAddressesFromContent(messageContent ?? null).forEach((a) => set.add(a));
  if (events?.length) {
    extractAccountAddressesFromEvents(events, decodeEventAttributes).forEach((a) => set.add(a));
  }
  if (ledgerAccount != null && ledgerAccount !== "") {
    const s = String(ledgerAccount).trim();
    if (isValidAccountAddress(s)) set.add(s);
  }
  return [...set];
}

export interface BalanceEntry {
  denom: string;
  amount: string;
}

const BALANCES_PATH = "/cosmos/bank/v1beta1/balances";
const FETCH_TIMEOUT_MS = 8000;

export function getLcdBaseUrl(): string {
  const env =
    (typeof process !== "undefined" && process.env?.LCD_ENDPOINT?.trim()) || "";
  const base = env || Network?.LCD || "";
  return base.replace(/\/$/, "");
}

export async function fetchAccountBalance(
  address: string
): Promise<BalanceEntry[] | null> {
  const baseUrl = getLcdBaseUrl();
  if (!baseUrl || !address?.trim()) return null;
  const encoded = encodeURIComponent(address.trim());
  const url = `${baseUrl}${BALANCES_PATH}/${encoded}`;
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(url, { signal: controller.signal });
    clearTimeout(timeoutId);
    if (!res.ok) return null;
    const data = (await res.json().catch(() => null)) as {
      balances?: Array<{ denom?: string; amount?: string }>;
    } | null;
    if (!data || !Array.isArray(data.balances)) return [];
    return data.balances.map((b) => ({
      denom: String(b?.denom ?? "").trim() || "uvna",
      amount: String(b?.amount ?? "0").trim() || "0",
    }));
  } catch {
    clearTimeout(timeoutId);
    return null;
  }
}
