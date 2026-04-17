import type { VerifiablePublicRegistry } from "@verana-labs/verre";

export function readBoolFromEnv(keys: string[]): boolean | null {
  for (const k of keys) {
    const raw = (process.env[k] ?? "").trim().toLowerCase();
    if (!raw) continue;
    if (raw === "true" || raw === "1" || raw === "yes" || raw === "on") return true;
    if (raw === "false" || raw === "0" || raw === "no" || raw === "off") return false;
  }
  return null;
}

export function guessProductionFromChainId(chainId: string): boolean {
  const s = chainId.toLowerCase();
  if (s.includes("devnet") || s.includes("testnet") || s.includes("local") || s.includes("test")) return false;
  return true;
}

export function defaultVprRegistriesFromEnv(): VerifiablePublicRegistry[] {
  const chainId = (process.env.CHAIN_ID ?? "").trim();
  if (!chainId) return [];
  const id = `vpr:verana:${chainId}`;
  return [{ id, baseUrls: [], production: guessProductionFromChainId(chainId) }];
}

export function readPositiveIntFromEnv(keys: string[]): number | null {
  for (const k of keys) {
    const raw = (process.env[k] ?? "").trim();
    if (!raw) continue;
    const n = Number(raw);
    if (Number.isFinite(n) && n > 0) return Math.floor(n);
  }
  return null;
}

export function parseVprRegistriesJson(raw: string | undefined | null): VerifiablePublicRegistry[] {
  const s = (raw ?? "").trim();
  if (!s) return [];
  try {
    const parsed = JSON.parse(s) as unknown;
    if (!Array.isArray(parsed)) return [];
    return parsed.filter((value) => {
      if (!value || typeof value !== "object") return false;
      const o = value as Record<string, unknown>;
      return (
        typeof o.id === "string" &&
        Array.isArray(o.baseUrls) &&
        o.baseUrls.every((u) => typeof u === "string") &&
        typeof o.production === "boolean"
      );
    }) as VerifiablePublicRegistry[];
  } catch {
    return [];
  }
}
