const DID_PATTERN = /^did:[a-z0-9]+:.+/i;

export function normalizeDid(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  if (!trimmed) return undefined;
  let decoded = trimmed;
  try {
    decoded = decodeURIComponent(trimmed).trim();
  } catch {
    decoded = trimmed;
  }
  return DID_PATTERN.test(decoded) ? decoded : undefined;
}

export function uniqueNormalizedDids(values: Iterable<unknown>): string[] {
  const dids = new Set<string>();
  for (const value of values) {
    const did = normalizeDid(value);
    if (did) dids.add(did);
  }
  return Array.from(dids).sort();
}

export function collectDidsDeep(value: unknown, out: Set<string> = new Set<string>()): Set<string> {
  const did = normalizeDid(value);
  if (did) {
    out.add(did);
    return out;
  }

  if (Array.isArray(value)) {
    value.forEach((item) => collectDidsDeep(item, out));
    return out;
  }

  if (value && typeof value === "object") {
    Object.values(value as Record<string, unknown>).forEach((item) => collectDidsDeep(item, out));
  }

  return out;
}

export function firstNormalizedDid(values: Iterable<unknown>): string | undefined {
  for (const value of values) {
    const did = normalizeDid(value);
    if (did) return did;
  }
  return undefined;
}

export function parseCsvList(raw: unknown): string[] {
  if (raw === undefined || raw === null) return [];
  if (Array.isArray(raw)) return raw.map((item) => String(item).trim()).filter(Boolean);
  return String(raw)
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean);
}

export function readBooleanFlag(raw: unknown): boolean {
  if (raw === true) return true;
  if (raw === undefined || raw === null) return false;
  return String(raw).trim().toLowerCase() === "true";
}

export function readPositiveInteger(value: unknown): number | null {
  const n = Number(value);
  return Number.isInteger(n) && n > 0 ? n : null;
}

export function readFirstPositiveInteger(source: unknown, keys: readonly string[]): number | null {
  if (!source || typeof source !== "object") return null;
  const obj = source as Record<string, unknown>;
  for (const key of keys) {
    const value = key.split(".").reduce<unknown>((acc, part) => {
      if (!acc || typeof acc !== "object") return undefined;
      return (acc as Record<string, unknown>)[part];
    }, obj);
    const n = readPositiveInteger(value);
    if (n) return n;
  }
  return null;
}
