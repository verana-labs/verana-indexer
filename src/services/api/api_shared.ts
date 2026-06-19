import { uniqueNormalizedDids } from "./indexer_event_utils";

export type LoggerLike = {
  info?: (...args: any[]) => void;
  warn?: (...args: any[]) => void;
  error?: (...args: any[]) => void;
};

export function createLogger(logger?: LoggerLike) {
  const base = logger ?? console;
  return {
    info: (...args: any[]) => (base.info ? base.info(...args) : console.log(...args)),
    warn: (...args: any[]) => (base.warn ? base.warn(...args) : console.warn(...args)),
    error: (...args: any[]) => (base.error ? base.error(...args) : console.error(...args)),
  };
}

export function isUnknownMessageError(errorMessage: string): boolean {
  if (!errorMessage) return false;
  return (
    errorMessage.includes("Unknown Verana message types") ||
    errorMessage.includes("UNKNOWN VERANA MESSAGE TYPES")
  );
}

export function isValidDid(value: unknown): value is string {
  return typeof value === "string" && /^did:[a-z0-9]+:.+/i.test(value.trim());
}

export function applyBlockHeightFilter(
  query: { andWhere: (...args: any[]) => any },
  args: { blockHeight?: unknown; afterBlockHeight?: unknown },
  column: string
) {
  if (Number.isInteger(args.blockHeight)) {
    query.andWhere(column, Number(args.blockHeight));
  } else if (Number.isInteger(args.afterBlockHeight)) {
    query.andWhere(column, ">", Number(args.afterBlockHeight));
  }
  return query;
}

export function toIsoSeconds(value: Date | string | null | undefined = new Date()): string {
  const date = value instanceof Date ? value : new Date(value ?? Date.now());
  const safe = Number.isFinite(date.getTime()) ? date : new Date();
  return safe.toISOString().replace(/\.\d{3}Z$/, "Z");
}

export type SubscribeMembership = { dids: string[] | null; corporationId: number | null };

export function parseCorporationId(
  raw: unknown
): { ok: true; value: number | null } | { ok: false; error: string } {
  if (raw === undefined || raw === null) return { ok: true, value: null };
  const n = typeof raw === "number" ? raw : Number(raw);
  if (!Number.isInteger(n) || n <= 0) {
    return { ok: false, error: `'corporationId' must be a positive integer; got ${String(raw)}` };
  }
  return { ok: true, value: n };
}

export function parseSubscribeMembership(
  source: Record<string, unknown>
): { ok: true; value: SubscribeMembership } | { ok: false; error: string } {
  const corp = parseCorporationId(source.corporationId);
  if (!corp.ok) return { ok: false, error: corp.error };
  const corporationId = corp.value;

  const rawDids = source.dids;
  if (rawDids === undefined || rawDids === null) {
    return { ok: true, value: { dids: null, corporationId } };
  }
  if (!Array.isArray(rawDids)) {
    return { ok: false, error: "'dids' must be an array of DID strings" };
  }
  if (rawDids.length === 0) {
    return { ok: true, value: { dids: null, corporationId } };
  }
  for (const candidate of rawDids) {
    if (!isValidDid(candidate)) {
      return { ok: false, error: `Invalid DID in 'dids': ${String(candidate)}` };
    }
  }
  const normalized = uniqueNormalizedDids(rawDids);
  if (normalized.length === 0) {
    return { ok: false, error: "No valid DIDs after normalization" };
  }
  return { ok: true, value: { dids: normalized, corporationId } };
}

export type MembershipTarget = {
  did: string;
  relatedDids?: Iterable<string>;
  corporationIds?: Iterable<number>;
};

export function matchesMembership(
  dids: Set<string> | null,
  corporationId: number | null,
  target: MembershipTarget
): boolean {
  const didOk =
    dids === null ||
    dids.has(target.did) ||
    (target.relatedDids ? [...target.relatedDids].some((d) => dids.has(d)) : false);
  const corpOk =
    corporationId === null ||
    (target.corporationIds ? [...target.corporationIds].some((c) => c === corporationId) : false);
  return didOk && corpOk;
}

