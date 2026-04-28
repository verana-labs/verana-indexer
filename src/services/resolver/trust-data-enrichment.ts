import knex from "../../common/utils/db_connection";
import config from "../../config.json" with { type: "json" };

export type TrustDataMode = "none" | "summary" | "full";
export type TrustSummaryPayload = {
  did: string;
  trustStatus: string;
  production: boolean;
  evaluatedAt: string;
  evaluatedAtBlock: number;
  expiresAt?: string;
};

type TrustRowLite = {
  did: string;
  height: number;
  resolve_result: unknown;
  full_result_json?: unknown;
  created_at?: unknown;
};

type ResolverRuntimeConfig = {
  trustEvaluationTtlSeconds?: number;
};

function toIsoSeconds(d: Date): string {
  return d.toISOString().replace(/\.\d{3}Z$/, "Z");
}

function getTrustEvaluationTtlSeconds(): number {
  const c = config as unknown as { resolver?: ResolverRuntimeConfig; trustResolve?: ResolverRuntimeConfig };
  const trustSeconds = Number(c?.resolver?.trustEvaluationTtlSeconds ?? c?.trustResolve?.trustEvaluationTtlSeconds ?? 3600);
  return Number.isFinite(trustSeconds) && trustSeconds > 0 ? Math.floor(trustSeconds) : 3600;
}

function mapOutcomeToTrustStatus(verified: boolean, outcome: unknown): string {
  if (!verified) return "UNTRUSTED";
  if (outcome === "verified") return "TRUSTED";
  if (outcome === "verified-test") return "PARTIAL";
  return "UNTRUSTED";
}

function mapOutcomeToProduction(verified: boolean, outcome: unknown): boolean {
  if (!verified) return false;
  return outcome === "verified";
}

function buildTrustSummaryFromStoredRow(args: {
  did: string;
  resolveResult: unknown;
  evaluatedAtBlock: number;
  createdAt: Date | string | null | undefined;
  trustTtlSeconds?: number;
}): TrustSummaryPayload {
  const evaluatedAt = args.createdAt != null ? new Date(args.createdAt as Date | string).toISOString() : new Date().toISOString();
  const trustTtlSeconds = args.trustTtlSeconds ?? getTrustEvaluationTtlSeconds();

  if (!args.resolveResult || typeof args.resolveResult !== "object" || (args.resolveResult as { error?: boolean }).error) {
    const base: TrustSummaryPayload = {
      did: args.did,
      trustStatus: "UNTRUSTED",
      production: false,
      evaluatedAt,
      evaluatedAtBlock: args.evaluatedAtBlock,
    };
    if (trustTtlSeconds > 0) {
      base.expiresAt = new Date(Date.now() + trustTtlSeconds * 1000).toISOString();
    }
    return base;
  }

  const r = args.resolveResult as { verified?: boolean; outcome?: unknown };
  const verified = Boolean(r.verified);
  const outcome = r.outcome;
  const summary: TrustSummaryPayload = {
    did: args.did,
    trustStatus: mapOutcomeToTrustStatus(verified, outcome),
    production: mapOutcomeToProduction(verified, outcome),
    evaluatedAt,
    evaluatedAtBlock: args.evaluatedAtBlock,
  };
  if (trustTtlSeconds > 0) {
    summary.expiresAt = new Date(new Date(evaluatedAt).getTime() + trustTtlSeconds * 1000).toISOString();
  }
  return summary;
}

function extractQ1CredentialArrays(resolveResult: unknown): {
  credentials: unknown[];
  failedCredentials: unknown[];
} {
  if (!resolveResult || typeof resolveResult !== "object") {
    return { credentials: [], failedCredentials: [] };
  }
  const r = resolveResult as Record<string, unknown>;
  const credentials = Array.isArray(r.credentials) ? r.credentials : Array.isArray(r.validCredentials) ? r.validCredentials : [];
  const failedCredentials = Array.isArray(r.failedCredentials) ? r.failedCredentials : [];
  return { credentials, failedCredentials };
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (!value || typeof value !== "object") return false;
  if (Array.isArray(value)) return false;
  if (value instanceof Date) return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

export function parseTrustDataMode(raw: unknown): { ok: true; mode: TrustDataMode } | { ok: false; message: string } {
  if (raw === undefined || raw === null || raw === "") return { ok: true, mode: "none" };
  const normalized = String(raw).trim().toLowerCase();
  if (normalized === "null" || normalized === "none") return { ok: true, mode: "none" };
  if (normalized === "summary") return { ok: true, mode: "summary" };
  if (normalized === "full") return { ok: true, mode: "full" };
  return { ok: false, message: 'Invalid "trustData". Allowed values: null, summary, full' };
}

function collectDidsDeep(value: unknown, dids: Set<string>, seen: WeakSet<object>): void {
  if (!value || typeof value !== "object") return;
  if (value instanceof Date) return;
  if (seen.has(value as object)) return;
  seen.add(value as object);

  if (Array.isArray(value)) {
    for (const item of value) collectDidsDeep(item, dids, seen);
    return;
  }

  if (!isPlainObject(value)) return;

  const record = value as Record<string, unknown>;
  if (typeof record.did === "string" && record.did.startsWith("did:")) {
    dids.add(record.did);
  }

  for (const nested of Object.values(record)) {
    collectDidsDeep(nested, dids, seen);
  }
}

async function fetchTrustResultsLatestByDid(dids: string[], blockHeight?: number): Promise<Map<string, TrustRowLite>> {
  const out = new Map<string, TrustRowLite>();
  const uniq = Array.from(new Set(dids.filter((d) => typeof d === "string" && d.startsWith("did:"))));
  if (uniq.length === 0) return out;

  const chunkSize = 500;
  for (let i = 0; i < uniq.length; i += chunkSize) {
    const chunk = uniq.slice(i, i + chunkSize);
    const q = knex("trust_results")
      .select(["did", "height", "resolve_result", "full_result_json", "created_at"])
      .whereIn("did", chunk)
      .orderBy("did", "asc")
      .orderBy("height", "desc");
    if (typeof blockHeight === "number" && Number.isFinite(blockHeight) && blockHeight >= 0) {
      q.andWhere("height", "<=", Math.trunc(blockHeight));
    }
    const rows = (await q) as any[];
    for (const row of rows) {
      const did = String(row?.did ?? "");
      const height = Number(row?.height ?? NaN);
      if (!did.startsWith("did:") || !Number.isFinite(height) || out.has(did)) continue;
      out.set(did, {
        did,
        height: Math.trunc(height),
        resolve_result: row?.resolve_result,
        full_result_json: row?.full_result_json,
        created_at: row?.created_at,
      });
    }
  }

  return out;
}

function buildTrustPayload(row: TrustRowLite, mode: TrustDataMode): Record<string, unknown> | null {
  if (mode === "none") return null;

  const trustTtlSeconds = getTrustEvaluationTtlSeconds();
  const summary = buildTrustSummaryFromStoredRow({
    did: row.did,
    resolveResult: row.resolve_result,
    evaluatedAtBlock: row.height,
    createdAt: row.created_at as any,
    trustTtlSeconds,
  });

  if (mode === "summary") return summary;

  if (row.full_result_json && typeof row.full_result_json === "object") {
    return row.full_result_json as Record<string, unknown>;
  }

  const evaluatedAtIso = row.created_at != null ? new Date(row.created_at as any).toISOString() : summary.evaluatedAt;
  const { credentials, failedCredentials } = extractQ1CredentialArrays(row.resolve_result);
  const payload: Record<string, unknown> = {
    did: summary.did,
    trustStatus: summary.trustStatus,
    production: summary.production,
    evaluatedAt: evaluatedAtIso,
    evaluatedAtBlock: row.height,
    credentials,
    failedCredentials,
  };
  if (trustTtlSeconds > 0) {
    payload.expiresAt = toIsoSeconds(new Date(new Date(evaluatedAtIso).getTime() + trustTtlSeconds * 1000));
  }
  return payload;
}

function injectTrustDataDeep<T>(
  value: T,
  mode: TrustDataMode,
  trustDataByDid: Map<string, Record<string, unknown> | null>,
  seen: WeakMap<object, unknown>
): T {
  if (!value || typeof value !== "object") return value;
  if (value instanceof Date) return value;
  if (seen.has(value as object)) return seen.get(value as object) as T;

  if (Array.isArray(value)) {
    const clonedArray: unknown[] = [];
    seen.set(value as object, clonedArray);
    for (const item of value) {
      clonedArray.push(injectTrustDataDeep(item, mode, trustDataByDid, seen));
    }
    return clonedArray as T;
  }

  if (!isPlainObject(value)) return value;

  const record = value as Record<string, unknown>;
  const cloned: Record<string, unknown> = {};
  seen.set(value as object, cloned);

  for (const [key, nested] of Object.entries(record)) {
    cloned[key] = injectTrustDataDeep(nested, mode, trustDataByDid, seen);
  }

  if (Object.prototype.hasOwnProperty.call(record, "did")) {
    const did = typeof record.did === "string" ? record.did : null;
    cloned.trustData = did ? (trustDataByDid.get(did) ?? null) : null;
    if (mode === "none") cloned.trustData = null;
  }

  return cloned as T;
}

export async function enrichTrustDataDeep<T>(value: T, mode: TrustDataMode, blockHeight?: number): Promise<T> {
  const dids = new Set<string>();
  collectDidsDeep(value, dids, new WeakSet<object>());

  const trustRowsByDid = mode === "none" ? new Map<string, TrustRowLite>() : await fetchTrustResultsLatestByDid(Array.from(dids), blockHeight);
  const trustDataByDid = new Map<string, Record<string, unknown> | null>();
  for (const did of dids) {
    const row = trustRowsByDid.get(did);
    trustDataByDid.set(did, row ? buildTrustPayload(row, mode) : null);
  }

  return injectTrustDataDeep(value, mode, trustDataByDid, new WeakMap<object, unknown>());
}
