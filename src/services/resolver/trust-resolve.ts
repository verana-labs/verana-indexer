import {
  InMemoryCache,
  resolveDID,
  type TrustResolution,
  type TrustResolutionCache,
  type VerifiablePublicRegistry,
} from "@verana-labs/verre";
import { BULL_JOB_NAME } from "../../common";
import knex from "../../common/utils/db_connection";
import { applySpeedToBatchSize, applySpeedToDelay, getRecommendedConcurrency } from "../../common/utils/crawl_speed_config";
import { detectStartMode } from "../../common/utils/start_mode_detector";
import config from "../../config.json" with { type: "json" };
import { DbDerefCache } from "./db-cache";
import {
  defaultVprRegistriesFromEnv,
  readBoolFromEnv,
} from "./trust-resolve.helpers";

export type ResolverTierConfig = {
  millisecondPoll?: number;
  blocksPerCall?: number;
  didResolveConcurrency?: number;
  maxDidsPerTrustBlock?: number;
};

export type ResolverRuntimeConfig = {
  enabled?: boolean;
  millisecondPoll?: number;
  millisecondCrawl?: number;
  blocksPerCall?: number;
  indexerApiBaseUrl?: string | null;
  verifiablePublicRegistries?: VerifiablePublicRegistry[];
  disableDigestSriVerification?: boolean;
  trustEvaluationTtlSeconds?: number;
  dereferenceCacheTtlSeconds?: number;
  pollObjectCachingRetryDays?: number;
 
  txPrefilterEnabled?: boolean;
  didResolveConcurrency?: number;
  maxDidsPerTrustBlock?: number;
  freshStart?: ResolverTierConfig;
  reindexing?: ResolverTierConfig;
};

export function getResolverRuntimeConfig(): ResolverRuntimeConfig | null {
  const c = config as unknown as { resolver?: ResolverRuntimeConfig; trustResolve?: ResolverRuntimeConfig };
  const next = c.resolver;
  const legacy = c.trustResolve;
  if (!next && !legacy) {
    const enabled = readBoolFromEnv(["RESOLVER_ENABLED", "TRUST_RESOLVE_ENABLED"]);
    if (enabled === null) return null;
    return { enabled };
  }
  return {
    ...legacy,
    ...next,
    enabled: next?.enabled ?? legacy?.enabled,
    millisecondPoll: next?.millisecondPoll ?? next?.millisecondCrawl ?? legacy?.millisecondCrawl,
    blocksPerCall: next?.blocksPerCall ?? legacy?.blocksPerCall,
    verifiablePublicRegistries: next?.verifiablePublicRegistries,
    disableDigestSriVerification: next?.disableDigestSriVerification,
    trustEvaluationTtlSeconds: next?.trustEvaluationTtlSeconds ?? legacy?.trustEvaluationTtlSeconds,
    dereferenceCacheTtlSeconds: next?.dereferenceCacheTtlSeconds ?? legacy?.dereferenceCacheTtlSeconds,
    pollObjectCachingRetryDays: next?.pollObjectCachingRetryDays ?? legacy?.pollObjectCachingRetryDays,
    didResolveConcurrency: next?.didResolveConcurrency ?? legacy?.didResolveConcurrency,
    maxDidsPerTrustBlock: next?.maxDidsPerTrustBlock ?? legacy?.maxDidsPerTrustBlock,
    freshStart: next?.freshStart ?? legacy?.freshStart,
    reindexing: next?.reindexing ?? legacy?.reindexing,
  };
}

export function getDeclaredDereferenceCacheTtlSeconds(): number | null {
  const cfg = getResolverRuntimeConfig();
  const c = Number(cfg?.dereferenceCacheTtlSeconds);
  if (Number.isFinite(c) && c > 0) return Math.floor(c);
  return null;
}

export function getTrustEvaluationTtlSeconds(): number {
  const cfg = getResolverRuntimeConfig();
  const s = Number(cfg?.trustEvaluationTtlSeconds ?? 3600);
  return Number.isFinite(s) && s > 0 ? Math.floor(s) : 3600;
}

export function getDeclaredPollObjectCachingRetryDays(): number | null {
  const cfg = getResolverRuntimeConfig();
  const c = Number(cfg?.pollObjectCachingRetryDays);
  if (Number.isFinite(c) && c > 0) return Math.floor(c);
  return null;
}

export function getVerreTrustEvaluationCallOptions(): {
  verifiablePublicRegistries: VerifiablePublicRegistry[];
  skipDigestSRICheck: boolean;
} {
  const cfg = getResolverRuntimeConfig();
  const registriesRaw =
    cfg?.verifiablePublicRegistries && cfg.verifiablePublicRegistries.length > 0
      ? cfg.verifiablePublicRegistries
      : defaultVprRegistriesFromEnv();

  const baseUrlOverride =
    (typeof cfg?.indexerApiBaseUrl === "string" ? cfg.indexerApiBaseUrl.trim() : "") ||
    null;

  const devFallbackBase =
    baseUrlOverride ??
    (() => {
      const port = (process.env.PORT ?? "").trim();
      if (!port) return null;
      return `http://127.0.0.1:${port}/verana`;
    })();

  const registries = registriesRaw.map((r) => {
    const baseUrls = Array.isArray(r.baseUrls) ? r.baseUrls.filter((u) => typeof u === "string" && u.trim() !== "") : [];
    if (baseUrls.length > 0) return r;
    if (devFallbackBase) return { ...r, baseUrls: [devFallbackBase] };
    return r;
  });

  return {
    verifiablePublicRegistries: registries,
    skipDigestSRICheck: cfg?.disableDigestSriVerification === true,
  };
}

const DEFAULT_POLL_MS = 3000;
const DEFAULT_BLOCKS = 200;
const DEFAULT_CONC = 6;
const DEFAULT_MAX_DIDS = 500;

function pickTier(cfg: ResolverRuntimeConfig, isFreshStart: boolean) {
  return isFreshStart ? cfg.freshStart : cfg.reindexing;
}

function toPositiveInt(n: number, fallback: number): number {
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.floor(n);
}

export async function getResolverTuning(logger?: unknown): Promise<{
  isReindexing: boolean;
  pollIntervalMs: number;
  blocksPerCall: number;
  didConcurrency: number;
  maxDidsPerBlock: number;
}> {
  const cfg = getResolverRuntimeConfig();
  if (!cfg) {
    return {
      isReindexing: true,
      pollIntervalMs: applySpeedToDelay(DEFAULT_POLL_MS, true),
      blocksPerCall: Math.max(1, applySpeedToBatchSize(DEFAULT_BLOCKS, true)),
      didConcurrency: getRecommendedConcurrency(DEFAULT_CONC, true),
      maxDidsPerBlock: Math.min(100_000, applySpeedToBatchSize(DEFAULT_MAX_DIDS, true)),
    };
  }

  const mode = await detectStartMode(BULL_JOB_NAME.CRAWL_BLOCK, logger);
  const isFreshStart = mode.isFreshStart;
  const isReindexing = !isFreshStart;
  const tier = pickTier(cfg, isFreshStart);

  const basePoll = tier?.millisecondPoll ?? cfg.millisecondPoll ?? cfg.millisecondCrawl ?? DEFAULT_POLL_MS;
  const baseBlocks = tier?.blocksPerCall ?? cfg.blocksPerCall ?? DEFAULT_BLOCKS;
  const baseConc = tier?.didResolveConcurrency ?? cfg.didResolveConcurrency ?? DEFAULT_CONC;
  const baseMax = tier?.maxDidsPerTrustBlock ?? cfg.maxDidsPerTrustBlock ?? DEFAULT_MAX_DIDS;

  const didConcurrency =
    getRecommendedConcurrency(Math.min(64, Math.max(1, toPositiveInt(baseConc, DEFAULT_CONC))), isReindexing);

  const maxDidsPerBlock =
    Math.min(100_000, applySpeedToBatchSize(toPositiveInt(baseMax, DEFAULT_MAX_DIDS), isReindexing));

  return {
    isReindexing,
    pollIntervalMs: applySpeedToDelay(toPositiveInt(basePoll, DEFAULT_POLL_MS), isReindexing),
    blocksPerCall: Math.max(1, applySpeedToBatchSize(toPositiveInt(baseBlocks, DEFAULT_BLOCKS), isReindexing)),
    didConcurrency,
    maxDidsPerBlock,
  };
}

export async function findHeightsWithTrustModuleMessages(fromExclusive: number, toInclusive: number): Promise<number[]> {
  if (!Number.isInteger(fromExclusive) || !Number.isInteger(toInclusive)) return [];
  if (fromExclusive >= toInclusive) return [];

  const res = await knex.raw(
    `
    SELECT DISTINCT t.height AS h
    FROM transaction t
    INNER JOIN transaction_message tm ON tm.tx_id = t.id
    WHERE t.height > ?
      AND t.height <= ?
      AND t.code = 0
      AND (
        tm.type LIKE '/verana.dd%'
        OR tm.type LIKE '/verana.tr%'
        OR tm.type LIKE '/verana.cs%'
        OR tm.type LIKE '/verana.perm%'
        OR tm.type LIKE '/veranablockchain.diddirectory%'
        OR tm.type LIKE '/veranablockchain.trustregistry%'
        OR tm.type LIKE '/veranablockchain.credentialschema%'
      )
    ORDER BY t.height ASC
    `,
    [fromExclusive, toInclusive]
  );

  const rows = (res as { rows?: Array<{ h?: unknown }> }).rows ?? [];
  const out: number[] = [];
  for (const row of rows) {
    const n = Number(row.h);
    if (Number.isInteger(n) && n > 0) out.push(n);
  }
  return out;
}

export function trustTxPrefilterEnabled(): boolean {
  const cfg = getResolverRuntimeConfig();
  return cfg?.txPrefilterEnabled !== false;
}

export type ReattemptableResourceRow = {
  resource_id: string;
  resource_type: string;
  first_failure: Date | string;
  last_failure?: Date | string | null;
  last_retry?: Date | string | null;
  next_retry?: Date | string | null;
  error_type?: string | null;
  last_error?: string | null;
  retry_count: number;
};

function nextRetryAt(now: Date): Date {
  const d = new Date(now);
  d.setUTCDate(d.getUTCDate() + 1);
  return d;
}

export async function markReattemptableFailure(args: {
  resourceId: string;
  resourceType: string;
  errorType?: string | null;
  lastError?: string | null;
  retryDays: number;
}): Promise<void> {
  const now = new Date();
  const retryDays = Math.max(0, Math.floor(args.retryDays));
  if (retryDays <= 0) return;

  const existing = (await knex("trust_reattemptable").where("resource_id", args.resourceId).first()) as
    | ReattemptableResourceRow
    | undefined;
  const retryCount = Number(existing?.retry_count ?? 0);
  if (retryCount >= retryDays) return;

  const row: any = {
    resource_id: args.resourceId,
    resource_type: args.resourceType,
    first_failure: existing?.first_failure ?? now,
    last_failure: now,
    error_type: args.errorType ?? existing?.error_type ?? null,
    last_error: args.lastError ?? null,
    next_retry: nextRetryAt(now),
    updated_at: now,
  };

  await knex("trust_reattemptable").insert(row).onConflict("resource_id").merge(row);
}

export async function markReattemptableAttempt(resourceId: string): Promise<void> {
  const now = new Date();
  await knex("trust_reattemptable")
    .where("resource_id", resourceId)
    .update({
      last_retry: now,
      retry_count: knex.raw("COALESCE(retry_count, 0) + 1"),
      updated_at: now,
      next_retry: nextRetryAt(now),
    });
}

export async function clearReattemptable(resourceId: string): Promise<void> {
  await knex("trust_reattemptable").where("resource_id", resourceId).delete();
}

export async function listDueReattemptables(limit: number): Promise<ReattemptableResourceRow[]> {
  const n = Math.max(1, Math.min(5000, Math.floor(limit)));
  const now = new Date();
  const rows = (await knex("trust_reattemptable")
    .where(function () {
      this.whereNull("next_retry").orWhere("next_retry", "<=", now);
    })
    .orderBy("next_retry", "asc")
    .limit(n)) as any[];
  return rows as ReattemptableResourceRow[];
}

function isDidString(value: string | null | undefined): value is string {
  return typeof value === "string" && value.startsWith("did:");
}

async function runPool<T>(items: T[], concurrency: number, fn: (item: T) => Promise<void>): Promise<void> {
  if (items.length === 0) return;
  const n = Math.max(1, Math.min(concurrency, items.length));
  const q = [...items];
  await Promise.all(
    Array.from({ length: n }, async () => {
      for (;;) {
        const item = q.shift();
        if (item === undefined) return;
        await fn(item);
      }
    })
  );
}

const IMPACTED_DIDS_SQL = `
  SELECT DISTINCT d FROM (
    SELECT did AS d FROM did_history WHERE height = ? AND is_deleted = false
    UNION ALL
    SELECT did AS d FROM dids WHERE height = ? AND is_deleted = false
    UNION ALL
    SELECT controller AS d FROM dids WHERE height = ? AND is_deleted = false AND controller IS NOT NULL
    UNION ALL
    SELECT did AS d FROM trust_registry_history WHERE height = ?
    UNION ALL
    SELECT controller AS d FROM trust_registry_history WHERE height = ? AND controller IS NOT NULL
    UNION ALL
    SELECT did AS d FROM permission_history WHERE height = ?
    UNION ALL
    SELECT grantee AS d FROM permission_history WHERE height = ? AND grantee IS NOT NULL
    UNION ALL
    SELECT created_by AS d FROM permission_history WHERE height = ? AND created_by IS NOT NULL
  ) AS x
  WHERE d LIKE 'did:%'
  LIMIT ?
`;

export type TrustResultsRow = {
  did: string;
  height: number;
  resolve_result: unknown;
  issuer_auth: unknown;
  verifier_auth: unknown;
  ecosystem_participant: unknown;
  trust_status?: string | null;
  production?: boolean | null;
  evaluated_at?: Date | string | null;
  expires_at?: Date | string | null;
  full_result_json?: unknown;
  created_at?: Date | string;
};

function toIsoSeconds(d: Date): string {
  return d.toISOString().replace(/\.\d{3}Z$/, "Z");
}

function buildStableApiPayload(args: {
  did: string;
  evaluatedAt: Date;
  evaluatedAtBlock: number;
  resolveResult: unknown;
  trustTtlSeconds: number;
}): Record<string, unknown> {
  const evaluatedAtIso = toIsoSeconds(args.evaluatedAt);
  const expiresAtIso =
    args.trustTtlSeconds > 0 ? toIsoSeconds(new Date(args.evaluatedAt.getTime() + args.trustTtlSeconds * 1000)) : null;

  const summary = buildTrustSummaryFromStoredRow({
    did: args.did,
    resolveResult: args.resolveResult,
    evaluatedAtBlock: args.evaluatedAtBlock,
    createdAt: args.evaluatedAt,
    trustTtlSeconds: args.trustTtlSeconds,
  });

  const { credentials, failedCredentials } = extractQ1CredentialArrays(args.resolveResult);

  const payload: Record<string, unknown> = {
    did: summary.did,
    trustStatus: summary.trustStatus,
    production: summary.production,
    evaluatedAt: evaluatedAtIso,
    evaluatedAtBlock: args.evaluatedAtBlock,
    credentials,
    failedCredentials,
  };
  if (expiresAtIso) payload.expiresAt = expiresAtIso;
  return payload;
}

export async function saveTrustResults(row: {
  did: string;
  height: number;
  resolve_result: unknown;
  issuer_auth: unknown;
  verifier_auth: unknown;
  ecosystem_participant: unknown;
}): Promise<void> {
  const trustTtlSeconds = getTrustEvaluationTtlSeconds();
  const evaluatedAt = new Date();
  const payload = buildStableApiPayload({
    did: row.did,
    evaluatedAt,
    evaluatedAtBlock: row.height,
    resolveResult: row.resolve_result,
    trustTtlSeconds,
  });

  const trustStatus = String(payload.trustStatus ?? "UNTRUSTED");
  const production = Boolean(payload.production);
  const expiresAt =
    trustTtlSeconds > 0 ? new Date(evaluatedAt.getTime() + trustTtlSeconds * 1000) : new Date(evaluatedAt.getTime());

  await knex("trust_results")
    .insert({
      did: row.did,
      height: row.height,
      resolve_result: row.resolve_result,
      issuer_auth: row.issuer_auth,
      verifier_auth: row.verifier_auth,
      ecosystem_participant: row.ecosystem_participant,
      trust_status: trustStatus,
      production,
      evaluated_at: evaluatedAt,
      expires_at: expiresAt,
      full_result_json: payload,
    })
    .onConflict(["did", "height"])
    .merge({
      resolve_result: row.resolve_result,
      issuer_auth: row.issuer_auth,
      verifier_auth: row.verifier_auth,
      ecosystem_participant: row.ecosystem_participant,
      trust_status: trustStatus,
      production,
      evaluated_at: evaluatedAt,
      expires_at: expiresAt,
      full_result_json: payload,
    });
}

export async function getTrustResultLatestByDidAtOrBeforeHeight(
  did: string,
  maxHeight: number
): Promise<TrustResultsRow | null> {
  const row = await knex("trust_results")
    .where({ did })
    .where("height", "<=", maxHeight)
    .orderBy("height", "desc")
    .first();
  return row ? (row as TrustResultsRow) : null;
}

export type TrustRoleSnapshot = {
  verified: boolean;
  outcome?: unknown;
  error?: string;
};

function snapshotFromResolution(result: TrustResolution): TrustRoleSnapshot {
  return {
    verified: Boolean(result?.verified),
    outcome: result?.outcome,
  };
}

function snapshotFromError(message: string): TrustRoleSnapshot {
  return { verified: false, error: message };
}

async function getImpactedDids(blockHeight: number, limit: number): Promise<string[]> {
  const h = blockHeight;
  const res = await knex.raw(IMPACTED_DIDS_SQL, [h, h, h, h, h, h, h, h, limit]);
  const rows = (res as { rows?: Array<{ d?: string }> }).rows ?? [];
  const out: string[] = [];
  for (const row of rows) {
    const d = row?.d;
    if (isDidString(d)) out.push(d);
  }
  return out;
}

export async function resolveTrustForDidAtHeight(
  did: string,
  blockHeight: number,
  verreCache?: TrustResolutionCache<string, Promise<TrustResolution>>
): Promise<void> {
  const { verifiablePublicRegistries, skipDigestSRICheck } = getVerreTrustEvaluationCallOptions();
  const derefTtlSeconds = getDeclaredDereferenceCacheTtlSeconds() ?? 0;
  const cache: any =
    verreCache ??
    (derefTtlSeconds > 0 ? new DbDerefCache(derefTtlSeconds * 1000) : new InMemoryCache(5 * 60 * 1000));
  const cfg = getResolverRuntimeConfig();
  const retryDays = Number(cfg?.pollObjectCachingRetryDays ?? 0) || 0;
  const resourceId = `did:${did}@${blockHeight}`;

  try {
    const result = (await resolveDID(did, {
      verifiablePublicRegistries,
      skipDigestSRICheck,
      cache,
    })) as TrustResolution;

    const snap = snapshotFromResolution(result);
    await saveTrustResults({
      did,
      height: blockHeight,
      resolve_result: result,
      issuer_auth: snap,
      verifier_auth: snap,
      ecosystem_participant: snap,
    });

    await clearReattemptable(resourceId);
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    const snap = snapshotFromError(message);
    const lower = message.toLowerCase();
    const errorCode =
      lower.includes("not found") || lower.includes("not_found") || lower.includes("404")
        ? "not_found"
        : "resolution_failed";

    await saveTrustResults({
      did,
      height: blockHeight,
      resolve_result: {
        error: true,
        message,
        dereferenceErrors: [],
        credentials: [],
        failedCredentials: [
          {
            id: did,
            error: `DID resolution failed for ${did}`,
            format: "N/A",
            errorCode,
            message,
          },
        ],
      },
      issuer_auth: snap,
      verifier_auth: snap,
      ecosystem_participant: snap,
    });

    await markReattemptableFailure({
      resourceId,
      resourceType: "did-evaluation",
      errorType: "resolveDID",
      lastError: message,
      retryDays,
    });
  }
}

export async function resolveTrustForBlock(blockHeight: number): Promise<void> {
  if (!Number.isInteger(blockHeight) || blockHeight < 0) return;

  const tuning = await getResolverTuning();
  const impactedDids = await getImpactedDids(blockHeight, tuning.maxDidsPerBlock);
  const verreCache = new InMemoryCache(20 * 60 * 1000);

  await runPool(impactedDids, tuning.didConcurrency, async (did) => resolveTrustForDidAtHeight(did, blockHeight, verreCache));
}

export type TrustSummaryPayload = {
  did: string;
  trustStatus: string;
  production: boolean;
  evaluatedAt: string;
  evaluatedAtBlock: number;
  expiresAt?: string;
};

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

export function buildTrustSummaryFromStoredRow(args: {
  did: string;
  resolveResult: unknown;
  evaluatedAtBlock: number;
  createdAt: Date | string | null | undefined;
  trustTtlSeconds?: number;
}): TrustSummaryPayload {
  const evaluatedAt =
    args.createdAt != null ? new Date(args.createdAt as Date | string).toISOString() : new Date().toISOString();
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

export function extractQ1CredentialArrays(resolveResult: unknown): {
  credentials: unknown[];
  failedCredentials: unknown[];
} {
  if (!resolveResult || typeof resolveResult !== "object") {
    return { credentials: [], failedCredentials: [] };
  }
  const r = resolveResult as Record<string, unknown>;
  const credentials = Array.isArray(r.credentials)
    ? r.credentials
    : Array.isArray(r.validCredentials)
      ? r.validCredentials
      : [];
  const failedCredentials = Array.isArray(r.failedCredentials) ? r.failedCredentials : [];
  return { credentials, failedCredentials };
}
