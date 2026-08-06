import {
  resolveDID,
  type TrustResolution,
  TrustResolutionOutcome,
  type VerifiablePublicRegistry,
} from '@verana-labs/verre'
import { BULL_JOB_NAME } from '../../common'
import {
  applySpeedToBatchSize,
  applySpeedToDelay,
  getRecommendedConcurrency,
} from '../../common/utils/crawl_speed_config'
import knex from '../../common/utils/db_connection'
import { detectStartMode } from '../../common/utils/start_mode_detector'
import { VeranaParticipantMessageTypes } from '../../common/verana-message-types'
import config from '../../config.json' with { type: 'json' }
import { isEcsAllowlistEnforced } from './ecs-allowlist'
import { defaultVprRegistriesFromEnv, readBoolFromEnv } from './trust-resolve.helpers'
import { hasAllowlistedEcsServiceCredential, resolveCorporationId } from './trust-resolve-v4.builders'
import { attachRegistryAdapters } from './verre-registry-adapter'

export type ResolverTierConfig = {
  millisecondPoll?: number
  blocksPerCall?: number
  didResolveConcurrency?: number
  maxDidsPerTrustBlock?: number
}

export type ResolverRuntimeConfig = {
  enabled?: boolean
  millisecondPoll?: number
  millisecondCrawl?: number
  blocksPerCall?: number
  useEmbeddedRegistryAdapter?: boolean
  disableDigestSriVerification?: boolean
  pollObjectCachingRetryDays?: number

  txPrefilterEnabled?: boolean
  didResolveConcurrency?: number
  maxDidsPerTrustBlock?: number
  freshStart?: ResolverTierConfig
  reindexing?: ResolverTierConfig
}

export function getResolverRuntimeConfig(): ResolverRuntimeConfig | null {
  const c = config as unknown as { resolver?: ResolverRuntimeConfig; trustResolve?: ResolverRuntimeConfig }
  const next = c.resolver
  const legacy = c.trustResolve
  if (!next && !legacy) {
    const enabled = readBoolFromEnv(['RESOLVER_ENABLED', 'TRUST_RESOLVE_ENABLED'])
    if (enabled === null) return null
    return { enabled }
  }
  return {
    ...legacy,
    ...next,
    enabled: next?.enabled ?? legacy?.enabled,
    millisecondPoll: next?.millisecondPoll ?? next?.millisecondCrawl ?? legacy?.millisecondCrawl,
    blocksPerCall: next?.blocksPerCall ?? legacy?.blocksPerCall,
    useEmbeddedRegistryAdapter: next?.useEmbeddedRegistryAdapter ?? legacy?.useEmbeddedRegistryAdapter,
    disableDigestSriVerification: next?.disableDigestSriVerification,
    pollObjectCachingRetryDays: next?.pollObjectCachingRetryDays ?? legacy?.pollObjectCachingRetryDays,
    didResolveConcurrency: next?.didResolveConcurrency ?? legacy?.didResolveConcurrency,
    maxDidsPerTrustBlock: next?.maxDidsPerTrustBlock ?? legacy?.maxDidsPerTrustBlock,
    freshStart: next?.freshStart ?? legacy?.freshStart,
    reindexing: next?.reindexing ?? legacy?.reindexing,
  }
}

export function getDeclaredPollObjectCachingRetryDays(): number | null {
  const cfg = getResolverRuntimeConfig()
  const c = Number(cfg?.pollObjectCachingRetryDays)
  if (Number.isFinite(c) && c > 0) return Math.floor(c)
  return null
}

export function getVerreTrustEvaluationCallOptions(): {
  verifiablePublicRegistries: VerifiablePublicRegistry[]
  skipDigestSRICheck: boolean
} {
  const cfg = getResolverRuntimeConfig()
  const registries = attachRegistryAdapters(defaultVprRegistriesFromEnv(), {
    enabled: cfg?.useEmbeddedRegistryAdapter !== false,
  })

  return {
    verifiablePublicRegistries: registries,
    skipDigestSRICheck: cfg?.disableDigestSriVerification === true,
  }
}

const DEFAULT_POLL_MS = 3000
const DEFAULT_BLOCKS = 200
const DEFAULT_CONC = 6
const DEFAULT_MAX_DIDS = 500

function pickTier(cfg: ResolverRuntimeConfig, isFreshStart: boolean) {
  return isFreshStart ? cfg.freshStart : cfg.reindexing
}

function toPositiveInt(n: number, fallback: number): number {
  if (!Number.isFinite(n) || n <= 0) return fallback
  return Math.floor(n)
}

export async function getResolverTuning(logger?: unknown): Promise<{
  isReindexing: boolean
  pollIntervalMs: number
  blocksPerCall: number
  didConcurrency: number
  maxDidsPerBlock: number
}> {
  const cfg = getResolverRuntimeConfig()
  if (!cfg) {
    return {
      isReindexing: true,
      pollIntervalMs: applySpeedToDelay(DEFAULT_POLL_MS, true),
      blocksPerCall: Math.max(1, applySpeedToBatchSize(DEFAULT_BLOCKS, true)),
      didConcurrency: getRecommendedConcurrency(DEFAULT_CONC, true),
      maxDidsPerBlock: Math.min(100_000, applySpeedToBatchSize(DEFAULT_MAX_DIDS, true)),
    }
  }

  const mode = await detectStartMode(BULL_JOB_NAME.CRAWL_BLOCK, logger)
  const isFreshStart = mode.isFreshStart
  const isReindexing = !isFreshStart
  const tier = pickTier(cfg, isFreshStart)

  const basePoll = tier?.millisecondPoll ?? cfg.millisecondPoll ?? cfg.millisecondCrawl ?? DEFAULT_POLL_MS
  const baseBlocks = tier?.blocksPerCall ?? cfg.blocksPerCall ?? DEFAULT_BLOCKS
  const baseConc = tier?.didResolveConcurrency ?? cfg.didResolveConcurrency ?? DEFAULT_CONC
  const baseMax = tier?.maxDidsPerTrustBlock ?? cfg.maxDidsPerTrustBlock ?? DEFAULT_MAX_DIDS

  const didConcurrency = getRecommendedConcurrency(
    Math.min(64, Math.max(1, toPositiveInt(baseConc, DEFAULT_CONC))),
    isReindexing
  )

  const maxDidsPerBlock = Math.min(
    100_000,
    applySpeedToBatchSize(toPositiveInt(baseMax, DEFAULT_MAX_DIDS), isReindexing)
  )

  return {
    isReindexing,
    pollIntervalMs: applySpeedToDelay(toPositiveInt(basePoll, DEFAULT_POLL_MS), isReindexing),
    blocksPerCall: Math.max(1, applySpeedToBatchSize(toPositiveInt(baseBlocks, DEFAULT_BLOCKS), isReindexing)),
    didConcurrency,
    maxDidsPerBlock,
  }
}

export async function findHeightsWithTrustModuleMessages(
  fromExclusive: number,
  toInclusive: number
): Promise<number[]> {
  if (!Number.isInteger(fromExclusive) || !Number.isInteger(toInclusive)) return []
  if (fromExclusive >= toInclusive) return []

  const res = await knex.raw(
    `
    SELECT DISTINCT t.height AS h
    FROM transaction t
    INNER JOIN transaction_message tm ON tm.tx_id = t.id
    WHERE t.height > ?
      AND t.height <= ?
      AND t.code = 0
      AND (
        tm.type LIKE '/verana.ec%'
        OR tm.type LIKE '/verana.cs%'
        OR tm.type LIKE '/verana.pp%'
        OR tm.type LIKE '/verana.co%'
        OR tm.type LIKE '/verana.td%'
      )
    ORDER BY t.height ASC
    `,
    [fromExclusive, toInclusive]
  )

  const rows = (res as { rows?: Array<{ h?: unknown }> }).rows ?? []
  const out: number[] = []
  for (const row of rows) {
    const n = Number(row.h)
    if (Number.isInteger(n) && n > 0) out.push(n)
  }
  return out
}

export function trustTxPrefilterEnabled(): boolean {
  const cfg = getResolverRuntimeConfig()
  return cfg?.txPrefilterEnabled !== false
}

export type ReattemptableResourceRow = {
  resource_id: string
  resource_type: string
  first_failure: Date | string
  last_failure?: Date | string | null
  last_retry?: Date | string | null
  next_retry?: Date | string | null
  error_type?: string | null
  last_error?: string | null
  retry_count: number
}

function nextRetryAt(now: Date): Date {
  const d = new Date(now)
  d.setUTCDate(d.getUTCDate() + 1)
  return d
}

export async function markReattemptableFailure(args: {
  resourceId: string
  resourceType: string
  errorType?: string | null
  lastError?: string | null
  retryDays: number
}): Promise<void> {
  const now = new Date()
  const retryDays = Math.max(0, Math.floor(args.retryDays))
  if (retryDays <= 0) return

  const existing = (await knex('trust_reattemptable').where('resource_id', args.resourceId).first()) as
    | ReattemptableResourceRow
    | undefined
  const retryCount = Number(existing?.retry_count ?? 0)
  if (retryCount >= retryDays) return

  const row: any = {
    resource_id: args.resourceId,
    resource_type: args.resourceType,
    first_failure: existing?.first_failure ?? now,
    last_failure: now,
    error_type: args.errorType ?? existing?.error_type ?? null,
    last_error: args.lastError ?? null,
    next_retry: nextRetryAt(now),
    updated_at: now,
  }

  await knex('trust_reattemptable').insert(row).onConflict('resource_id').merge(row)
}

export async function markReattemptableAttempt(resourceId: string): Promise<void> {
  const now = new Date()
  await knex('trust_reattemptable')
    .where('resource_id', resourceId)
    .update({
      last_retry: now,
      retry_count: knex.raw('COALESCE(retry_count, 0) + 1'),
      updated_at: now,
      next_retry: nextRetryAt(now),
    })
}

export async function clearReattemptable(resourceId: string): Promise<void> {
  await knex('trust_reattemptable').where('resource_id', resourceId).delete()
}

export async function listDueReattemptables(limit: number): Promise<ReattemptableResourceRow[]> {
  const n = Math.max(1, Math.min(5000, Math.floor(limit)))
  const now = new Date()
  const rows = (await knex('trust_reattemptable')
    .where(function () {
      this.whereNull('next_retry').orWhere('next_retry', '<=', now)
    })
    .orderBy('next_retry', 'asc')
    .limit(n)) as any[]
  return rows as ReattemptableResourceRow[]
}

function isDidString(value: string | null | undefined): value is string {
  return typeof value === 'string' && value.startsWith('did:')
}

async function runPool<T>(items: T[], concurrency: number, fn: (item: T) => Promise<void>): Promise<void> {
  if (items.length === 0) return
  const n = Math.max(1, Math.min(concurrency, items.length))
  const q = [...items]
  await Promise.all(
    Array.from({ length: n }, async () => {
      for (;;) {
        const item = q.shift()
        if (item === undefined) return
        await fn(item)
      }
    })
  )
}

const IMPACTED_DIDS_SQL = `
  SELECT DISTINCT d FROM (
    SELECT did AS d FROM ecosystem_history WHERE height = ?
    UNION ALL
    SELECT did AS d FROM participant_history WHERE height = ? AND did IS NOT NULL
  ) AS x
  WHERE d LIKE 'did:%'
  LIMIT ?
`

const TRIGGERED_DIDS_SQL = `
  SELECT DISTINCT p.did AS d
  FROM transaction t
  INNER JOIN transaction_message tm ON tm.tx_id = t.id
  INNER JOIN participants p ON p.id = (tm.content->>'id')::bigint
  WHERE t.height = ?
    AND t.code = 0
    AND tm.type = ?
    AND tm.content->>'id' ~ '^[0-9]+$'
    AND p.did LIKE 'did:%'
  LIMIT ?
`

// The issuer filter is applied twice on purpose. The `anchored` CTE narrows the table down to the
// DIDs that were anchored on one of these issuers at some point, which is an index lookup on
// trust_results_service_issuer_idx; without it the DISTINCT ON would have to scan and sort the whole
// table before any filtering could happen. `anchored` is only a candidate set (a DID may have moved
// to another issuer since), so the final WHERE re-applies the filter to the latest row per DID,
// which is what actually decides membership.
const SERVICES_ANCHORED_ON_ISSUERS_SQL = `
  WITH anchored AS (
    SELECT DISTINCT did
    FROM trust_results
    WHERE resolve_result->'service'->>'issuer' IS NOT NULL
      AND resolve_result->'service'->>'issuer' = ANY(?::text[])
      AND height <= ?
      AND did LIKE 'did:%'
  ),
  latest AS (
    SELECT DISTINCT ON (tr.did) tr.did, tr.resolve_result
    FROM trust_results tr
    INNER JOIN anchored a ON a.did = tr.did
    WHERE tr.height <= ?
    ORDER BY tr.did, tr.height DESC
  )
  SELECT latest.did AS d
  FROM latest
  WHERE latest.resolve_result->'service'->>'issuer' = ANY(?::text[])
  LIMIT ?
`

// How many issuer-anchoring hops a single block may fan out to (service -> its issuer's dependents
// -> theirs, ...). Termination does not depend on this: `evaluated` resolves each DID at most once
// even on a cyclic issuer graph, and maxDidsPerBlock caps the total work. It only bounds how many
// extra fan-out queries one block can trigger, so a pathological chain cannot stall block
// processing. Anything deeper is picked up by the TTL refresh in resolver-poll, so truncating a
// cascade delays a re-evaluation rather than losing it.
const MAX_CASCADE_DEPTH = 4

export type TrustResultsRow = {
  did: string
  height: number
  resolve_result: unknown
  issuer_auth: unknown
  verifier_auth: unknown
  ecosystem_participant: unknown
  trust_status?: string | null
  production?: boolean | null
  evaluated_at?: Date | string | null
  expires_at?: Date | string | null
  corporation_id?: number | null
  created_at?: Date | string
}

function readExpiresAtTime(resolveResult: unknown): string | null {
  if (!resolveResult || typeof resolveResult !== 'object') return null
  const value = (resolveResult as { expiresAtTime?: unknown }).expiresAtTime
  return typeof value === 'string' && value.length > 0 ? value : null
}

export async function saveTrustResults(row: {
  did: string
  height: number
  resolve_result: unknown
  issuer_auth: unknown
  verifier_auth: unknown
  ecosystem_participant: unknown
}): Promise<void> {
  const evaluatedAt = new Date()
  const { trustStatus, production } = deriveStoredTrustState(row.resolve_result)
  const expiresAt = readExpiresAtTime(row.resolve_result)
  const corporationId = await resolveCorporationId(row.did, row.height)

  await knex('trust_results')
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
      corporation_id: corporationId,
    })
    .onConflict(['did', 'height'])
    .merge({
      resolve_result: row.resolve_result,
      issuer_auth: row.issuer_auth,
      verifier_auth: row.verifier_auth,
      ecosystem_participant: row.ecosystem_participant,
      trust_status: trustStatus,
      production,
      evaluated_at: evaluatedAt,
      expires_at: expiresAt,
      corporation_id: corporationId,
    })
}

export async function getTrustResultLatestByDidAtOrBeforeHeight(
  did: string,
  maxHeight: number
): Promise<TrustResultsRow | null> {
  const row = await knex('trust_results')
    .where({ did })
    .where('height', '<=', maxHeight)
    .orderBy('height', 'desc')
    .first()
  return row ? (row as TrustResultsRow) : null
}

export type TrustRoleSnapshot = {
  verified: boolean
  outcome?: unknown
  error?: string
}

function snapshotFromResolution(result: TrustResolution): TrustRoleSnapshot {
  return {
    verified: Boolean(result?.verified),
    outcome: result?.outcome,
  }
}

function snapshotFromError(message: string): TrustRoleSnapshot {
  return { verified: false, error: message }
}

function didsFromRows(res: unknown): string[] {
  const rows = (res as { rows?: Array<{ d?: string }> }).rows ?? []
  const out: string[] = []
  for (const row of rows) {
    const d = row?.d
    if (isDidString(d)) out.push(d)
  }
  return out
}

async function getImpactedDids(blockHeight: number, limit: number): Promise<string[]> {
  const h = blockHeight
  return didsFromRows(await knex.raw(IMPACTED_DIDS_SQL, [h, h, limit]))
}

async function getTriggeredDids(blockHeight: number, limit: number): Promise<string[]> {
  return didsFromRows(
    await knex.raw(TRIGGERED_DIDS_SQL, [blockHeight, VeranaParticipantMessageTypes.TriggerResolver, limit])
  )
}

async function getServicesAnchoredOnIssuers(
  issuerDids: string[],
  blockHeight: number,
  limit: number
): Promise<string[]> {
  if (issuerDids.length === 0 || limit <= 0) return []
  return didsFromRows(
    await knex.raw(SERVICES_ANCHORED_ON_ISSUERS_SQL, [issuerDids, blockHeight, blockHeight, issuerDids, limit])
  )
}

function isResolveError(resolveResult: unknown): boolean {
  return Boolean(resolveResult && typeof resolveResult === 'object' && (resolveResult as { error?: unknown }).error)
}

function trustResultMeaningfullyChanged(nextResolve: unknown, prevRow: TrustResultsRow | null): boolean {
  const next = deriveStoredTrustState(nextResolve)
  const prevStatus = prevRow ? String(prevRow.trust_status ?? 'UNTRUSTED').toUpperCase() : 'UNTRUSTED'
  const prevProduction = prevRow ? Boolean(prevRow.production) : false
  if (next.trustStatus !== prevStatus) return true
  if (next.production !== prevProduction) return true
  const prevError = prevRow ? isResolveError(prevRow.resolve_result) : true
  return isResolveError(nextResolve) !== prevError
}

export async function resolveTrustForDidAtHeight(
  did: string,
  blockHeight: number,
  landingHeight?: number
): Promise<boolean> {
  const { verifiablePublicRegistries, skipDigestSRICheck } = getVerreTrustEvaluationCallOptions()
  const cfg = getResolverRuntimeConfig()
  const retryDays = Number(cfg?.pollObjectCachingRetryDays ?? 0) || 0
  const resourceId = `${did}@${blockHeight}`

  let resolveResult: unknown
  let snap: TrustRoleSnapshot
  let failureMessage: string | null = null

  try {
    const result = (await resolveDID(did, {
      verifiablePublicRegistries,
      skipDigestSRICheck,
    })) as TrustResolution

    // WL-ECS is not enforced by verre; drop the verdict until it is (see verre ResolverConfig.ecsEcosystems).
    if (isEcsAllowlistEnforced() && result.verified && !(await hasAllowlistedEcsServiceCredential(result))) {
      result.verified = false
      result.outcome = TrustResolutionOutcome.NOT_TRUSTED
    }

    resolveResult = result
    snap = snapshotFromResolution(result)
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err)
    failureMessage = message
    const lower = message.toLowerCase()
    const errorCode =
      lower.includes('not found') || lower.includes('not_found') || lower.includes('404')
        ? 'not_found'
        : 'resolution_failed'

    resolveResult = {
      error: true,
      message,
      dereferenceErrors: [],
      credentials: [],
      failedCredentials: [
        {
          id: did,
          error: `DID resolution failed for ${did}`,
          format: 'N/A',
          errorCode,
          message,
        },
      ],
    }
    snap = snapshotFromError(message)
  }

  let saveHeight = blockHeight
  let forwarded = false
  if (typeof landingHeight === 'number' && Number.isInteger(landingHeight) && landingHeight !== blockHeight) {
    const prevRow = await getTrustResultLatestByDidAtOrBeforeHeight(did, blockHeight)
    if (trustResultMeaningfullyChanged(resolveResult, prevRow)) {
      saveHeight = landingHeight
      forwarded = true
    }
  }

  await saveTrustResults({
    did,
    height: saveHeight,
    resolve_result: resolveResult,
    issuer_auth: snap,
    verifier_auth: snap,
    ecosystem_participant: snap,
  })

  if (failureMessage !== null) {
    await markReattemptableFailure({
      resourceId,
      resourceType: 'did-evaluation',
      errorType: 'resolveDID',
      lastError: failureMessage,
      retryDays,
    })
  } else {
    await clearReattemptable(resourceId)
  }

  return forwarded
}

export async function resolveTrustForBlock(blockHeight: number): Promise<void> {
  if (!Number.isInteger(blockHeight) || blockHeight < 0) return

  const tuning = await getResolverTuning()
  const [impactedDids, triggeredDids] = await Promise.all([
    getImpactedDids(blockHeight, tuning.maxDidsPerBlock),
    getTriggeredDids(blockHeight, tuning.maxDidsPerBlock),
  ])

  const evaluated = new Set<string>()
  let frontier = [...new Set([...impactedDids, ...triggeredDids])]

  for (let depth = 0; depth <= MAX_CASCADE_DEPTH && frontier.length > 0; depth++) {
    const budget = tuning.maxDidsPerBlock - evaluated.size
    if (budget <= 0) break

    const batch = frontier.filter((did) => !evaluated.has(did)).slice(0, budget)
    if (batch.length === 0) break
    for (const did of batch) evaluated.add(did)

    await runPool(batch, tuning.didConcurrency, async (did) => {
      await resolveTrustForDidAtHeight(did, blockHeight)
    })

    const remaining = tuning.maxDidsPerBlock - evaluated.size
    const children = await getServicesAnchoredOnIssuers(batch, blockHeight, remaining)
    frontier = children.filter((did) => !evaluated.has(did))
  }
}

function mapOutcomeToTrustStatus(verified: boolean, outcome: unknown): string {
  if (!verified) return 'UNTRUSTED'
  if (outcome === 'verified') return 'TRUSTED'
  if (outcome === 'verified-test') return 'PARTIAL'
  return 'UNTRUSTED'
}

function mapOutcomeToProduction(verified: boolean, outcome: unknown): boolean {
  if (!verified) return false
  return outcome === 'verified'
}

export function deriveStoredTrustState(resolveResult: unknown): { trustStatus: string; production: boolean } {
  if (!resolveResult || typeof resolveResult !== 'object' || (resolveResult as { error?: boolean }).error) {
    return { trustStatus: 'UNTRUSTED', production: false }
  }
  const r = resolveResult as { verified?: boolean; outcome?: unknown }
  const verified = Boolean(r.verified)
  return {
    trustStatus: mapOutcomeToTrustStatus(verified, r.outcome),
    production: mapOutcomeToProduction(verified, r.outcome),
  }
}
