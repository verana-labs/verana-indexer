import { getTrustEvaluationTtlSeconds } from './trust-resolve'
import { buildEcsCredentials, resolveCorporationId } from './trust-resolve-v4.builders'

export type VtResponseCore = {
  did: string
  trusted: boolean
  evaluatedAtTime: string
  evaluatedAtBlock: number
  expiresAtTime: string
  corporationId: number
}

export type VtResponseSummary = VtResponseCore
export type VtResponseFull = VtResponseCore & { ecsCredentials: Array<Record<string, unknown>> }

export function computeTrusted(resolveResult: unknown): boolean {
  if (!resolveResult || typeof resolveResult !== 'object') return false
  if ((resolveResult as { error?: unknown }).error) return false
  const r = resolveResult as { verified?: unknown; outcome?: unknown }
  if (!r.verified) return false
  return r.outcome === 'verified' || r.outcome === 'verified-test'
}

export type VtResponseCoreArgs = {
  did: string
  resolveResult: unknown
  evaluatedAtBlock: number
  evaluatedAtSource?: Date | string | null
  fallbackEvaluatedAtTime?: string
  ttlSeconds?: number
  atHeight?: number
}

export async function buildVtResponseCore(args: VtResponseCoreArgs): Promise<VtResponseCore> {
  const ttlSeconds = args.ttlSeconds ?? getTrustEvaluationTtlSeconds()
  const evaluatedAtTime =
    args.evaluatedAtSource != null
      ? new Date(args.evaluatedAtSource as Date | string).toISOString()
      : (args.fallbackEvaluatedAtTime ?? new Date().toISOString())
  const expiresAtTime = new Date(new Date(evaluatedAtTime).getTime() + Math.max(0, ttlSeconds) * 1000).toISOString()

  return {
    did: args.did,
    trusted: computeTrusted(args.resolveResult),
    evaluatedAtTime,
    evaluatedAtBlock: args.evaluatedAtBlock,
    expiresAtTime,
    corporationId: await resolveCorporationId(args.did, args.atHeight),
  }
}

export async function buildVtResponse(
  args: VtResponseCoreArgs,
  mode: 'summary' | 'full'
): Promise<VtResponseSummary | VtResponseFull> {
  const core = await buildVtResponseCore(args)
  if (mode === 'summary') return core
  return { ...core, ecsCredentials: await buildEcsCredentials(args.resolveResult) }
}
