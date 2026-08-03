import { buildEcsCredentials, resolveCorporationId } from './trust-resolve-v4.builders'

export type VtResponseCore = {
  did: string
  trusted: boolean
  evaluatedAtTime: string
  evaluatedAtBlock: number
  expiresAtTime: string | null
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

export function trustedFromStatus(trustStatus: unknown): boolean {
  const status = String(trustStatus ?? 'UNTRUSTED').toUpperCase()
  return status === 'TRUSTED' || status === 'PARTIAL'
}

export type VtResponseCoreArgs = {
  did: string
  resolveResult: unknown
  evaluatedAtBlock: number
  evaluatedAtSource?: Date | string | null
  fallbackEvaluatedAtTime?: string
  expiresAtSource?: Date | string | null
  trustStatusSource?: string | null
  corporationIdSource?: number | null
  atHeight?: number
}

export async function buildVtResponseCore(args: VtResponseCoreArgs): Promise<VtResponseCore> {
  const evaluatedAtTime =
    args.evaluatedAtSource != null
      ? new Date(args.evaluatedAtSource as Date | string).toISOString()
      : (args.fallbackEvaluatedAtTime ?? new Date().toISOString())
  const expiresAtTime =
    args.expiresAtSource != null ? new Date(args.expiresAtSource as Date | string).toISOString() : null

  return {
    did: args.did,
    trusted:
      args.trustStatusSource != null ? trustedFromStatus(args.trustStatusSource) : computeTrusted(args.resolveResult),
    evaluatedAtTime,
    evaluatedAtBlock: args.evaluatedAtBlock,
    expiresAtTime,
    corporationId: args.corporationIdSource ?? (await resolveCorporationId(args.did, args.atHeight)),
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
