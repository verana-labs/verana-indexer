import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended'
import { Context, ServiceBroker } from 'moleculer'
import BaseService from '../../base/base.service'
import { BULL_JOB_NAME, SERVICE } from '../../common'
import { ALL_PARTICIPANT_STATES, type ParticipantState } from '../../common/types/types'
import ApiResponder from '../../common/utils/apiResponse'
import knex from '../../common/utils/db_connection'
import { BlockCheckpoint } from '../../models'
import {
  getTrustEvaluationTtlSeconds,
  getTrustResultLatestByDidAtOrBeforeHeight,
  resolveTrustForDidAtHeight,
} from './trust-resolve'
import {
  buildCorporation,
  buildEcosystems,
  buildEcsCredentials,
  buildParticipations,
  buildPresentations,
  buildServices,
  type EcosystemsOptions,
  type PresentationsOptions,
} from './trust-resolve-v4.builders'
import { buildVtResponseCore, computeTrusted } from './trust-vt-response'

function isDidParam(did: string): did is string {
  return did.startsWith('did:')
}

@Service({
  name: SERVICE.V1.TrustV1ApiService.key,
  version: 1,
})
export class TrustApiService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker)
  }

  private async didExistsAtHeight(did: string, atHeight?: number): Promise<boolean> {
    const blockTime = atHeight != null ? await this.blockTimeAtHeight(atHeight) : null
    const byHeight = (q: any) => (atHeight != null ? q.andWhere('height', '<=', atHeight) : q)
    const byCreated = (q: any) => (blockTime != null ? q.andWhere('created', '<=', blockTime) : q)

    const checks: Array<[string, (q: any) => any, string]> = [
      ['trust_results', byHeight, 'did'],
      ['corporation', byCreated, 'id'],
      ['participants', byCreated, 'id'],
      ['participant_history', byHeight, 'id'],
      ['ecosystem', byCreated, 'id'],
      ['ecosystem_history', byHeight, 'id'],
    ]

    for (const [table, scope, column] of checks) {
      try {
        const hit = await scope(knex(table).select(column).where({ did })).first()
        if (hit) return true
      } catch {
        this.logger.warn(`Failed to query ${table} for existence check of DID ${did}.`, { did })
      }
    }
    return false
  }

  private async ensureDidExistsOr404(ctx: Context, did: string, atHeight?: number) {
    if (await this.didExistsAtHeight(did, atHeight)) return null
    return ApiResponder.error(ctx, 'DID not found', 404)
  }

  private isTrustRowExpired(row: { evaluated_at?: unknown; created_at?: unknown } | null | undefined): boolean {
    if (!row) return true
    const ttlSeconds = getTrustEvaluationTtlSeconds()
    const evaluatedAtSource = row.evaluated_at ?? row.created_at
    if (evaluatedAtSource == null) return true
    const evaluatedMs = new Date(evaluatedAtSource as Date | string).getTime()
    if (!Number.isFinite(evaluatedMs)) return true
    return Date.now() >= evaluatedMs + ttlSeconds * 1000
  }

  private isTrustRowTrusted(
    row:
      | { did?: string; resolve_result?: unknown; height?: number; evaluated_at?: unknown; created_at?: unknown }
      | null
      | undefined
  ): boolean {
    if (!row) return false
    return computeTrusted(row.resolve_result ?? null)
  }

  private shouldReevaluateTrustRow(
    row:
      | { did?: string; resolve_result?: unknown; height?: number; evaluated_at?: unknown; created_at?: unknown }
      | null
      | undefined
  ): boolean {
    return this.isTrustRowExpired(row) || !this.isTrustRowTrusted(row)
  }

  private async getLastProcessedTrustBlockHeight(): Promise<number> {
    const trustRow = await BlockCheckpoint.query().where('job_name', BULL_JOB_NAME.HANDLE_TRUST_RESOLVE).first()
    const h = Number(trustRow?.height ?? 0)
    return Number.isFinite(h) && h >= 0 ? Math.trunc(h) : 0
  }

  private async blockTimeAtHeight(height: number): Promise<Date | null> {
    if (!Number.isInteger(height) || height < 0) return null
    const row = await knex('block').select('time').where('height', height).first()
    const t = (row as { time?: Date | string } | undefined)?.time
    const d = t != null ? new Date(t as Date | string) : null
    return d && Number.isFinite(d.getTime()) ? d : null
  }

  private parseParticipationsSelector(value: unknown): ParticipantState[] | null {
    if (value === undefined || value === false || value === null) return null
    if (value === true) return ['ACTIVE']
    if (typeof value === 'object') {
      const states = (value as { states?: unknown }).states
      if (Array.isArray(states)) {
        const valid = states
          .map((s) => String(s).toUpperCase())
          .filter((s): s is ParticipantState => (ALL_PARTICIPANT_STATES as string[]).includes(s))
        return valid.length > 0 ? [...new Set(valid)] : ['ACTIVE']
      }
      return ['ACTIVE']
    }
    return null
  }

  private parsePresentationsSelector(value: unknown): PresentationsOptions | null {
    if (value === undefined || value === false || value === null) return null
    if (value === true) return { unresolvableCredentialIds: false, invalidCredentialIds: false }
    if (typeof value === 'object') {
      const obj = value as { unresolvableCredentialIds?: unknown; invalidCredentialIds?: unknown }
      return {
        unresolvableCredentialIds: obj.unresolvableCredentialIds === true,
        invalidCredentialIds: obj.invalidCredentialIds === true,
      }
    }
    return null
  }

  private parseEcosystemsSelector(value: unknown): EcosystemsOptions | null {
    if (value === undefined || value === false || value === null) return null
    if (value === true) {
      return { includeArchived: false, credentialSchemas: { include: false, includeArchived: false } }
    }
    if (typeof value === 'object') {
      const obj = value as { includeArchived?: unknown; credentialSchemas?: unknown }
      const cs = obj.credentialSchemas
      let credentialSchemas = { include: false, includeArchived: false }
      if (cs === true) {
        credentialSchemas = { include: true, includeArchived: false }
      } else if (cs && typeof cs === 'object') {
        credentialSchemas = {
          include: true,
          includeArchived: (cs as { includeArchived?: unknown }).includeArchived === true,
        }
      }
      return { includeArchived: obj.includeArchived === true, credentialSchemas }
    }
    return null
  }

  @Action({
    rest: 'POST /v4/verifiable-trust/resolve',
    params: {
      did: { type: 'string' },
      corporation: { type: 'boolean', optional: true },
      ecsCredentials: { type: 'boolean', optional: true },
      services: { type: 'boolean', optional: true },
      participations: { type: 'any', optional: true },
      presentations: { type: 'any', optional: true },
      ecosystems: { type: 'any', optional: true },
    },
  })
  public async resolveV4(
    ctx: Context<{
      did: string
      corporation?: boolean
      ecsCredentials?: boolean
      services?: boolean
      participations?: unknown
      presentations?: unknown
      ecosystems?: unknown
    }>
  ) {
    const did = ctx.params.did
    if (!isDidParam(did)) return ApiResponder.error(ctx, 'Missing or invalid "did". Must start with "did:".', 400)

    const metaBlockHeight = (ctx.meta as { blockHeight?: number } | undefined)?.blockHeight
    const requestedHeight = typeof metaBlockHeight === 'number' ? metaBlockHeight : undefined
    const isLive = requestedHeight == null
    const effectiveHeight =
      requestedHeight != null && Number.isInteger(requestedHeight) && requestedHeight >= 0
        ? requestedHeight
        : await this.getLastProcessedTrustBlockHeight()
    let row = await getTrustResultLatestByDidAtOrBeforeHeight(did, effectiveHeight)

    if (isLive && this.shouldReevaluateTrustRow(row)) {
      try {
        await resolveTrustForDidAtHeight(did, effectiveHeight)
        const refreshed = await getTrustResultLatestByDidAtOrBeforeHeight(did, effectiveHeight)
        if (refreshed) row = refreshed
      } catch (err) {
        this.logger.warn(`Live trust re-evaluation failed for DID ${did}.`, { did, err })
      }
    }

    if (!row) {
      const didErr = await this.ensureDidExistsOr404(ctx, did, requestedHeight)
      if (didErr) return didErr
    }

    const resolveResult = row?.resolve_result ?? null
    const ttlSeconds = getTrustEvaluationTtlSeconds()
    const blockTime = await this.blockTimeAtHeight(effectiveHeight)

    const core = await buildVtResponseCore({
      did,
      resolveResult,
      evaluatedAtBlock: row ? row.height : effectiveHeight,
      evaluatedAtSource: row ? (row.evaluated_at ?? row.created_at) : null,
      fallbackEvaluatedAtTime: (blockTime ?? new Date()).toISOString(),
      ttlSeconds,
      atHeight: requestedHeight,
    })

    const body: Record<string, unknown> = { ...core }

    if (ctx.params.corporation === true) {
      const corporation = await buildCorporation(did, requestedHeight)
      if (corporation) body.corporation = corporation
    }

    const now = blockTime ?? new Date(core.evaluatedAtTime)
    const states = this.parseParticipationsSelector(ctx.params.participations)
    if (states) body.participations = await buildParticipations(did, now, states, requestedHeight)
    const ecosystemsOpts = this.parseEcosystemsSelector(ctx.params.ecosystems)
    if (ecosystemsOpts) body.ecosystems = await buildEcosystems(did, ecosystemsOpts, requestedHeight)
    if (ctx.params.ecsCredentials === true) body.ecsCredentials = await buildEcsCredentials(resolveResult)
    const presentationsOpts = this.parsePresentationsSelector(ctx.params.presentations)
    if (presentationsOpts) body.presentations = await buildPresentations(resolveResult, presentationsOpts)
    if (ctx.params.services === true) body.services = buildServices(resolveResult)
    return ApiResponder.success(ctx, body, 200)
  }
}

export { TrustApiService as TrustV1ApiService }

export default TrustApiService
