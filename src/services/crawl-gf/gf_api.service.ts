/* eslint-disable @typescript-eslint/no-explicit-any */

import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended'
import { Context, ServiceBroker } from 'moleculer'
import BaseService from '../../base/base.service'
import { SERVICE } from '../../common'
import ApiResponder from '../../common/utils/apiResponse'
import { getBlockChainTimeAsOf } from '../../common/utils/block_time'
import { getBlockHeight } from '../../common/utils/blockHeight'
import { CoGovernanceFrameworkVersion } from '../../models/co_governance_framework_version'
import { Corporation } from '../../models/corporation'
import { Ecosystem } from '../../models/ecosystem'
import { GovernanceFrameworkVersion } from '../../models/governance_framework_version'
import { parseCorporationListPagination } from '../crawl-co/co_stats'

interface GfvDocumentRow {
  gfd_id?: number | null
  created: string | Date
  language: string
  url: string
  digest_sri: string
}

interface GfvRowPlain {
  corporation_id?: number | null
  ecosystem_id: number
  version: number
  created: string | Date
  active_since?: string | Date | null
  gfv_id?: number | null
  documents?: GfvDocumentRow[]
}

// One document in preferred_language when present, else all (chain MOD-GF-QRY-1-3 "preferring").
export function selectGfvDocuments(documents: GfvDocumentRow[], preferredLanguage?: string): GfvDocumentRow[] {
  if (preferredLanguage) {
    const preferred = documents.find((d) => d.language === preferredLanguage)
    if (preferred) return [preferred]
  }
  return documents
}

export function buildGfvObject(
  plain: GfvRowPlain,
  source: 'corporation' | 'ecosystem',
  preferredLanguage?: string,
  asOf?: Date
): Record<string, unknown> {
  const chainGfvId = plain.gfv_id ?? null
  const activeSince = plain.active_since && (!asOf || new Date(plain.active_since) <= asOf) ? plain.active_since : null
  const isCgf = source === 'corporation' && Number(plain.ecosystem_id) === 0

  let documents = (plain.documents ?? []).filter((d) => !asOf || new Date(d.created) <= asOf)
  documents = selectGfvDocuments(documents, preferredLanguage)

  return {
    id: chainGfvId,
    ecosystem_id: isCgf ? null : Number(plain.ecosystem_id),
    corporation_id: isCgf ? Number(plain.corporation_id) : null,
    created: plain.created,
    version: plain.version,
    active_since: activeSince,
    documents: documents.map((d) => ({
      id: d.gfd_id ?? null,
      gfv_id: chainGfvId,
      created: d.created,
      language: d.language,
      url: d.url,
      digest_sri: d.digest_sri,
    })),
  }
}

@Service({
  name: SERVICE.V1.GovernanceFrameworkApiService.key,
  version: 1,
})
export default class GovernanceFrameworkApiService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker)
  }

  @Action()
  public async getGovernanceFrameworkVersionV4(ctx: Context<{ id: string; preferred_language?: string }>) {
    try {
      const idStr = String(ctx.params.id ?? '').trim()
      if (!/^\d+$/.test(idStr) || BigInt(idStr) <= BigInt(0)) {
        return ApiResponder.error(ctx, `Invalid governance framework version id '${ctx.params.id}'`, 400)
      }
      const preferredLanguage = ctx.params.preferred_language

      const blockHeight = getBlockHeight(ctx)
      const asOf =
        typeof blockHeight === 'number'
          ? await getBlockChainTimeAsOf(blockHeight, { logContext: '[gf_api:get]' })
          : undefined

      // CGF lives in co_governance_framework_version (ecosystem_id = 0); EGF is authoritative in
      // governance_framework_version. The co table also mirrors EGF rows, so scope this to CGF.
      const coRow = await CoGovernanceFrameworkVersion.query()
        .where('gfv_id', idStr)
        .where('ecosystem_id', 0)
        .withGraphFetched('documents')
        .first()
      const row =
        coRow ?? (await GovernanceFrameworkVersion.query().where('gfv_id', idStr).withGraphFetched('documents').first())
      if (!row) {
        return ApiResponder.error(ctx, `GovernanceFrameworkVersion ${idStr} not found`, 404)
      }

      const plain = row.toJSON() as unknown as GfvRowPlain
      if (asOf && new Date(plain.created) > asOf) {
        return ApiResponder.error(ctx, `GovernanceFrameworkVersion ${idStr} not found`, 404)
      }

      const version = buildGfvObject(plain, coRow ? 'corporation' : 'ecosystem', preferredLanguage, asOf)
      return ApiResponder.success(ctx, { version })
    } catch (err: any) {
      this.logger.error('Error in getGovernanceFrameworkVersionV4:', err)
      return ApiResponder.error(ctx, 'Internal Server Error', 500)
    }
  }

  // IDX-GF-QRY-2 List Governance Framework Versions (v4)
  @Action()
  public async listGovernanceFrameworkVersionsV4(
    ctx: Context<{
      ecosystem_id?: string
      corporation_id?: string
      active_only?: string
      preferred_language?: string
      limit?: string
      min_id?: string
      max_id?: string
      sort?: string
    }>
  ) {
    try {
      const ecoRaw = ctx.params.ecosystem_id
      const corpRaw = ctx.params.corporation_id
      const hasEco = ecoRaw !== undefined && String(ecoRaw) !== ''
      const hasCorp = corpRaw !== undefined && String(corpRaw) !== ''
      if (hasEco === hasCorp) {
        return ApiResponder.error(ctx, 'Exactly one of "ecosystem_id" or "corporation_id" must be set', 400)
      }
      const subjectId = String(hasEco ? ecoRaw : corpRaw).trim()
      if (!/^\d+$/.test(subjectId) || BigInt(subjectId) <= BigInt(0)) {
        return ApiResponder.error(ctx, `Invalid ${hasEco ? 'ecosystem_id' : 'corporation_id'} '${subjectId}'`, 400)
      }

      const activeOnlyRaw = ctx.params.active_only
      if (activeOnlyRaw !== undefined && !['true', 'false', ''].includes(String(activeOnlyRaw).toLowerCase())) {
        return ApiResponder.error(ctx, '"active_only" must be a boolean', 400)
      }
      const activeOnly = String(activeOnlyRaw).toLowerCase() === 'true'

      const pageParsed = parseCorporationListPagination(ctx.params)
      if (!pageParsed.ok) {
        return ApiResponder.error(ctx, pageParsed.message, 400)
      }
      const { limit, minId, maxId, direction } = pageParsed.value

      const preferredLanguage = ctx.params.preferred_language
      const blockHeight = getBlockHeight(ctx)
      const asOf =
        typeof blockHeight === 'number'
          ? await getBlockChainTimeAsOf(blockHeight, { logContext: '[gf_api:list]' })
          : undefined

      let activeVersion: number | null = null
      if (activeOnly) {
        const subject = hasEco
          ? await Ecosystem.query().findById(subjectId)
          : await Corporation.query().findById(subjectId)
        activeVersion = (subject?.active_version as number | null | undefined) ?? null
        if (activeVersion == null) {
          return ApiResponder.success(ctx, { versions: [] })
        }
      }

      let query = hasCorp
        ? CoGovernanceFrameworkVersion.query().where('corporation_id', subjectId).where('ecosystem_id', 0)
        : GovernanceFrameworkVersion.query().where('ecosystem_id', subjectId)
      query = query.whereNotNull('gfv_id').withGraphFetched('documents')
      if (activeOnly) query = query.where('version', activeVersion as number)
      if (asOf) query = query.where('created', '<=', asOf.toISOString())
      if (minId !== undefined) query = query.where('gfv_id', '>=', minId)
      if (maxId !== undefined) query = query.where('gfv_id', '<', maxId)
      const rows = await query.orderBy('gfv_id', direction).limit(limit)

      const source = hasCorp ? 'corporation' : 'ecosystem'
      const versions = rows.map((r) =>
        buildGfvObject(r.toJSON() as unknown as GfvRowPlain, source, preferredLanguage, asOf)
      )
      return ApiResponder.success(ctx, { versions })
    } catch (err: any) {
      this.logger.error('Error in listGovernanceFrameworkVersionsV4:', err)
      return ApiResponder.error(ctx, 'Internal Server Error', 500)
    }
  }
}
