/* eslint-disable @typescript-eslint/no-explicit-any */

import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended'
import { Context, ServiceBroker } from 'moleculer'
import BaseService from '../../base/base.service'
import { SERVICE } from '../../common'
import ApiResponder from '../../common/utils/apiResponse'
import { getBlockChainTimeAsOf } from '../../common/utils/block_time'
import { getBlockHeight } from '../../common/utils/blockHeight'
import { CoGovernanceFrameworkVersion } from '../../models/co_governance_framework_version'
import { GovernanceFrameworkVersion } from '../../models/governance_framework_version'

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

  // IDX-GF-QRY-1 Get Governance Framework Version (v4)
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
}
