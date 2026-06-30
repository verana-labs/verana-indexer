/* eslint-disable @typescript-eslint/no-explicit-any */

import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended'
import { Context, ServiceBroker } from 'moleculer'
import BaseService from '../../base/base.service'
import { SERVICE } from '../../common'
import ApiResponder from '../../common/utils/apiResponse'
import { getBlockHeight } from '../../common/utils/blockHeight'
import { Corporation } from '../../models/corporation'
import { CorporationHistory } from '../../models/corporation_history'
import { enrichTrustDataDeep, parseTrustDataMode } from '../resolver/trust-data-enrichment'
import {
  applyGfData,
  type CorporationGfVersion,
  calculateCorporationParticipantStats,
  countControlledEcosystems,
  deriveActiveVersion,
  getCorporationTrustDeposit,
  getResolvedBlockHeight,
  parseGfDataMode,
} from './co_stats'

@Service({
  name: SERVICE.V1.CorporationApiService.key,
  version: 1,
})
export default class CorporationApiService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker)
  }

  @Action()
  public async getCorporation(ctx: Context<{ id: string }>) {
    try {
      const { id } = ctx.params
      const query = Corporation.query().withGraphFetched('[members, governanceFrameworkVersions.documents]')

      const numericId = Number(id)
      const corporation =
        Number.isInteger(numericId) && numericId > 0
          ? await query.findById(numericId)
          : await query.where('did', id).orWhere('corporation', id).first()

      if (!corporation) {
        return ApiResponder.error(ctx, `Corporation '${id}' not found`, 404)
      }
      return ApiResponder.success(ctx, corporation)
    } catch (err: any) {
      return ApiResponder.error(ctx, err?.message || String(err), 500)
    }
  }

  // IDX-CO-QRY-1 Get Corporation (v4)
  @Action()
  public async getCorporationV4(
    ctx: Context<{ id: string; gf_data?: string; preferred_language?: string; trust_data?: string }>
  ) {
    try {
      const { id, preferred_language: preferredLanguage } = ctx.params

      const gfDataParsed = parseGfDataMode(ctx.params.gf_data)
      if (!gfDataParsed.ok) {
        return ApiResponder.error(ctx, gfDataParsed.message, 400)
      }
      const gfData = gfDataParsed.mode

      const trustDataParsed = parseTrustDataMode(ctx.params.trust_data)
      if (!trustDataParsed.ok) {
        return ApiResponder.error(ctx, trustDataParsed.message, 400)
      }
      const trustDataMode = trustDataParsed.mode

      // uint64 id: query by digit-string to avoid Number() precision loss above 2^53
      const idStr = String(id).trim()
      if (!/^\d+$/.test(idStr) || BigInt(idStr) <= BigInt(0)) {
        return ApiResponder.error(ctx, `Invalid corporation id '${id}'`, 400)
      }

      const blockHeight = getBlockHeight(ctx)

      const corporationRow = await Corporation.query()
        .findById(idStr)
        .withGraphFetched(gfData === 'none' ? 'governanceFrameworkVersions' : 'governanceFrameworkVersions.documents')

      if (!corporationRow) {
        return ApiResponder.error(ctx, `Corporation ${id} not found`, 404)
      }

      const plain = corporationRow.toJSON() as Record<string, unknown>
      const corporationId = plain.id as number | string
      // CGF only — the relation also returns EGF rows (non-zero ecosystem_id) owned by the corporation
      const allVersions = (plain.governanceFrameworkVersions ?? []) as CorporationGfVersion[]
      const cgfVersions = allVersions.filter((v) => !v.ecosystem_id)
      const policyAddress = (plain.policy_address as string | null) ?? (plain.corporation as string | null) ?? null

      // participants honor At-Block-Height; controlled_ecosystems + trust-deposit stay latest (point-in-time deferred)
      const [participantStats, controlledEcosystems, trustDeposit, resolvedBlockHeight] = await Promise.all([
        calculateCorporationParticipantStats(corporationId, blockHeight),
        countControlledEcosystems(corporationId),
        getCorporationTrustDeposit(policyAddress),
        getResolvedBlockHeight(blockHeight),
      ])

      const corporation: Record<string, unknown> = {
        id: corporationId,
        did: plain.did,
        policy_address: policyAddress,
        language: plain.language ?? null,
        active_version: deriveActiveVersion(cgfVersions),
        // not indexed yet (no archived column / ArchiveCorporation handler)
        archived: null,
        created: plain.created,
        modified: plain.modified,
        controlled_ecosystems: controlledEcosystems,
        ...participantStats,
        ...trustDeposit,
      }

      if (gfData !== 'none') {
        corporation.versions = applyGfData(cgfVersions, gfData, preferredLanguage)
      }

      const responsePayload = { corporation, block_height: resolvedBlockHeight }
      const enriched =
        trustDataMode === 'none'
          ? responsePayload
          : await enrichTrustDataDeep(responsePayload, trustDataMode, blockHeight)

      return ApiResponder.success(ctx, enriched)
    } catch (err) {
      this.logger.error('Error in getCorporationV4:', err)
      return ApiResponder.error(ctx, 'Internal Server Error', 500)
    }
  }

  @Action()
  public async listCorporations(ctx: Context<{ limit?: string; offset?: string }>) {
    try {
      const limit = Math.min(Number(ctx.params.limit) || 100, 1000)
      const offset = Number(ctx.params.offset) || 0

      const corporations = await Corporation.query()
        .withGraphFetched('[members, governanceFrameworkVersions.documents]')
        .orderBy('id', 'asc')
        .limit(limit)
        .offset(offset)

      return ApiResponder.success(ctx, { corporations, limit, offset })
    } catch (err: any) {
      return ApiResponder.error(ctx, err?.message || String(err), 500)
    }
  }

  @Action()
  public async getCorporationHistory(ctx: Context<{ id: string }>) {
    try {
      const { id } = ctx.params
      const numericId = Number(id)

      let corporationId = Number.isInteger(numericId) && numericId > 0 ? numericId : null
      if (!corporationId) {
        const corporation = await Corporation.query().where('did', id).orWhere('corporation', id).first()
        if (!corporation) {
          return ApiResponder.error(ctx, `Corporation '${id}' not found`, 404)
        }
        corporationId = corporation.id
      }

      const history = await CorporationHistory.query()
        .where('corporation_id', corporationId)
        .orderBy('height', 'asc')
        .orderBy('id', 'asc')

      return ApiResponder.success(ctx, { history })
    } catch (err: any) {
      return ApiResponder.error(ctx, err?.message || String(err), 500)
    }
  }
}
