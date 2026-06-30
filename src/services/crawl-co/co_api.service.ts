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
  type GfDataMode,
  getCorporationTrustDeposit,
  getResolvedBlockHeight,
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
      const gfData = (ctx.params.gf_data ?? 'only_active') as GfDataMode

      const trustDataParsed = parseTrustDataMode(ctx.params.trust_data)
      if (!trustDataParsed.ok) {
        return ApiResponder.error(ctx, trustDataParsed.message, 400)
      }
      const trustDataMode = trustDataParsed.mode

      const numericId = Number(id)
      if (!Number.isInteger(numericId) || numericId <= 0) {
        return ApiResponder.error(ctx, `Invalid corporation id '${id}'`, 400)
      }

      const blockHeight = getBlockHeight(ctx)

      const corporationRow = await Corporation.query()
        .findById(numericId)
        .withGraphFetched('governanceFrameworkVersions.documents')

      if (!corporationRow) {
        return ApiResponder.error(ctx, `Corporation ${id} not found`, 404)
      }

      const plain = corporationRow.toJSON() as Record<string, unknown>
      const corporationId = Number(plain.id)
      const allVersions = (plain.governanceFrameworkVersions ?? []) as CorporationGfVersion[]
      const policyAddress = (plain.corporation as string | null) ?? (plain.policy_address as string | null) ?? null

      // Aggregates are latest-state; point-in-time reconstruction is deferred (no corporation_snapshot).
      const [participantStats, controlledEcosystems, trustDeposit] = await Promise.all([
        calculateCorporationParticipantStats(corporationId),
        countControlledEcosystems(corporationId),
        getCorporationTrustDeposit(policyAddress),
      ])

      const corporation: Record<string, unknown> = {
        id: corporationId,
        did: plain.did,
        policy_address: plain.policy_address ?? null,
        language: plain.language ?? null,
        active_version: deriveActiveVersion(allVersions),
        archived: null,
        created: plain.created,
        modified: plain.modified,
        controlled_ecosystems: controlledEcosystems,
        ...participantStats,
        ...trustDeposit,
      }

      if (gfData !== 'none') {
        corporation.versions = applyGfData(allVersions, gfData, preferredLanguage)
      }

      const responsePayload = { corporation, block_height: await getResolvedBlockHeight(blockHeight) }
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
