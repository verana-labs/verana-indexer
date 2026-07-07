/* eslint-disable @typescript-eslint/no-explicit-any */

import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended'
import { Context, ServiceBroker } from 'moleculer'
import BaseService from '../../base/base.service'
import { MODULE_DISPLAY_NAMES, ModulesParamsNamesTypes, SERVICE } from '../../common'
import ApiResponder from '../../common/utils/apiResponse'
import { getBlockChainTimeAsOf } from '../../common/utils/block_time'
import { getBlockHeight } from '../../common/utils/blockHeight'
import { Corporation } from '../../models/corporation'
import { CorporationHistory } from '../../models/corporation_history'
import { enrichTrustDataDeep, parseTrustDataMode } from '../resolver/trust-data-enrichment'
import {
  applyGfData,
  buildCorporationObject,
  type CorporationGfVersion,
  calculateCorporationParticipantStats,
  calculateCorporationParticipantStatsBatch,
  countControlledEcosystems,
  countControlledEcosystemsAtHeight,
  countControlledEcosystemsBatch,
  deriveActiveVersion,
  emptyParticipantStats,
  emptyTrustDepositSnapshot,
  getCorporationBaseAtHeight,
  getCorporationTrustDeposit,
  getCorporationTrustDepositAtHeight,
  getCorporationTrustDepositBatch,
  getResolvedBlockHeight,
  parseCorporationListPagination,
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
      const livePolicyAddress = (plain.policy_address as string | null) ?? (plain.corporation as string | null) ?? null

      const pointInTime = typeof blockHeight === 'number'
      const asOf = pointInTime
        ? await getBlockChainTimeAsOf(blockHeight as number, { logContext: '[co_api:get]' })
        : undefined

      // At a height, reconstruct the whole corporation as of that block from the history tables;
      // otherwise use the latest indexed state.
      let base = {
        did: (plain.did as string | null) ?? null,
        policy_address: livePolicyAddress,
        language: (plain.language as string | null) ?? null,
        modified: (plain.modified as string | Date | null) ?? null,
      }
      if (pointInTime) {
        const atHeight = await getCorporationBaseAtHeight(corporationId, blockHeight as number)
        if (!atHeight) {
          return ApiResponder.error(ctx, `Corporation ${id} not found`, 404)
        }
        base = {
          did: atHeight.did,
          policy_address: atHeight.policy_address,
          language: atHeight.language,
          modified: atHeight.modified,
        }
      }
      const policyAddress = base.policy_address ?? livePolicyAddress

      const [participantStats, controlledEcosystems, trustDeposit, resolvedBlockHeight] = await Promise.all([
        calculateCorporationParticipantStats(corporationId, blockHeight),
        pointInTime
          ? countControlledEcosystemsAtHeight(corporationId, blockHeight as number)
          : countControlledEcosystems(corporationId),
        pointInTime
          ? getCorporationTrustDepositAtHeight(policyAddress, blockHeight as number)
          : getCorporationTrustDeposit(policyAddress),
        getResolvedBlockHeight(blockHeight),
      ])

      const activeVersion = pointInTime
        ? deriveActiveVersion(cgfVersions, asOf)
        : ((plain.active_version as number | null) ?? null)

      const corporation: Record<string, unknown> = {
        id: corporationId,
        did: base.did,
        policy_address: policyAddress,
        language: base.language,
        active_version: activeVersion,
        created: plain.created,
        modified: base.modified,
        controlled_ecosystems: controlledEcosystems,
        ...participantStats,
        ...trustDeposit,
      }

      if (gfData !== 'none') {
        corporation.versions = applyGfData(cgfVersions, gfData, preferredLanguage, asOf)
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

  // IDX-CO-QRY-2 List Corporations (v4)
  @Action()
  public async listCorporationsV4(
    ctx: Context<{
      gf_data?: string
      preferred_language?: string
      did?: string
      modified_after?: string
      trust_data?: string
      limit?: string
      min_id?: string
      max_id?: string
      sort?: string
    }>
  ) {
    try {
      const { did, preferred_language: preferredLanguage } = ctx.params

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

      const pageParsed = parseCorporationListPagination(ctx.params)
      if (!pageParsed.ok) {
        return ApiResponder.error(ctx, pageParsed.message, 400)
      }
      const { limit, minId, maxId, direction } = pageParsed.value

      let modifiedAfterIso: string | undefined
      if (ctx.params.modified_after) {
        const ts = new Date(ctx.params.modified_after)
        if (Number.isNaN(ts.getTime())) {
          return ApiResponder.error(ctx, '"modified_after" must be a valid ISO 8601 datetime', 400)
        }
        modifiedAfterIso = ts.toISOString()
      }

      const blockHeight = getBlockHeight(ctx)

      let query = Corporation.query().withGraphFetched(
        gfData === 'none' ? 'governanceFrameworkVersions' : 'governanceFrameworkVersions.documents'
      )
      if (did) query = query.where('did', String(did))
      if (modifiedAfterIso) query = query.where('modified', '>', modifiedAfterIso)
      if (minId !== undefined) query = query.where('id', '>=', minId)
      if (maxId !== undefined) query = query.where('id', '<', maxId)
      // At-Block-Height: exclude corporations not yet created at that height (created is immutable).
      // Field-level point-in-time (e.g. language, active_version) stays latest-state.
      if (typeof blockHeight === 'number') {
        const asOf = await getBlockChainTimeAsOf(blockHeight, { logContext: '[co_api:list]' })
        query = query.where('created', '<=', asOf.toISOString())
      }
      query = query.orderBy('id', direction).limit(limit)

      const rows = await query
      if (rows.length === 0) {
        return ApiResponder.success(ctx, { corporations: [] })
      }

      const plains = rows.map((row) => row.toJSON() as Record<string, unknown>)
      const ids = plains.map((plain) => plain.id as number | string)
      const addresses = plains.map(
        (plain) => (plain.policy_address as string | null) ?? (plain.corporation as string | null) ?? null
      )

      const [statsMap, ecosystemMap, trustDepositMap] = await Promise.all([
        calculateCorporationParticipantStatsBatch(ids, blockHeight),
        countControlledEcosystemsBatch(ids),
        getCorporationTrustDepositBatch(addresses),
      ])

      const corporations = plains.map((plain) => {
        // CGF only — the relation also returns EGF rows (non-zero ecosystem_id) owned by the corporation
        const allVersions = (plain.governanceFrameworkVersions ?? []) as CorporationGfVersion[]
        const cgfVersions = allVersions.filter((v) => !v.ecosystem_id)
        const policyAddress = (plain.policy_address as string | null) ?? (plain.corporation as string | null) ?? null
        return buildCorporationObject({
          plain,
          cgfVersions,
          participantStats: statsMap.get(String(plain.id)) ?? emptyParticipantStats(),
          controlledEcosystems: ecosystemMap.get(String(plain.id)) ?? 0,
          trustDeposit: (policyAddress && trustDepositMap.get(policyAddress)) || emptyTrustDepositSnapshot(),
          gfData,
          preferredLanguage,
        })
      })

      const responsePayload = { corporations }
      const enriched =
        trustDataMode === 'none'
          ? responsePayload
          : await enrichTrustDataDeep(responsePayload, trustDataMode, blockHeight)

      return ApiResponder.success(ctx, enriched)
    } catch (err) {
      this.logger.error('Error in listCorporationsV4:', err)
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

  @Action()
  public async getCorporationParams(ctx: Context) {
    const { getModuleParamsAction } = await import('../../common/utils/params_service')
    return getModuleParamsAction(ctx, ModulesParamsNamesTypes.CO, MODULE_DISPLAY_NAMES.CORPORATION)
  }
}
