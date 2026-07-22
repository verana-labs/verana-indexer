import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended'
import { Context, ServiceBroker } from 'moleculer'
import BullableService from '../../base/bullable.service'
import { MODULE_DISPLAY_NAMES, ModulesParamsNamesTypes, SERVICE } from '../../common'
import { buildActivityTimeline } from '../../common/utils/activity_timeline_helper'
import ApiResponder from '../../common/utils/apiResponse'
import knex from '../../common/utils/db_connection'
import { getModuleParams, getModuleParamsAction } from '../../common/utils/params_service'
import { mapTrustDepositApiFields } from '../../common/vpr-v4-mapping'
import TrustDeposit from '../../models/trust_deposit'
import { resolveAddressByCorporationId } from '../crawl-co/corporation_resolve'

@Service({
  name: SERVICE.V1.TrustDepositApiService.key,
  version: 1,
})
export default class TrustDepositApiService extends BullableService {
  public constructor(public broker: ServiceBroker) {
    super(broker)
  }

  @Action({
    name: 'getTrustDeposit',
    params: {
      corporation_id: [{ type: 'string' }, { type: 'number' }],
    },
  })
  public async getTrustDeposit(ctx: Context<{ corporation_id: string | number }>) {
    try {
      const corporationId = this.parseCorporationId(ctx.params.corporation_id)
      if (corporationId == null) {
        return ApiResponder.error(ctx, 'Invalid corporation_id: must be a positive integer', 400)
      }

      const account = await resolveAddressByCorporationId(corporationId)
      if (!account) {
        return ApiResponder.error(ctx, `No corporation found for id: ${corporationId}`, 404)
      }

      const blockHeight = (ctx.meta as any)?.blockHeight
      const shareValue = await this.getTrustDepositShareValue(typeof blockHeight === 'number' ? blockHeight : undefined)

      // If AtBlockHeight is provided, query historical state
      if (typeof blockHeight === 'number') {
        const historyRecord = await knex('trust_deposit_history')
          .where({ corporation: account })
          .where('height', '<=', blockHeight)
          .orderBy('height', 'desc')
          .orderBy('created_at', 'desc')
          .first()

        if (!historyRecord) {
          this.logger.info(`No trust deposit found for corporation_id: ${corporationId}`)
          return ApiResponder.error(ctx, `No trust deposit found for corporation_id: ${corporationId}`, 404)
        }

        const result = {
          trust_deposit: this.buildTrustDepositResponse(historyRecord, corporationId, shareValue),
        }

        return ApiResponder.success(ctx, result, 200)
      }

      // Otherwise, return latest state
      const trustDeposit = await TrustDeposit.query().findOne({ corporation: account })

      if (!trustDeposit) {
        this.logger.info(`No trust deposit found for corporation_id: ${corporationId}`)
        return ApiResponder.error(ctx, `No trust deposit found for corporation_id: ${corporationId}`, 404)
      }
      const result = {
        trust_deposit: this.buildTrustDepositResponse(trustDeposit, corporationId, shareValue),
      }
      return ApiResponder.success(ctx, result, 200)
    } catch (err: any) {
      this.logger.error('Error in getTrustDeposit:', err)
      return ApiResponder.error(ctx, 'Internal Server Error', 500)
    }
  }

  @Action({
    name: 'getModuleParams',
  })
  public async getModuleParams(ctx: Context) {
    return getModuleParamsAction(ctx, ModulesParamsNamesTypes.TD, MODULE_DISPLAY_NAMES.TRUST_DEPOSIT)
  }

  @Action({
    name: 'getTrustDepositHistory',
    params: {
      corporation_id: [{ type: 'string' }, { type: 'number' }],
      limit: { type: 'number', integer: true, optional: true, convert: true },
      min_id: { type: 'number', integer: true, optional: true, convert: true },
      max_id: { type: 'number', integer: true, optional: true, convert: true },
      sort: { type: 'string', optional: true },
    },
  })
  public async getTrustDepositHistory(
    ctx: Context<{ corporation_id: string | number; limit?: number; min_id?: number; max_id?: number; sort?: string }>
  ) {
    try {
      const corporationId = this.parseCorporationId(ctx.params.corporation_id)
      if (corporationId == null) {
        return ApiResponder.error(ctx, 'Invalid corporation_id: must be a positive integer', 400)
      }

      const sortDir = this.parseSortDirection(ctx.params.sort)
      if (sortDir === null) {
        return ApiResponder.error(ctx, "Invalid sort: only 'id', '+id' or '-id' are supported", 400)
      }

      const { min_id: minId, max_id: maxId } = ctx.params
      const limit = Math.min(Math.max(Number(ctx.params.limit) || 64, 1), 1024)

      const account = await resolveAddressByCorporationId(corporationId)
      if (!account) {
        return ApiResponder.error(ctx, `No corporation found for id: ${corporationId}`, 404)
      }

      const atBlockHeight =
        (ctx.meta as any)?.$headers?.['at-block-height'] || (ctx.meta as any)?.$headers?.['At-Block-Height']

      const activity = await buildActivityTimeline(
        {
          entityType: 'TrustDeposit',
          historyTable: 'trust_deposit_history',
          idField: 'corporation',
          entityId: account,
          msgTypePrefixes: ['/verana.td.v1'],
        },
        {
          atBlockHeight,
          pagination: {
            minId,
            maxId,
            limit,
            sort: sortDir === 'asc' ? 'id' : '-id',
          },
        }
      )

      const items = (activity || []).map((item: Record<string, unknown>) => ({
        ...item,
        entity_id: String(corporationId),
      }))

      const result = {
        entity_type: 'TrustDeposit',
        entity_id: String(corporationId),
        activity: items,
      }

      return ApiResponder.success(ctx, result, 200)
    } catch (err: any) {
      this.logger.error('Error in getTrustDepositHistory:', err)
      return ApiResponder.error(ctx, 'Internal Server Error', 500)
    }
  }

  private async getTrustDepositShareValue(blockHeight?: number): Promise<number> {
    const result = await getModuleParams(ModulesParamsNamesTypes.TD, blockHeight)
    return Number(result?.params?.trust_deposit_share_value ?? 0)
  }

  private buildTrustDepositResponse(
    row: Record<string, any>,
    corporationId: number,
    shareValue: number
  ): Record<string, unknown> {
    const share = Number(row.share ?? 0)
    const deposit = Number(row.deposit ?? 0)
    return mapTrustDepositApiFields({
      corporation_id: corporationId,
      share,
      deposit,
      refunded: Number(row.claimable ?? 0),
      claimable: Math.max(0, share * shareValue - deposit),
      slashed_deposit: Number(row.slashed_deposit ?? 0),
      repaid_deposit: Number(row.repaid_deposit ?? 0),
      last_slashed: row.last_slashed ?? null,
      last_repaid: row.last_repaid ?? null,
      slash_count: Number(row.slash_count ?? 0),
    } as Record<string, unknown>)
  }

  private parseCorporationId(value: string | number): number | null {
    const raw = String(value).trim()
    if (!/^\d+$/.test(raw)) return null
    const id = Number(raw)
    return Number.isInteger(id) && id > 0 ? id : null
  }

  private parseSortDirection(sort?: string): 'asc' | 'desc' | null {
    if (!sort || !sort.trim()) return 'desc'
    const normalized = sort.trim().toLowerCase()
    if (normalized === 'id' || normalized === '+id') return 'asc'
    if (normalized === '-id') return 'desc'
    return null
  }
}
