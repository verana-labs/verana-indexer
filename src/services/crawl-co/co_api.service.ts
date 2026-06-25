/* eslint-disable @typescript-eslint/no-explicit-any */

import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended'
import { Context, ServiceBroker } from 'moleculer'
import BaseService from '../../base/base.service'
import { SERVICE } from '../../common'
import ApiResponder from '../../common/utils/apiResponse'
import { Corporation } from '../../models/corporation'
import { CorporationHistory } from '../../models/corporation_history'

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
