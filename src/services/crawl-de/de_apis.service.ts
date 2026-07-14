import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended'
import { Context, ServiceBroker } from 'moleculer'
import BaseService from '../../base/base.service'
import { SERVICE } from '../../common'
import ApiResponder from '../../common/utils/apiResponse'
import { getBlockHeight } from '../../common/utils/blockHeight'
import { dateToIsoOrNull } from '../../common/utils/date_utils'
import OperatorAuthorization from '../../models/operator_authorization'
import OperatorAuthorizationHistory from '../../models/operator_authorization_history'

function serializeOperatorAuthorizationRow(row: any) {
  const spendLimit = row.spend_limit ?? null
  const feeSpendLimit = row.fee_spend_limit ?? null

  return {
    id: Number(row.operator_authorization_id ?? row.id),
    corporation_id: Number(row.corporation_id),
    operator: String(row.operator),
    msg_types: row.msg_types ?? [],
    ...(spendLimit ? { spend_limit: spendLimit, remaining_spend: row.remaining_spend ?? [] } : {}),
    ...(feeSpendLimit ? { fee_spend_limit: feeSpendLimit, remaining_fee_spend: row.remaining_fee_spend ?? [] } : {}),
    ...(row.expiration ? { expiration: dateToIsoOrNull(row.expiration) } : {}),
    ...(row.period ? { period: String(row.period) } : {}),
  }
}

interface GetOperatorAuthorizationParams {
  id: number
}

@Service({
  name: SERVICE.V1.DelegationApiService.key,
  version: 1,
})
export default class DelegationApiService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker)
  }

  private async resolveAtHeight(id: number, blockHeight: number): Promise<any | undefined> {
    return OperatorAuthorizationHistory.query()
      .where('operator_authorization_id', id)
      .where('height', '<=', blockHeight)
      .orderBy('height', 'desc')
      .orderBy('id', 'desc')
      .first()
  }

  @Action({
    rest: 'GET operator-authorization/:id',
    params: {
      id: { type: 'number', integer: true, positive: true, convert: true },
    },
  })
  async getOperatorAuthorization(ctx: Context<GetOperatorAuthorizationParams>) {
    try {
      const { id } = ctx.params
      const blockHeight = getBlockHeight(ctx)

      const row =
        blockHeight !== undefined
          ? await this.resolveAtHeight(id, blockHeight)
          : await OperatorAuthorization.query().findById(id)

      if (!row || row.revoked) {
        return ApiResponder.error(ctx, 'Operator authorization not found', 404)
      }

      return ApiResponder.success(ctx, {
        authorization: serializeOperatorAuthorizationRow(row),
      })
    } catch (err: any) {
      this.logger.error('Error in Delegation.getOperatorAuthorization:', err)
      return ApiResponder.error(ctx, `Failed to get operator authorization: ${err?.message || String(err)}`, 500)
    }
  }
}
