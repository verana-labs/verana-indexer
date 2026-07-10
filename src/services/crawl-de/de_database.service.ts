import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended'
import { ServiceBroker } from 'moleculer'
import BaseService from '../../base/base.service'
import { SERVICE } from '../../common'
import { toJsonbColumn } from '../../common/utils/helper'
import knex from '../../common/utils/db_connection'
import type {
  FeeAllowanceSnapshot,
  OperatorAuthorizationRow,
  VSOperatorAuthorizationRow,
} from '../../modules/de-height-sync/de_height_sync_helpers'

@Service({
  name: SERVICE.V1.DelegationDatabaseService.key,
  version: 1,
})
export default class DelegationDatabaseService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker)
  }

  @Action({ name: 'syncOperatorAuthorization' })
  async syncOperatorAuthorization(ctx: {
    params: {
      authorization: OperatorAuthorizationRow
      feeAllowance: FeeAllowanceSnapshot | null
      blockHeight: number
    }
  }): Promise<{ success: boolean }> {
    const { authorization, feeAllowance, blockHeight } = ctx.params

    const row = {
      id: authorization.id,
      corporation_id: authorization.corporation_id,
      operator: authorization.operator,
      msg_types: toJsonbColumn(authorization.msg_types),
      spend_limit: toJsonbColumn(authorization.spend_limit),
      remaining_spend: toJsonbColumn(authorization.remaining_spend),
      fee_spend_limit: toJsonbColumn(feeAllowance?.fee_spend_limit ?? null),
      remaining_fee_spend: toJsonbColumn(feeAllowance?.remaining_fee_spend ?? null),
      expiration: authorization.expiration,
      period: authorization.period,
      height: blockHeight,
    }

    await knex.transaction(async (trx) => {
      await trx('operator_authorizations')
        .insert(row)
        .onConflict('id')
        .merge([
          'corporation_id',
          'operator',
          'msg_types',
          'spend_limit',
          'remaining_spend',
          'fee_spend_limit',
          'remaining_fee_spend',
          'expiration',
          'period',
          'height',
        ])

      await trx('operator_authorization_history').insert({
        operator_authorization_id: row.id,
        corporation_id: row.corporation_id,
        operator: row.operator,
        msg_types: row.msg_types,
        spend_limit: row.spend_limit,
        remaining_spend: row.remaining_spend,
        fee_spend_limit: row.fee_spend_limit,
        remaining_fee_spend: row.remaining_fee_spend,
        expiration: row.expiration,
        period: row.period,
        revoked: false,
        height: blockHeight,
      })
    })

    return { success: true }
  }

  @Action({ name: 'revokeOperatorAuthorization' })
  async revokeOperatorAuthorization(ctx: {
    params: { id: number; corporationId: number; operator: string; blockHeight: number }
  }): Promise<{ success: boolean }> {
    const { id, corporationId, operator, blockHeight } = ctx.params

    await knex.transaction(async (trx) => {
      await trx('operator_authorizations').where('id', id).delete()

      await trx('operator_authorization_history').insert({
        operator_authorization_id: id,
        corporation_id: corporationId,
        operator,
        revoked: true,
        height: blockHeight,
      })
    })

    return { success: true }
  }

  @Action({ name: 'syncVSOperatorAuthorization' })
  async syncVSOperatorAuthorization(ctx: {
    params: { authorization: VSOperatorAuthorizationRow; blockHeight: number }
  }): Promise<{ success: boolean }> {
    const { authorization, blockHeight } = ctx.params

    await knex('vs_operator_authorizations')
      .insert({
        id: authorization.id,
        corporation_id: authorization.corporation_id,
        vs_operator: authorization.vs_operator,
        records: toJsonbColumn(authorization.records),
        height: blockHeight,
      })
      .onConflict('id')
      .merge(['corporation_id', 'vs_operator', 'records', 'height'])

    return { success: true }
  }

  @Action({ name: 'deleteVSOperatorAuthorization' })
  async deleteVSOperatorAuthorization(ctx: { params: { id: number } }): Promise<{ success: boolean }> {
    await knex('vs_operator_authorizations').where('id', ctx.params.id).delete()
    return { success: true }
  }
}
