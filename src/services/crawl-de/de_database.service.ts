import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended'
import { ServiceBroker } from 'moleculer'
import BaseService from '../../base/base.service'
import { SERVICE } from '../../common'
import { getBlockChainTimeAsOf } from '../../common/utils/block_time'
import knex from '../../common/utils/db_connection'
import { toJsonbColumn } from '../../common/utils/helper'
import type {
  DenomAmount,
  FeeAllowanceSnapshot,
  FeeGrantAllowanceSnapshot,
  OperatorAuthorizationRow,
  VSOperatorAuthorizationRow,
} from '../../modules/de-height-sync/de_height_sync_helpers'

function parseJsonb<T>(value: unknown): T | null {
  if (value == null) return null
  if (typeof value === 'string') return JSON.parse(value)
  return value as T
}

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

    const modified = await getBlockChainTimeAsOf(blockHeight, { logger: this.logger })

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
      modified,
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
          'modified',
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
        modified,
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

    const modified = await getBlockChainTimeAsOf(blockHeight, { logger: this.logger })

    await knex.transaction(async (trx) => {
      await trx('operator_authorizations').where('id', id).delete()

      await trx('operator_authorization_history').insert({
        operator_authorization_id: id,
        corporation_id: corporationId,
        operator,
        modified,
        revoked: true,
        height: blockHeight,
      })
    })

    return { success: true }
  }

  @Action({ name: 'syncFeeGrant' })
  async syncFeeGrant(ctx: {
    params: {
      grantorCorporationId: number
      grantee: string
      snapshot: FeeGrantAllowanceSnapshot
      blockHeight: number
    }
  }): Promise<{ success: boolean }> {
    const { grantorCorporationId, grantee, snapshot, blockHeight } = ctx.params

    const modified = await getBlockChainTimeAsOf(blockHeight, { logger: this.logger })

    const row = {
      grantor_corporation_id: grantorCorporationId,
      grantee,
      msg_types: toJsonbColumn(snapshot.msg_types),
      spend_limit: toJsonbColumn(snapshot.spend_limit),
      remaining_spend: toJsonbColumn(snapshot.remaining_spend),
      expiration: snapshot.expiration,
      period: snapshot.period,
      modified,
      height: blockHeight,
    }

    await knex.transaction(async (trx) => {
      const [upserted] = await trx('fee_grants')
        .insert(row)
        .onConflict(['grantor_corporation_id', 'grantee'])
        .merge(['msg_types', 'spend_limit', 'remaining_spend', 'expiration', 'period', 'modified', 'height'])
        .returning('id')

      await trx('fee_grant_history').insert({
        fee_grant_id: upserted.id,
        ...row,
        revoked: false,
      })
    })

    return { success: true }
  }

  @Action({ name: 'refreshFeeGrantSpend' })
  async refreshFeeGrantSpend(ctx: {
    params: {
      grantorCorporationId: number
      grantee: string
      snapshot: FeeGrantAllowanceSnapshot | null
      blockHeight: number
    }
  }): Promise<{ success: boolean }> {
    const { grantorCorporationId, grantee, snapshot, blockHeight } = ctx.params

    const existing = await knex('fee_grants')
      .where('grantor_corporation_id', grantorCorporationId)
      .where('grantee', grantee)
      .first()
    if (!existing) return { success: true }

    const modified = await getBlockChainTimeAsOf(blockHeight, { logger: this.logger })

    const spendLimit = parseJsonb<DenomAmount[]>(existing.spend_limit)
    // Allowance gone while the grant row persists means fully exhausted: report zero.
    const remainingSpend = snapshot
      ? snapshot.remaining_spend
      : (spendLimit?.map((coin) => ({ denom: coin.denom, amount: '0' })) ?? null)
    const expiration = snapshot ? snapshot.expiration : existing.expiration

    await knex.transaction(async (trx) => {
      await trx('fee_grants')
        .where('id', existing.id)
        .update({
          remaining_spend: toJsonbColumn(remainingSpend),
          expiration,
          modified,
          height: blockHeight,
        })

      await trx('fee_grant_history').insert({
        fee_grant_id: existing.id,
        grantor_corporation_id: grantorCorporationId,
        grantee,
        msg_types: toJsonbColumn(parseJsonb<string[]>(existing.msg_types)),
        spend_limit: toJsonbColumn(spendLimit),
        remaining_spend: toJsonbColumn(remainingSpend),
        expiration,
        period: existing.period,
        modified,
        revoked: false,
        height: blockHeight,
      })
    })

    return { success: true }
  }

  @Action({ name: 'revokeFeeGrant' })
  async revokeFeeGrant(ctx: {
    params: { grantorCorporationId: number; grantee: string; blockHeight: number }
  }): Promise<{ success: boolean }> {
    const { grantorCorporationId, grantee, blockHeight } = ctx.params

    const existing = await knex('fee_grants')
      .where('grantor_corporation_id', grantorCorporationId)
      .where('grantee', grantee)
      .first()
    if (!existing) return { success: true }

    const modified = await getBlockChainTimeAsOf(blockHeight, { logger: this.logger })

    await knex.transaction(async (trx) => {
      await trx('fee_grants').where('id', existing.id).delete()

      await trx('fee_grant_history').insert({
        fee_grant_id: existing.id,
        grantor_corporation_id: grantorCorporationId,
        grantee,
        modified,
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

    const modified = await getBlockChainTimeAsOf(blockHeight, { logger: this.logger })
    const records = toJsonbColumn(authorization.records)

    await knex.transaction(async (trx) => {
      await trx('vs_operator_authorizations')
        .insert({
          id: authorization.id,
          corporation_id: authorization.corporation_id,
          vs_operator: authorization.vs_operator,
          records,
          modified,
          height: blockHeight,
        })
        .onConflict('id')
        .merge(['corporation_id', 'vs_operator', 'records', 'modified', 'height'])

      await trx('vs_operator_authorization_history').insert({
        vs_operator_authorization_id: authorization.id,
        corporation_id: authorization.corporation_id,
        vs_operator: authorization.vs_operator,
        records,
        modified,
        revoked: false,
        height: blockHeight,
      })
    })

    return { success: true }
  }

  @Action({ name: 'deleteVSOperatorAuthorization' })
  async deleteVSOperatorAuthorization(ctx: {
    params: { id: number; blockHeight: number }
  }): Promise<{ success: boolean }> {
    const { id, blockHeight } = ctx.params

    const existing = await knex('vs_operator_authorizations').where('id', id).first()
    if (!existing) return { success: true }

    const modified = await getBlockChainTimeAsOf(blockHeight, { logger: this.logger })

    await knex.transaction(async (trx) => {
      await trx('vs_operator_authorizations').where('id', id).delete()

      await trx('vs_operator_authorization_history').insert({
        vs_operator_authorization_id: id,
        corporation_id: existing.corporation_id,
        vs_operator: existing.vs_operator,
        modified,
        revoked: true,
        height: blockHeight,
      })
    })

    return { success: true }
  }
}
