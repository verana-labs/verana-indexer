import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended'
import { Context, ServiceBroker } from 'moleculer'
import BaseService from '../../base/base.service'
import { SERVICE } from '../../common'
import ApiResponder from '../../common/utils/apiResponse'
import { getBlockChainTimeAsOf } from '../../common/utils/block_time'
import { getBlockHeight } from '../../common/utils/blockHeight'
import { dateToIsoOrNull } from '../../common/utils/date_utils'
import knex from '../../common/utils/db_connection'
import OperatorAuthorization from '../../models/operator_authorization'
import OperatorAuthorizationHistory from '../../models/operator_authorization_history'
import VSOperatorAuthorization from '../../models/vs_operator_authorization'
import VSOperatorAuthorizationHistory from '../../models/vs_operator_authorization_history'
import { parseIdSortDirection } from '../crawl-co/co_stats'

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

function serializeParticipantRecord(record: any) {
  const spendLimit = record.spend_limit ?? null
  const feeSpendLimit = record.fee_spend_limit ?? null

  return {
    participant_id: Number(record.participant_id),
    msg_types: record.msg_types ?? [],
    ...(spendLimit ? { spend_limit: spendLimit, remaining_spend: record.remaining_spend ?? [] } : {}),
    ...(feeSpendLimit ? { fee_spend_limit: feeSpendLimit, remaining_fee_spend: record.remaining_fee_spend ?? [] } : {}),
    with_feegrant: Boolean(record.with_feegrant),
    ...(record.expiration ? { expiration: dateToIsoOrNull(record.expiration) } : {}),
    ...(record.period ? { period: String(record.period) } : {}),
  }
}

function serializeVSOperatorAuthorizationRow(row: any) {
  return {
    id: Number(row.vs_operator_authorization_id ?? row.id),
    corporation_id: Number(row.corporation_id),
    vs_operator: String(row.vs_operator),
    records: (row.records ?? []).map(serializeParticipantRecord),
  }
}

function serializeParticipantRecord(record: any) {
  const spendLimit = record.spend_limit ?? null
  const feeSpendLimit = record.fee_spend_limit ?? null

  return {
    participant_id: Number(record.participant_id),
    msg_types: record.msg_types ?? [],
    ...(spendLimit ? { spend_limit: spendLimit, remaining_spend: record.remaining_spend ?? [] } : {}),
    ...(feeSpendLimit ? { fee_spend_limit: feeSpendLimit, remaining_fee_spend: record.remaining_fee_spend ?? [] } : {}),
    with_feegrant: Boolean(record.with_feegrant),
    ...(record.expiration ? { expiration: dateToIsoOrNull(record.expiration) } : {}),
    ...(record.period ? { period: String(record.period) } : {}),
  }
}

function serializeVSOperatorAuthorizationRow(row: any) {
  return {
    id: Number(row.vs_operator_authorization_id ?? row.id),
    corporation_id: Number(row.corporation_id),
    vs_operator: String(row.vs_operator),
    records: (row.records ?? []).map(serializeParticipantRecord),
  }
}

function serializeParticipantRecord(record: any) {
  const spendLimit = record.spend_limit ?? null
  const feeSpendLimit = record.fee_spend_limit ?? null

  return {
    participant_id: Number(record.participant_id),
    msg_types: record.msg_types ?? [],
    ...(spendLimit ? { spend_limit: spendLimit, remaining_spend: record.remaining_spend ?? [] } : {}),
    ...(feeSpendLimit ? { fee_spend_limit: feeSpendLimit, remaining_fee_spend: record.remaining_fee_spend ?? [] } : {}),
    with_feegrant: Boolean(record.with_feegrant),
    ...(record.expiration ? { expiration: dateToIsoOrNull(record.expiration) } : {}),
    ...(record.period ? { period: String(record.period) } : {}),
  }
}

function serializeVSOperatorAuthorizationRow(row: any) {
  return {
    id: Number(row.vs_operator_authorization_id ?? row.id),
    corporation_id: Number(row.corporation_id),
    vs_operator: String(row.vs_operator),
    records: (row.records ?? []).map(serializeParticipantRecord),
  }
}

interface ListOperatorAuthorizationsParams {
  corporation_id?: number
  operator?: string
  msg_type?: string
  only_active?: boolean
  modified_after?: string
  limit?: number
  min_id?: number
  max_id?: number
  sort?: string
}

interface ListVSOperatorAuthorizationsParams {
  corporation_id?: number
  vs_operator?: string
  participant_id?: number
  only_active?: boolean
  modified_after?: string
  limit?: number
  min_id?: number
  max_id?: number
  sort?: string
}

interface ListOperatorAuthorizationsParams {
  corporation_id?: number
  operator?: string
  msg_type?: string
  only_active?: boolean
  modified_after?: string
  limit?: number
  min_id?: number
  max_id?: number
  sort?: string
}

interface ListVSOperatorAuthorizationsParams {
  corporation_id?: number
  vs_operator?: string
  participant_id?: number
  only_active?: boolean
  modified_after?: string
  limit?: number
  min_id?: number
  max_id?: number
  sort?: string
}

interface ListOperatorAuthorizationsParams {
  corporation_id?: number
  operator?: string
  msg_type?: string
  only_active?: boolean
  modified_after?: string
  limit?: number
  min_id?: number
  max_id?: number
  sort?: string
}

interface ListVSOperatorAuthorizationsParams {
  corporation_id?: number
  vs_operator?: string
  participant_id?: number
  only_active?: boolean
  modified_after?: string
  limit?: number
  min_id?: number
  max_id?: number
  sort?: string
}

interface ListOperatorAuthorizationsParams {
  corporation_id?: number
  operator?: string
  msg_type?: string
  only_active?: boolean
  modified_after?: string
  limit?: number
  min_id?: number
  max_id?: number
  sort?: string
}

interface ListVSOperatorAuthorizationsParams {
  corporation_id?: number
  vs_operator?: string
  participant_id?: number
  only_active?: boolean
  modified_after?: string
  limit?: number
  min_id?: number
  max_id?: number
  sort?: string
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
  async getOperatorAuthorization(ctx: Context<{ id: number }>) {
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

  @Action({
    rest: 'GET operator-authorizations',
    params: {
      corporation_id: { type: 'number', integer: true, positive: true, optional: true, convert: true },
      operator: { type: 'string', optional: true },
      msg_type: { type: 'string', optional: true },
      only_active: { type: 'boolean', optional: true, convert: true },
      modified_after: { type: 'string', optional: true },
      limit: { type: 'number', integer: true, optional: true, convert: true },
      min_id: { type: 'number', integer: true, optional: true, convert: true },
      max_id: { type: 'number', integer: true, optional: true, convert: true },
      sort: { type: 'string', optional: true },
    },
  })
  async listOperatorAuthorizations(ctx: Context<ListOperatorAuthorizationsParams>) {
    try {
      const p = ctx.params

      const sortParsed = parseIdSortDirection(p.sort)
      if (!sortParsed.ok) {
        return ApiResponder.error(ctx, sortParsed.message, 400)
      }
      const sortDir = sortParsed.direction

      let modifiedAfter: Date | undefined
      if (p.modified_after) {
        modifiedAfter = new Date(p.modified_after)
        if (Number.isNaN(modifiedAfter.getTime())) {
          return ApiResponder.error(ctx, 'Invalid modified_after datetime format', 400)
        }
      }

      const limit = Math.min(Math.max(Number(p.limit) || 64, 1), 1024)
      const blockHeight = getBlockHeight(ctx)

      const query =
        blockHeight !== undefined
          ? this.buildAtHeightListQuery(blockHeight)
          : knex<any>('operator_authorizations').select('*')
      const idColumn = blockHeight !== undefined ? 'operator_authorization_id' : 'id'

      let now: Date | undefined
      if (p.only_active) {
        now = blockHeight !== undefined ? await getBlockChainTimeAsOf(blockHeight, { logger: this.logger }) : new Date()
      }

      this.applyListFilters(query, p, { modifiedAfter, now, idColumn })
      const rows = await query.orderBy(idColumn, sortDir).limit(limit)

      return ApiResponder.success(ctx, {
        authorizations: rows.map(serializeOperatorAuthorizationRow),
      })
    } catch (err: any) {
      this.logger.error('Error in Delegation.listOperatorAuthorizations:', err)
      return ApiResponder.error(ctx, `Failed to list operator authorizations: ${err?.message || String(err)}`, 500)
    }
  }

  private buildAtHeightListQuery(blockHeight: number) {
    const latestPerId = knex('operator_authorization_history')
      .distinctOn('operator_authorization_id')
      .select('*')
      .where('height', '<=', blockHeight)
      .orderBy('operator_authorization_id', 'asc')
      .orderBy('height', 'desc')
    return knex.from(latestPerId.as('oa')).select('*').where('revoked', false)
  }

  private applyListFilters(
    query: any,
    p: ListOperatorAuthorizationsParams,
    ctx: { modifiedAfter?: Date; now?: Date; idColumn: string }
  ) {
    if (p.corporation_id !== undefined) query.where('corporation_id', p.corporation_id)
    if (p.operator) query.where('operator', p.operator)
    if (p.msg_type) query.whereRaw('msg_types @> ?::jsonb', [JSON.stringify([p.msg_type])])
    if (ctx.now) {
      query.where((builder: any) => builder.whereNull('expiration').orWhere('expiration', '>', ctx.now))
    }
    if (ctx.modifiedAfter) query.where('modified', '>', ctx.modifiedAfter)
    if (p.min_id !== undefined) query.where(ctx.idColumn, '>=', p.min_id)
    if (p.max_id !== undefined) query.where(ctx.idColumn, '<', p.max_id)
  }

  @Action({
    rest: 'GET vs-operator-authorizations',
    params: {
      corporation_id: { type: 'number', integer: true, positive: true, optional: true, convert: true },
      vs_operator: { type: 'string', optional: true },
      participant_id: { type: 'number', integer: true, positive: true, optional: true, convert: true },
      only_active: { type: 'boolean', optional: true, convert: true },
      modified_after: { type: 'string', optional: true },
      limit: { type: 'number', integer: true, optional: true, convert: true },
      min_id: { type: 'number', integer: true, optional: true, convert: true },
      max_id: { type: 'number', integer: true, optional: true, convert: true },
      sort: { type: 'string', optional: true },
    },
  })
  async listVSOperatorAuthorizations(ctx: Context<ListVSOperatorAuthorizationsParams>) {
    try {
      const p = ctx.params

      const sortParsed = parseIdSortDirection(p.sort)
      if (!sortParsed.ok) {
        return ApiResponder.error(ctx, sortParsed.message, 400)
      }
      const sortDir = sortParsed.direction

      let modifiedAfter: Date | undefined
      if (p.modified_after) {
        modifiedAfter = new Date(p.modified_after)
        if (Number.isNaN(modifiedAfter.getTime())) {
          return ApiResponder.error(ctx, 'Invalid modified_after datetime format', 400)
        }
      }

      const limit = Math.min(Math.max(Number(p.limit) || 64, 1), 1024)
      const blockHeight = getBlockHeight(ctx)

      const query =
        blockHeight !== undefined
          ? this.buildAtHeightVSOAListQuery(blockHeight)
          : knex<any>('vs_operator_authorizations').select('*')
      const idColumn = blockHeight !== undefined ? 'vs_operator_authorization_id' : 'id'

      let now: Date | undefined
      if (p.only_active) {
        now = blockHeight !== undefined ? await getBlockChainTimeAsOf(blockHeight, { logger: this.logger }) : new Date()
      }

      this.applyVSOAListFilters(query, p, { modifiedAfter, now, idColumn })
      const rows = await query.orderBy(idColumn, sortDir).limit(limit)

      return ApiResponder.success(ctx, {
        authorizations: rows.map(serializeVSOperatorAuthorizationRow),
      })
    } catch (err: any) {
      this.logger.error('Error in Delegation.listVSOperatorAuthorizations:', err)
      return ApiResponder.error(ctx, `Failed to list VS operator authorizations: ${err?.message || String(err)}`, 500)
    }
  }

  private buildAtHeightVSOAListQuery(blockHeight: number) {
    const latestPerId = knex('vs_operator_authorization_history')
      .distinctOn('vs_operator_authorization_id')
      .select('*')
      .where('height', '<=', blockHeight)
      .orderBy('vs_operator_authorization_id', 'asc')
      .orderBy('height', 'desc')
    return knex.from(latestPerId.as('vsoa')).select('*').where('revoked', false)
  }

  private applyVSOAListFilters(
    query: any,
    p: ListVSOperatorAuthorizationsParams,
    ctx: { modifiedAfter?: Date; now?: Date; idColumn: string }
  ) {
    if (p.corporation_id !== undefined) query.where('corporation_id', p.corporation_id)
    if (p.vs_operator) query.where('vs_operator', p.vs_operator)
    if (p.participant_id !== undefined) {
      query.whereRaw('records @> ?::jsonb', [JSON.stringify([{ participant_id: p.participant_id }])])
    }
    if (ctx.now) {
      query.whereRaw(
        "EXISTS (SELECT 1 FROM jsonb_array_elements(records) rec WHERE rec->>'expiration' IS NULL OR (rec->>'expiration')::timestamptz > ?)",
        [ctx.now]
      )
    }
    if (ctx.modifiedAfter) query.where('modified', '>', ctx.modifiedAfter)
    if (p.min_id !== undefined) query.where(ctx.idColumn, '>=', p.min_id)
    if (p.max_id !== undefined) query.where(ctx.idColumn, '<', p.max_id)
  }

  private async resolveVSOAAtHeight(id: number, blockHeight: number): Promise<any | undefined> {
    return VSOperatorAuthorizationHistory.query()
      .where('vs_operator_authorization_id', id)
      .where('height', '<=', blockHeight)
      .orderBy('height', 'desc')
      .orderBy('id', 'desc')
      .first()
  }

  @Action({
    rest: 'GET vs-operator-authorization/:id',
    params: {
      id: { type: 'number', integer: true, positive: true, convert: true },
    },
  })
  async getVSOperatorAuthorization(ctx: Context<{ id: number }>) {
    try {
      const { id } = ctx.params
      const blockHeight = getBlockHeight(ctx)

      const row =
        blockHeight !== undefined
          ? await this.resolveVSOAAtHeight(id, blockHeight)
          : await VSOperatorAuthorization.query().findById(id)

      if (!row || row.revoked) {
        return ApiResponder.error(ctx, 'VS operator authorization not found', 404)
      }

      return ApiResponder.success(ctx, {
        authorization: serializeVSOperatorAuthorizationRow(row),
      })
    } catch (err: any) {
      this.logger.error('Error in Delegation.getVSOperatorAuthorization:', err)
      return ApiResponder.error(ctx, `Failed to get VS operator authorization: ${err?.message || String(err)}`, 500)
    }
  }
}
