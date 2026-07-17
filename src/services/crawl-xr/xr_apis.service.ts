import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended'
import { Context, ServiceBroker } from 'moleculer'
import BaseService from '../../base/base.service'
import ApiResponder from '../../common/utils/apiResponse'
import { getBlockHeight } from '../../common/utils/blockHeight'
import { dateToIsoOrNull } from '../../common/utils/date_utils'
import knex from '../../common/utils/db_connection'
import { parseIdSortDirection } from '../../common/utils/query_ordering'
import ExchangeRate from '../../models/exchange_rate'
import ExchangeRateHistory from '../../models/exchange_rate_history'
import { parseIdSortDirection } from '../crawl-co/co_stats'

function computePrice(amount: string, rate: string, rateScale: number): string {
  const scaled = BigInt(amount) * BigInt(rate)
  let divisor = BigInt(1)
  const ten = BigInt(10)
  for (let i = 0; i < rateScale; i += 1) {
    divisor *= ten
  }
  return (scaled / divisor).toString()
}

function serializeExchangeRateRow(row: any) {
  return {
    id: Number(row.id ?? row.exchange_rate_id),
    base_asset_type: row.base_asset_type,
    base_asset: row.base_asset,
    quote_asset_type: row.quote_asset_type,
    quote_asset: row.quote_asset,
    rate: String(row.rate),
    rate_scale: Number(row.rate_scale),
    validity_duration: Number(row.validity_duration),
    updated: dateToIsoOrNull(row.updated),
    expires: dateToIsoOrNull(row.expires),
    state: Boolean(row.state),
  }
}

interface GetExchangeRateParams {
  id?: string
  base_asset_type?: string
  base_asset?: string
  quote_asset_type?: string
  quote_asset?: string
  state?: boolean
  expire_ts?: string
}

interface GetPriceParams {
  base_asset_type: string
  base_asset: string
  quote_asset_type: string
  quote_asset: string
  amount: string
}

interface ListExchangeRatesParams {
  base_asset_type?: string
  base_asset?: string
  quote_asset_type?: string
  quote_asset?: string
  state?: boolean
  expire?: string
  limit?: number
  min_id?: number
  max_id?: number
  sort?: string
}

type AssetPair = {
  base_asset_type: string
  base_asset: string
  quote_asset_type: string
  quote_asset: string
}

@Service({
  name: 'ExchangeRateApiService',
  version: 1,
})
export default class ExchangeRateApiService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker)
  }

  private async resolveAtHeight(
    selector: { id?: number; pair?: AssetPair },
    blockHeight: number
  ): Promise<any | undefined> {
    let query = ExchangeRateHistory.query()
      .where('height', '<=', blockHeight)
      .orderBy('height', 'desc')
      .orderBy('id', 'desc')
    if (selector.id !== undefined) {
      query = query.where('exchange_rate_id', selector.id)
    } else if (selector.pair) {
      query = query
        .where('base_asset_type', selector.pair.base_asset_type)
        .where('base_asset', selector.pair.base_asset)
        .where('quote_asset_type', selector.pair.quote_asset_type)
        .where('quote_asset', selector.pair.quote_asset)
    }
    return query.first()
  }

  private async resolveLatest(selector: { id?: number; pair?: AssetPair }): Promise<any | undefined> {
    let query = ExchangeRate.query()
    if (selector.id !== undefined) {
      query = query.where('id', selector.id)
    } else if (selector.pair) {
      query = query
        .where('base_asset_type', selector.pair.base_asset_type)
        .where('base_asset', selector.pair.base_asset)
        .where('quote_asset_type', selector.pair.quote_asset_type)
        .where('quote_asset', selector.pair.quote_asset)
    }
    return query.first()
  }

  @Action({
    rest: 'GET get',
    params: {
      id: { type: 'string', optional: true, convert: true },
      base_asset_type: { type: 'enum', values: ['TU', 'COIN', 'FIAT'], optional: true },
      base_asset: { type: 'string', optional: true },
      quote_asset_type: { type: 'enum', values: ['TU', 'COIN', 'FIAT'], optional: true },
      quote_asset: { type: 'string', optional: true },
      state: { type: 'boolean', optional: true, convert: true },
      expire_ts: { type: 'string', optional: true },
    },
  })
  async getExchangeRate(ctx: Context<GetExchangeRateParams>) {
    try {
      const {
        id,
        base_asset_type: baseAssetType,
        base_asset: baseAsset,
        quote_asset_type: quoteAssetType,
        quote_asset: quoteAsset,
        state,
        expire_ts: expireTs,
      } = ctx.params

      const hasId = id !== undefined && id.trim() !== ''
      const hasAssets =
        baseAssetType !== undefined &&
        baseAsset !== undefined &&
        quoteAssetType !== undefined &&
        quoteAsset !== undefined

      if (hasId === hasAssets) {
        return ApiResponder.error(
          ctx,
          "Provide either 'id' or the full four-tuple (base_asset_type, base_asset, quote_asset_type, quote_asset), but not both",
          400
        )
      }

      let expireDate: Date | undefined
      if (expireTs) {
        expireDate = new Date(expireTs)
        if (Number.isNaN(expireDate.getTime())) {
          return ApiResponder.error(ctx, 'Invalid expire_ts datetime format', 400)
        }
      }

      const selector = hasId
        ? { id: Number(id) }
        : {
            pair: {
              base_asset_type: baseAssetType as string,
              base_asset: baseAsset as string,
              quote_asset_type: quoteAssetType as string,
              quote_asset: quoteAsset as string,
            },
          }

      const blockHeight = getBlockHeight(ctx)
      const row =
        blockHeight !== undefined
          ? await this.resolveAtHeight(selector, blockHeight)
          : await this.resolveLatest(selector)

      if (!row) {
        return ApiResponder.error(ctx, 'Exchange rate not found', 404)
      }

      if (state !== undefined && Boolean(row.state) !== state) {
        return ApiResponder.error(ctx, 'Exchange rate not found', 404)
      }
      if (expireDate && !(row.expires && new Date(row.expires) > expireDate)) {
        return ApiResponder.error(ctx, 'Exchange rate not found', 404)
      }

      return ApiResponder.success(ctx, {
        exchange_rate: serializeExchangeRateRow(row),
      })
    } catch (err: any) {
      this.logger.error('Error in ExchangeRate.getExchangeRate:', err)
      return ApiResponder.error(ctx, `Failed to get exchange rate: ${err?.message || String(err)}`, 500)
    }
  }

  @Action({
    rest: 'GET list',
    params: {
      base_asset_type: { type: 'enum', values: ['TU', 'COIN', 'FIAT'], optional: true },
      base_asset: { type: 'string', optional: true },
      quote_asset_type: { type: 'enum', values: ['TU', 'COIN', 'FIAT'], optional: true },
      quote_asset: { type: 'string', optional: true },
      state: { type: 'boolean', optional: true, convert: true },
      expire: { type: 'string', optional: true },
      limit: { type: 'number', integer: true, optional: true, convert: true },
      min_id: { type: 'number', integer: true, optional: true, convert: true },
      max_id: { type: 'number', integer: true, optional: true, convert: true },
      sort: { type: 'string', optional: true },
    },
  })
  async listExchangeRates(ctx: Context<ListExchangeRatesParams>) {
    try {
      const p = ctx.params

      const sortParsed = parseIdSortDirection(p.sort)
      if (!sortParsed.ok) {
        return ApiResponder.error(ctx, sortParsed.message, 400)
      }
      const sortDir = sortParsed.direction

      let expireDate: Date | undefined
      if (p.expire) {
        expireDate = new Date(p.expire)
        if (Number.isNaN(expireDate.getTime())) {
          return ApiResponder.error(ctx, 'Invalid expire datetime format', 400)
        }
      }

      const limit = Math.min(Math.max(Number(p.limit) || 64, 1), 1024)
      const blockHeight = getBlockHeight(ctx)

      const query =
        blockHeight !== undefined ? this.buildAtHeightListQuery(blockHeight) : knex<any>('exchange_rates').select('*')
      const idColumn = blockHeight !== undefined ? 'exchange_rate_id' : 'id'

      this.applyListFilters(query, p, expireDate, idColumn)
      const rows = await query.orderBy(idColumn, sortDir).limit(limit)

      return ApiResponder.success(ctx, {
        exchange_rates: rows.map(serializeExchangeRateRow),
      })
    } catch (err: any) {
      this.logger.error('Error in ExchangeRate.listExchangeRates:', err)
      return ApiResponder.error(ctx, `Failed to list exchange rates: ${err?.message || String(err)}`, 500)
    }
  }

  private buildAtHeightListQuery(blockHeight: number) {
    const latestPerId = knex('exchange_rate_history')
      .distinctOn('exchange_rate_id')
      .select('*')
      .where('height', '<=', blockHeight)
      .orderBy('exchange_rate_id', 'asc')
      .orderBy('height', 'desc')
    return knex.from(latestPerId.as('er')).select('*')
  }

  private applyListFilters(query: any, p: ListExchangeRatesParams, expireDate: Date | undefined, idColumn: string) {
    if (p.base_asset_type) query.where('base_asset_type', p.base_asset_type)
    if (p.base_asset) query.where('base_asset', p.base_asset)
    if (p.quote_asset_type) query.where('quote_asset_type', p.quote_asset_type)
    if (p.quote_asset) query.where('quote_asset', p.quote_asset)
    if (p.state !== undefined) query.where('state', p.state)
    if (expireDate) query.where('expires', '>', expireDate)
    if (p.min_id !== undefined) query.where(idColumn, '>=', p.min_id)
    if (p.max_id !== undefined) query.where(idColumn, '<', p.max_id)
  }

  @Action({
    rest: 'GET price',
    params: {
      base_asset_type: { type: 'enum', values: ['TU', 'COIN', 'FIAT'] },
      base_asset: { type: 'string', empty: false },
      quote_asset_type: { type: 'enum', values: ['TU', 'COIN', 'FIAT'] },
      quote_asset: { type: 'string', empty: false },
      amount: { type: 'string', empty: false, pattern: /^[0-9]+$/ },
    },
  })
  async getPrice(ctx: Context<GetPriceParams>) {
    try {
      const {
        base_asset_type: baseAssetType,
        base_asset: baseAsset,
        quote_asset_type: quoteAssetType,
        quote_asset: quoteAsset,
        amount,
      } = ctx.params

      if (baseAssetType === quoteAssetType && baseAsset === quoteAsset) {
        return ApiResponder.success(ctx, {
          price: amount,
          base_asset_type: baseAssetType,
          base_asset: baseAsset,
          quote_asset_type: quoteAssetType,
          quote_asset: quoteAsset,
        })
      }

      const pair: AssetPair = {
        base_asset_type: baseAssetType,
        base_asset: baseAsset,
        quote_asset_type: quoteAssetType,
        quote_asset: quoteAsset,
      }
      const blockHeight = getBlockHeight(ctx)
      const row =
        blockHeight !== undefined
          ? await this.resolveAtHeight({ pair }, blockHeight)
          : await this.resolveLatest({ pair })

      if (!row) {
        return ApiResponder.error(ctx, 'Exchange rate not found', 404)
      }

      const isExpired = !row.expires || new Date(row.expires) <= new Date()
      if (!row.state || isExpired) {
        return ApiResponder.error(ctx, 'Exchange rate is disabled or expired', 410)
      }

      const price = computePrice(amount, String(row.rate), Number(row.rate_scale))
      const serialized = serializeExchangeRateRow(row)

      return ApiResponder.success(ctx, {
        price,
        base_asset_type: baseAssetType,
        base_asset: baseAsset,
        quote_asset_type: quoteAssetType,
        quote_asset: quoteAsset,
        rate: serialized.rate,
        rate_scale: serialized.rate_scale,
        expires: serialized.expires,
      })
    } catch (err: any) {
      this.logger.error('Error in ExchangeRate.getPrice:', err)
      return ApiResponder.error(ctx, `Failed to get price: ${err?.message || String(err)}`, 500)
    }
  }
}
