import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended'
import { PricingAssetType, pricingAssetTypeFromJSON } from '@verana-labs/verana-types/codec/verana/cs/v1/types'
import {
  QueryGetExchangeRateRequest,
  StateFilter,
  QueryClientImpl as XrQueryClientImpl,
} from '@verana-labs/verana-types/codec/verana/xr/v1/query'
import type { ExchangeRate } from '@verana-labs/verana-types/codec/verana/xr/v1/tx'
import { Context, ServiceBroker } from 'moleculer'
import BaseService from '../../base/base.service'
import ApiResponder from '../../common/utils/apiResponse'
import { getBlockHeight } from '../../common/utils/blockHeight'
import { dateToIsoOrNull } from '../../common/utils/date_utils'
import { withAbciQueryClient } from '../../common/utils/grpc_query'

interface GetPriceParams {
  base_asset_type: string
  base_asset: string
  quote_asset_type: string
  quote_asset: string
  amount: string
}

function parseAssetType(value: string): PricingAssetType {
  const assetType = pricingAssetTypeFromJSON(value)
  if (assetType === PricingAssetType.PRICING_ASSET_TYPE_UNSPECIFIED) {
    throw new Error(`Invalid asset type: ${value}`)
  }
  return assetType
}

function computePrice(amount: string, rate: string, rateScale: number): string {
  const scaled = BigInt(amount) * BigInt(rate)
  let divisor = BigInt(1)
  const ten = BigInt(10)
  for (let i = 0; i < rateScale; i += 1) {
    divisor *= ten
  }
  return (scaled / divisor).toString()
}

@Service({
  name: 'ExchangeRateApiService',
  version: 1,
})
export default class ExchangeRateApiService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker)
  }

  private fetchExchangeRate(
    baseAssetType: PricingAssetType,
    baseAsset: string,
    quoteAssetType: PricingAssetType,
    quoteAsset: string,
    state: StateFilter,
    expireTs: Date | undefined,
    blockHeight: number | undefined
  ): Promise<ExchangeRate | undefined> {
    return withAbciQueryClient(blockHeight, async (rpc) => {
      const query = new XrQueryClientImpl(rpc)
      try {
        const res = await query.GetExchangeRate(
          QueryGetExchangeRateRequest.fromPartial({
            baseAssetType,
            baseAsset,
            quoteAssetType,
            quoteAsset,
            state,
            expireTs,
          })
        )
        return res?.exchangeRate
      } catch (err: any) {
        if (err?.code === 5 || err?.message?.includes('key not found')) {
          return undefined
        }
        throw err
      }
    })
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
      const { base_asset: baseAsset, quote_asset: quoteAsset, amount } = ctx.params
      const baseAssetType = parseAssetType(ctx.params.base_asset_type)
      const quoteAssetType = parseAssetType(ctx.params.quote_asset_type)
      const blockHeight = getBlockHeight(ctx)

      if (baseAssetType === quoteAssetType && baseAsset === quoteAsset) {
        return ApiResponder.success(ctx, {
          price: amount,
          base_asset_type: ctx.params.base_asset_type,
          base_asset: baseAsset,
          quote_asset_type: ctx.params.quote_asset_type,
          quote_asset: quoteAsset,
        })
      }

      const active = await this.fetchExchangeRate(
        baseAssetType,
        baseAsset,
        quoteAssetType,
        quoteAsset,
        StateFilter.STATE_FILTER_ACTIVE,
        new Date(),
        blockHeight
      )

      if (!active) {
        const anyEntry = await this.fetchExchangeRate(
          baseAssetType,
          baseAsset,
          quoteAssetType,
          quoteAsset,
          StateFilter.STATE_FILTER_UNSPECIFIED,
          undefined,
          blockHeight
        )
        if (anyEntry) {
          return ApiResponder.error(ctx, 'Exchange rate is disabled or expired', 410)
        }
        return ApiResponder.error(ctx, 'Exchange rate not found', 404)
      }

      const price = computePrice(amount, active.rate, active.rateScale)

      return ApiResponder.success(ctx, {
        price,
        base_asset_type: ctx.params.base_asset_type,
        base_asset: baseAsset,
        quote_asset_type: ctx.params.quote_asset_type,
        quote_asset: quoteAsset,
        rate: active.rate,
        rate_scale: active.rateScale,
        expires: dateToIsoOrNull(active.expires),
      })
    } catch (err: any) {
      this.logger.error('Error in ExchangeRate.getPrice:', err)
      return ApiResponder.error(ctx, `Failed to get price: ${err?.message || String(err)}`, 500)
    }
  }
}
