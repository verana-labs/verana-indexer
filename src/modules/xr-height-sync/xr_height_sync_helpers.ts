import {
  QueryClientImpl as XrQueryClientImpl,
  QueryListExchangeRatesRequest,
} from "@verana-labs/verana-types/codec/verana/xr/v1/query";
import type { ExchangeRate as LedgerExchangeRate } from "@verana-labs/verana-types/codec/verana/xr/v1/tx";
import { pricingAssetTypeToJSON } from "@verana-labs/verana-types/codec/verana/cs/v1/types";
import { dateToIsoOrNull } from "../../common/utils/date_utils";
import { withAbciQueryClient } from "../../common/utils/grpc_query";
import type { ExchangeRateRow } from "../../services/crawl-xr/xr_database.service";

export function serializeLedgerExchangeRate(rate: LedgerExchangeRate): ExchangeRateRow {
  return {
    id: rate.id,
    base_asset_type: pricingAssetTypeToJSON(rate.baseAssetType),
    base_asset: rate.baseAsset,
    quote_asset_type: pricingAssetTypeToJSON(rate.quoteAssetType),
    quote_asset: rate.quoteAsset,
    rate: rate.rate,
    rate_scale: rate.rateScale,
    validity_duration: rate.validityDuration ? Number(rate.validityDuration.seconds) : 0,
    updated: dateToIsoOrNull(rate.updated),
    expires: dateToIsoOrNull(rate.expires),
    state: rate.state,
  };
}

export async function fetchExchangeRates(
  blockHeight: number | undefined
): Promise<LedgerExchangeRate[]> {
  return withAbciQueryClient(blockHeight, async (rpc) => {
    const query = new XrQueryClientImpl(rpc);
    const res = await query.ListExchangeRates(
      QueryListExchangeRatesRequest.fromPartial({})
    );
    return res?.exchangeRates ?? [];
  });
}
