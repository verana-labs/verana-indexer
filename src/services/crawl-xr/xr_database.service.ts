import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended'
import { ServiceBroker } from 'moleculer'
import BaseService from '../../base/base.service'
import { SERVICE } from '../../common'
import knex from '../../common/utils/db_connection'

export interface ExchangeRateRow {
  id: number
  base_asset_type: string
  base_asset: string
  quote_asset_type: string
  quote_asset: string
  rate: string
  rate_scale: number
  validity_duration: number
  updated: string | null
  expires: string | null
  state: boolean
}

const HISTORY_FIELDS: (keyof ExchangeRateRow)[] = ['rate', 'rate_scale', 'validity_duration', 'expires', 'state']

function computeExchangeRateChanges(
  previous: ExchangeRateRow | undefined,
  next: ExchangeRateRow
): Record<string, unknown> | null {
  const changes: Record<string, unknown> = {}
  if (!previous) {
    for (const field of HISTORY_FIELDS) {
      changes[field] = next[field]
    }
    return changes
  }
  for (const field of HISTORY_FIELDS) {
    if (String(previous[field]) !== String(next[field])) {
      changes[field] = { before: previous[field], after: next[field] }
    }
  }
  return Object.keys(changes).length ? changes : null
}

@Service({
  name: SERVICE.V1.ExchangeRateDatabaseService.key,
  version: 1,
})
export default class ExchangeRateDatabaseService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker)
  }

  @Action({ name: 'syncFromLedger' })
  async syncFromLedger(ctx: {
    params: { exchangeRate: ExchangeRateRow; blockHeight: number; eventType?: string }
  }): Promise<{ success: boolean }> {
    const { exchangeRate, blockHeight } = ctx.params
    const eventType = ctx.params.eventType ?? 'SYNC_LEDGER'

    await knex.transaction(async (trx) => {
      const previous = await trx<ExchangeRateRow>('exchange_rates').where({ id: exchangeRate.id }).first()

      const row = {
        id: exchangeRate.id,
        base_asset_type: exchangeRate.base_asset_type,
        base_asset: exchangeRate.base_asset,
        quote_asset_type: exchangeRate.quote_asset_type,
        quote_asset: exchangeRate.quote_asset,
        rate: exchangeRate.rate,
        rate_scale: exchangeRate.rate_scale,
        validity_duration: exchangeRate.validity_duration,
        updated: exchangeRate.updated,
        expires: exchangeRate.expires,
        state: exchangeRate.state,
      }

      await trx('exchange_rates').insert(row).onConflict('id').merge()

      const changes = computeExchangeRateChanges(previous, exchangeRate)
      if (!changes) {
        return
      }

      await trx('exchange_rate_history')
        .insert({
          exchange_rate_id: exchangeRate.id,
          base_asset_type: exchangeRate.base_asset_type,
          base_asset: exchangeRate.base_asset,
          quote_asset_type: exchangeRate.quote_asset_type,
          quote_asset: exchangeRate.quote_asset,
          rate: exchangeRate.rate,
          rate_scale: exchangeRate.rate_scale,
          validity_duration: exchangeRate.validity_duration,
          updated: exchangeRate.updated,
          expires: exchangeRate.expires,
          state: exchangeRate.state,
          event_type: eventType,
          height: blockHeight,
          changes: JSON.stringify(changes),
        })
        .onConflict(['exchange_rate_id', 'height'])
        .merge()
    })

    return { success: true }
  }
}
