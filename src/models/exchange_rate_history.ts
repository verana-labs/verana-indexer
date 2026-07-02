import BaseModel from './base'

export default class ExchangeRateHistory extends BaseModel {
  static tableName = 'exchange_rate_history'

  id!: number
  exchange_rate_id!: number
  base_asset_type!: string
  base_asset!: string
  quote_asset_type!: string
  quote_asset!: string
  rate!: string
  rate_scale!: number
  validity_duration!: number
  updated!: string | null
  expires!: string | null
  state!: boolean
  event_type!: string
  height!: number
  changes!: Record<string, unknown> | null

  static get jsonSchema() {
    return {
      type: 'object',
      required: ['exchange_rate_id', 'event_type', 'height'],
      properties: {
        id: { type: 'integer' },
        exchange_rate_id: { type: 'integer' },
        base_asset_type: { type: 'string' },
        base_asset: { type: 'string', maxLength: 255 },
        quote_asset_type: { type: 'string' },
        quote_asset: { type: 'string', maxLength: 255 },
        rate: { type: 'string' },
        rate_scale: { type: 'integer' },
        validity_duration: { type: 'integer' },
        updated: { type: ['string', 'null'] },
        expires: { type: ['string', 'null'] },
        state: { type: 'boolean' },
        event_type: { type: 'string' },
        height: { type: 'integer' },
        changes: { type: ['object', 'null'] },
      },
    }
  }
}
