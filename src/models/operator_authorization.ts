import BaseModel from './base'

export interface DenomAmount {
  denom: string
  amount: string
}

export default class OperatorAuthorization extends BaseModel {
  static tableName = 'operator_authorizations'

  id!: number
  corporation_id!: number
  operator!: string
  msg_types!: string[]
  spend_limit!: DenomAmount[] | null
  remaining_spend!: DenomAmount[] | null
  fee_spend_limit!: DenomAmount[] | null
  remaining_fee_spend!: DenomAmount[] | null
  expiration!: string | null
  period!: string | null
  modified!: string | null
  height!: number

  static get jsonSchema() {
    return {
      type: 'object',
      required: ['id', 'corporation_id', 'operator', 'msg_types', 'height'],
      properties: {
        id: { type: 'integer' },
        corporation_id: { type: 'integer' },
        operator: { type: 'string', maxLength: 255 },
        msg_types: { type: 'array', items: { type: 'string' } },
        spend_limit: { type: ['array', 'null'] },
        remaining_spend: { type: ['array', 'null'] },
        fee_spend_limit: { type: ['array', 'null'] },
        remaining_fee_spend: { type: ['array', 'null'] },
        expiration: { type: ['string', 'null'] },
        period: { type: ['string', 'null'] },
        modified: { type: ['string', 'null'] },
        height: { type: 'integer' },
      },
    }
  }
}
