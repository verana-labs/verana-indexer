import BaseModel from './base'
import type { DenomAmount } from './operator_authorization'

export default class OperatorAuthorizationHistory extends BaseModel {
  static tableName = 'operator_authorization_history'

  id!: number
  operator_authorization_id!: number
  corporation_id!: number
  operator!: string
  msg_types!: string[] | null
  spend_limit!: DenomAmount[] | null
  remaining_spend!: DenomAmount[] | null
  fee_spend_limit!: DenomAmount[] | null
  remaining_fee_spend!: DenomAmount[] | null
  expiration!: string | null
  period!: string | null
  modified!: string | null
  revoked!: boolean
  height!: number

  static get jsonSchema() {
    return {
      type: 'object',
      required: ['operator_authorization_id', 'corporation_id', 'operator', 'height'],
      properties: {
        operator_authorization_id: { type: 'integer' },
        corporation_id: { type: 'integer' },
        operator: { type: 'string', maxLength: 255 },
        msg_types: { type: ['array', 'null'], items: { type: 'string' } },
        spend_limit: { type: ['array', 'null'] },
        remaining_spend: { type: ['array', 'null'] },
        fee_spend_limit: { type: ['array', 'null'] },
        remaining_fee_spend: { type: ['array', 'null'] },
        expiration: { type: ['string', 'null'] },
        period: { type: ['string', 'null'] },
        modified: { type: ['string', 'null'] },
        revoked: { type: 'boolean' },
        height: { type: 'integer' },
      },
    }
  }
}
