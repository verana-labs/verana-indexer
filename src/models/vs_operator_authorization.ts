import BaseModel from './base'
import type { DenomAmount } from './operator_authorization'

export interface ParticipantAuthorizationRecord {
  participant_id: number
  msg_types: string[]
  spend_limit: DenomAmount[] | null
  remaining_spend: DenomAmount[] | null
  fee_spend_limit: DenomAmount[] | null
  remaining_fee_spend: DenomAmount[] | null
  with_feegrant: boolean
  expiration: string | null
  period: string | null
}

export default class VSOperatorAuthorization extends BaseModel {
  static tableName = 'vs_operator_authorizations'

  id!: number
  corporation_id!: number
  vs_operator!: string
  records!: ParticipantAuthorizationRecord[]
  modified!: string | null
  height!: number

  static get jsonSchema() {
    return {
      type: 'object',
      required: ['id', 'corporation_id', 'vs_operator', 'records', 'height'],
      properties: {
        id: { type: 'integer' },
        corporation_id: { type: 'integer' },
        vs_operator: { type: 'string', maxLength: 255 },
        records: { type: 'array' },
        modified: { type: ['string', 'null'] },
        height: { type: 'integer' },
      },
    }
  }
}
