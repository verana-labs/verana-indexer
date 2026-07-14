import BaseModel from './base'
import type { ParticipantAuthorizationRecord } from './vs_operator_authorization'

export default class VSOperatorAuthorizationHistory extends BaseModel {
  static tableName = 'vs_operator_authorization_history'

  id!: number
  vs_operator_authorization_id!: number
  corporation_id!: number
  vs_operator!: string
  records!: ParticipantAuthorizationRecord[] | null
  modified!: string | null
  revoked!: boolean
  height!: number

  static get jsonSchema() {
    return {
      type: 'object',
      required: ['vs_operator_authorization_id', 'corporation_id', 'vs_operator', 'height'],
      properties: {
        vs_operator_authorization_id: { type: 'integer' },
        corporation_id: { type: 'integer' },
        vs_operator: { type: 'string', maxLength: 255 },
        records: { type: ['array', 'null'] },
        modified: { type: ['string', 'null'] },
        revoked: { type: 'boolean' },
        height: { type: 'integer' },
      },
    }
  }
}
