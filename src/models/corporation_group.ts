import BaseModel from './base'

export default class CorporationGroup extends BaseModel {
  static tableName = 'corporation_group'

  static get idColumn() {
    return 'corporation_id'
  }

  corporation_id!: number
  group_id!: number
  policy_address!: string
  group_version!: number
  policy_version!: number
  total_weight!: string
  decision_policy!: Record<string, unknown> | null
  created!: string
  modified!: string
  height!: number

  static get jsonSchema() {
    return {
      type: 'object',
      required: ['corporation_id', 'group_id', 'policy_address', 'height'],
      properties: {
        corporation_id: { type: 'integer' },
        group_id: { type: 'integer' },
        policy_address: { type: 'string' },
        group_version: { type: 'integer' },
        policy_version: { type: 'integer' },
        total_weight: { type: 'string' },
        decision_policy: { type: ['object', 'null'] },
        height: { type: 'integer' },
      },
    }
  }
}
