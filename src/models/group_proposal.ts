import BaseModel from './base'

export default class GroupProposal extends BaseModel {
  static tableName = 'group_proposal'

  id!: number
  corporation_id!: number
  group_policy_address!: string
  metadata!: string | null
  proposers!: string[]
  submit_time!: string
  group_version!: number | null
  group_policy_version!: number | null
  status!: string
  executor_result!: string
  voting_period_end!: string | null
  messages!: Array<Record<string, unknown>>
  final_tally_result!: Record<string, unknown> | null
  modified!: string
  height!: number

  static get jsonSchema() {
    return {
      type: 'object',
      required: ['id', 'corporation_id', 'group_policy_address', 'status', 'executor_result', 'height'],
      properties: {
        id: { type: 'integer' },
        corporation_id: { type: 'integer' },
        group_policy_address: { type: 'string' },
        metadata: { type: ['string', 'null'] },
        proposers: { type: 'array', items: { type: 'string' } },
        group_version: { type: ['integer', 'null'] },
        group_policy_version: { type: ['integer', 'null'] },
        status: { type: 'string', maxLength: 16 },
        executor_result: { type: 'string', maxLength: 16 },
        messages: { type: 'array' },
        final_tally_result: { type: ['object', 'null'] },
        height: { type: 'integer' },
      },
    }
  }
}
