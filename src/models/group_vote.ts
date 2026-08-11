import BaseModel from './base'

export default class GroupVote extends BaseModel {
  static tableName = 'group_vote'

  id!: number
  proposal_id!: number
  voter!: string
  option!: string
  metadata!: string | null
  submit_time!: string
  height!: number

  static get jsonSchema() {
    return {
      type: 'object',
      required: ['proposal_id', 'voter', 'option', 'height'],
      properties: {
        id: { type: 'integer' },
        proposal_id: { type: 'integer' },
        voter: { type: 'string' },
        option: { type: 'string', maxLength: 16 },
        metadata: { type: ['string', 'null'] },
        height: { type: 'integer' },
      },
    }
  }
}
