import BaseModel from './base'

export default class Digest extends BaseModel {
  static tableName = 'digests'

  static get idColumn() {
    return 'digest'
  }

  digest!: string
  created!: string
  height!: number

  static get jsonSchema() {
    return {
      type: 'object',
      required: ['digest', 'created', 'height'],
      properties: {
        digest: { type: 'string', maxLength: 512 },
        created: { type: 'string' },
        height: { type: 'integer' },
      },
    }
  }
}
