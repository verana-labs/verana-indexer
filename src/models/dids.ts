import BaseModel from './base';

export default class Did extends BaseModel {
  static tableName = 'dids';

  id!: number;
  height!: number;
  did!: string;
  controller!: string;
  created!: Date;
  modified!: Date;
  exp!: Date;
  deposit!: string;
  event_type?: string;
  years?: string;
  is_deleted?: boolean;
  deleted_at?: Date;

  static useTimestamps = false;

  static get jsonSchema() {
    return {
      type: 'object',
      required: ['did', 'controller', 'height'],
      properties: {
        id: { type: 'integer' },
        height: { type: 'integer' },
        did: { type: 'string', maxLength: 255, unique: true },
        controller: { type: 'string', maxLength: 255 },
        created: { type: 'string', format: 'date-time' },
        modified: { type: 'string', format: 'date-time' },
        exp: { type: 'string', format: 'date-time' },
        deposit: { type: 'string' },
        event_type: { type: 'string', maxLength: 255 },
        years: { type: 'string' },
        is_deleted: { type: 'boolean' },
        deleted_at: { type: 'string', format: 'date-time' }
      }
    };
  }
}
