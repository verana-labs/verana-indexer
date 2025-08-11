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

    static useTimestamps = false;
    static get jsonSchema() {
        return {
            type: 'object',
            required: ['did', 'controller'],
            properties: {
                id: { type: 'integer' },
                height: { type: 'integer' },
                did: { type: 'string', maxLength: 255 },
                controller: { type: 'string', maxLength: 255 },
                created: { type: 'string', format: 'date-time' },
                modified: { type: 'string', format: 'date-time' },
                exp: { type: 'string', format: 'date-time' },
                deposit: { type: 'string' }
            }
        };
    }
}
