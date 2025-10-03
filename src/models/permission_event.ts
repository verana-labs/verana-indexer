import BaseModel from './base';
import { Model } from 'objection';
import Permission from './permission';

export type PermissionEventAction =
    | 'CREATE'
    | 'UPDATE'
    | 'VALIDATE'
    | 'REVOKE'
    | 'EXTEND'
    | 'SLASH'
    | 'REPAY'
    | 'CANCEL_VP'
    | 'START_VP'
    | 'RENEW_VP'
    | 'SET_VP_VALIDATED';

export default class PermissionEvent extends BaseModel {
    static tableName = 'permission_events';

    id!: number;
    perm_id!: string;
    action!: PermissionEventAction;
    actor!: string; // account address
    data?: any; // Event-specific data
    created!: string;

    static get jsonSchema() {
        return {
            type: 'object',
            required: ['perm_id', 'action', 'actor'],
            properties: {
                id: { type: 'integer' },
                perm_id: { type: 'string' },
                action: {
                    type: 'string',
                    enum: [
                        'CREATE', 'UPDATE', 'VALIDATE', 'REVOKE', 'EXTEND',
                        'SLASH', 'REPAY', 'CANCEL_VP', 'START_VP', 'RENEW_VP', 'SET_VP_VALIDATED'
                    ]
                },
                actor: { type: 'string', maxLength: 255 },
                data: { type: 'object' },
                created: { type: 'string' }
            }
        };
    }

    static get jsonAttributes() {
        return ['data'];
    }

    static get relationMappings() {
        return {
            permission: {
                relation: Model.BelongsToOneRelation,
                modelClass: Permission,
                join: {
                    from: 'permission_events.perm_id',
                    to: 'permissions.id'
                }
            }
        };
    }

    // Helper method to create event
    static async createEvent(
        permId: string,
        action: PermissionEventAction,
        actor: string,
        data?: any
    ): Promise<PermissionEvent> {
        return this.query().insert({
            perm_id: permId,
            action,
            actor,
            data,
            created: new Date().toISOString()
        });
    }
}