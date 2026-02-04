import { Model } from 'objection';
import BaseModel from './base';
import type _CredentialSchema from './credential_schema';
import type _PermissionSession from './permission_session';

export type PermissionType =
    | 'ECOSYSTEM'
    | 'ISSUER_GRANTOR'
    | 'VERIFIER_GRANTOR'
    | 'ISSUER'
    | 'VERIFIER'
    | 'HOLDER';

export type ValidationState = 'VALIDATION_STATE_UNSPECIFIED' | 'PENDING' | 'VALIDATED' | 'TERMINATED';

export default class Permission extends BaseModel {
    static tableName = 'permissions';

    id!: string;
    schema_id!: string;
    type!: PermissionType;
    did?: string;
    grantee!: string; // account address
    created!: string;
    created_by!: string;
    extended?: string | null;
    extended_by?: string;
    slashed?: string | null;
    slashed_by?: string;
    repaid?: string | null;
    repaid_by?: string;
    effective_from?: string | null;
    effective_until?: string | null;
    modified!: string;
    validation_fees!: string;
    issuance_fees!: string;
    verification_fees!: string;
    deposit!: string;
    slashed_deposit!: string;
    repaid_deposit!: string;
    revoked?: string | null;
    revoked_by?: string;
    country?: string; // ISO 3166 alpha-2
    validator_perm_id?: string | null; // Can be null for ECOSYSTEM permissions
    vp_state?: ValidationState;
    vp_exp?: string | null;
    vp_last_state_change?: string | null;
    vp_validator_deposit!: string;
    vp_current_fees!: string;
    vp_current_deposit!: string;
    vp_summary_digest_sri?: string;
    vp_term_requested?: string | null;
    expire_soon?: boolean | null;
    participants?: number;
    weight?: string;
    issued?: number;
    verified?: number;
    ecosystem_slash_events?: number;
    ecosystem_slashed_amount?: string;
    ecosystem_slashed_amount_repaid?: string;
    network_slash_events?: number;
    network_slashed_amount?: string;
    network_slashed_amount_repaid?: string;

    static get jsonSchema() {
        return {
            type: 'object',
            required: ['id', 'schema_id', 'type', 'grantee', 'created_by', 'created', 'modified'],
            additionalProperties: false,
            properties: {
                id: { type: 'string' },
                schema_id: { type: 'string' },
                type: {
                    type: 'string',
                    enum: ['ECOSYSTEM', 'ISSUER_GRANTOR', 'VERIFIER_GRANTOR', 'ISSUER', 'VERIFIER', 'HOLDER']
                },
                did: { type: 'string', maxLength: 255 },
                grantee: { type: 'string', maxLength: 255 },
                created_by: { type: 'string', maxLength: 255 },
                country: { type: 'string', maxLength: 2 },
                validation_fees: { type: 'string' },
                issuance_fees: { type: 'string' },
                verification_fees: { type: 'string' },
                deposit: { type: 'string' },
                validator_perm_id: { type: ['string', 'null'] },
                created: { type: 'string' },
                modified: { type: 'string' },
                extended: { type: ['string', 'null'] },
                slashed: { type: ['string', 'null'] },
                repaid: { type: ['string', 'null'] },
                effective_from: { type: ['string', 'null'] },
                effective_until: { type: ['string', 'null'] },
                revoked: { type: ['string', 'null'] },
                vp_exp: { type: ['string', 'null'] },
                vp_last_state_change: { type: ['string', 'null'] },
                vp_term_requested: { type: ['string', 'null'] },
                slashed_deposit: { type: 'string' },
                repaid_deposit: { type: 'string' },
                vp_validator_deposit: { type: 'string' },
                vp_current_fees: { type: 'string' },
                vp_current_deposit: { type: 'string' },
                vp_state: {
                    type: 'string',
                    enum: ['VALIDATION_STATE_UNSPECIFIED', 'PENDING', 'VALIDATED', 'TERMINATED']
                },
                vp_summary_digest_sri: { type: 'string', maxLength: 512 },
                revoked_by: { type: 'string', maxLength: 255 },
                slashed_by: { type: 'string', maxLength: 255 },
                repaid_by: { type: 'string', maxLength: 255 },
                extended_by: { type: 'string', maxLength: 255 },
                expire_soon: { type: ['boolean', 'null'] },
                participants: { type: 'integer' },
                weight: { type: 'string', maxLength: 50 },
                issued: { type: 'integer' },
                verified: { type: 'integer' },
                ecosystem_slash_events: { type: 'integer' },
                ecosystem_slashed_amount: { type: 'string', maxLength: 50 },
                ecosystem_slashed_amount_repaid: { type: 'string', maxLength: 50 },
                network_slash_events: { type: 'integer' },
                network_slashed_amount: { type: 'string', maxLength: 50 },
                network_slashed_amount_repaid: { type: 'string', maxLength: 50 }
            }
        };
    }

    static get relationMappings() {
        return {
            schema: {
                relation: Model.BelongsToOneRelation,
                modelClass: 'CredentialSchema',
                join: {
                    from: 'permissions.schema_id',
                    to: 'credential_schemas.id'
                }
            },
            validator_permission: {
                relation: Model.BelongsToOneRelation,
                modelClass: Permission,
                join: {
                    from: 'permissions.validator_perm_id',
                    to: 'permissions.id'
                }
            },
            dependent_permissions: {
                relation: Model.HasManyRelation,
                modelClass: Permission,
                join: {
                    from: 'permissions.id',
                    to: 'permissions.validator_perm_id'
                }
            },
            sessions: {
                relation: Model.HasManyRelation,
                modelClass: 'PermissionSession',
                join: {
                    from: 'permissions.id',
                    to: 'permission_sessions.agent_perm_id'
                }
            }
        };
    }

    // Helper methods
    isActive(): boolean {
        const now = new Date();
        const effectiveFrom = this.effective_from ? new Date(this.effective_from) : null;
        const effectiveUntil = this.effective_until ? new Date(this.effective_until) : null;

        if (effectiveFrom && now < effectiveFrom) return false;
        if (effectiveUntil && now > effectiveUntil) return false;
        if (this.revoked) return false;
        if (this.slashed && !this.repaid) return false;

        return this.vp_state === 'VALIDATED' || this.type === 'ECOSYSTEM';
    }

    isExpired(): boolean {
        if (!this.effective_until) return false;
        return new Date() > new Date(this.effective_until);
    }

    canBeValidatorFor(applicantType: PermissionType): boolean {
        // Implementation based on MOD-PERM-MSG-1-2-2 rules
        switch (applicantType) {
            case 'ISSUER':
                return this.type === 'ISSUER_GRANTOR' || this.type === 'ECOSYSTEM';
            case 'ISSUER_GRANTOR':
                return this.type === 'ECOSYSTEM';
            case 'VERIFIER':
                return this.type === 'VERIFIER_GRANTOR' || this.type === 'ECOSYSTEM';
            case 'VERIFIER_GRANTOR':
                return this.type === 'ECOSYSTEM';
            case 'HOLDER':
                return this.type === 'ISSUER';
            default:
                return false;
        }
    }

    // Helper method to check if permission is valid (not expired, revoked, or slashed)
    isValidPermission(): boolean {
        const now = new Date();

        // Check if revoked
        if (this.revoked) return false;

        // Check if slashed and not repaid
        if (this.slashed && !this.repaid) return false;

        // Check expiration
        if (this.effective_until && now > new Date(this.effective_until)) return false;

        // Check if effective yet
        if (this.effective_from && now < new Date(this.effective_from)) return false;

        return true;
    }

    // Helper method to get ancestors in permission tree
    async getAncestors(): Promise<Permission[]> {
        const ancestors: Permission[] = [];
        let currentPerm: Permission | undefined = this as Permission;

        while (currentPerm?.validator_perm_id) {
            const parent = await Permission.query().findById(currentPerm.validator_perm_id);
            if (!parent) break;
            ancestors.push(parent);
            currentPerm = parent;
        }

        return ancestors;
    }
}