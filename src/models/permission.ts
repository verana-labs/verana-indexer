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

    id!: number;
    schema_id!: number;
    type!: PermissionType;
    did?: string;
    corporation!: string;
    created!: Date;
    slashed?: Date | null;
    repaid?: Date | null;
    effective_from?: Date | null;
    effective_until?: Date | null;
    modified!: Date;
    validation_fees!: number;
    issuance_fees!: number;
    verification_fees!: number;
    issuance_fee_discount?: number;
    verification_fee_discount?: number;
    deposit!: number;
    slashed_deposit!: number;
    repaid_deposit!: number;
    revoked?: Date | null;
    validator_perm_id?: number | null; // Can be null for ECOSYSTEM permissions
    vp_state?: ValidationState;
    vp_exp?: Date | null;
    vp_last_state_change?: string | null;
    vp_validator_deposit!: number;
    vp_current_fees!: number;
    vp_current_deposit!: number;
    vp_summary_digest?: string;
    vs_operator?: string | null;
    adjusted?: Date | string | null;
    adjusted_by?: string | null;
    vs_operator_authz_enabled?: boolean;
    vs_operator_authz_spend_limit?: unknown;
    vs_operator_authz_with_feegrant?: boolean;
    vs_operator_authz_fee_spend_limit?: unknown;
    vs_operator_authz_spend_period?: string | null;
    expire_soon?: boolean | null;
    participants?: number;
    participants_ecosystem?: number;
    participants_issuer_grantor?: number;
    participants_issuer?: number;
    participants_verifier_grantor?: number;
    participants_verifier?: number;
    participants_holder?: number;
    weight?: number;
    issued?: number;
    verified?: number;
    ecosystem_slash_events?: number;
    ecosystem_slashed_amount?: number;
    ecosystem_slashed_amount_repaid?: number;
    network_slash_events?: number;
    network_slashed_amount?: number;
    network_slashed_amount_repaid?: number;

    static get jsonSchema() {
        return {
            type: 'object',
            required: ['id', 'schema_id', 'type', 'corporation', 'created', 'modified'],
            additionalProperties: false,
            properties: {
                id: { type: 'integer' },
                schema_id: { type: 'integer' },
                type: {
                    type: 'string',
                    enum: ['ECOSYSTEM', 'ISSUER_GRANTOR', 'VERIFIER_GRANTOR', 'ISSUER', 'VERIFIER', 'HOLDER']
                },
                did: { type: 'string', maxLength: 255 },
                corporation: { type: 'string', maxLength: 255 },
                validation_fees: { type: 'integer' },
                issuance_fees: { type: 'integer' },
                verification_fees: { type: 'integer' },
                deposit: { type: 'integer' },
                slashed_deposit: { type: 'integer' },
                repaid_deposit: { type: 'integer' },
                vp_validator_deposit: { type: 'integer' },
                vp_current_fees: { type: 'integer' },
                vp_current_deposit: { type: 'integer' },
                validator_perm_id: { type: ['integer', 'null'] },
                created: { type: 'string' },
                modified: { type: 'string' },
                adjusted: { type: ['string', 'null'] },
                adjusted_by: { type: ['string', 'null'] },
                slashed: { type: ['string', 'null'] },
                repaid: { type: ['string', 'null'] },
                effective_from: { type: ['string', 'null'] },
                effective_until: { type: ['string', 'null'] },
                revoked: { type: ['string', 'null'] },
                vp_exp: { type: ['string', 'null'] },
                vp_last_state_change: { type: ['string', 'null'] },
                vp_state: {
                    type: 'string',
                    enum: ['VALIDATION_STATE_UNSPECIFIED', 'PENDING', 'VALIDATED', 'TERMINATED']
                },
                vp_summary_digest: { type: 'string', maxLength: 512 },
                vs_operator: { type: ['string', 'null'] },
                vs_operator_authz_enabled: { type: ['boolean', 'null'] },
                vs_operator_authz_spend_limit: { type: ['object', 'array', 'null'] },
                vs_operator_authz_with_feegrant: { type: ['boolean', 'null'] },
                vs_operator_authz_fee_spend_limit: { type: ['object', 'array', 'null'] },
                vs_operator_authz_spend_period: { type: ['string', 'null'] },
                expire_soon: { type: ['boolean', 'null'] },
                participants: { type: 'integer' },
                participants_ecosystem: { type: 'integer' },
                participants_issuer_grantor: { type: 'integer' },
                participants_issuer: { type: 'integer' },
                participants_verifier_grantor: { type: 'integer' },
                participants_verifier: { type: 'integer' },
                participants_holder: { type: 'integer' },
                weight: { type: 'integer' },
                issued: { type: 'integer' },
                verified: { type: 'integer' },
                ecosystem_slash_events: { type: 'integer' },
                ecosystem_slashed_amount: { type: 'integer' },
                ecosystem_slashed_amount_repaid: { type: 'integer' },
                network_slash_events: { type: 'integer' },
                network_slashed_amount: { type: 'integer' },
                network_slashed_amount_repaid: { type: 'integer' }
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
