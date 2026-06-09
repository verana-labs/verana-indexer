import { Model } from 'objection';
import BaseModel from './base';
import type _CredentialSchema from './credential_schema';
import type _ParticipantSession from './participant_session';

export type ParticipantType =
    | 'ECOSYSTEM'
    | 'UNSPECIFIED'
    | 'ISSUER_GRANTOR'
    | 'VERIFIER_GRANTOR'
    | 'ISSUER'
    | 'VERIFIER'
    | 'HOLDER';

export type ValidationState = 'VALIDATION_STATE_UNSPECIFIED' | 'PENDING' | 'VALIDATED' | 'TERMINATED';

export type DenomAmount = {
    denom: string;
    amount: string;
};

export default class Participant extends BaseModel {
    static tableName = 'participants';

    id!: number;
    schema_id!: number;
    role!: ParticipantType;
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
    validator_participant_id?: number | null; // Can be null for ECOSYSTEM participants
    op_state?: ValidationState;
    op_exp?: Date | null;
    op_last_state_change?: string | null;
    op_validator_deposit!: number;
    op_current_fees!: number;
    op_current_deposit!: number;
    op_summary_digest?: string;
    vs_operator?: string | null;
    adjusted?: Date | string | null;
    vs_operator_authz_enabled?: boolean;
    vs_operator_authz_spend_limit?: DenomAmount[] | null;
    vs_operator_authz_with_feegrant?: boolean;
    vs_operator_authz_fee_spend_limit?: DenomAmount[] | null;
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
            required: ['id', 'schema_id', 'role', 'corporation', 'created', 'modified'],
            additionalProperties: false,
            properties: {
                id: { type: 'integer' },
                schema_id: { type: 'integer' },
                role: {
                    type: 'string',
                    enum: ['UNSPECIFIED', 'ECOSYSTEM', 'ISSUER_GRANTOR', 'VERIFIER_GRANTOR', 'ISSUER', 'VERIFIER', 'HOLDER']
                },
                did: { type: 'string', maxLength: 255 },
                corporation: { type: 'string', maxLength: 255 },
                validation_fees: { type: 'integer' },
                issuance_fees: { type: 'integer' },
                verification_fees: { type: 'integer' },
                deposit: { type: 'integer' },
                slashed_deposit: { type: 'integer' },
                repaid_deposit: { type: 'integer' },
                op_validator_deposit: { type: 'integer' },
                op_current_fees: { type: 'integer' },
                op_current_deposit: { type: 'integer' },
                validator_participant_id: { type: ['integer', 'null'] },
                created: { type: 'string' },
                modified: { type: 'string' },
                adjusted: { type: ['string', 'null'] },
                slashed: { type: ['string', 'null'] },
                repaid: { type: ['string', 'null'] },
                effective_from: { type: ['string', 'null'] },
                effective_until: { type: ['string', 'null'] },
                revoked: { type: ['string', 'null'] },
                op_exp: { type: ['string', 'null'] },
                op_last_state_change: { type: ['string', 'null'] },
                op_state: {
                    type: 'string',
                    enum: ['VALIDATION_STATE_UNSPECIFIED', 'PENDING', 'VALIDATED', 'TERMINATED']
                },
                op_summary_digest: { type: 'string', maxLength: 512 },
                vs_operator: { type: ['string', 'null'] },
                vs_operator_authz_enabled: { type: ['boolean', 'null'] },
                vs_operator_authz_spend_limit: {
                    anyOf: [
                        {
                            type: 'array',
                            items: {
                                type: 'object',
                                required: ['denom', 'amount'],
                                additionalProperties: false,
                                properties: {
                                    denom: { type: 'string' },
                                    amount: { type: 'string' }
                                }
                            }
                        },
                        { type: 'null' }
                    ]
                },
                vs_operator_authz_with_feegrant: { type: ['boolean', 'null'] },
                vs_operator_authz_fee_spend_limit: {
                    anyOf: [
                        {
                            type: 'array',
                            items: {
                                type: 'object',
                                required: ['denom', 'amount'],
                                additionalProperties: false,
                                properties: {
                                    denom: { type: 'string' },
                                    amount: { type: 'string' }
                                }
                            }
                        },
                        { type: 'null' }
                    ]
                },
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
                    from: 'participants.schema_id',
                    to: 'credential_schemas.id'
                }
            },
            validator_participant: {
                relation: Model.BelongsToOneRelation,
                modelClass: Participant,
                join: {
                    from: 'participants.validator_participant_id',
                    to: 'participants.id'
                }
            },
            dependent_participants: {
                relation: Model.HasManyRelation,
                modelClass: Participant,
                join: {
                    from: 'participants.id',
                    to: 'participants.validator_participant_id'
                }
            },
            sessions: {
                relation: Model.HasManyRelation,
                modelClass: 'ParticipantSession',
                join: {
                    from: 'participants.id',
                    to: 'participant_sessions.agent_participant_id'
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

        return this.op_state === 'VALIDATED' || this.role === 'ECOSYSTEM';
    }

    isExpired(): boolean {
        if (!this.effective_until) return false;
        return new Date() > new Date(this.effective_until);
    }

    canBeValidatorFor(applicantType: ParticipantType): boolean {
        // Implementation based on MOD-PP-MSG-1-2-2 rules
        switch (applicantType) {
            case 'ISSUER':
                return this.role === 'ISSUER_GRANTOR' || this.role === 'ECOSYSTEM';
            case 'ISSUER_GRANTOR':
                return this.role === 'ECOSYSTEM';
            case 'VERIFIER':
                return this.role === 'VERIFIER_GRANTOR' || this.role === 'ECOSYSTEM';
            case 'VERIFIER_GRANTOR':
                return this.role === 'ECOSYSTEM';
            case 'HOLDER':
                return this.role === 'ISSUER';
            default:
                return false;
        }
    }

    // Helper method to check if participant is valid (not expired, revoked, or slashed)
    isValidParticipant(): boolean {
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

    // Helper method to get ancestors in participant tree
    async getAncestors(): Promise<Participant[]> {
        const ancestors: Participant[] = [];
        let currentParticipant: Participant | undefined = this as Participant;

        while (currentParticipant?.validator_participant_id) {
            const parent = await Participant.query().findById(currentParticipant.validator_participant_id);
            if (!parent) break;
            ancestors.push(parent);
            currentParticipant = parent;
        }

        return ancestors;
    }
}
