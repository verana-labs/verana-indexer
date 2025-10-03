import Permission, { PermissionType, ValidationState } from '../models/permission';
import PermissionSession from '../models/permission_session';
import PermissionEvent, { PermissionAction } from '../models/permission_event';

export class PermissionRepository {
    async createPermission(params: {
        schema_id: number;
        type: PermissionType;
        grantee: string;
        created_by: string;
        did?: string;
        country?: string;
        effective_from?: Date;
        effective_until?: Date;
        validation_fees?: string;
        issuance_fees?: string;
        verification_fees?: string;
        validator_perm_id?: number;
    }): Promise<Permission> {
        return await Permission.query().insert({
            ...params,
            effective_from: params.effective_from ? params.effective_from.toISOString() as any : undefined,
            effective_until: params.effective_until ? params.effective_until.toISOString() as any : undefined,
        } as any);
    }

    async startValidationProcess(params: {
        type: PermissionType;
        validator_perm_id: number;
        country: string;
        grantee: string;
        validation_fees: string;
        trust_deposit: string;
    }): Promise<Permission> {
        return await Permission.query().insert({
            type: params.type,
            validator_perm_id: params.validator_perm_id,
            country: params.country,
            grantee: params.grantee,
            created_by: params.grantee,
            vp_state: 'PENDING',
            vp_current_fees: params.validation_fees,
            vp_current_deposit: params.trust_deposit,
            vp_last_state_change: new Date().toISOString()
        });
    }

    async setValidationToValidated(params: {
        perm_id: number;
        validator: string;
        effective_until?: Date;
        validation_fees?: string;
        issuance_fees?: string;
        verification_fees?: string;
        country?: string;
        vp_summary_digest_sri?: string;
    }): Promise<Permission> {
        return await Permission.query()
            .patchAndFetchById(params.perm_id, {
                vp_state: 'VALIDATED',
                vp_last_state_change: new Date().toISOString(),
                vp_current_fees: '0',
                vp_current_deposit: '0',
                effective_until: params.effective_until?.toISOString(),
                validation_fees: params.validation_fees,
                issuance_fees: params.issuance_fees,
                verification_fees: params.verification_fees,
                country: params.country,
                vp_summary_digest_sri: params.vp_summary_digest_sri,
                modified: new Date().toISOString()
            });
    }

    async findValidatorsForType(params: {
        schema_id: number;
        applicant_type: PermissionType;
        country?: string;
        only_active?: boolean;
    }): Promise<Permission[]> {
        let query = Permission.query()
            .where('schema_id', params.schema_id)
            .where('type', 'ECOSYSTEM'); // Start with ecosystem perms

        if (params.country) {
            query = query.where((builder) => {
                builder.where('country', params.country as string).orWhereNull('country');
            });
        }

        if (params.only_active !== false) {
            query = query.where('effective_until', '>', new Date().toISOString())
                .whereNull('revoked_at')
                .whereNull('slashed_at');
        }

        return await query;
    }

    async logEvent(params: {
        perm_id: number;
        action: PermissionAction;
        actor: string;
        data?: any;
    }): Promise<PermissionEvent> {
        return await PermissionEvent.query().insert(params);
    }

    async createPermissionSession(params: {
        id: string;
        controller: string;
        agent_perm_id: number;
        wallet_agent_perm_id: number;
        issuer_perm_id?: number;
        verifier_perm_id?: number;
    }): Promise<PermissionSession> {
        const session = await PermissionSession.query().insert({
            id: params.id,
            controller: params.controller,
            agent_perm_id: params.agent_perm_id,
            wallet_agent_perm_id: params.wallet_agent_perm_id,
            authz: [{ issuer_perm_id: params.issuer_perm_id, verifier_perm_id: params.verifier_perm_id }]
        });

        await this.logEvent({
            perm_id: params.agent_perm_id,
            action: 'CREATE',
            actor: params.controller,
            data: { session_id: params.id }
        });

        return session;
    }
}

export default new PermissionRepository();