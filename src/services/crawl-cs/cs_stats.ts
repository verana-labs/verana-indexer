import knex from "../../common/utils/db_connection";
import { calculatePermState } from "../crawl-perm/perm_state_utils";

export interface CredentialSchemaStats {
    participants: number;
    weight: number;
    issued: number;
    verified: number;
    ecosystem_slash_events: number;
    ecosystem_slashed_amount: number;
    ecosystem_slashed_amount_repaid: number;
    network_slash_events: number;
    network_slashed_amount: number;
    network_slashed_amount_repaid: number;
}

export async function getSchemaController(schemaId: number, blockHeight?: number): Promise<string | null> {
    if (typeof blockHeight === "number") {
        const schemaHistory = await knex("credential_schema_history")
            .where("credential_schema_id", schemaId)
            .where("height", "<=", blockHeight)
            .orderBy("height", "desc")
            .orderBy("created_at", "desc")
            .first();
        
        if (!schemaHistory) {
            return null;
        }

        const trHistory = await knex("trust_registry_history")
            .where("tr_id", schemaHistory.tr_id)
            .where("height", "<=", blockHeight)
            .orderBy("height", "desc")
            .orderBy("created_at", "desc")
            .first();
        
        return trHistory?.controller || null;
    }
    
    const schema = await knex("credential_schemas")
        .where("id", schemaId)
        .first();
    
    if (!schema) {
        return null;
    }

    const tr = await knex("trust_registry")
        .where("id", schema.tr_id)
        .first();
    
    return tr?.controller || null;
}

export async function getPermissionsForSchema(schemaId: number, blockHeight?: number): Promise<any[]> {
    if (typeof blockHeight === "number") {
        const permHistory = await knex("permission_history")
            .where("schema_id", Number(schemaId))
            .where("height", "<=", blockHeight)
            .orderBy("permission_id", "asc")
            .orderBy("height", "desc")
            .orderBy("created_at", "desc");

        const permMap = new Map<string, any>();
        for (const perm of permHistory) {
            if (!permMap.has(String(perm.permission_id))) {
                permMap.set(String(perm.permission_id), perm);
            }
        }
        return Array.from(permMap.values());
    }
    return await knex("permissions")
        .where("schema_id", Number(schemaId))
        .select("*");
}

export async function calculateIssuedVerifiedForSchema(
    schemaId: number,
    permissionIds: Set<number>,
    blockHeight?: number
): Promise<{ issued: number; verified: number }> {
    let totalIssued = 0;
    let totalVerified = 0;

    if (typeof blockHeight === "number") {
        const latestSessionSubquery = knex("permission_session_history")
            .select("session_id")
            .select(
                knex.raw(
                    `ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY height DESC, created_at DESC) as rn`
                )
            )
            .where("height", "<=", blockHeight)
            .as("ranked");

        const sessions = await knex
            .from(latestSessionSubquery)
            .join("permission_session_history as psh", function () {
                this.on("ranked.session_id", "=", "psh.session_id")
                    .andOn("ranked.rn", "=", knex.raw("1"));
            })
            .select("psh.authz");

        for (const session of sessions) {
            const authz = typeof session.authz === "string" ? JSON.parse(session.authz) : session.authz;
            if (Array.isArray(authz)) {
                for (const entry of authz) {
                    if (entry.issuer_perm_id && permissionIds.has(Number(entry.issuer_perm_id))) {
                        totalIssued += 1;
                    }
                    if (entry.verifier_perm_id && permissionIds.has(Number(entry.verifier_perm_id))) {
                        totalVerified += 1;
                    }
                }
            }
        }
    } else {
        const sessions = await knex("permission_sessions")
            .select("authz");

        for (const session of sessions) {
            const authz = typeof session.authz === "string" ? JSON.parse(session.authz) : session.authz;
            if (Array.isArray(authz)) {
                for (const entry of authz) {
                    if (entry.issuer_perm_id && permissionIds.has(Number(entry.issuer_perm_id))) {
                        totalIssued += 1;
                    }
                    if (entry.verifier_perm_id && permissionIds.has(Number(entry.verifier_perm_id))) {
                        totalVerified += 1;
                    }
                }
            }
        }
    }

    return { issued: totalIssued, verified: totalVerified };
}

export async function calculateSlashStatsForSchema(
    schemaId: number,
    permissionIds: Set<number>,
    trController: string | null,
    blockHeight?: number
): Promise<{
    ecosystem_slash_events: number;
    ecosystem_slashed_amount: number;
    ecosystem_slashed_amount_repaid: number;
    network_slash_events: number;
    network_slashed_amount: number;
    network_slashed_amount_repaid: number;
}> {
    let ecosystemSlashEvents = 0;
    let ecosystemSlashedAmount = 0;
    let ecosystemSlashedAmountRepaid = 0;
    let networkSlashEvents = 0;
    let networkSlashedAmount = 0;
    let networkSlashedAmountRepaid = 0;

    if (permissionIds.size === 0) {
        return {
            ecosystem_slash_events: 0,
            ecosystem_slashed_amount: 0,
            ecosystem_slashed_amount_repaid: 0,
            network_slash_events: 0,
            network_slashed_amount: 0,
            network_slashed_amount_repaid: 0,
        };
    }

    const permissionIdArray = Array.from(permissionIds);

    let slashEvents: any[];
    if (typeof blockHeight === "number") {
        slashEvents = await knex("permission_history")
            .whereIn("permission_id", permissionIdArray)
            .whereRaw("schema_id = ?", [Number(schemaId)])
            .where("event_type", "SLASH_PERMISSION_TRUST_DEPOSIT")
            .where("height", "<=", blockHeight)
            .select("permission_id", "slashed_by", "type", "slashed_deposit", "repaid_deposit", "height", "created_at")
            .orderBy("permission_id", "asc")
            .orderBy("height", "asc")
            .orderBy("created_at", "asc");
    } else {
        slashEvents = await knex("permission_history")
            .whereIn("permission_id", permissionIdArray)
            .whereRaw("schema_id = ?", [Number(schemaId)])
            .where("event_type", "SLASH_PERMISSION_TRUST_DEPOSIT")
            .select("permission_id", "slashed_by", "type", "slashed_deposit", "repaid_deposit", "height", "created_at")
            .orderBy("permission_id", "asc")
            .orderBy("height", "asc")
            .orderBy("created_at", "asc");
    }

    const prevSlashedDeposits = new Map<string, number>();
    const prevRepaidDeposits = new Map<string, number>();

    for (const event of slashEvents) {
        const permIdStr = String(event.permission_id);
        const prevSlashed = prevSlashedDeposits.get(permIdStr) || 0;
        const currentSlashed = typeof event.slashed_deposit === 'number' ? event.slashed_deposit : Number(event.slashed_deposit);
        const incrementalSlashed = currentSlashed - prevSlashed;

        if (incrementalSlashed <= 0) {
            prevSlashedDeposits.set(permIdStr, currentSlashed);
            const currentRepaid = typeof event.repaid_deposit === 'number' ? event.repaid_deposit : Number(event.repaid_deposit);
            prevRepaidDeposits.set(permIdStr, currentRepaid);
            continue;
        }

        prevSlashedDeposits.set(permIdStr, currentSlashed);

        const isEcosystemPermission = event.type === "ECOSYSTEM";
        const isSlashedByEcosystemGov = trController && event.slashed_by === trController;

        if (isEcosystemPermission) {
            networkSlashEvents++;
            networkSlashedAmount += incrementalSlashed;

            const repaid = typeof event.repaid_deposit === 'number' ? event.repaid_deposit : Number(event.repaid_deposit);
            const prevRepaid = prevRepaidDeposits.get(permIdStr) || 0;
            const incrementalRepaid = repaid - prevRepaid;
            if (incrementalRepaid > 0) {
                networkSlashedAmountRepaid += incrementalRepaid;
            }
            prevRepaidDeposits.set(permIdStr, repaid);
        } else if (isSlashedByEcosystemGov) {
            ecosystemSlashEvents++;
            ecosystemSlashedAmount += incrementalSlashed;

            const repaid = typeof event.repaid_deposit === 'number' ? event.repaid_deposit : Number(event.repaid_deposit);
            const prevRepaid = prevRepaidDeposits.get(permIdStr) || 0;
            const incrementalRepaid = repaid - prevRepaid;
            if (incrementalRepaid > 0) {
                ecosystemSlashedAmountRepaid += incrementalRepaid;
            }
            prevRepaidDeposits.set(permIdStr, repaid);
        } else {
            const repaid = typeof event.repaid_deposit === 'number' ? event.repaid_deposit : Number(event.repaid_deposit);
            prevRepaidDeposits.set(permIdStr, repaid);
        }
    }

    return {
        ecosystem_slash_events: ecosystemSlashEvents,
        ecosystem_slashed_amount: ecosystemSlashedAmount,
        ecosystem_slashed_amount_repaid: ecosystemSlashedAmountRepaid,
        network_slash_events: networkSlashEvents,
        network_slashed_amount: networkSlashedAmount,
        network_slashed_amount_repaid: networkSlashedAmountRepaid,
    };
}

export async function calculateCredentialSchemaStats(
    schemaId: number,
    blockHeight?: number
): Promise<CredentialSchemaStats> {
    const now = new Date();
    const permissions = await getPermissionsForSchema(schemaId, blockHeight);
    const trController = await getSchemaController(schemaId, blockHeight);

    let totalWeight = 0;
    let totalIssued = 0;
    let totalVerified = 0;
    let ecosystemSlashEvents = 0;
    let ecosystemSlashedAmount = 0;
    let ecosystemSlashedAmountRepaid = 0;
    let networkSlashEvents = 0;
    let networkSlashedAmount = 0;
    let networkSlashedAmountRepaid = 0;
    const activeParticipants = new Set<number>();
    const permissionIds = new Set<number>();

    for (const perm of permissions) {
        const permId = perm.permission_id || perm.id;
        permissionIds.add(permId);

        const permState = calculatePermState(
            {
                repaid: perm.repaid,
                slashed: perm.slashed,
                revoked: perm.revoked,
                effective_from: perm.effective_from,
                effective_until: perm.effective_until,
                type: perm.type,
                vp_state: perm.vp_state,
                vp_exp: perm.vp_exp,
                validator_perm_id: perm.validator_perm_id,
            },
            now
        );

        if (permState === "ACTIVE") {
            activeParticipants.add(permId);
        }

        if (perm.weight != null) {
            const weightValue = typeof perm.weight === 'number' ? perm.weight : Number(perm.weight);
            totalWeight += weightValue;
        } else if (perm.deposit != null) {
            const depositValue = typeof perm.deposit === 'number' ? perm.deposit : Number(perm.deposit);
            totalWeight += depositValue;
        }
    }

    const { issued, verified } = await calculateIssuedVerifiedForSchema(schemaId, permissionIds, blockHeight);
    totalIssued += issued;
    totalVerified += verified;

    const slashStats = await calculateSlashStatsForSchema(schemaId, permissionIds, trController, blockHeight);
    ecosystemSlashEvents += slashStats.ecosystem_slash_events;
    ecosystemSlashedAmount += slashStats.ecosystem_slashed_amount;
    ecosystemSlashedAmountRepaid += slashStats.ecosystem_slashed_amount_repaid;
    networkSlashEvents += slashStats.network_slash_events;
    networkSlashedAmount += slashStats.network_slashed_amount;
    networkSlashedAmountRepaid += slashStats.network_slashed_amount_repaid;

    return {
        participants: activeParticipants.size,
        weight: totalWeight,
        issued: totalIssued,
        verified: totalVerified,
        ecosystem_slash_events: ecosystemSlashEvents,
        ecosystem_slashed_amount: ecosystemSlashedAmount,
        ecosystem_slashed_amount_repaid: ecosystemSlashedAmountRepaid,
        network_slash_events: networkSlashEvents,
        network_slashed_amount: networkSlashedAmount,
        network_slashed_amount_repaid: networkSlashedAmountRepaid,
    };
}
