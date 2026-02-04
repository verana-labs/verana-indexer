import knex from "../../common/utils/db_connection";
import { calculatePermState } from "../crawl-perm/perm_state_utils";

export interface CredentialSchemaStats {
    participants: number;
    weight: string;
    issued: number;
    verified: number;
    ecosystem_slash_events: number;
    ecosystem_slashed_amount: string;
    ecosystem_slashed_amount_repaid: string;
    network_slash_events: number;
    network_slashed_amount: string;
    network_slashed_amount_repaid: string;
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
        .where("id", Number(schema.tr_id))
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
        .where("schema_id", String(schemaId))
        .select("*");
}

export async function calculateIssuedVerifiedForSchema(
    schemaId: number,
    permissionIds: Set<string>,
    blockHeight?: number
): Promise<{ issued: bigint; verified: bigint }> {
    let totalIssued = BigInt(0);
    let totalVerified = BigInt(0);

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
                    if (entry.issuer_perm_id && permissionIds.has(String(entry.issuer_perm_id))) {
                        totalIssued += BigInt(1);
                    }
                    if (entry.verifier_perm_id && permissionIds.has(String(entry.verifier_perm_id))) {
                        totalVerified += BigInt(1);
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
                    if (entry.issuer_perm_id && permissionIds.has(String(entry.issuer_perm_id))) {
                        totalIssued += BigInt(1);
                    }
                    if (entry.verifier_perm_id && permissionIds.has(String(entry.verifier_perm_id))) {
                        totalVerified += BigInt(1);
                    }
                }
            }
        }
    }

    return { issued: totalIssued, verified: totalVerified };
}

export async function calculateSlashStatsForSchema(
    schemaId: number,
    permissionIds: Set<string>,
    trController: string | null,
    blockHeight?: number
): Promise<{
    ecosystem_slash_events: number;
    ecosystem_slashed_amount: bigint;
    ecosystem_slashed_amount_repaid: bigint;
    network_slash_events: number;
    network_slashed_amount: bigint;
    network_slashed_amount_repaid: bigint;
}> {
    let ecosystemSlashEvents = 0;
    let ecosystemSlashedAmount = BigInt(0);
    let ecosystemSlashedAmountRepaid = BigInt(0);
    let networkSlashEvents = 0;
    let networkSlashedAmount = BigInt(0);
    let networkSlashedAmountRepaid = BigInt(0);

    if (permissionIds.size === 0) {
        return {
            ecosystem_slash_events: 0,
            ecosystem_slashed_amount: BigInt(0),
            ecosystem_slashed_amount_repaid: BigInt(0),
            network_slash_events: 0,
            network_slashed_amount: BigInt(0),
            network_slashed_amount_repaid: BigInt(0),
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

    const prevSlashedDeposits = new Map<string, string>();
    const prevRepaidDeposits = new Map<string, string>();

    for (const event of slashEvents) {
        const permIdStr = String(event.permission_id);
        const prevSlashed = prevSlashedDeposits.get(permIdStr) || "0";
        const currentSlashed = event.slashed_deposit || "0";
        const incrementalSlashed = BigInt(currentSlashed) - BigInt(prevSlashed);

        if (incrementalSlashed <= 0) {
            prevSlashedDeposits.set(permIdStr, currentSlashed);
            const currentRepaid = event.repaid_deposit || "0";
            prevRepaidDeposits.set(permIdStr, currentRepaid);
            continue;
        }

        prevSlashedDeposits.set(permIdStr, currentSlashed);

        const isEcosystemPermission = event.type === "ECOSYSTEM";
        const isSlashedByEcosystemGov = trController && event.slashed_by === trController;

        if (isEcosystemPermission) {
            networkSlashEvents++;
            networkSlashedAmount += incrementalSlashed;

            const repaid = event.repaid_deposit || "0";
            const prevRepaid = prevRepaidDeposits.get(permIdStr) || "0";
            const incrementalRepaid = BigInt(repaid) - BigInt(prevRepaid);
            if (incrementalRepaid > 0) {
                networkSlashedAmountRepaid += incrementalRepaid;
            }
            prevRepaidDeposits.set(permIdStr, repaid);
        } else if (isSlashedByEcosystemGov) {
            ecosystemSlashEvents++;
            ecosystemSlashedAmount += incrementalSlashed;

            const repaid = event.repaid_deposit || "0";
            const prevRepaid = prevRepaidDeposits.get(permIdStr) || "0";
            const incrementalRepaid = BigInt(repaid) - BigInt(prevRepaid);
            if (incrementalRepaid > 0) {
                ecosystemSlashedAmountRepaid += incrementalRepaid;
            }
            prevRepaidDeposits.set(permIdStr, repaid);
        } else {
            const repaid = event.repaid_deposit || "0";
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

    let totalWeight = BigInt(0);
    let totalIssued = BigInt(0);
    let totalVerified = BigInt(0);
    let ecosystemSlashEvents = 0;
    let ecosystemSlashedAmount = BigInt(0);
    let ecosystemSlashedAmountRepaid = BigInt(0);
    let networkSlashEvents = 0;
    let networkSlashedAmount = BigInt(0);
    let networkSlashedAmountRepaid = BigInt(0);
    const activeParticipants = new Set<string>();
    const permissionIds = new Set<string>();

    for (const perm of permissions) {
        const permId = String(perm.permission_id || perm.id);
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

        if (perm.weight) {
            totalWeight += BigInt(perm.weight || "0");
        } else if (perm.deposit) {
            totalWeight += BigInt(perm.deposit || "0");
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

    // Convert BigInt to number for issued and verified (counts, not amounts)
    // Using Number() is safe here as credential counts are unlikely to exceed Number.MAX_SAFE_INTEGER
    const issuedNumber = Number(totalIssued);
    const verifiedNumber = Number(totalVerified);
    
    if (issuedNumber > Number.MAX_SAFE_INTEGER || verifiedNumber > Number.MAX_SAFE_INTEGER) {
        console.warn(`Warning: issued (${totalIssued}) or verified (${totalVerified}) exceeds safe integer range for schema ${schemaId}`);
    }

    return {
        participants: activeParticipants.size,
        weight: totalWeight.toString(),
        issued: issuedNumber,
        verified: verifiedNumber,
        ecosystem_slash_events: ecosystemSlashEvents,
        ecosystem_slashed_amount: ecosystemSlashedAmount.toString(),
        ecosystem_slashed_amount_repaid: ecosystemSlashedAmountRepaid.toString(),
        network_slash_events: networkSlashEvents,
        network_slashed_amount: networkSlashedAmount.toString(),
        network_slashed_amount_repaid: networkSlashedAmountRepaid.toString(),
    };
}
