import knex from "../../common/utils/db_connection";
import { calculatePermState } from "../crawl-perm/perm_state_utils";

const IS_PG_CLIENT = String((knex as any)?.client?.config?.client || "").includes("pg");
const MAX_SESSION_COUNTER_CACHE_ENTRIES = 24;

type SessionCounters = {
    issuer: Map<number, number>;
    verifier: Map<number, number>;
};

const sessionCountersCache = new Map<string, Promise<SessionCounters>>();

function getSessionCounterCacheKey(blockHeight?: number): string {
    return typeof blockHeight === "number" ? `h:${blockHeight}` : "live";
}

export async function getPermissionSessionCounters(blockHeight?: number): Promise<SessionCounters> {
    const cacheKey = getSessionCounterCacheKey(blockHeight);
    const cached = sessionCountersCache.get(cacheKey);
    if (cached) return cached;
    if (sessionCountersCache.size >= MAX_SESSION_COUNTER_CACHE_ENTRIES) {
        sessionCountersCache.clear();
    }

    const loadPromise = (async (): Promise<SessionCounters> => {
        let sessionRows: Array<{ authz: any }> = [];

        if (typeof blockHeight === "number") {
            if (IS_PG_CLIENT) {
                sessionRows = await knex("permission_session_history as psh")
                    .distinctOn("psh.session_id")
                    .select("psh.authz")
                    .where("psh.height", "<=", blockHeight)
                    .orderBy("psh.session_id", "asc")
                    .orderBy("psh.height", "desc")
                    .orderBy("psh.created_at", "desc")
                    .orderBy("psh.id", "desc");
            } else {
                const ranked = knex("permission_session_history as psh")
                    .select(
                        "psh.authz",
                        knex.raw("ROW_NUMBER() OVER (PARTITION BY psh.session_id ORDER BY psh.height DESC, psh.created_at DESC, psh.id DESC) as rn")
                    )
                    .where("psh.height", "<=", blockHeight)
                    .as("ranked");
                sessionRows = await knex.from(ranked).select("authz").where("rn", 1);
            }
        } else {
            sessionRows = await knex("permission_sessions").select("authz");
        }

        const issuer = new Map<number, number>();
        const verifier = new Map<number, number>();

        for (const session of sessionRows) {
            const authz = typeof session.authz === "string" ? JSON.parse(session.authz) : session.authz;
            if (!Array.isArray(authz)) continue;
            for (const entry of authz) {
                if (entry?.issuer_perm_id !== undefined && entry?.issuer_perm_id !== null) {
                    const issuerPermId = Number(entry.issuer_perm_id);
                    if (Number.isFinite(issuerPermId) && issuerPermId > 0) {
                        issuer.set(issuerPermId, (issuer.get(issuerPermId) || 0) + 1);
                    }
                }
                if (entry?.verifier_perm_id !== undefined && entry?.verifier_perm_id !== null) {
                    const verifierPermId = Number(entry.verifier_perm_id);
                    if (Number.isFinite(verifierPermId) && verifierPermId > 0) {
                        verifier.set(verifierPermId, (verifier.get(verifierPermId) || 0) + 1);
                    }
                }
            }
        }

        return { issuer, verifier };
    })();

    sessionCountersCache.set(cacheKey, loadPromise);
    return loadPromise;
}

export interface CredentialSchemaStats {
    participants: number;
    participants_ecosystem: number;
    participants_issuer_grantor: number;
    participants_issuer: number;
    participants_verifier_grantor: number;
    participants_verifier: number;
    participants_holder: number;
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
        if (IS_PG_CLIENT) {
            return await knex("permission_history as ph")
                .distinctOn("ph.permission_id")
                .select("ph.*")
                .where("ph.schema_id", Number(schemaId))
                .where("ph.height", "<=", blockHeight)
                .orderBy("ph.permission_id", "asc")
                .orderBy("ph.height", "desc")
                .orderBy("ph.created_at", "desc")
                .orderBy("ph.id", "desc");
        }
        const ranked = knex("permission_history as ph")
            .select(
                "ph.*",
                knex.raw("ROW_NUMBER() OVER (PARTITION BY ph.permission_id ORDER BY ph.height DESC, ph.created_at DESC, ph.id DESC) as rn")
            )
            .where("ph.schema_id", Number(schemaId))
            .where("ph.height", "<=", blockHeight)
            .as("ranked");
        return await knex.from(ranked).select("*").where("rn", 1);
    }
    return await knex("permissions")
        .where("schema_id", Number(schemaId))
        .select("*");
}

export async function calculateIssuedVerifiedForSchema(
    _schemaId: number,
    permissionIds: Set<number>,
    blockHeight?: number
): Promise<{ issued: number; verified: number }> {
    let totalIssued = 0;
    let totalVerified = 0;

    if (permissionIds.size === 0) {
        return { issued: 0, verified: 0 };
    }

    const counters = await getPermissionSessionCounters(blockHeight);
    for (const permissionId of permissionIds) {
        totalIssued += counters.issuer.get(Number(permissionId)) || 0;
        totalVerified += counters.verifier.get(Number(permissionId)) || 0;
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
    const batch = await calculateCredentialSchemaStatsBatch([schemaId], blockHeight);
    return batch.get(Number(schemaId)) || {
        participants: 0,
        participants_ecosystem: 0,
        participants_issuer_grantor: 0,
        participants_issuer: 0,
        participants_verifier_grantor: 0,
        participants_verifier: 0,
        participants_holder: 0,
        weight: 0,
        issued: 0,
        verified: 0,
        ecosystem_slash_events: 0,
        ecosystem_slashed_amount: 0,
        ecosystem_slashed_amount_repaid: 0,
        network_slash_events: 0,
        network_slashed_amount: 0,
        network_slashed_amount_repaid: 0,
    };
}

export async function calculateCredentialSchemaStatsBatch(
    schemaIdsInput: number[],
    blockHeight?: number
): Promise<Map<number, CredentialSchemaStats>> {
    const schemaIds = Array.from(new Set(schemaIdsInput.map((id) => Number(id)).filter((id) => Number.isFinite(id) && id > 0)));
    const result = new Map<number, CredentialSchemaStats>();
    if (schemaIds.length === 0) return result;

    const now = new Date();

    let schemaRows: any[] = [];
    if (typeof blockHeight === "number") {
        if (IS_PG_CLIENT) {
            schemaRows = await knex("credential_schema_history as csh")
                .distinctOn("csh.credential_schema_id")
                .select("csh.credential_schema_id", "csh.tr_id")
                .whereIn("csh.credential_schema_id", schemaIds)
                .where("csh.height", "<=", blockHeight)
                .orderBy("csh.credential_schema_id", "asc")
                .orderBy("csh.height", "desc")
                .orderBy("csh.created_at", "desc")
                .orderBy("csh.id", "desc");
        } else {
            const rankedSchemas = knex("credential_schema_history as csh")
                .select(
                    "csh.credential_schema_id",
                    "csh.tr_id",
                    knex.raw("ROW_NUMBER() OVER (PARTITION BY csh.credential_schema_id ORDER BY csh.height DESC, csh.created_at DESC, csh.id DESC) as rn")
                )
                .whereIn("csh.credential_schema_id", schemaIds)
                .where("csh.height", "<=", blockHeight)
                .as("ranked");
            schemaRows = await knex.from(rankedSchemas).select("credential_schema_id", "tr_id").where("rn", 1);
        }
    } else {
        schemaRows = await knex("credential_schemas")
            .select("id as credential_schema_id", "tr_id")
            .whereIn("id", schemaIds);
    }

    const schemaToTr = new Map<number, number>();
    const trIds: number[] = [];
    for (const row of schemaRows) {
        const schemaId = Number(row.credential_schema_id);
        const trId = Number(row.tr_id);
        if (!Number.isFinite(schemaId) || !Number.isFinite(trId)) continue;
        schemaToTr.set(schemaId, trId);
        trIds.push(trId);
    }

    const uniqueTrIds = Array.from(new Set(trIds));
    const trControllerMap = new Map<number, string | null>();
    if (uniqueTrIds.length > 0) {
        if (typeof blockHeight === "number") {
            if (IS_PG_CLIENT) {
                const trRows = await knex("trust_registry_history as trh")
                    .distinctOn("trh.tr_id")
                    .select("trh.tr_id", "trh.controller")
                    .whereIn("trh.tr_id", uniqueTrIds)
                    .where("trh.height", "<=", blockHeight)
                    .orderBy("trh.tr_id", "asc")
                    .orderBy("trh.height", "desc")
                    .orderBy("trh.created_at", "desc")
                    .orderBy("trh.id", "desc");
                for (const row of trRows) trControllerMap.set(Number(row.tr_id), row.controller || null);
            } else {
                const rankedTrs = knex("trust_registry_history as trh")
                    .select(
                        "trh.tr_id",
                        "trh.controller",
                        knex.raw("ROW_NUMBER() OVER (PARTITION BY trh.tr_id ORDER BY trh.height DESC, trh.created_at DESC, trh.id DESC) as rn")
                    )
                    .whereIn("trh.tr_id", uniqueTrIds)
                    .where("trh.height", "<=", blockHeight)
                    .as("ranked");
                const trRows = await knex.from(rankedTrs).select("tr_id", "controller").where("rn", 1);
                for (const row of trRows) trControllerMap.set(Number(row.tr_id), row.controller || null);
            }
        } else {
            const trRows = await knex("trust_registry").select("id", "controller").whereIn("id", uniqueTrIds);
            for (const row of trRows) trControllerMap.set(Number(row.id), row.controller || null);
        }
    }

    let permissions: any[] = [];
    if (typeof blockHeight === "number") {
        if (IS_PG_CLIENT) {
            permissions = await knex("permission_history as ph")
                .distinctOn("ph.permission_id")
                .select("ph.*")
                .whereIn("ph.schema_id", schemaIds)
                .where("ph.height", "<=", blockHeight)
                .orderBy("ph.permission_id", "asc")
                .orderBy("ph.height", "desc")
                .orderBy("ph.created_at", "desc")
                .orderBy("ph.id", "desc");
        } else {
            const rankedPerms = knex("permission_history as ph")
                .select(
                    "ph.*",
                    knex.raw("ROW_NUMBER() OVER (PARTITION BY ph.permission_id ORDER BY ph.height DESC, ph.created_at DESC, ph.id DESC) as rn")
                )
                .whereIn("ph.schema_id", schemaIds)
                .where("ph.height", "<=", blockHeight)
                .as("ranked");
            permissions = await knex.from(rankedPerms).select("*").where("rn", 1);
        }
    } else {
        permissions = await knex("permissions").whereIn("schema_id", schemaIds).select("*");
    }

    const counters = await getPermissionSessionCounters(blockHeight);
    // Key = numeric schema id, value = unique participant account identifier (address string).
    const activeParticipantsBySchema = new Map<number, Set<string>>();
    const activeParticipantsEcosystemBySchema = new Map<number, Set<string>>();
    const activeParticipantsIssuerGrantorBySchema = new Map<number, Set<string>>();
    const activeParticipantsIssuerBySchema = new Map<number, Set<string>>();
    const activeParticipantsVerifierGrantorBySchema = new Map<number, Set<string>>();
    const activeParticipantsVerifierBySchema = new Map<number, Set<string>>();
    const activeParticipantsHolderBySchema = new Map<number, Set<string>>();
    const permissionIdsBySchema = new Map<number, Set<number>>();

    for (const schemaId of schemaIds) {
        result.set(schemaId, {
            participants: 0,
            participants_ecosystem: 0,
            participants_issuer_grantor: 0,
            participants_issuer: 0,
            participants_verifier_grantor: 0,
            participants_verifier: 0,
            participants_holder: 0,
            weight: 0,
            issued: 0,
            verified: 0,
            ecosystem_slash_events: 0,
            ecosystem_slashed_amount: 0,
            ecosystem_slashed_amount_repaid: 0,
            network_slash_events: 0,
            network_slashed_amount: 0,
            network_slashed_amount_repaid: 0,
        });
        activeParticipantsBySchema.set(schemaId, new Set<string>());
        activeParticipantsEcosystemBySchema.set(schemaId, new Set<string>());
        activeParticipantsIssuerGrantorBySchema.set(schemaId, new Set<string>());
        activeParticipantsIssuerBySchema.set(schemaId, new Set<string>());
        activeParticipantsVerifierGrantorBySchema.set(schemaId, new Set<string>());
        activeParticipantsVerifierBySchema.set(schemaId, new Set<string>());
        activeParticipantsHolderBySchema.set(schemaId, new Set<string>());
        permissionIdsBySchema.set(schemaId, new Set<number>());
    }

    for (const perm of permissions) {
        const schemaId = Number(perm.schema_id);
        if (!result.has(schemaId)) continue;

        const permId = Number(perm.permission_id || perm.id);
        if (!Number.isFinite(permId) || permId <= 0) continue;

        permissionIdsBySchema.get(schemaId)?.add(permId);

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

        const grantee = perm.grantee === null || perm.grantee === undefined
            ? ""
            : String(perm.grantee).trim();
        if (permState === "ACTIVE" && grantee) {
            activeParticipantsBySchema.get(schemaId)?.add(grantee);
            if (perm.type === "ECOSYSTEM") activeParticipantsEcosystemBySchema.get(schemaId)?.add(grantee);
            if (perm.type === "ISSUER_GRANTOR") activeParticipantsIssuerGrantorBySchema.get(schemaId)?.add(grantee);
            if (perm.type === "ISSUER") activeParticipantsIssuerBySchema.get(schemaId)?.add(grantee);
            if (perm.type === "VERIFIER_GRANTOR") activeParticipantsVerifierGrantorBySchema.get(schemaId)?.add(grantee);
            if (perm.type === "VERIFIER") activeParticipantsVerifierBySchema.get(schemaId)?.add(grantee);
            if (perm.type === "HOLDER") activeParticipantsHolderBySchema.get(schemaId)?.add(grantee);
        }

        const stats = result.get(schemaId)!;
        if (perm.weight != null) {
            stats.weight += typeof perm.weight === "number" ? perm.weight : Number(perm.weight || 0);
        } else if (perm.deposit != null) {
            stats.weight += typeof perm.deposit === "number" ? perm.deposit : Number(perm.deposit || 0);
        }

        stats.issued += counters.issuer.get(permId) || 0;
        stats.verified += counters.verifier.get(permId) || 0;
    }

    const slashEvents = await knex("permission_history")
        .select("schema_id", "permission_id", "slashed_by", "type", "slashed_deposit", "repaid_deposit", "height", "created_at", "id")
        .whereIn("schema_id", schemaIds)
        .where("event_type", "SLASH_PERMISSION_TRUST_DEPOSIT")
        .modify((qb) => {
            if (typeof blockHeight === "number") qb.where("height", "<=", blockHeight);
        })
        .orderBy("permission_id", "asc")
        .orderBy("height", "asc")
        .orderBy("created_at", "asc")
        .orderBy("id", "asc");

    const prevSlashedDeposits = new Map<number, number>();
    const prevRepaidDeposits = new Map<number, number>();

    for (const event of slashEvents) {
        const schemaId = Number(event.schema_id);
        const permId = Number(event.permission_id);
        if (!result.has(schemaId) || !Number.isFinite(permId)) continue;

        if (!(permissionIdsBySchema.get(schemaId)?.has(permId))) continue;

        const prevSlashed = prevSlashedDeposits.get(permId) || 0;
        const currentSlashed = typeof event.slashed_deposit === "number" ? event.slashed_deposit : Number(event.slashed_deposit || 0);
        const incrementalSlashed = currentSlashed - prevSlashed;
        prevSlashedDeposits.set(permId, currentSlashed);

        const repaid = typeof event.repaid_deposit === "number" ? event.repaid_deposit : Number(event.repaid_deposit || 0);
        const prevRepaid = prevRepaidDeposits.get(permId) || 0;
        const incrementalRepaid = repaid - prevRepaid;
        prevRepaidDeposits.set(permId, repaid);

        if (incrementalSlashed <= 0) continue;

        const stats = result.get(schemaId)!;
        const trId = schemaToTr.get(schemaId);
        const trController = trId !== undefined ? trControllerMap.get(trId) : null;

        if (event.type === "ECOSYSTEM") {
            stats.network_slash_events += 1;
            stats.network_slashed_amount += incrementalSlashed;
            if (incrementalRepaid > 0) stats.network_slashed_amount_repaid += incrementalRepaid;
        } else if (trController && event.slashed_by === trController) {
            stats.ecosystem_slash_events += 1;
            stats.ecosystem_slashed_amount += incrementalSlashed;
            if (incrementalRepaid > 0) stats.ecosystem_slashed_amount_repaid += incrementalRepaid;
        }
    }

    for (const [schemaId] of activeParticipantsBySchema.entries()) {
        const stats = result.get(schemaId);
        if (stats) {
            stats.participants_ecosystem = activeParticipantsEcosystemBySchema.get(schemaId)?.size || 0;
            stats.participants_issuer_grantor = activeParticipantsIssuerGrantorBySchema.get(schemaId)?.size || 0;
            stats.participants_issuer = activeParticipantsIssuerBySchema.get(schemaId)?.size || 0;
            stats.participants_verifier_grantor = activeParticipantsVerifierGrantorBySchema.get(schemaId)?.size || 0;
            stats.participants_verifier = activeParticipantsVerifierBySchema.get(schemaId)?.size || 0;
            stats.participants_holder = activeParticipantsHolderBySchema.get(schemaId)?.size || 0;
            stats.participants = activeParticipantsBySchema.get(schemaId)?.size || 0;
        }
    }

    return result;
}
