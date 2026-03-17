import knex from "../../common/utils/db_connection";
import { TrustRegistry } from "../../models/trust_registry";
import { TR_STATS_FIELDS, statsToUpdateObject } from "../../common/utils/stats_fields";
import { calculateCredentialSchemaStatsBatch, getPermissionSessionCounters } from "../crawl-cs/cs_stats";

const IS_PG_CLIENT = String((knex as any)?.client?.config?.client || "").includes("pg");

export interface TrustRegistryStats {
    participants: number;
    participants_ecosystem: number;
    participants_issuer_grantor: number;
    participants_issuer: number;
    participants_verifier_grantor: number;
    participants_verifier: number;
    participants_holder: number;
    active_schemas: number;
    archived_schemas: number;
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

export { TR_STATS_FIELDS };

export function trustRegistryStatsToUpdateObject(stats: TrustRegistryStats | null | undefined): Record<string, number> {
    return statsToUpdateObject((stats ?? null) as Record<string, unknown> | null, TR_STATS_FIELDS);
}

export async function getSchemasForTrustRegistry(trId: number, blockHeight?: number): Promise<any[]> {
    if (typeof blockHeight === "number") {
        if (IS_PG_CLIENT) {
            return await knex("credential_schema_history as csh")
                .distinctOn("csh.credential_schema_id")
                .select("csh.*")
                .where("csh.tr_id", String(trId))
                .where("csh.height", "<=", blockHeight)
                .orderBy("csh.credential_schema_id", "asc")
                .orderBy("csh.height", "desc")
                .orderBy("csh.created_at", "desc")
                .orderBy("csh.id", "desc");
        }
        const ranked = knex("credential_schema_history as csh")
            .select(
                "csh.*",
                knex.raw("ROW_NUMBER() OVER (PARTITION BY csh.credential_schema_id ORDER BY csh.height DESC, csh.created_at DESC, csh.id DESC) as rn")
            )
            .where("csh.tr_id", String(trId))
            .where("csh.height", "<=", blockHeight)
            .as("ranked");
        return await knex.from(ranked).select("*").where("rn", 1);
    }
    return await knex("credential_schemas")
        .where("tr_id", String(trId))
        .select("*");
}

export async function getTrustRegistryController(trId: number, blockHeight?: number): Promise<string | null> {
    if (typeof blockHeight === "number") {
        const trHistory = await knex("trust_registry_history")
            .where("tr_id", trId)
            .where("height", "<=", blockHeight)
            .orderBy("height", "desc")
            .orderBy("created_at", "desc")
            .first();
        return trHistory?.controller || null;
    }
    const tr = await TrustRegistry.query().findById(trId);
    return tr?.controller || null;
}

export async function getPermissionsForSchema(schemaId: number, blockHeight?: number): Promise<any[]> {
    if (typeof blockHeight === "number") {
        if (IS_PG_CLIENT) {
            return await knex("permission_history as ph")
                .distinctOn("ph.permission_id")
                .select("ph.*")
                .where("ph.schema_id", schemaId)
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
            .where("ph.schema_id", schemaId)
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
            .whereRaw("schema_id = ?", [schemaId])
            .where("event_type", "SLASH_PERMISSION_TRUST_DEPOSIT")
            .where("height", "<=", blockHeight)
            .select("permission_id", "slashed_by", "type", "slashed_deposit", "repaid_deposit", "height", "created_at")
            .orderBy("permission_id", "asc")
            .orderBy("height", "asc")
            .orderBy("created_at", "asc");
    } else {
        slashEvents = await knex("permission_history")
            .whereIn("permission_id", permissionIdArray)
            .whereRaw("schema_id = ?", [schemaId])
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

export async function calculateTrustRegistryStats(
    trId: number,
    blockHeight?: number
): Promise<TrustRegistryStats> {
    const batch = await calculateTrustRegistryStatsBatch([trId], blockHeight);
    return batch.get(Number(trId)) || {
        participants: 0,
        participants_ecosystem: 0,
        participants_issuer_grantor: 0,
        participants_issuer: 0,
        participants_verifier_grantor: 0,
        participants_verifier: 0,
        participants_holder: 0,
        active_schemas: 0,
        archived_schemas: 0,
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

export async function calculateTrustRegistryStatsBatch(
    trIdsInput: number[],
    blockHeight?: number
): Promise<Map<number, TrustRegistryStats>> {
    const trIds = Array.from(new Set(trIdsInput.map((id) => Number(id)).filter((id) => Number.isFinite(id) && id > 0)));
    const result = new Map<number, TrustRegistryStats>();
    if (trIds.length === 0) return result;

    let schemas: any[] = [];
    if (typeof blockHeight === "number") {
        if (IS_PG_CLIENT) {
            schemas = await knex("credential_schema_history as csh")
                .distinctOn("csh.credential_schema_id")
                .select("csh.credential_schema_id", "csh.tr_id", "csh.archived")
                .whereIn("csh.tr_id", trIds)
                .where("csh.height", "<=", blockHeight)
                .orderBy("csh.credential_schema_id", "asc")
                .orderBy("csh.height", "desc")
                .orderBy("csh.created_at", "desc")
                .orderBy("csh.id", "desc");
        } else {
            const ranked = knex("credential_schema_history as csh")
                .select(
                    "csh.credential_schema_id",
                    "csh.tr_id",
                    "csh.archived",
                    knex.raw("ROW_NUMBER() OVER (PARTITION BY csh.credential_schema_id ORDER BY csh.height DESC, csh.created_at DESC, csh.id DESC) as rn")
                )
                .whereIn("csh.tr_id", trIds)
                .where("csh.height", "<=", blockHeight)
                .as("ranked");
            schemas = await knex.from(ranked).select("credential_schema_id", "tr_id", "archived").where("rn", 1);
        }
    } else {
        schemas = await knex("credential_schemas").select("id as credential_schema_id", "tr_id", "archived").whereIn("tr_id", trIds);
    }

    for (const trId of trIds) {
        result.set(trId, {
            participants: 0,
            participants_ecosystem: 0,
            participants_issuer_grantor: 0,
            participants_issuer: 0,
            participants_verifier_grantor: 0,
            participants_verifier: 0,
            participants_holder: 0,
            active_schemas: 0,
            archived_schemas: 0,
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
    }

    const schemaIds: number[] = [];
    const schemaToTr = new Map<number, number>();
    for (const schema of schemas) {
        const trId = Number(schema.tr_id);
        const schemaId = Number(schema.credential_schema_id);
        if (!Number.isFinite(trId) || !Number.isFinite(schemaId)) continue;
        schemaToTr.set(schemaId, trId);
        schemaIds.push(schemaId);

        const trStats = result.get(trId);
        if (!trStats) continue;
        if (schema.archived !== null && schema.archived !== undefined) trStats.archived_schemas += 1;
        else trStats.active_schemas += 1;
    }

    const schemaStats = await calculateCredentialSchemaStatsBatch(schemaIds, blockHeight);
    for (const [schemaId, stats] of schemaStats.entries()) {
        const trId = schemaToTr.get(schemaId);
        if (!trId) continue;
        const trStats = result.get(trId);
        if (!trStats) continue;

        trStats.participants += Number(stats.participants || 0);
        trStats.participants_ecosystem += Number(stats.participants_ecosystem || 0);
        trStats.participants_issuer_grantor += Number(stats.participants_issuer_grantor || 0);
        trStats.participants_issuer += Number(stats.participants_issuer || 0);
        trStats.participants_verifier_grantor += Number(stats.participants_verifier_grantor || 0);
        trStats.participants_verifier += Number(stats.participants_verifier || 0);
        trStats.participants_holder += Number(stats.participants_holder || 0);
        trStats.weight += Number(stats.weight || 0);
        trStats.issued += Number(stats.issued || 0);
        trStats.verified += Number(stats.verified || 0);
        trStats.ecosystem_slash_events += Number(stats.ecosystem_slash_events || 0);
        trStats.ecosystem_slashed_amount += Number(stats.ecosystem_slashed_amount || 0);
        trStats.ecosystem_slashed_amount_repaid += Number(stats.ecosystem_slashed_amount_repaid || 0);
        trStats.network_slash_events += Number(stats.network_slash_events || 0);
        trStats.network_slashed_amount += Number(stats.network_slashed_amount || 0);
        trStats.network_slashed_amount_repaid += Number(stats.network_slashed_amount_repaid || 0);
    }

    return result;
}
