/* eslint-disable @typescript-eslint/no-explicit-any */

import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BaseService from "../../base/base.service";
import { ModulesParamsNamesTypes, MODULE_DISPLAY_NAMES, SERVICE } from "../../common";
import ApiResponder from "../../common/utils/apiResponse";
import { TrustRegistry } from "../../models/trust_registry";
import knex from "../../common/utils/db_connection";
import { applyOrdering, validateSortParameter, sortByStandardAttributes } from "../../common/utils/query_ordering";
import { calculateTrustRegistryStats } from "./tr_stats";

@Service({
    name: SERVICE.V1.TrustRegistryDatabaseService.key,
    version: 1
})
export default class TrustRegistryDatabaseService extends BaseService {
    public constructor(public broker: ServiceBroker) {
        super(broker);
    }
    @Action()
    public async getTrustRegistry(ctx: Context<{
        tr_id: number;
        active_gf_only?: string | boolean;
        preferred_language?: string;
    }>) {
        try {
            // eslint-disable-next-line @typescript-eslint/naming-convention
            const { tr_id, preferred_language } = ctx.params;
            const activeGfOnly =
                String(ctx.params.active_gf_only).toLowerCase() === "true";
            const blockHeight = (ctx.meta as any)?.blockHeight;

            // If AtBlockHeight is provided, query historical state
            if (typeof blockHeight === "number") {
                const trHistory = await knex("trust_registry_history")
                    .where({ tr_id })
                    .where("height", "<=", blockHeight)
                    .orderBy("height", "desc")
                    .orderBy("created_at", "desc")
                    .first();

                if (!trHistory) {
                    return ApiResponder.error(ctx, `TrustRegistry with id ${tr_id} not found`, 404);
                }

                // Get governance framework versions at this block height
                const gfvHistory = await knex("governance_framework_version_history")
                    .where({ tr_id })
                    .where("height", "<=", blockHeight)
                    .orderBy("height", "desc")
                    .orderBy("created_at", "desc");

                // Get unique versions (latest state for each version at block height)
                const versionMap = new Map<number, any>();
                for (const gfv of gfvHistory) {
                    if (!versionMap.has(gfv.version)) {
                        versionMap.set(gfv.version, gfv);
                    }
                }

                const versions = Array.from(versionMap.values()).map(async (gfv: any) => {
                    // Get documents for this version at block height
                    const gfdHistory = await knex("governance_framework_document_history")
                        .where({ gfv_id: gfv.id, tr_id })
                        .where("height", "<=", blockHeight)
                        .orderBy("height", "desc")
                        .orderBy("created_at", "desc");

                    // Get unique documents (latest state for each document ID at block height)
                    // IMPORTANT: Deduplicate by gfd_id, not by url+language, because multiple documents
                    // can have the same URL and language but different IDs
                    const docMap = new Map<number, any>();
                    for (const gfd of gfdHistory) {
                        // Validate that gfd_id exists
                        if (!gfd.gfd_id) {
                            this.logger.error(`❌ CRITICAL: Document history record missing gfd_id! gfv_id=${gfd.gfv_id}, tr_id=${tr_id}, url=${gfd.url}, height=${blockHeight}`);
                            console.error("FATAL ID MISMATCH: Document history record missing gfd_id!", {
                                gfv_id: gfd.gfv_id,
                                tr_id,
                                url: gfd.url,
                                height: blockHeight,
                                record: gfd
                            });
                        }
                        // Validate gfd_id doesn't equal gfv_id (which would indicate a bug)
                        if (gfd.gfd_id === gfd.gfv_id) {
                            this.logger.error(`❌ CRITICAL: Document ID equals GFV ID! gfd_id=${gfd.gfd_id}, gfv_id=${gfd.gfv_id}, tr_id=${tr_id}, url=${gfd.url}, height=${blockHeight}`);
                            console.error("FATAL ID MISMATCH: Document ID equals GFV ID!", {
                                gfd_id: gfd.gfd_id,
                                gfv_id: gfd.gfv_id,
                                tr_id,
                                url: gfd.url,
                                height: blockHeight,
                                record: gfd
                            });
                        }
                        // Keep the latest state of each document (by gfd_id)
                        if (!docMap.has(gfd.gfd_id)) {
                            docMap.set(gfd.gfd_id, gfd);
                        }
                    }

                    const documents = Array.from(docMap.values()).map((gfd: any) => ({
                        id: gfd.gfd_id,
                        created: gfd.created,
                        language: gfd.language,
                        url: gfd.url,
                        digest_sri: gfd.digest_sri,
                    }));

                    return {
                        id: gfv.id,
                        tr_id: gfv.tr_id,
                        created: gfv.created,
                        version: gfv.version,
                        active_since: gfv.active_since,
                        documents,
                    };
                });

                const resolvedVersions = await Promise.all(versions);

                let filteredVersions = resolvedVersions;
                if (activeGfOnly) {
                    filteredVersions = resolvedVersions
                        .sort(
                            (a, b) =>
                                new Date(b.active_since).getTime() - new Date(a.active_since).getTime()
                        )
                        .slice(0, 1);
                }

                if (preferred_language) {
                    for (const v of filteredVersions) {
                        v.documents =
                            v.documents?.filter((d) => d.language === preferred_language) ?? [];
                    }
                }

                const stats = await calculateTrustRegistryStats(trHistory.tr_id, blockHeight);

                const trustRegistry = {
                    id: trHistory.tr_id,
                    did: trHistory.did,
                    controller: trHistory.controller,
                    created: trHistory.created,
                    modified: trHistory.modified,
                    archived: trHistory.archived,
                    deposit: trHistory.deposit,
                    aka: trHistory.aka,
                    language: trHistory.language,
                    active_version: trHistory.active_version,
                    versions: filteredVersions,
                    participants: stats.participants,
                    active_schemas: stats.active_schemas,
                    archived_schemas: stats.archived_schemas,
                    weight: stats.weight,
                    issued: stats.issued,
                    verified: stats.verified,
                    ecosystem_slash_events: stats.ecosystem_slash_events,
                    ecosystem_slashed_amount: stats.ecosystem_slashed_amount,
                    ecosystem_slashed_amount_repaid: stats.ecosystem_slashed_amount_repaid,
                    network_slash_events: stats.network_slash_events,
                    network_slashed_amount: stats.network_slashed_amount,
                    network_slashed_amount_repaid: stats.network_slashed_amount_repaid,
                };

                return ApiResponder.success(ctx, { trust_registry: trustRegistry }, 200);
            }

            // Otherwise, return latest state
            const registry = await TrustRegistry.query()
                .findById(tr_id)
                .withGraphFetched("governanceFrameworkVersions.documents");

            if (!registry) {
                return ApiResponder.error(ctx, `TrustRegistry with id ${tr_id} not found`, 404);
            }

            const plain = registry.toJSON();
            let versions = plain.governanceFrameworkVersions ?? [];

            if (activeGfOnly) {
                versions = versions
                    .sort(
                        (a, b) => {
                            if (!a.active_since && !b.active_since) return 0;
                            if (!a.active_since) return 1;
                            if (!b.active_since) return -1;
                            return new Date(b.active_since).getTime() - new Date(a.active_since).getTime();
                        }
                    )
                    .slice(0, 1);
            }

            if (preferred_language) {
                for (const v of versions) {
                    v.documents =
                        v.documents?.filter((d) => d.language === preferred_language) ?? [];
                }
            }
            delete plain.governanceFrameworkVersions;
            delete (plain as any).height;

            const stats = await calculateTrustRegistryStats(tr_id);

            return ApiResponder.success(ctx, {
                trust_registry: {
                    ...plain,
                    versions,
                    participants: stats.participants,
                    active_schemas: stats.active_schemas,
                    archived_schemas: stats.archived_schemas,
                    weight: stats.weight,
                    issued: stats.issued,
                    verified: stats.verified,
                    ecosystem_slash_events: stats.ecosystem_slash_events,
                    ecosystem_slashed_amount: stats.ecosystem_slashed_amount,
                    ecosystem_slashed_amount_repaid: stats.ecosystem_slashed_amount_repaid,
                    network_slash_events: stats.network_slash_events,
                    network_slashed_amount: stats.network_slashed_amount,
                    network_slashed_amount_repaid: stats.network_slashed_amount_repaid,
                },
            }, 200);
        } catch (err: any) {
            return ApiResponder.error(ctx, err.message, 500);
        }
    }

    @Action()
    public async listTrustRegistries(ctx: Context<{
        controller?: string;
        modified_after?: string;
        only_active?: string | boolean;
        active_gf_only?: string | boolean;
        preferred_language?: string;
        response_max_size?: number;
        sort?: string;
        min_active_schemas?: number;
        max_active_schemas?: number;
        min_participants?: number;
        max_participants?: number;
        min_weight?: string;
        max_weight?: string;
        min_issued?: string;
        max_issued?: string;
        min_verified?: string;
        max_verified?: string;
        min_ecosystem_slash_events?: number;
        max_ecosystem_slash_events?: number;
        min_network_slash_events?: number;
        max_network_slash_events?: number;
    }>) {
        try {
            const {
                controller,
                modified_after: modifiedAfter,
                preferred_language: preferredLanguage,
                only_active: onlyActiveRaw,
                response_max_size: responseMaxSizeRaw,
                sort,
                min_active_schemas: minActiveSchemas,
                max_active_schemas: maxActiveSchemas,
                min_participants: minParticipants,
                max_participants: maxParticipants,
                min_weight: minWeight,
                max_weight: maxWeight,
                min_issued: minIssued,
                max_issued: maxIssued,
                min_verified: minVerified,
                max_verified: maxVerified,
                min_ecosystem_slash_events: minEcosystemSlashEvents,
                max_ecosystem_slash_events: maxEcosystemSlashEvents,
                min_network_slash_events: minNetworkSlashEvents,
                max_network_slash_events: maxNetworkSlashEvents,
            } = ctx.params;

            try {
                validateSortParameter(sort);
            } catch (err: any) {
                return ApiResponder.error(ctx, err.message, 400);
            }

            const hasOnlyActive = typeof onlyActiveRaw !== "undefined";
            const onlyActive =
                String(onlyActiveRaw).toLowerCase() === "true";
            const activeGfOnly =
                String(ctx.params.active_gf_only).toLowerCase() === "true";
            const blockHeight = (ctx.meta as any)?.blockHeight;

            const responseMaxSize =
                !responseMaxSizeRaw ? 64 : Math.min(Math.max(responseMaxSizeRaw, 1), 1024);

            if (responseMaxSizeRaw && (responseMaxSizeRaw < 1 || responseMaxSizeRaw > 1024)) {
                return ApiResponder.error(ctx, "response_max_size must be between 1 and 1024", 400);
            }

            // If AtBlockHeight is provided, query historical state
            if (typeof blockHeight === "number") {
                let filteredSubquery = knex("trust_registry_history")
                    .select("*")
                    .where("height", "<=", blockHeight);

                if (controller) {
                    filteredSubquery = filteredSubquery.where("controller", controller);
                }
                if (modifiedAfter) {
                    const ts = new Date(modifiedAfter);
                    if (!Number.isNaN(ts.getTime())) {
                        filteredSubquery = filteredSubquery.where("modified", ">", ts.toISOString());
                    }
                }
                if (hasOnlyActive) {
                    if (onlyActive) {
                        filteredSubquery = filteredSubquery.whereNull("archived");
                    } else {
                        filteredSubquery = filteredSubquery.whereNotNull("archived");
                    }
                }

                const rankedSubquery = knex
                    .select("*")
                    .select(knex.raw("ROW_NUMBER() OVER (PARTITION BY tr_id ORDER BY height DESC, created_at DESC) as rn"))
                    .from(filteredSubquery.as("filtered"))
                    .as("ranked");

                const latestTrHistory = await knex
                    .from(rankedSubquery)
                    .where("rn", 1);

                const sortedHistory = sortByStandardAttributes(latestTrHistory, sort, {
                    getId: (row) => row.tr_id,
                    getCreated: (row) => row.created,
                    getModified: (row) => row.modified,
                    defaultAttribute: "modified",
                    defaultDirection: "desc",
                }).slice(0, responseMaxSize);

                if (sortedHistory.length === 0) {
                    return ApiResponder.success(ctx, { trust_registries: [] }, 200);
                }

                const registriesWithStats = await Promise.all(
                    sortedHistory.map(async (trHistory: any) => {
                        const trId = trHistory.tr_id;

                        // Get versions (simplified - just get latest state for each version)
                        const gfvHistory = await knex("governance_framework_version_history")
                            .where({ tr_id: trId })
                            .where("height", "<=", blockHeight)
                            .orderBy("height", "desc")
                            .orderBy("created_at", "desc");

                        const versionMap = new Map<number, any>();
                        for (const gfv of gfvHistory) {
                            if (!versionMap.has(gfv.version)) {
                                versionMap.set(gfv.version, gfv);
                            }
                        }

                        const versions = await Promise.all(
                            Array.from(versionMap.values()).map(async (gfv: any) => {
                                const gfdHistory = await knex("governance_framework_document_history")
                                    .where({ gfv_id: gfv.id, tr_id: trId })
                                    .where("height", "<=", blockHeight)
                                    .orderBy("height", "desc")
                                    .orderBy("created_at", "desc");

                                // Get unique documents (latest state for each document ID at block height)
                                // IMPORTANT: Deduplicate by gfd_id, not by url+language, because multiple documents
                                // can have the same URL and language but different IDs
                                const docMap = new Map<number, any>();
                                for (const gfd of gfdHistory) {
                                    // Validate that gfd_id exists
                                    if (!gfd.gfd_id) {
                                        this.logger.error(`❌ CRITICAL: Document history record missing gfd_id! gfv_id=${gfd.gfv_id}, tr_id=${trId}, url=${gfd.url}, height=${blockHeight}`);
                                        console.error("FATAL ID MISMATCH: Document history record missing gfd_id!", {
                                            gfv_id: gfd.gfv_id,
                                            tr_id: trId,
                                            url: gfd.url,
                                            height: blockHeight,
                                            record: gfd
                                        });
                                    }
                                    // Validate gfd_id doesn't equal gfv_id (which would indicate a bug)
                                    if (gfd.gfd_id === gfd.gfv_id) {
                                        this.logger.error(`❌ CRITICAL: Document ID equals GFV ID! gfd_id=${gfd.gfd_id}, gfv_id=${gfd.gfv_id}, tr_id=${trId}, url=${gfd.url}, height=${blockHeight}`);
                                        console.error("FATAL ID MISMATCH: Document ID equals GFV ID!", {
                                            gfd_id: gfd.gfd_id,
                                            gfv_id: gfd.gfv_id,
                                            tr_id: trId,
                                            url: gfd.url,
                                            height: blockHeight,
                                            record: gfd
                                        });
                                    }
                                    // Keep the latest state of each document (by gfd_id)
                                    if (!docMap.has(gfd.gfd_id)) {
                                        docMap.set(gfd.gfd_id, gfd);
                                    }
                                }

                                const documents = Array.from(docMap.values()).map((gfd: any) => ({
                                    id: gfd.gfd_id,
                                    created: gfd.created,
                                    language: gfd.language,
                                    url: gfd.url,
                                    digest_sri: gfd.digest_sri,
                                }));

                                return {
                                    id: gfv.id,
                                    tr_id: gfv.tr_id,
                                    created: gfv.created,
                                    version: gfv.version,
                                    active_since: gfv.active_since,
                                    documents,
                                };
                            })
                        );

                        let filteredVersions = versions;
                        if (activeGfOnly) {
                            filteredVersions = versions
                                .sort((a, b) => new Date(b.active_since).getTime() - new Date(a.active_since).getTime())
                                .slice(0, 1);
                        }

                        if (preferredLanguage) {
                            for (const v of filteredVersions) {
                                v.documents = v.documents?.filter((d) => d.language === preferredLanguage) ?? [];
                            }
                        }

                        const stats = await calculateTrustRegistryStats(trId, blockHeight);

                        return {
                            id: trHistory.tr_id,
                            did: trHistory.did,
                            controller: trHistory.controller,
                            created: trHistory.created,
                            modified: trHistory.modified,
                            archived: trHistory.archived,
                            deposit: trHistory.deposit,
                            aka: trHistory.aka,
                            language: trHistory.language,
                            active_version: trHistory.active_version,
                            versions: filteredVersions,
                            participants: stats.participants,
                            active_schemas: stats.active_schemas,
                            archived_schemas: stats.archived_schemas,
                            weight: stats.weight,
                            issued: stats.issued,
                            verified: stats.verified,
                            ecosystem_slash_events: stats.ecosystem_slash_events,
                            ecosystem_slashed_amount: stats.ecosystem_slashed_amount,
                            ecosystem_slashed_amount_repaid: stats.ecosystem_slashed_amount_repaid,
                            network_slash_events: stats.network_slash_events,
                            network_slashed_amount: stats.network_slashed_amount,
                            network_slashed_amount_repaid: stats.network_slashed_amount_repaid,
                        };
                    })
                );

                let filteredRegistries = registriesWithStats.filter((r): r is NonNullable<typeof registriesWithStats[0]> => r !== null);

                if (minActiveSchemas !== undefined && maxActiveSchemas !== undefined && minActiveSchemas === maxActiveSchemas) {
                    // Range [min, max) is empty when min === max, return no results
                    filteredRegistries = [];
                } else {
                    if (minActiveSchemas !== undefined) {
                        filteredRegistries = filteredRegistries.filter((r) => r.active_schemas >= minActiveSchemas);
                    }
                    if (maxActiveSchemas !== undefined) {
                        filteredRegistries = filteredRegistries.filter((r) => r.active_schemas < maxActiveSchemas);
                    }
                }
                if (minParticipants !== undefined && maxParticipants !== undefined && minParticipants === maxParticipants) {
                    // empty range for participants
                    filteredRegistries = [];
                } else {
                    if (minParticipants !== undefined) {
                        filteredRegistries = filteredRegistries.filter((r) => r.participants >= minParticipants);
                    }
                    if (maxParticipants !== undefined) {
                        filteredRegistries = filteredRegistries.filter((r) => r.participants < maxParticipants);
                    }
                }
                if (minWeight !== undefined && maxWeight !== undefined && minWeight === maxWeight) {
                    // empty range for weight when using [min, max)
                    filteredRegistries = [];
                } else {
                    if (minWeight !== undefined) {
                        const minWeightBigInt = BigInt(minWeight);
                        filteredRegistries = filteredRegistries.filter((r) => BigInt(r.weight) >= minWeightBigInt);
                    }
                    if (maxWeight !== undefined) {
                        const maxWeightBigInt = BigInt(maxWeight);
                        filteredRegistries = filteredRegistries.filter((r) => BigInt(r.weight) < maxWeightBigInt);
                    }
                }
                if (minIssued !== undefined && maxIssued !== undefined && minIssued === maxIssued) {
                    // empty range for issued
                    filteredRegistries = [];
                } else {
                    if (minIssued !== undefined) {
                        const minIssuedNum = Number(minIssued);
                        filteredRegistries = filteredRegistries.filter((r) => r.issued >= minIssuedNum);
                    }
                    if (maxIssued !== undefined) {
                        const maxIssuedNum = Number(maxIssued);
                        filteredRegistries = filteredRegistries.filter((r) => r.issued < maxIssuedNum);
                    }
                }
                if (minVerified !== undefined && maxVerified !== undefined && minVerified === maxVerified) {
                    // empty range for verified
                    filteredRegistries = [];
                } else {
                    if (minVerified !== undefined) {
                        const minVerifiedNum = Number(minVerified);
                        filteredRegistries = filteredRegistries.filter((r) => r.verified >= minVerifiedNum);
                    }
                    if (maxVerified !== undefined) {
                        const maxVerifiedNum = Number(maxVerified);
                        filteredRegistries = filteredRegistries.filter((r) => r.verified < maxVerifiedNum);
                    }
                }
                if (minEcosystemSlashEvents !== undefined && maxEcosystemSlashEvents !== undefined && minEcosystemSlashEvents === maxEcosystemSlashEvents) {
                    // empty range for ecosystem slash events
                    filteredRegistries = [];
                } else {
                    if (minEcosystemSlashEvents !== undefined) {
                        filteredRegistries = filteredRegistries.filter((r) => r.ecosystem_slash_events >= minEcosystemSlashEvents);
                    }
                    if (maxEcosystemSlashEvents !== undefined) {
                        filteredRegistries = filteredRegistries.filter((r) => r.ecosystem_slash_events < maxEcosystemSlashEvents);
                    }
                }
                if (minNetworkSlashEvents !== undefined && maxNetworkSlashEvents !== undefined && minNetworkSlashEvents === maxNetworkSlashEvents) {
                    // empty range for network slash events
                    filteredRegistries = [];
                } else {
                    if (minNetworkSlashEvents !== undefined) {
                        filteredRegistries = filteredRegistries.filter((r) => r.network_slash_events >= minNetworkSlashEvents);
                    }
                    if (maxNetworkSlashEvents !== undefined) {
                        filteredRegistries = filteredRegistries.filter((r) => r.network_slash_events < maxNetworkSlashEvents);
                    }
                }

                const sortedRegistries = sortByStandardAttributes(filteredRegistries, sort, {
                    getId: (row) => row.id,
                    getCreated: (row) => row.created,
                    getModified: (row) => row.modified,
                    getParticipants: (row) => row.participants,
                    getActiveSchemas: (row) => row.active_schemas,
                    getWeight: (row) => row.weight,
                    getIssued: (row) => row.issued,
                    getVerified: (row) => row.verified,
                    getEcosystemSlashEvents: (row) => row.ecosystem_slash_events,
                    getEcosystemSlashedAmount: (row) => row.ecosystem_slashed_amount,
                    getNetworkSlashEvents: (row) => row.network_slash_events,
                    getNetworkSlashedAmount: (row) => row.network_slashed_amount,
                    defaultAttribute: "modified",
                    defaultDirection: "desc",
                }).slice(0, responseMaxSize);

                return ApiResponder.success(ctx, { trust_registries: sortedRegistries }, 200);
            }

            let query = TrustRegistry.query().withGraphFetched("governanceFrameworkVersions.documents");

            if (controller) {
                query = query.where("controller", controller);
            }

            if (modifiedAfter) {
                query = query.where("modified", ">", modifiedAfter);
            }

            if (hasOnlyActive) {
                if (onlyActive) {
                    query = query.whereNull("archived");
                } else {
                    query = query.whereNotNull("archived");
                }
            }

            applyOrdering(query as any, sort);

            const registries = await query.limit(responseMaxSize * 2);

            let registriesWithStats;
            if (typeof blockHeight === "number") {
                registriesWithStats = await Promise.all(
                    registries.map(async (tr) => {
                        const plain = tr.toJSON();
                        let versions = plain.governanceFrameworkVersions ?? [];

                        if (activeGfOnly) {
                            versions = versions
                                .sort((a, b) => {
                                    if (!a.active_since && !b.active_since) return 0;
                                    if (!a.active_since) return 1;
                                    if (!b.active_since) return -1;
                                    return new Date(b.active_since).getTime() - new Date(a.active_since).getTime();
                                })
                                .slice(0, 1);
                        }

                        if (preferredLanguage) {
                            for (const v of versions) {
                                v.documents =
                                    v.documents?.filter((d) => d.language === preferredLanguage) ?? [];
                            }
                        }

                        delete plain.governanceFrameworkVersions;
                        delete (plain as any).height;

                        const stats = await calculateTrustRegistryStats(plain.id, blockHeight);

                        return {
                            ...plain,
                            versions,
                            participants: stats.participants,
                            active_schemas: stats.active_schemas,
                            archived_schemas: stats.archived_schemas,
                            weight: stats.weight,
                            issued: stats.issued,
                            verified: stats.verified,
                            ecosystem_slash_events: stats.ecosystem_slash_events,
                            ecosystem_slashed_amount: stats.ecosystem_slashed_amount,
                            ecosystem_slashed_amount_repaid: stats.ecosystem_slashed_amount_repaid,
                            network_slash_events: stats.network_slash_events,
                            network_slashed_amount: stats.network_slashed_amount,
                            network_slashed_amount_repaid: stats.network_slashed_amount_repaid,
                        };
                    })
                );
            } else {
                const trIds = registries.map((tr) => tr.id);
                const trStatsMap = new Map<number, any>();
                
                if (trIds.length > 0) {
                    const trStats = await knex("trust_registry")
                        .whereIn("id", trIds)
                        .select(
                            "id",
                            "participants",
                            "active_schemas",
                            "archived_schemas",
                            "weight",
                            "issued",
                            "verified",
                            "ecosystem_slash_events",
                            "ecosystem_slashed_amount",
                            "ecosystem_slashed_amount_repaid",
                            "network_slash_events",
                            "network_slashed_amount",
                            "network_slashed_amount_repaid"
                        );
                    
                    for (const stat of trStats) {
                        trStatsMap.set(stat.id, stat);
                    }
                }

                registriesWithStats = registries.map((tr) => {
                    const plain = tr.toJSON();
                    let versions = plain.governanceFrameworkVersions ?? [];

                    if (activeGfOnly) {
                        versions = versions
                            .sort((a, b) => {
                                if (!a.active_since && !b.active_since) return 0;
                                if (!a.active_since) return 1;
                                if (!b.active_since) return -1;
                                return new Date(b.active_since).getTime() - new Date(a.active_since).getTime();
                            })
                            .slice(0, 1);
                    }

                    if (preferredLanguage) {
                        for (const v of versions) {
                            v.documents =
                                v.documents?.filter((d) => d.language === preferredLanguage) ?? [];
                        }
                    }

                    delete plain.governanceFrameworkVersions;
                    delete (plain as any).height;

                    const stats = trStatsMap.get(plain.id) || {
                        participants: 0,
                        active_schemas: 0,
                        archived_schemas: 0,
                        weight: "0",
                        issued: 0,
                        verified: 0,
                        ecosystem_slash_events: 0,
                        ecosystem_slashed_amount: "0",
                        ecosystem_slashed_amount_repaid: "0",
                        network_slash_events: 0,
                        network_slashed_amount: "0",
                        network_slashed_amount_repaid: "0",
                    };

                    return {
                        ...plain,
                        versions,
                        participants: stats.participants || 0,
                        active_schemas: stats.active_schemas || 0,
                        archived_schemas: stats.archived_schemas || 0,
                        weight: stats.weight || "0",
                        issued: stats.issued || 0,
                        verified: stats.verified || 0,
                        ecosystem_slash_events: stats.ecosystem_slash_events || 0,
                        ecosystem_slashed_amount: stats.ecosystem_slashed_amount || "0",
                        ecosystem_slashed_amount_repaid: stats.ecosystem_slashed_amount_repaid || "0",
                        network_slash_events: stats.network_slash_events || 0,
                        network_slashed_amount: stats.network_slashed_amount || "0",
                        network_slashed_amount_repaid: stats.network_slashed_amount_repaid || "0",
                    };
                });
            }

            let filteredRegistries = registriesWithStats;

            if (minActiveSchemas !== undefined && maxActiveSchemas !== undefined && minActiveSchemas === maxActiveSchemas) {
                // empty range when min === max for [min, max)
                filteredRegistries = [];
            } else {
                if (minActiveSchemas !== undefined) {
                    filteredRegistries = filteredRegistries.filter((r) => r.active_schemas >= minActiveSchemas);
                }
                if (maxActiveSchemas !== undefined) {
                    filteredRegistries = filteredRegistries.filter((r) => r.active_schemas < maxActiveSchemas);
                }
            }
            if (minParticipants !== undefined && maxParticipants !== undefined && minParticipants === maxParticipants) {
                // empty range for participants
                filteredRegistries = [];
            } else {
                if (minParticipants !== undefined) {
                    filteredRegistries = filteredRegistries.filter((r) => r.participants >= minParticipants);
                }
                if (maxParticipants !== undefined) {
                    filteredRegistries = filteredRegistries.filter((r) => r.participants < maxParticipants);
                }
            }
            if (minWeight !== undefined && maxWeight !== undefined && minWeight === maxWeight) {
                // empty range for weight
                filteredRegistries = [];
            } else {
                if (minWeight !== undefined) {
                    const minWeightBigInt = BigInt(minWeight);
                    filteredRegistries = filteredRegistries.filter((r) => BigInt(r.weight) >= minWeightBigInt);
                }
                if (maxWeight !== undefined) {
                    const maxWeightBigInt = BigInt(maxWeight);
                    filteredRegistries = filteredRegistries.filter((r) => BigInt(r.weight) < maxWeightBigInt);
                }
            }
            if (minIssued !== undefined && maxIssued !== undefined && minIssued === maxIssued) {
                // empty range for issued
                filteredRegistries = [];
            } else {
                if (minIssued !== undefined) {
                    const minIssuedNum = Number(minIssued);
                    filteredRegistries = filteredRegistries.filter((r) => r.issued >= minIssuedNum);
                }
                if (maxIssued !== undefined) {
                    const maxIssuedNum = Number(maxIssued);
                    filteredRegistries = filteredRegistries.filter((r) => r.issued < maxIssuedNum);
                }
            }
            if (minVerified !== undefined && maxVerified !== undefined && minVerified === maxVerified) {
                // empty range for verified
                filteredRegistries = [];
            } else {
                if (minVerified !== undefined) {
                    const minVerifiedNum = Number(minVerified);
                    filteredRegistries = filteredRegistries.filter((r) => r.verified >= minVerifiedNum);
                }
                if (maxVerified !== undefined) {
                    const maxVerifiedNum = Number(maxVerified);
                    filteredRegistries = filteredRegistries.filter((r) => r.verified < maxVerifiedNum);
                }
            }
            if (minEcosystemSlashEvents !== undefined && maxEcosystemSlashEvents !== undefined && minEcosystemSlashEvents === maxEcosystemSlashEvents) {
                // empty range for ecosystem slash events
                filteredRegistries = [];
            } else {
                if (minEcosystemSlashEvents !== undefined) {
                    filteredRegistries = filteredRegistries.filter((r) => r.ecosystem_slash_events >= minEcosystemSlashEvents);
                }
                if (maxEcosystemSlashEvents !== undefined) {
                    filteredRegistries = filteredRegistries.filter((r) => r.ecosystem_slash_events < maxEcosystemSlashEvents);
                }
            }
            if (minNetworkSlashEvents !== undefined && maxNetworkSlashEvents !== undefined && minNetworkSlashEvents === maxNetworkSlashEvents) {
                // empty range for network slash events
                filteredRegistries = [];
            } else {
                if (minNetworkSlashEvents !== undefined) {
                    filteredRegistries = filteredRegistries.filter((r) => r.network_slash_events >= minNetworkSlashEvents);
                }
                if (maxNetworkSlashEvents !== undefined) {
                    filteredRegistries = filteredRegistries.filter((r) => r.network_slash_events < maxNetworkSlashEvents);
                }
            }

            const sortedRegistries = sortByStandardAttributes(filteredRegistries, sort, {
                getId: (row) => row.id,
                getCreated: (row) => row.created,
                getModified: (row) => row.modified,
                getParticipants: (row) => row.participants,
                getActiveSchemas: (row) => row.active_schemas,
                getWeight: (row) => row.weight,
                getIssued: (row) => row.issued,
                getVerified: (row) => row.verified,
                getEcosystemSlashEvents: (row) => row.ecosystem_slash_events,
                getEcosystemSlashedAmount: (row) => row.ecosystem_slashed_amount,
                getNetworkSlashEvents: (row) => row.network_slash_events,
                getNetworkSlashedAmount: (row) => row.network_slashed_amount,
                defaultAttribute: "modified",
                defaultDirection: "desc",
            }).slice(0, responseMaxSize);

            return ApiResponder.success(ctx, { trust_registries: sortedRegistries }, 200);
        } catch (err: any) {
            return ApiResponder.error(ctx, err.message, 500);
        }
    }

    @Action()
    public async getParams(ctx: Context) {
        const { getModuleParamsAction } = await import("../../common/utils/params_service");
        return getModuleParamsAction(ctx, ModulesParamsNamesTypes.TR, MODULE_DISPLAY_NAMES.TRUST_REGISTRY);
    }
}