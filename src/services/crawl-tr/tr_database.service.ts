/* eslint-disable @typescript-eslint/no-explicit-any */

import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BaseService from "../../base/base.service";
import { ModulesParamsNamesTypes, MODULE_DISPLAY_NAMES, SERVICE } from "../../common";
import ApiResponder from "../../common/utils/apiResponse";
import { TrustRegistry } from "../../models/trust_registry";
import knex from "../../common/utils/db_connection";
import { applyOrdering, validateSortParameter, sortByStandardAttributes } from "../../common/utils/query_ordering";

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

                    // Get unique documents (latest state for each document at block height)
                    const docMap = new Map<string, any>();
                    for (const gfd of gfdHistory) {
                        const key = `${gfd.url}-${gfd.language}`;
                        if (!docMap.has(key)) {
                            docMap.set(key, gfd);
                        }
                    }

                    const documents = Array.from(docMap.values()).map((gfd: any) => ({
                        id: gfd.id,
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
                        (a, b) =>
                            new Date(b.active_since).getTime() - new Date(a.active_since).getTime()
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
            return ApiResponder.success(ctx, { trust_registry: { ...plain, versions } }, 200);
        } catch (err: any) {
            return ApiResponder.error(ctx, err.message, 500);
        }
    }

    @Action()
    public async listTrustRegistries(ctx: Context<{
        controller?: string;
        modified_after?: string;
        active_gf_only?: string | boolean;
        preferred_language?: string;
        response_max_size?: number;
        sort?: string;
    }>) {
        try {
            const {
                controller,
                modified_after: modifiedAfter,
                preferred_language: preferredLanguage,
                response_max_size: responseMaxSizeRaw,
                sort
            } = ctx.params;

            try {
                validateSortParameter(sort);
            } catch (err: any) {
                return ApiResponder.error(ctx, err.message, 400);
            }

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
                    defaultAttribute: "id",
                    defaultDirection: "desc",
                }).slice(0, responseMaxSize);

                if (sortedHistory.length === 0) {
                    return ApiResponder.success(ctx, { trust_registries: [] }, 200);
                }

                const registries = await Promise.all(
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

                                const docMap = new Map<string, any>();
                                for (const gfd of gfdHistory) {
                                    const key = `${gfd.url}-${gfd.language}`;
                                    if (!docMap.has(key)) {
                                        docMap.set(key, gfd);
                                    }
                                }

                                const documents = Array.from(docMap.values()).map((gfd: any) => ({
                                    id: gfd.id,
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
                        };
                    })
                );

                const filteredRegistries = registries.filter((r): r is NonNullable<typeof registries[0]> => r !== null);

                return ApiResponder.success(ctx, { trust_registries: filteredRegistries }, 200);
            }

            // Otherwise, return latest state
            let query = TrustRegistry.query().withGraphFetched("governanceFrameworkVersions.documents");

            if (controller) {
                query = query.where("controller", controller);
            }

            if (modifiedAfter) {
                query = query.where("modified", ">", modifiedAfter);
            }

            applyOrdering(query as any, sort);

            const registries = await query.limit(responseMaxSize);

            const result = registries.map((tr) => {
                const plain = tr.toJSON();
                let versions = plain.governanceFrameworkVersions ?? [];

                if (activeGfOnly) {
                    versions = versions
                        .sort((a, b) => new Date(b.active_since).getTime() - new Date(a.active_since).getTime())
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

                return { ...plain, versions };
            });

            return ApiResponder.success(ctx, { trust_registries: result }, 200);
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