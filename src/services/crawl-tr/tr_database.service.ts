/* eslint-disable @typescript-eslint/no-explicit-any */

import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BaseService from "../../base/base.service";
import { ModulesParamsNamesTypes, MODULE_DISPLAY_NAMES, SERVICE } from "../../common";
import { validateParticipantParam } from "../../common/utils/accountValidation";
import ApiResponder from "../../common/utils/apiResponse";
import { TrustRegistry } from "../../models/trust_registry";
import knex from "../../common/utils/db_connection";
import { applyOrdering, validateSortParameter, sortByStandardAttributes } from "../../common/utils/query_ordering";
import { calculateTrustRegistryStats, calculateTrustRegistryStatsBatch } from "./tr_stats";

@Service({
    name: SERVICE.V1.TrustRegistryDatabaseService.key,
    version: 1
})
export default class TrustRegistryDatabaseService extends BaseService {
    private trHistoryColumnExistsCache = new Map<string, boolean>();

    public constructor(public broker: ServiceBroker) {
        super(broker);
    }

    private async hasTrHistoryColumn(tableName: string, columnName: string): Promise<boolean> {
        const key = `${tableName}.${columnName}`;
        const cached = this.trHistoryColumnExistsCache.get(key);
        if (cached !== undefined) {
            return cached;
        }
        const exists = await knex.schema.hasColumn(tableName, columnName);
        this.trHistoryColumnExistsCache.set(key, exists);
        return exists;
    }

    private usesDerivedMetricSort(sort?: string): boolean {
        if (!sort || typeof sort !== "string") return false;
        const lower = sort.toLowerCase();
        const derivedKeys = [
            "participants",
            "active_schemas",
            "weight",
            "issued",
            "verified",
            "ecosystem_slash_events",
            "ecosystem_slashed_amount",
            "network_slash_events",
            "network_slashed_amount",
        ];
        return derivedKeys.some((key) => lower.includes(key));
    }

    private static toFiniteNumber(value: unknown): number {
        const num = typeof value === "number" ? value : Number(value ?? 0);
        return Number.isFinite(num) ? num : 0;
    }

    private applyRangeToQuery(query: any, column: string, minValue?: number, maxValue?: number): any {
        if (minValue !== undefined && maxValue !== undefined && minValue === maxValue) {
            return query.whereRaw("1 = 0");
        }
        let nextQuery = query;
        if (minValue !== undefined) {
            nextQuery = nextQuery.where(column, ">=", minValue);
        }
        if (maxValue !== undefined) {
            nextQuery = nextQuery.where(column, "<", maxValue);
        }
        return nextQuery;
    }

    private applyRangeToRows<T>(
        rows: T[],
        minValue: number | string | undefined,
        maxValue: number | string | undefined,
        readValue: (row: T) => number
    ): T[] {
        if (minValue !== undefined && maxValue !== undefined && minValue === maxValue) {
            return [];
        }

        let filtered = rows;
        if (minValue !== undefined) {
            const minNum = Number(minValue);
            filtered = filtered.filter((row) => readValue(row) >= minNum);
        }
        if (maxValue !== undefined) {
            const maxNum = Number(maxValue);
            filtered = filtered.filter((row) => readValue(row) < maxNum);
        }
        return filtered;
    }

    private applyMetricFiltersToRegistries(
        rows: any[],
        filters: {
            minActiveSchemas?: number;
            maxActiveSchemas?: number;
            minParticipants?: number;
            maxParticipants?: number;
            minParticipantsEcosystem?: number;
            maxParticipantsEcosystem?: number;
            minParticipantsIssuerGrantor?: number;
            maxParticipantsIssuerGrantor?: number;
            minParticipantsIssuer?: number;
            maxParticipantsIssuer?: number;
            minParticipantsVerifierGrantor?: number;
            maxParticipantsVerifierGrantor?: number;
            minParticipantsVerifier?: number;
            maxParticipantsVerifier?: number;
            minParticipantsHolder?: number;
            maxParticipantsHolder?: number;
            minWeight?: string;
            maxWeight?: string;
            minIssued?: string;
            maxIssued?: string;
            minVerified?: string;
            maxVerified?: string;
            minEcosystemSlashEvents?: number;
            maxEcosystemSlashEvents?: number;
            minNetworkSlashEvents?: number;
            maxNetworkSlashEvents?: number;
        }
    ): any[] {
        let filtered = rows;
        filtered = this.applyRangeToRows(filtered, filters.minActiveSchemas, filters.maxActiveSchemas, (r) => TrustRegistryDatabaseService.toFiniteNumber(r.active_schemas));
        filtered = this.applyRangeToRows(filtered, filters.minParticipants, filters.maxParticipants, (r) => TrustRegistryDatabaseService.toFiniteNumber(r.participants));
        filtered = this.applyRangeToRows(filtered, filters.minParticipantsEcosystem, filters.maxParticipantsEcosystem, (r) => TrustRegistryDatabaseService.toFiniteNumber(r.participants_ecosystem));
        filtered = this.applyRangeToRows(filtered, filters.minParticipantsIssuerGrantor, filters.maxParticipantsIssuerGrantor, (r) => TrustRegistryDatabaseService.toFiniteNumber(r.participants_issuer_grantor));
        filtered = this.applyRangeToRows(filtered, filters.minParticipantsIssuer, filters.maxParticipantsIssuer, (r) => TrustRegistryDatabaseService.toFiniteNumber(r.participants_issuer));
        filtered = this.applyRangeToRows(filtered, filters.minParticipantsVerifierGrantor, filters.maxParticipantsVerifierGrantor, (r) => TrustRegistryDatabaseService.toFiniteNumber(r.participants_verifier_grantor));
        filtered = this.applyRangeToRows(filtered, filters.minParticipantsVerifier, filters.maxParticipantsVerifier, (r) => TrustRegistryDatabaseService.toFiniteNumber(r.participants_verifier));
        filtered = this.applyRangeToRows(filtered, filters.minParticipantsHolder, filters.maxParticipantsHolder, (r) => TrustRegistryDatabaseService.toFiniteNumber(r.participants_holder));
        filtered = this.applyRangeToRows(filtered, filters.minWeight, filters.maxWeight, (r) => TrustRegistryDatabaseService.toFiniteNumber(r.weight));
        filtered = this.applyRangeToRows(filtered, filters.minIssued, filters.maxIssued, (r) => TrustRegistryDatabaseService.toFiniteNumber(r.issued));
        filtered = this.applyRangeToRows(filtered, filters.minVerified, filters.maxVerified, (r) => TrustRegistryDatabaseService.toFiniteNumber(r.verified));
        filtered = this.applyRangeToRows(filtered, filters.minEcosystemSlashEvents, filters.maxEcosystemSlashEvents, (r) => TrustRegistryDatabaseService.toFiniteNumber(r.ecosystem_slash_events));
        filtered = this.applyRangeToRows(filtered, filters.minNetworkSlashEvents, filters.maxNetworkSlashEvents, (r) => TrustRegistryDatabaseService.toFiniteNumber(r.network_slash_events));
        return filtered;
    }

    private sortRegistries(rows: any[], sort: string | undefined, limit: number): any[] {
        if (!this.usesDerivedMetricSort(sort)) {
            return rows.slice(0, limit);
        }
        return sortByStandardAttributes(rows, sort, {
            getId: (row) => row.id,
            getCreated: (row) => row.created,
            getModified: (row) => row.modified,
            getParticipants: (row) => row.participants,
            getParticipantsEcosystem: (row) => row.participants_ecosystem,
            getParticipantsIssuerGrantor: (row) => row.participants_issuer_grantor,
            getParticipantsIssuer: (row) => row.participants_issuer,
            getParticipantsVerifierGrantor: (row) => row.participants_verifier_grantor,
            getParticipantsVerifier: (row) => row.participants_verifier,
            getParticipantsHolder: (row) => row.participants_holder,
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
        }).slice(0, limit);
    }

    private hasImpossibleMetricRanges(filters: {
        minActiveSchemas?: number;
        maxActiveSchemas?: number;
        minParticipants?: number;
        maxParticipants?: number;
        minParticipantsEcosystem?: number;
        maxParticipantsEcosystem?: number;
        minParticipantsIssuerGrantor?: number;
        maxParticipantsIssuerGrantor?: number;
        minParticipantsIssuer?: number;
        maxParticipantsIssuer?: number;
        minParticipantsVerifierGrantor?: number;
        maxParticipantsVerifierGrantor?: number;
        minParticipantsVerifier?: number;
        maxParticipantsVerifier?: number;
        minParticipantsHolder?: number;
        maxParticipantsHolder?: number;
        minWeight?: string;
        maxWeight?: string;
        minIssued?: string;
        maxIssued?: string;
        minVerified?: string;
        maxVerified?: string;
        minEcosystemSlashEvents?: number;
        maxEcosystemSlashEvents?: number;
        minNetworkSlashEvents?: number;
        maxNetworkSlashEvents?: number;
    }): boolean {
        return (
            (filters.minActiveSchemas !== undefined && filters.maxActiveSchemas !== undefined && filters.minActiveSchemas === filters.maxActiveSchemas) ||
            (filters.minParticipants !== undefined && filters.maxParticipants !== undefined && filters.minParticipants === filters.maxParticipants) ||
            (filters.minParticipantsEcosystem !== undefined && filters.maxParticipantsEcosystem !== undefined && filters.minParticipantsEcosystem === filters.maxParticipantsEcosystem) ||
            (filters.minParticipantsIssuerGrantor !== undefined && filters.maxParticipantsIssuerGrantor !== undefined && filters.minParticipantsIssuerGrantor === filters.maxParticipantsIssuerGrantor) ||
            (filters.minParticipantsIssuer !== undefined && filters.maxParticipantsIssuer !== undefined && filters.minParticipantsIssuer === filters.maxParticipantsIssuer) ||
            (filters.minParticipantsVerifierGrantor !== undefined && filters.maxParticipantsVerifierGrantor !== undefined && filters.minParticipantsVerifierGrantor === filters.maxParticipantsVerifierGrantor) ||
            (filters.minParticipantsVerifier !== undefined && filters.maxParticipantsVerifier !== undefined && filters.minParticipantsVerifier === filters.maxParticipantsVerifier) ||
            (filters.minParticipantsHolder !== undefined && filters.maxParticipantsHolder !== undefined && filters.minParticipantsHolder === filters.maxParticipantsHolder) ||
            (filters.minWeight !== undefined && filters.maxWeight !== undefined && filters.minWeight === filters.maxWeight) ||
            (filters.minIssued !== undefined && filters.maxIssued !== undefined && filters.minIssued === filters.maxIssued) ||
            (filters.minVerified !== undefined && filters.maxVerified !== undefined && filters.minVerified === filters.maxVerified) ||
            (filters.minEcosystemSlashEvents !== undefined && filters.maxEcosystemSlashEvents !== undefined && filters.minEcosystemSlashEvents === filters.maxEcosystemSlashEvents) ||
            (filters.minNetworkSlashEvents !== undefined && filters.maxNetworkSlashEvents !== undefined && filters.minNetworkSlashEvents === filters.maxNetworkSlashEvents)
        );
    }

    private async buildHistoricalVersionsByTrIds(
        trIdsInput: number[],
        blockHeight: number,
        activeGfOnly: boolean,
        preferredLanguage?: string
    ): Promise<Map<number, any[]>> {
        const trIds = Array.from(new Set(trIdsInput.map((id) => Number(id)).filter((id) => Number.isFinite(id) && id > 0)));
        const versionsByTr = new Map<number, any[]>();
        if (trIds.length === 0) return versionsByTr;

        const gfvHistoryRows = await knex("governance_framework_version_history")
            .select("id", "tr_id", "created", "version", "active_since", "height", "created_at")
            .whereIn("tr_id", trIds)
            .where("height", "<=", blockHeight)
            .orderBy("tr_id", "asc")
            .orderBy("version", "asc")
            .orderBy("height", "desc")
            .orderBy("created_at", "desc");

        const gfvRows = await knex("governance_framework_version")
            .select("id", "tr_id", "version")
            .whereIn("tr_id", trIds);
        const gfvIdByTrVersion = new Map<string, number>();
        for (const row of gfvRows) {
            gfvIdByTrVersion.set(`${Number(row.tr_id)}::${Number(row.version)}`, Number(row.id));
        }

        const latestByTrVersion = new Map<string, any>();
        for (const gfv of gfvHistoryRows) {
            const trId = Number(gfv.tr_id);
            const version = Number(gfv.version);
            const key = `${trId}::${version}`;
            if (latestByTrVersion.has(key)) continue;

            const actualGfvId = gfvIdByTrVersion.get(key);
            const entry = {
                id: Number.isFinite(actualGfvId as number) ? Number(actualGfvId) : Number(gfv.id),
                tr_id: trId,
                created: gfv.created,
                version,
                active_since: gfv.active_since,
                documents: [] as any[],
            };
            latestByTrVersion.set(key, entry);
            const trList = versionsByTr.get(trId) || [];
            trList.push(entry);
            versionsByTr.set(trId, trList);
        }

        const gfvIds = Array.from(new Set(Array.from(latestByTrVersion.values()).map((v: any) => Number(v.id)).filter((id) => Number.isFinite(id) && id > 0)));
        if (gfvIds.length > 0) {
            const hasGfdIdColumn = await this.hasTrHistoryColumn("governance_framework_document_history", "gfd_id");
            const gfdSelectColumns: Array<string | any> = [
                "id",
                "gfv_id",
                "tr_id",
                "created",
                "language",
                "url",
                "digest_sri",
                "height",
                "created_at",
            ];
            if (hasGfdIdColumn) {
                gfdSelectColumns.splice(1, 0, "gfd_id");
            }
            const gfdHistoryRows = await knex("governance_framework_document_history")
                .select(...gfdSelectColumns)
                .whereIn("tr_id", trIds)
                .whereIn("gfv_id", gfvIds)
                .where("height", "<=", blockHeight)
                .orderBy("gfv_id", "asc")
                .orderBy("height", "desc")
                .orderBy("created_at", "desc")
                .orderBy("id", "desc");

            const seenDocs = new Set<string>();
            const entryByGfvId = new Map<number, any>();
            for (const versionEntry of latestByTrVersion.values()) {
                entryByGfvId.set(Number(versionEntry.id), versionEntry);
            }

            for (const gfd of gfdHistoryRows) {
                const gfvId = Number(gfd.gfv_id);
                const versionEntry = entryByGfvId.get(gfvId);
                if (!versionEntry) continue;

                const docId = Number(hasGfdIdColumn ? gfd.gfd_id : gfd.id);
                const dedupeId = Number.isFinite(docId) && docId > 0 ? `doc:${docId}` : `url:${gfd.url || ""}:${gfd.language || ""}`;
                const dedupeKey = `${Number(versionEntry.tr_id)}::${gfvId}::${dedupeId}`;
                if (seenDocs.has(dedupeKey)) continue;
                seenDocs.add(dedupeKey);

                versionEntry.documents.push({
                    id: Number.isFinite(docId) && docId > 0 ? docId : Number(gfd.id),
                    gfv_id: gfvId,
                    created: gfd.created,
                    language: gfd.language,
                    url: gfd.url,
                    digest_sri: gfd.digest_sri,
                });
            }
        }

        for (const [trId, versions] of versionsByTr.entries()) {
            let normalized = versions;
            if (activeGfOnly) {
                normalized = versions
                    .sort((a, b) => new Date(b.active_since).getTime() - new Date(a.active_since).getTime())
                    .slice(0, 1);
            }
            if (preferredLanguage) {
                normalized = normalized.map((v) => ({
                    ...v,
                    documents: (v.documents || []).filter((d: any) => d.language === preferredLanguage),
                }));
            }
            versionsByTr.set(trId, normalized);
        }

        return versionsByTr;
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

                const versionsByTrId = await this.buildHistoricalVersionsByTrIds([Number(tr_id)], blockHeight, activeGfOnly, preferred_language);
                const filteredVersions = versionsByTrId.get(Number(tr_id)) || [];

                const stats = await calculateTrustRegistryStats(trHistory.tr_id, blockHeight);

                const trustRegistry = {
                    id: trHistory.tr_id,
                    did: trHistory.did,
                    controller: trHistory.controller,
                    created: trHistory.created,
                    modified: trHistory.modified,
                    archived: trHistory.archived,
                    deposit: trHistory.deposit ?? 0,
                    aka: trHistory.aka,
                    language: trHistory.language,
                    active_version: trHistory.active_version,
                    versions: filteredVersions,
                    participants: stats.participants,
                    participants_ecosystem: stats.participants_ecosystem,
                    participants_issuer_grantor: stats.participants_issuer_grantor,
                    participants_issuer: stats.participants_issuer,
                    participants_verifier_grantor: stats.participants_verifier_grantor,
                    participants_verifier: stats.participants_verifier,
                    participants_holder: stats.participants_holder,
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

            return ApiResponder.success(ctx, {
                trust_registry: {
                    ...plain,
                    id: plain.id,
                    deposit: plain.deposit ?? 0,
                    versions,
                    participants: Number((plain as any).participants || 0),
                    participants_ecosystem: Number((plain as any).participants_ecosystem || 0),
                    participants_issuer_grantor: Number((plain as any).participants_issuer_grantor || 0),
                    participants_issuer: Number((plain as any).participants_issuer || 0),
                    participants_verifier_grantor: Number((plain as any).participants_verifier_grantor || 0),
                    participants_verifier: Number((plain as any).participants_verifier || 0),
                    participants_holder: Number((plain as any).participants_holder || 0),
                    active_schemas: Number((plain as any).active_schemas || 0),
                    archived_schemas: Number((plain as any).archived_schemas || 0),
                    weight: Number((plain as any).weight || 0),
                    issued: Number((plain as any).issued || 0),
                    verified: Number((plain as any).verified || 0),
                    ecosystem_slash_events: Number((plain as any).ecosystem_slash_events || 0),
                    ecosystem_slashed_amount: Number((plain as any).ecosystem_slashed_amount || 0),
                    ecosystem_slashed_amount_repaid: Number((plain as any).ecosystem_slashed_amount_repaid || 0),
                    network_slash_events: Number((plain as any).network_slash_events || 0),
                    network_slashed_amount: Number((plain as any).network_slashed_amount || 0),
                    network_slashed_amount_repaid: Number((plain as any).network_slashed_amount_repaid || 0),
                },
            }, 200);
        } catch (err: any) {
            return ApiResponder.error(ctx, err.message, 500);
        }
    }

    @Action({
        params: {
            controller: { type: "any", optional: true },
            participant: { type: "any", optional: true },
            modified_after: { type: "string", optional: true },
            only_active: { type: "any", optional: true },
            active_gf_only: { type: "any", optional: true },
            preferred_language: { type: "string", optional: true },
            response_max_size: { type: "number", optional: true, default: 64 },
            sort: { type: "string", optional: true },
            min_active_schemas: { type: "number", optional: true },
            max_active_schemas: { type: "number", optional: true },
            min_participants: { type: "number", optional: true },
            max_participants: { type: "number", optional: true },
            min_participants_ecosystem: { type: "number", optional: true },
            max_participants_ecosystem: { type: "number", optional: true },
            min_participants_issuer_grantor: { type: "number", optional: true },
            max_participants_issuer_grantor: { type: "number", optional: true },
            min_participants_issuer: { type: "number", optional: true },
            max_participants_issuer: { type: "number", optional: true },
            min_participants_verifier_grantor: { type: "number", optional: true },
            max_participants_verifier_grantor: { type: "number", optional: true },
            min_participants_verifier: { type: "number", optional: true },
            max_participants_verifier: { type: "number", optional: true },
            min_participants_holder: { type: "number", optional: true },
            max_participants_holder: { type: "number", optional: true },
            min_weight: { type: "string", optional: true },
            max_weight: { type: "string", optional: true },
            min_issued: { type: "string", optional: true },
            max_issued: { type: "string", optional: true },
            min_verified: { type: "string", optional: true },
            max_verified: { type: "string", optional: true },
            min_ecosystem_slash_events: { type: "number", optional: true },
            max_ecosystem_slash_events: { type: "number", optional: true },
            min_network_slash_events: { type: "number", optional: true },
            max_network_slash_events: { type: "number", optional: true },
        },
    })
    public async listTrustRegistries(ctx: Context<{
        controller?: string;
        participant?: string;
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
        min_participants_ecosystem?: number;
        max_participants_ecosystem?: number;
        min_participants_issuer_grantor?: number;
        max_participants_issuer_grantor?: number;
        min_participants_issuer?: number;
        max_participants_issuer?: number;
        min_participants_verifier_grantor?: number;
        max_participants_verifier_grantor?: number;
        min_participants_verifier?: number;
        max_participants_verifier?: number;
        min_participants_holder?: number;
        max_participants_holder?: number;
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
                participant,
                modified_after: modifiedAfter,
                preferred_language: preferredLanguage,
                only_active: onlyActiveRaw,
                response_max_size: responseMaxSizeRaw,
                sort,
                min_active_schemas: minActiveSchemas,
                max_active_schemas: maxActiveSchemas,
                min_participants: minParticipants,
                max_participants: maxParticipants,
                min_participants_ecosystem: minParticipantsEcosystem,
                max_participants_ecosystem: maxParticipantsEcosystem,
                min_participants_issuer_grantor: minParticipantsIssuerGrantor,
                max_participants_issuer_grantor: maxParticipantsIssuerGrantor,
                min_participants_issuer: minParticipantsIssuer,
                max_participants_issuer: maxParticipantsIssuer,
                min_participants_verifier_grantor: minParticipantsVerifierGrantor,
                max_participants_verifier_grantor: maxParticipantsVerifierGrantor,
                min_participants_verifier: minParticipantsVerifier,
                max_participants_verifier: maxParticipantsVerifier,
                min_participants_holder: minParticipantsHolder,
                max_participants_holder: maxParticipantsHolder,
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

            const participantValidation = validateParticipantParam(participant, "participant");
            if (!participantValidation.valid) {
                return ApiResponder.error(ctx, participantValidation.error, 400);
            }
            const participantAccount = participantValidation.value;

            const controllerValidation = validateParticipantParam(controller, "controller");
            if (!controllerValidation.valid) {
                return ApiResponder.error(ctx, controllerValidation.error, 400);
            }
            const controllerAccount = controllerValidation.value;

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

            const metricFilters = {
                minActiveSchemas,
                maxActiveSchemas,
                minParticipants,
                maxParticipants,
                minParticipantsEcosystem,
                maxParticipantsEcosystem,
                minParticipantsIssuerGrantor,
                maxParticipantsIssuerGrantor,
                minParticipantsIssuer,
                maxParticipantsIssuer,
                minParticipantsVerifierGrantor,
                maxParticipantsVerifierGrantor,
                minParticipantsVerifier,
                maxParticipantsVerifier,
                minParticipantsHolder,
                maxParticipantsHolder,
                minWeight,
                maxWeight,
                minIssued,
                maxIssued,
                minVerified,
                maxVerified,
                minEcosystemSlashEvents,
                maxEcosystemSlashEvents,
                minNetworkSlashEvents,
                maxNetworkSlashEvents,
            };

            if (this.hasImpossibleMetricRanges(metricFilters)) {
                return ApiResponder.success(ctx, { trust_registries: [] }, 200);
            }

            // If AtBlockHeight is provided, query historical state
            if (typeof blockHeight === "number") {
                let participantTrIds: number[] | undefined;
                if (participantAccount) {
                    participantTrIds = await this.getTrustRegistryIdsForParticipantAtHeight(participantAccount, blockHeight);
                    if (participantTrIds.length === 0) {
                        return ApiResponder.success(ctx, { trust_registries: [] }, 200);
                    }
                }

                let filteredSubquery = knex("trust_registry_history")
                    .select("*")
                    .where("height", "<=", blockHeight);

                if (controllerAccount) {
                    filteredSubquery = filteredSubquery.where("controller", controllerAccount);
                }
                if (participantTrIds !== undefined) {
                    filteredSubquery = filteredSubquery.whereIn("tr_id", participantTrIds);
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

                const latestTrHistoryQuery = knex
                    .from(rankedSubquery)
                    .where("rn", 1);

                const latestTrHistory = await applyOrdering(latestTrHistoryQuery as any, sort).limit(Math.max(responseMaxSize * 2, 256)) as any[];
                const hasDerivedSort = this.usesDerivedMetricSort(sort);
                const sortedHistory = hasDerivedSort
                    ? latestTrHistory.slice(0, Math.max(responseMaxSize * 2, 256))
                    : latestTrHistory.slice(0, responseMaxSize);

                if (sortedHistory.length === 0) {
                    return ApiResponder.success(ctx, { trust_registries: [] }, 200);
                }
                const versionsByTrId = await this.buildHistoricalVersionsByTrIds(
                    sortedHistory.map((row: any) => Number(row.tr_id)),
                    blockHeight,
                    activeGfOnly,
                    preferredLanguage
                );
                const statsByTrId = await calculateTrustRegistryStatsBatch(
                    sortedHistory.map((row: any) => Number(row.tr_id)).filter((id: number) => Number.isFinite(id) && id > 0),
                    blockHeight
                );

                const registriesWithStats = await Promise.all(
                    sortedHistory.map(async (trHistory: any) => {
                        const trId = trHistory.tr_id;
                        const filteredVersions = versionsByTrId.get(Number(trId)) || [];

                        const stats = statsByTrId.get(Number(trId)) || {
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
                            participants_ecosystem: stats.participants_ecosystem,
                            participants_issuer_grantor: stats.participants_issuer_grantor,
                            participants_issuer: stats.participants_issuer,
                            participants_verifier_grantor: stats.participants_verifier_grantor,
                            participants_verifier: stats.participants_verifier,
                            participants_holder: stats.participants_holder,
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

                const filteredRegistries = this.applyMetricFiltersToRegistries(
                    registriesWithStats.filter((r): r is NonNullable<typeof registriesWithStats[0]> => r !== null),
                    metricFilters
                );

                const sortedRegistries = this.sortRegistries(filteredRegistries, sort, responseMaxSize);

                return ApiResponder.success(ctx, { trust_registries: sortedRegistries }, 200);
            }

            let query = TrustRegistry.query();

            if (participantAccount) {
                const participantTrIds = await this.getTrustRegistryIdsForParticipant(participantAccount);
                if (participantTrIds.length === 0) {
                    return ApiResponder.success(ctx, { trust_registries: [] }, 200);
                }
                query = query.where("id", "in", participantTrIds) as any;
            }
            if (controllerAccount) {
                query = query.where("controller", controllerAccount);
            }

            query = query.withGraphFetched("governanceFrameworkVersions.documents") as any;
            query = this.applyRangeToQuery(query, "participants", minParticipants, maxParticipants);
            query = this.applyRangeToQuery(query, "participants_ecosystem", minParticipantsEcosystem, maxParticipantsEcosystem);
            query = this.applyRangeToQuery(query, "participants_issuer_grantor", minParticipantsIssuerGrantor, maxParticipantsIssuerGrantor);
            query = this.applyRangeToQuery(query, "participants_issuer", minParticipantsIssuer, maxParticipantsIssuer);
            query = this.applyRangeToQuery(query, "participants_verifier_grantor", minParticipantsVerifierGrantor, maxParticipantsVerifierGrantor);
            query = this.applyRangeToQuery(query, "participants_verifier", minParticipantsVerifier, maxParticipantsVerifier);
            query = this.applyRangeToQuery(query, "participants_holder", minParticipantsHolder, maxParticipantsHolder);
            query = this.applyRangeToQuery(query, "active_schemas", minActiveSchemas, maxActiveSchemas);
            query = this.applyRangeToQuery(
                query,
                "weight",
                minWeight !== undefined ? Number(minWeight) : undefined,
                maxWeight !== undefined ? Number(maxWeight) : undefined
            );
            query = this.applyRangeToQuery(
                query,
                "issued",
                minIssued !== undefined ? Number(minIssued) : undefined,
                maxIssued !== undefined ? Number(maxIssued) : undefined
            );
            query = this.applyRangeToQuery(
                query,
                "verified",
                minVerified !== undefined ? Number(minVerified) : undefined,
                maxVerified !== undefined ? Number(maxVerified) : undefined
            );
            query = this.applyRangeToQuery(query, "ecosystem_slash_events", minEcosystemSlashEvents, maxEcosystemSlashEvents);
            query = this.applyRangeToQuery(query, "network_slash_events", minNetworkSlashEvents, maxNetworkSlashEvents);

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

            const registries = await query.limit(responseMaxSize);

            const registriesWithStats = registries.map((tr) => {
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

                return {
                    ...plain,
                    versions,
                    participants: Number(plain.participants || 0),
                    participants_ecosystem: Number((plain as any).participants_ecosystem || 0),
                    participants_issuer_grantor: Number((plain as any).participants_issuer_grantor || 0),
                    participants_issuer: Number((plain as any).participants_issuer || 0),
                    participants_verifier_grantor: Number((plain as any).participants_verifier_grantor || 0),
                    participants_verifier: Number((plain as any).participants_verifier || 0),
                    participants_holder: Number((plain as any).participants_holder || 0),
                    active_schemas: Number(plain.active_schemas || 0),
                    archived_schemas: Number(plain.archived_schemas || 0),
                    weight: Number(plain.weight || 0),
                    issued: Number(plain.issued || 0),
                    verified: Number(plain.verified || 0),
                    ecosystem_slash_events: Number(plain.ecosystem_slash_events || 0),
                    ecosystem_slashed_amount: Number(plain.ecosystem_slashed_amount || 0),
                    ecosystem_slashed_amount_repaid: Number(plain.ecosystem_slashed_amount_repaid || 0),
                    network_slash_events: Number(plain.network_slash_events || 0),
                    network_slashed_amount: Number(plain.network_slashed_amount || 0),
                    network_slashed_amount_repaid: Number(plain.network_slashed_amount_repaid || 0),
                };
            });

            const sortedRegistries = this.sortRegistries(registriesWithStats, sort, responseMaxSize);

            return ApiResponder.success(ctx, { trust_registries: sortedRegistries }, 200);
        } catch (err: any) {
            return ApiResponder.error(ctx, err.message, 500);
        }
    }


    private async getTrustRegistryIdsForParticipant(account: string): Promise<number[]> {
        const controllerRows = await knex("trust_registry")
            .where("controller", account)
            .select("id");
        const controllerIds = controllerRows.map((r: { id: number }) => r.id);

        const granteeSchemaIds = await knex("permissions")
            .where("grantee", account)
            .distinct("schema_id");
        const schemaIds = granteeSchemaIds
            .map((r: { schema_id: string }) => {
                const id = r.schema_id ? parseFloat(r.schema_id) : null;
                return id !== null && !Number.isNaN(id) ? id : null;
            })
            .filter((id): id is number => id !== null);
        if (schemaIds.length === 0) {
            return [...new Set(controllerIds)];
        }
        const trIdRows = await knex("credential_schemas")
            .whereIn("id", schemaIds)
            .distinct("tr_id");
        const granteeTrIds = trIdRows.map((r: { tr_id: number }) => r.tr_id);

        return [...new Set([...controllerIds, ...granteeTrIds])];
    }


    private async getTrustRegistryIdsForParticipantAtHeight(account: string, blockHeight: number): Promise<number[]> {
        const trHistoryRows = await knex("trust_registry_history")
            .where("height", "<=", blockHeight)
            .where("controller", account)
            .select("tr_id");
        const controllerTrIds = [...new Set(trHistoryRows.map((r: { tr_id: number }) => r.tr_id))];

        const granteePermRows = await knex("permission_history")
            .where("height", "<=", blockHeight)
            .where("grantee", account)
            .distinct("schema_id");
        const schemaIds = granteePermRows.map((r: { schema_id: number }) => r.schema_id).filter((id): id is number => id != null);
        if (schemaIds.length === 0) {
            return controllerTrIds;
        }

        const cshSub = knex("credential_schema_history")
            .select("credential_schema_id", "tr_id")
            .select(knex.raw("ROW_NUMBER() OVER (PARTITION BY credential_schema_id ORDER BY height DESC, created_at DESC) as rn"))
            .where("height", "<=", blockHeight)
            .whereIn("credential_schema_id", schemaIds)
            .as("ranked");
        const latestCsh = await knex.from(cshSub).where("rn", 1).select("tr_id");
        const granteeTrIds = [...new Set(latestCsh.map((r: { tr_id: number }) => r.tr_id))];

        return [...new Set([...controllerTrIds, ...granteeTrIds])];
    }

    @Action()
    public async getParams(ctx: Context) {
        const { getModuleParamsAction } = await import("../../common/utils/params_service");
        return getModuleParamsAction(ctx, ModulesParamsNamesTypes.TR, MODULE_DISPLAY_NAMES.TRUST_REGISTRY);
    }
}
