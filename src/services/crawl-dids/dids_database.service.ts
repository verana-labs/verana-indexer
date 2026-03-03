import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import { ModulesParamsNamesTypes, MODULE_DISPLAY_NAMES, SERVICE } from "../../common";
import { validateParticipantParam } from "../../common/utils/accountValidation";
import ApiResponder from "../../common/utils/apiResponse";
import knex from "../../common/utils/db_connection";
import { applyOrdering, validateSortParameter } from "../../common/utils/query_ordering";

function isValidDid(did: string): boolean {
    const didRegex = /^did:[a-z0-9]+:[A-Za-z0-9.\-_%]+$/;
    // return didRegex.test(did);
    return true;
}

@Service({
    name: SERVICE.V1.DidDatabaseService.key,
    version: 1
})
export default class DidDatabaseService extends BullableService {
    private didHistoryColumnExistsCache = new Map<string, boolean>();

    constructor(broker: ServiceBroker) {
        super(broker);
    }

    private async hasDidHistoryColumn(column: string): Promise<boolean> {
        const cached = this.didHistoryColumnExistsCache.get(column);
        if (cached !== undefined) {
            return cached;
        }
        const exists = await knex.schema.hasColumn("did_history", column);
        this.didHistoryColumnExistsCache.set(column, exists);
        return exists;
    }

    @Action({ name: "upsertProcessedDid" })
    async upsertProcessedDid(ctx: Context<{ did: string;[key: string]: unknown }>) {
        try {
            const result = await knex("dids")
                .insert(ctx.params)
                .onConflict("did")
                .merge();

            return ApiResponder.success(ctx, { success: true, result }, 200);
        } catch (err: any) {
            this.logger.error("Error in upsertProcessedDid:", err);
            console.error("FATAL DID UPSERT ERROR:", err);
            return ApiResponder.error(ctx, "Internal Server Error", 500);
        }
    }

    @Action({ name: "delete" })
    async deleteDid(ctx: Context<{ did: string }>) {
        try {
            const did = ctx.params.did?.trim();

            if (!did) {
                this.logger.warn("Missing DID in delete action");
                return ApiResponder.error(ctx, "Missing DID", 400);
            }

            this.logger.info(`Attempting to delete DID: ${did}`);

            const deletedRows = await knex("dids")
                .where({ did })
                .update({
                    is_deleted: true,
                    deleted_at: new Date().toISOString(),
                    event_type: "remove_did"
                });

            if (deletedRows === 0) {
                this.logger.warn(`No DID found to delete: ${did}`);
                return ApiResponder.error(ctx, `No record found for DID: ${did}`, 404);
            }

            this.logger.info(`Marked DID as deleted: ${did}`);
            return ApiResponder.success(ctx, { success: true }, 200);
        } catch (err: any) {
            this.logger.error("Error in deleteDid:", err);
            console.error("FATAL DID DELETE ERROR:", err);
            return ApiResponder.error(ctx, "Internal Server Error", 500);
        }
    }

    @Action({ name: 'get' })
    async getDid(ctx: any) {
        const { did } = ctx.params;
        return await knex('dids').where({ did }).first();
    }



    @Action({ rest: "GET get/:did", params: { did: "string" } })
    async getSingleDid(ctx: Context<{ did: string }>) {
        try {
            const { did } = ctx.params;
            const blockHeight = (ctx.meta as any)?.blockHeight;

            if (!isValidDid(did)) {
                this.logger.warn(`Invalid DID syntax received: ${did}`);
                return ApiResponder.error(ctx, "Invalid DID syntax", 400);
            }

            // If AtBlockHeight is provided, query historical state
            if (typeof blockHeight === "number") {
                const historyRecord = await knex("did_history")
                    .where({ did })
                    .where("height", "<=", blockHeight)
                    .orderBy("height", "desc")
                    .orderBy("created_at", "desc")
                    .first();

                if (!historyRecord || historyRecord.is_deleted) {
                    return ApiResponder.error(ctx, "Not Found", 404);
                }

                const historicalDid = {
                    did: historyRecord.did,
                    controller: historyRecord.controller,
                    deposit: historyRecord.deposit ?? 0,
                    exp: historyRecord.exp,
                    created: historyRecord.created,
                    modified: historyRecord.modified,
                };

                return ApiResponder.success(ctx, { did: historicalDid }, 200);
            }

            // Otherwise, return latest state
            const record = await knex("dids")
                .where({ did })
                .select("did", "controller", "deposit", "exp", "created", "modified")
                .first();

            if (!record) {
                return ApiResponder.error(ctx, "Not Found", 404);
            }

            return ApiResponder.success(ctx, { did: record }, 200);
        } catch (err: any) {
            this.logger.error("DB error in getSingleDid:", err);
            return ApiResponder.error(ctx, "Internal Server Error", 500);
        }
    }



    @Action({
        rest: "GET list",
        params: {
            account: { type: "string", optional: true },
            modified: { type: "string", optional: true },
            expired: { type: "boolean", optional: true, convert: true },
            over_grace: { type: "boolean", optional: true, convert: true },
            response_max_size: { type: "number", optional: true, default: 64, convert: true },
            sort: { type: "string", optional: true }
        }
    })

    async getDidList(ctx: Context<{
        account?: string;
        modified?: string;
        expired?: boolean;
        over_grace?: boolean;
        response_max_size?: number;
        sort?: string;
    }>) {
        try {
            const {
                account,
                modified,
                expired,
                over_grace: overGrace,
                response_max_size: responseMaxSize,
                sort
            } = ctx.params;

            const accountValidation = validateParticipantParam(account, "account");
            if (!accountValidation.valid) {
                return ApiResponder.error(ctx, accountValidation.error, 400);
            }
            const accountFilter = accountValidation.value;

            if (modified) {
                const { isValidISO8601UTC } = await import("../../common/utils/date_utils");
                if (!isValidISO8601UTC(modified)) {
                    return ApiResponder.error(
                        ctx,
                        "Invalid modified format. Must be ISO 8601 UTC format (e.g., '2026-01-18T10:00:00Z' or '2026-01-18T10:00:00.000Z')",
                        400
                    );
                }
                const modifiedDate = new Date(modified);
                if (Number.isNaN(modifiedDate.getTime())) {
                    return ApiResponder.error(ctx, "Invalid modified format", 400);
                }
            }

            try {
                validateSortParameter(sort);
            } catch (err: any) {
                return ApiResponder.error(ctx, err.message, 400);
            }

            const blockHeight = (ctx.meta as any)?.blockHeight;
            const effectiveLimit = Math.min(responseMaxSize || 64, 1024);
            const now = new Date().toISOString();
            const graceThreshold = new Date(Date.now() - (30 * 24 * 60 * 60 * 1000)).toISOString();

            // If AtBlockHeight is provided, query historical state
            if (typeof blockHeight === "number") {
                const [hasModifiedColumn, hasIsDeletedColumn] = await Promise.all([
                    this.hasDidHistoryColumn("modified"),
                    this.hasDidHistoryColumn("is_deleted"),
                ]);
                const modifiedIso = modified && !Number.isNaN(new Date(modified).getTime())
                    ? new Date(modified).toISOString()
                    : undefined;
                const modifiedSelect = hasModifiedColumn
                    ? "dh.modified"
                    : knex.raw("dh.created as modified");
                let latestQuery: any;
                if (String((knex as any)?.client?.config?.client || "").includes("pg")) {
                    const latest = knex("did_history as dh")
                        .distinctOn("dh.did")
                        .select("dh.id", "dh.did", "dh.controller", "dh.deposit", "dh.exp", "dh.created", modifiedSelect)
                        .where("dh.height", "<=", blockHeight)
                        .orderBy("dh.did", "asc")
                        .orderBy("dh.height", "desc")
                        .orderBy("dh.created_at", "desc")
                        .orderBy("dh.id", "desc")
                        .as("latest");
                    if (hasIsDeletedColumn) {
                        latest.where("dh.is_deleted", false);
                    }
                    latestQuery = knex.from(latest).select("*");
                } else {
                    const ranked = knex("did_history as dh")
                        .select(
                            "dh.id",
                            "dh.did",
                            "dh.controller",
                            "dh.deposit",
                            "dh.exp",
                            "dh.created",
                            modifiedSelect,
                            knex.raw("ROW_NUMBER() OVER (PARTITION BY dh.did ORDER BY dh.height DESC, dh.created_at DESC, dh.id DESC) as rn")
                        )
                        .where("dh.height", "<=", blockHeight)
                        .as("ranked");
                    if (hasIsDeletedColumn) {
                        ranked.where("dh.is_deleted", false);
                    }
                    latestQuery = knex.from(ranked).select("id", "did", "controller", "deposit", "exp", "created", "modified").where("rn", 1);
                }

                if (accountFilter) latestQuery.where("controller", accountFilter);
                if (modifiedIso) {
                    latestQuery.where(hasModifiedColumn ? "modified" : "created", ">", modifiedIso);
                }
                if (expired !== undefined) {
                    if (expired) latestQuery.where("exp", "<", now);
                    else latestQuery.where("exp", ">", now);
                }
                if (overGrace !== undefined) {
                    if (overGrace) latestQuery.where("exp", "<", graceThreshold);
                    else latestQuery.where("exp", ">=", graceThreshold);
                }

                const orderedQuery = applyOrdering(latestQuery, sort);
                const filteredItems = await orderedQuery.limit(effectiveLimit);
                const dids = filteredItems.map((item: any) => {
                    const { id, ...did } = item;
                    return did;
                });
                return ApiResponder.success(ctx, { dids }, 200);
            }

            // Otherwise, return latest state
            let query = knex("dids").where({ is_deleted: false }).select(
                "did",
                "controller",
                "deposit",
                "exp",
                "created",
                "modified",
            );

            if (accountFilter) query = query.andWhere("controller", accountFilter);
            if (modified) {
                const modifiedDate = new Date(modified);
                if (!Number.isNaN(modifiedDate.getTime())) {
                    query = query.andWhere("modified", ">", modifiedDate.toISOString());
                }
            }

            if (expired !== undefined) {
                query = expired
                    ? query.andWhere("exp", "<", now)
                    : query.andWhere("exp", ">", now);
            }

            if (overGrace !== undefined) {
                query = overGrace
                    ? query.andWhere("exp", "<", graceThreshold)
                    : query.andWhere("exp", ">=", graceThreshold);
            }

            // Apply ordering
            const orderedQuery = applyOrdering(query, sort);
            const items = await orderedQuery.limit(effectiveLimit);

            return ApiResponder.success(ctx, { dids: items }, 200);
        } catch (err: any) {
            this.logger.error("DB error in getDidList:", err);
            return ApiResponder.error(ctx, "Internal Server Error", 500);
        }
    }

    @Action({ rest: "GET params" })
    public async getDidParams(ctx: Context) {
        const { getModuleParamsAction } = await import("../../common/utils/params_service");
        return getModuleParamsAction(ctx, ModulesParamsNamesTypes.DD, MODULE_DISPLAY_NAMES.DID_DIRECTORY);
    }
}
