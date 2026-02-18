import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import { ModulesParamsNamesTypes, MODULE_DISPLAY_NAMES, SERVICE } from "../../common";
import { validateParticipantParam } from "../../common/utils/accountValidation";
import ApiResponder from "../../common/utils/apiResponse";
import knex from "../../common/utils/db_connection";
import { applyOrdering, validateSortParameter, sortByStandardAttributes } from "../../common/utils/query_ordering";

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
    constructor(broker: ServiceBroker) {
        super(broker);
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

            // If AtBlockHeight is provided, query historical state
            if (typeof blockHeight === "number") {
                // Get all unique DIDs that existed at or before the block height
                const historyQuery = knex("did_history")
                    .select("did")
                    .where("height", "<=", blockHeight)
                    .where("is_deleted", false)
                    .groupBy("did");

                // Get the latest state for each DID at the given block height
                const subquery = knex("did_history")
                    .select("did")
                    .select(
                        knex.raw(
                            `ROW_NUMBER() OVER (PARTITION BY did ORDER BY height DESC, created_at DESC) as rn`
                        )
                    )
                    .where("height", "<=", blockHeight)
                    .where("is_deleted", false)
                    .as("ranked");

                const latestHistory = await knex
                    .from(subquery)
                    .select("did")
                    .where("rn", 1);

                const didsAtHeight = latestHistory.map((r: any) => r.did);

                if (didsAtHeight.length === 0) {
                    return ApiResponder.success(ctx, { dids: [] }, 200);
                }

                const items = await Promise.all(
                    didsAtHeight.map(async (did: string) => {
                        const historyRecord = await knex("did_history")
                            .where({ did })
                            .where("height", "<=", blockHeight)
                            .where("is_deleted", false)
                            .orderBy("height", "desc")
                            .orderBy("created_at", "desc")
                            .first();

                        if (!historyRecord) return null;

                        return {
                            did: historyRecord.did,
                            controller: historyRecord.controller,
                            deposit: historyRecord.deposit ?? 0,
                            exp: historyRecord.exp,
                            created: historyRecord.created,
                            modified: historyRecord.modified,
                        };
                    })
                );

                // Filter out nulls and apply filters
                let filteredItems = items.filter((item): item is NonNullable<typeof items[0]> => item !== null);

                if (accountFilter) filteredItems = filteredItems.filter(item => item.controller === accountFilter);
                if (modified) {
                    const modifiedDate = new Date(modified);
                    if (!Number.isNaN(modifiedDate.getTime())) {
                        filteredItems = filteredItems.filter(item => new Date(item.modified) > modifiedDate);
                    }
                }
                if (expired !== undefined) {
                    filteredItems = expired
                        ? filteredItems.filter(item => new Date(item.exp) < new Date(now))
                        : filteredItems.filter(item => new Date(item.exp) > new Date(now));
                }
                if (overGrace !== undefined) {
                    filteredItems = overGrace
                        ? filteredItems.filter(item => {
                            const graceDate = new Date(item.exp);
                            graceDate.setDate(graceDate.getDate() + 30);
                            return graceDate < new Date(now);
                        })
                        : filteredItems.filter(item => {
                            const graceDate = new Date(item.exp);
                            graceDate.setDate(graceDate.getDate() + 30);
                            return graceDate >= new Date(now);
                        });
                }

                // Sort and limit (reusable in‑memory helper)
                type FilteredDidItem = {
                    did: string;
                    controller: any;
                    deposit: any;
                    exp: any;
                    created: string;
                    modified: string;
                };
                const typedFilteredItems = filteredItems as FilteredDidItem[];
                filteredItems = sortByStandardAttributes<FilteredDidItem>(typedFilteredItems, sort, {
                    getId: (item) => item.did,
                    getCreated: (item) => item.created,
                    getModified: (item) => item.modified,
                    defaultAttribute: "modified",
                    defaultDirection: "desc",
                }).slice(0, effectiveLimit) as typeof filteredItems;

                return ApiResponder.success(ctx, { dids: filteredItems }, 200);
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
                    ? query.andWhereRaw(`exp + interval '30 days' < ?`, [now])
                    : query.andWhereRaw(`exp + interval '30 days' >= ?`, [now]);
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
