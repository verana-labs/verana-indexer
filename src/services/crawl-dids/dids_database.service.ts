import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import { ModulesParamsNamesTypes, SERVICE } from "../../common";
import ApiResponder from "../../common/utils/apiResponse";
import knex from "../../common/utils/db_connection";
import ModuleParams from "../../models/modules_params";

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
                    deposit: historyRecord.deposit,
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
            response_max_size: { type: "number", optional: true, default: 64, convert: true }
        }
    })

    async getDidList(ctx: Context<{
        account?: string;
        modified?: string;
        expired?: boolean;
        over_grace?: boolean;
        response_max_size?: number;
    }>) {
        try {
            const {
                account,
                modified,
                expired,
                over_grace: overGrace,
                response_max_size: responseMaxSize
            } = ctx.params;

            const blockHeight = (ctx.meta as any)?.blockHeight;
            const effectiveLimit = Math.min(responseMaxSize || 64, 1024);
            const now = new Date().toISOString();

            if (typeof blockHeight === "number") {
                const historyQuery = knex("did_history")
                    .select("did")
                    .where("height", "<=", blockHeight)
                    .where("is_deleted", false)
                    .groupBy("did");

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
                            deposit: historyRecord.deposit,
                            exp: historyRecord.exp,
                            created: historyRecord.created,
                            modified: historyRecord.modified,
                        };
                    })
                );

                let filteredItems = items.filter((item): item is NonNullable<typeof items[0]> => item !== null);

                if (account) filteredItems = filteredItems.filter(item => item.controller === account);
                if (modified) {
                    const modifiedDate = new Date(modified);
                    filteredItems = filteredItems.filter(item => new Date(item.modified) > modifiedDate);
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

                filteredItems.sort((a, b) => new Date(a.modified).getTime() - new Date(b.modified).getTime());
                filteredItems = filteredItems.slice(0, effectiveLimit);

                return ApiResponder.success(ctx, { dids: filteredItems }, 200);
            }

            let query = knex("dids").where({ is_deleted: false }).select(
                "did",
                "controller",
                "deposit",
                "exp",
                "created",
                "modified",
            );

            if (account) query = query.andWhere("controller", account);
            if (modified) query = query.andWhere("modified", ">", modified);

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

            const items = await query
                .orderBy("modified", "asc")
                .limit(effectiveLimit);

            return ApiResponder.success(ctx, { dids: items }, 200);
        } catch (err: any) {
            this.logger.error("DB error in getDidList:", err);
            return ApiResponder.error(ctx, "Internal Server Error", 500);
        }
    }

    @Action({ rest: "GET params" })
    public async getDidParams(ctx: Context) {
        try {
            const blockHeight = (ctx.meta as any)?.blockHeight;

            if (typeof blockHeight === "number") {
                const historyRecord = await knex("module_params_history")
                    .where({ module: ModulesParamsNamesTypes?.DD })
                    .where("height", "<=", blockHeight)
                    .orderBy("height", "desc")
                    .orderBy("created_at", "desc")
                    .first();

                if (!historyRecord || !historyRecord.params) {
                    return ApiResponder.error(ctx, "Module parameters not found: diddirectory", 404);
                }

                const parsedParams =
                    typeof historyRecord.params === "string"
                        ? JSON.parse(historyRecord.params)
                        : historyRecord.params;

                return ApiResponder.success(ctx, { params: parsedParams.params || parsedParams }, 200);
            }

            const module = await ModuleParams.query().findOne({ module: ModulesParamsNamesTypes?.DD });

            if (!module || !module.params) {
                return ApiResponder.error(ctx, "Module parameters not found: diddirectory", 404);
            }

            const parsedParams =
                typeof module.params === "string"
                    ? JSON.parse(module.params)
                    : module.params;

            return ApiResponder.success(ctx, { params: parsedParams.params }, 200);
        } catch (err: any) {
            this.logger.error("Error fetching diddirectory params", err);
            return ApiResponder.error(ctx, "Internal Server Error", 500);
        }
    }
}
