import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import { SERVICE } from "../../common";
import ApiResponder from "../../common/utils/apiResponse";
import knex from "../../common/utils/db_connection";
import ModuleParams from "../../models/modules_params";


@Service({
    name: SERVICE.V1.DidDatabaseService.key,
    version: 1
})
export default class DidDatabaseService extends BullableService {
    constructor(broker: ServiceBroker) {
        super(broker);
    }

    @Action({ name: "upsertProcessedDid" })
    async upsertProcessedDid(ctx: Context<{ did: string; [key: string]: unknown }>) {
        try {
            const result = await knex("dids")
                .insert(ctx.params)
                .onConflict("did")
                .merge();

            return ApiResponder.success(ctx, { success: true, result }, 200);
        } catch (err: any) {
            this.logger.error("Error in upsertProcessedDid:", err);
            return ApiResponder.error(ctx, "Failed to upsert DID", 500);
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
            return ApiResponder.error(ctx, "Internal Server Error", 500);
        }
    }

    @Action({ name: "get" })
    async getDid(ctx: Context<{ did: string }>) {
        try {
            const did = await knex("dids").where({ did: ctx.params.did }).first();

            if (!did) {
                return ApiResponder.error(ctx, "DID not found", 404);
            }

            return ApiResponder.success(ctx, did, 200);
        } catch (err: any) {
            this.logger.error("Error in getDid:", err);
            return ApiResponder.error(ctx, "Internal Server Error", 500);
        }
    }

    @Action({ rest: "GET get/:did", params: { did: "string" } })
    async getSingleDid(ctx: Context<{ did: string }>) {
        try {
            const did = await knex("dids").where({ did: ctx.params.did }).first();
            if (!did) {
                return ApiResponder.error(ctx, "Not Found", 404);
            }
            return ApiResponder.success(ctx, did, 200);
        } catch (err: any) {
            this.logger.error("DB error in getSingleDid:", err);
            return ApiResponder.error(ctx, "Internal Server Error", 500);
        }
    }

    @Action({
        rest: "GET list",
        params: {
            page: { type: "number", optional: true, default: 1 },
            account: { type: "string", optional: true },
            modified: { type: "string", optional: true },
            expired: { type: "boolean", optional: true },
            over_grace: { type: "boolean", optional: true },
            response_max_size: { type: "number", optional: true, default: 64 }
        }
    })
    async getDidList(ctx: Context<{
        page: number;
        account?: string;
        modified?: string;
        expired?: boolean;
        over_grace?: boolean;
        response_max_size?: number;
    }>) {
        try {
            const {
                page,
                account,
                modified,
                expired,
                over_grace: overGrace,
                response_max_size: responseMaxSize
            } = ctx.params;

            const effectiveLimit = Math.min(responseMaxSize || 64, 1024);
            const offset = (page - 1) * effectiveLimit;
            const now = new Date().toISOString();

            let query = knex("dids").where({ is_deleted: false });

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

            const totalResult = await query.clone().count("did as count");
            const total = Number(totalResult[0].count);

            const items = await query
                .orderBy("modified", "asc")
                .limit(effectiveLimit)
                .offset(offset);

            return ApiResponder.success(ctx, {
                total,
                page,
                responseMaxSize: effectiveLimit,
                items
            }, 200);
        } catch (err: any) {
            this.logger.error("DB error in getDidList:", err);
            return ApiResponder.error(ctx, "Internal Server Error", 500);
        }
    }

    @Action({ rest: "GET params" })
    public async getDidParams(ctx: Context) {
        try {
            const module = await ModuleParams.query().findOne({ module: "diddirectory" });

            if (!module || !module.params) {
                return ApiResponder.error(ctx, "Module parameters not found: diddirectory", 404);
            }

            const parsedParams =
                typeof module.params === "string"
                    ? JSON.parse(module.params)
                    : module.params;

            return ApiResponder.success(ctx, parsedParams.params || {}, 200);
        } catch (err: any) {
            this.logger.error("Error fetching diddirectory params", err);
            return ApiResponder.error(ctx, "Internal Server Error", 500);
        }
    }
}
