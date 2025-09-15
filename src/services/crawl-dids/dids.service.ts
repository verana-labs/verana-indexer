import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended';
import { Context, Errors, ServiceBroker } from 'moleculer';
import BullableService from '../../base/bullable.service';
import { SERVICE } from '../../common';
import knex from '../../common/utils/db_connection';

const { MoleculerClientError, MoleculerServerError } = Errors;

@Service({
    name: SERVICE.V1.DidDatabaseService.key,
    version: 1
})
export default class DidDatabaseService extends BullableService {
    constructor(broker: ServiceBroker) {
        super(broker);
    }

    @Action({ name: 'upsertProcessedDid' })
    async upsertProcessedDid(ctx: Context<{ did: string; [key: string]: unknown }>) {
        return knex('dids')
            .insert(ctx.params)
            .onConflict('did')
            .merge();
    }

    @Action({ name: 'delete' })
    async deleteDid(ctx: Context<{ did: string }>) {
        const did = ctx.params.did?.trim();

        if (!did) {
            this.logger.warn('Missing DID in delete action');
            throw new Error('DID is required for deletion');
        }

        this.logger.info(`Attempting to delete DID: ${did}`);

        const deletedRows = await knex('dids')
            .where({ did })
            .update({
                is_deleted: true,
                deleted_at: new Date().toISOString(),
                event_type: "remove_did"
            });

        if (deletedRows === 0) {
            this.logger.warn(`No DID found to delete: ${did}`);
            return { success: false, message: `No record found for DID: ${did}` };
        }

        this.logger.info(`Marked DID as deleted: ${did}`);
        return { success: true };
    }

    @Action({ name: 'get' })
    async getDid(ctx: Context<{ did: string }>) {
        return knex('dids').where({ did: ctx.params.did }).first();
    }

    @Action({ rest: "GET get/:did", params: { did: "string" } })
    async getSingleDid(ctx: Context<{ did: string }>) {
        try {
            const did = await knex("dids").where({ did: ctx.params.did }).first();
            if (!did) {
                throw new MoleculerClientError("Not Found", 404, "NOT_FOUND");
            }
            return did;
        } catch (err) {
            this.logger.error("DB error in getSingleDid:", err);
            throw new MoleculerServerError("Internal Server Error", 500);
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

            if (account) {
                query = query.andWhere("controller", account);
            }

            if (modified) {
                query = query.andWhere("modified", ">", modified);
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

            const totalResult = await query.clone().count("did as count");
            const total = Number(totalResult[0].count);

            const items = await query
                .orderBy("modified", "asc")
                .limit(effectiveLimit)
                .offset(offset);

            return {
                total,
                page,
                responseMaxSize: effectiveLimit,
                items
            };
        } catch (err) {
            this.logger.error("DB error in getDidList:", err);
            throw new MoleculerServerError("Internal Server Error", 500);
        }
    }
}
