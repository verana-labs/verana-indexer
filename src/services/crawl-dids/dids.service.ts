import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended';
import { Context, ServiceBroker } from 'moleculer';
import BullableService from '../../base/bullable.service';
import { SERVICE } from '../../common';
import knex from '../../common/utils/db_connection';

@Service({
    name: SERVICE.V1.DidDatabaseService.key,
    version: 1
})
export default class DidDatabaseService extends BullableService {

    constructor(broker: ServiceBroker) {
        super(broker);
    }

    @Action({ name: 'upsertProcessedDid' })
    async upsertProcessedDid(ctx: any) {
        return await knex('dids').insert(ctx.params)
            .onConflict('did')
            .merge();
    }

    @Action({ name: 'delete' })
    async deleteDid(ctx: any) {
        const did = ctx.params.did.trim();

        if (!did) {
            this.logger.warn('Missing DID in delete action');
            throw new Error('DID is required for deletion');
        }

        this.logger.info(`Attempting to delete DID: ${did}`);

        const deletedRows = await knex('dids')
            .where({ did })
            .update({ is_deleted: true, deleted_at: new Date().toISOString(), event_type: "remove_did" });

        if (deletedRows === 0) {
            this.logger.warn(`No DID found to delete: ${did}`);
            return { success: false, message: `No record found for DID: ${did}` };
        }

        this.logger.info(`Marked DID as deleted: ${did}`);
        return { success: true };
    }

    @Action({ name: 'get' })
    async getDid(ctx: any) {
        const { did } = ctx.params;
        return await knex('dids').where({ did }).first();
    }


    @Action({ rest: "GET get/:did", params: { did: "string" } })
    async getSingleDid(ctx: Context<{ did: string }>) {
        const did = await knex("dids").where({ did: ctx.params.did }).first();
        if (!did) throw new Error("DID not found");
        return did;
    }

    @Action({ rest: "GET list", params: { page: { type: "number", optional: true, default: 1 }, limit: { type: "number", optional: true, default: 20 } } })
    async getDidList(ctx: Context<{ page: number; limit: number }>) {
        const { page, limit } = ctx.params;
        const offset = (page - 1) * limit;

        const totalResult = await knex("dids").count("did as count");
        const total = Number(totalResult[0].count);

        const items = await knex("dids").select("*").limit(limit).offset(offset);
        return { total, page, limit, items };
    }
}
