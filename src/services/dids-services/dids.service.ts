import { ServiceBroker } from 'moleculer';
import { Service, Action, Event, Method } from '@ourparentcenter/moleculer-decorators-extended';
import BullableService from '../../base/bullable.service';
import knex from '../../common/utils/db_connection';
import { SERVICE } from '../../common';

@Service({
    name: SERVICE.V1.DIDDatabaseService.key, 
    version: 1
})

export default class DIDDatabaseService extends BullableService {

    constructor(broker: ServiceBroker) {
        super(broker);
    }

    @Action({
        name: 'upsert'
    })

    async upsertDID(ctx: any) {
        const { did, controller, created, modified, exp, deposit ,height } = ctx.params;
        return await knex('dids')
            .insert({ did, controller, created, modified, exp, deposit ,height})
            .onConflict('did')
            .merge();
    }

    @Action({
        name: 'delete'
    })
    async deleteDID(ctx: any) {
        const did = ctx.params.did.trim();

        if (!did) {
            this.logger.warn('Missing DID in delete action');
            throw new Error('DID is required for deletion');
        }

        this.logger.info(`Attempting to delete DID: ${ctx.params}`);

        const deletedRows = await knex('dids').where({ did }).del();

        if (deletedRows === 0) {
            this.logger.warn(`No DID found to delete: ${did}`);
            return { success: false, message: `No record found for DID: ${did}` };
        }

        this.logger.info(`Deleted DID: ${did}`);
        return { success: true };
    }


    @Action({
        name: 'get'
    })
    async getDID(ctx: any) {
        const { did } = ctx.params;
        return await knex('dids').where({ did }).first();
    }
}