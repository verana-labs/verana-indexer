import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { ServiceBroker, Context } from "moleculer";
import BullableService from "../../base/bullable.service";
import { SERVICE, trustRegistryEvents } from "../../common";
import knex from "../../common/utils/db_connection";
import { formatTimestamp } from "../../common/utils/date_utils";

@Service({
    name: SERVICE.V1.ProcessTREventsService.key,
    version: 1,
})
export default class ProcessTREventsService extends BullableService {
    constructor(broker: ServiceBroker) {
        super(broker);
    }

    @Action({ name: "handleTREvents" })
    async handleTREvents(ctx: Context<{ trustRegistryList: any[] }>) {
        const { trustRegistryList } = ctx.params;

        for (const event of trustRegistryList) {
            const flattened: any = { ...event, ...event.content };

            if (event.content?.id) {
                flattened.trust_registry_id = event.content.id;
            }

            delete flattened?.content;
            delete flattened?.id;
            delete flattened?.tx_id;
            delete flattened?.['@type'];


            if (flattened.type === trustRegistryEvents[0] || flattened.type === trustRegistryEvents[1]) {
                await this.processCreateTREvent(flattened);
            }
            if (flattened.type === trustRegistryEvents[4]) {
                await this.processAddGovFrameworkDocEvent(flattened);
            }
            if (flattened.type === trustRegistryEvents[2]) {
                await this.processUpdateTREvent(flattened);
            }
            if (flattened.type === trustRegistryEvents[5]) {
                await this.processIncreaseActiveGFVEvent(flattened);
            }
            if (flattened.type === trustRegistryEvents[3]) {
                await this.processArchiveTREvent(flattened);
            }

            // console.log(flattened, "trustRegistryList");

        }

    }


    private async processArchiveTREvent(event: any) {
        const trx = await knex.transaction();

        try {
            if (typeof event.trust_registry_id !== "number" || typeof event.archive !== "boolean") {
                this.logger.warn("⚠️ Missing or invalid parameters in ArchiveTrustRegistry event. Skipping.");
                await trx.rollback();
                return;
            }

            const tr = await trx("trust_registry")
                .where({ id: event.trust_registry_id })
                .first();

            if (!tr) {
                this.logger.warn(`⚠️ TR with id ${event.trust_registry_id} not found. Skipping archive.`);
                await trx.rollback();
                return;
            }

            if (tr.controller !== event.creator) {
                this.logger.warn(`⚠️ Creator ${event.creator} is not controller of TR ${tr.id}. Skipping archive.`);
                await trx.rollback();
                return;
            }

            if (event.archive === true && tr.archived !== null) {
                this.logger.warn(`⚠️ TR ${tr.id} is already archived. Skipping.`);
                await trx.rollback();
                return;
            }
            if (event.archive === false && tr.archived === null) {
                this.logger.warn(`⚠️ TR ${tr.id} is not archived. Skipping.`);
                await trx.rollback();
                return;
            }

            const timestamp = formatTimestamp(event.timestamp);

            await trx("trust_registry")
                .where({ id: tr.id })
                .update({
                    archived: event.archive ? timestamp : null,
                    modified: timestamp,
                });

            await trx.commit();
            this.logger.info(
                `✅ TR ${tr.id} ${event.archive ? "archived" : "unarchived"} at ${timestamp}`
            );
        } catch (err) {
            await trx.rollback();
            this.logger.error("❌ Failed to process ArchiveTrustRegistry", err);
        }
    }

    private async processUpdateTREvent(event: any) {
        const trx = await knex.transaction();
        try {
            const tr = await trx("trust_registry")
                .where({ id: event.trust_registry_id })
                .first();

            if (!tr) {
                this.logger.warn(`⚠️ TR with id ${event.trust_registry_id} not found. Skipping update.`);
                await trx.rollback();
                return;
            }

            if (tr.controller !== event.creator) {
                this.logger.warn(`⚠️ Creator ${event.creator} is not controller of TR ${tr.id}. Skipping update.`);
                await trx.rollback();
                return;
            }

            const updateData: any = {};
            if (event.did) updateData.did = event.did;
            if (event.aka) updateData.aka = event.aka;
            if (event.language) updateData.language = event.language;
            if (event.deposit) updateData.deposit = event.deposit;
            if (event.height) updateData.height = event.height;

            updateData.modified = formatTimestamp(event.timestamp);

            if (Object.keys(updateData).length === 1) {
                this.logger.warn(`⚠️ No updatable fields found for TR ${tr.id}. Skipping.`);
                await trx.rollback();
                return;
            }

            await trx("trust_registry")
                .where({ id: tr.id })
                .update(updateData);

            await trx.commit();
            this.logger.info(
                `✅ TR ${tr.id} updated with fields: ${Object.keys(updateData).join(", ")}`
            );
        } catch (err) {
            await trx.rollback();
            this.logger.error("❌ Failed to process UpdateTrustRegistry", err);
        }
    }

    private async processCreateTREvent(event: any) {
        const trx = await knex.transaction();

        try {
            const params = await knex("module_params")
                .where({ module: "trustregistry" })
                .first()
                .transacting(trx);

            if (!params) {
                this.logger.warn("⚠️ Missing trustregistry params in module_params table. Skipping event.");
                await trx.rollback();
                return;
            }

            const parsedParams =
                typeof params.params === "string"
                    ? JSON.parse(params.params)
                    : params.params;

            const trustDepositDenom =
                parsedParams?.params?.trust_registry_trust_deposit || 0;
            const trustUnitPrice = parsedParams?.params?.trust_unit_price || 1;

            const deposit = Number(trustDepositDenom) * Number(trustUnitPrice);

            const timestamp = formatTimestamp(event.timestamp);

            const [trId] = await trx("trust_registry")
                .insert({
                    did: event.did,
                    controller: event.creator,
                    created: timestamp,
                    modified: timestamp,
                    aka: event.aka,
                    language: event.language,
                    height: event.height,
                    active_version: 1,
                    deposit,
                })
                .onConflict("height")
                .merge()
                .returning("id");

            const [gfvId] = await trx("governance_framework_version")
                .insert({
                    tr_id: trId?.id,
                    created: timestamp,
                    version: 1,
                    active_since: timestamp,
                })
                .onConflict(["tr_id", "version"])
                .merge()
                .returning("id");

            await trx("governance_framework_document").insert({
                gfv_id: gfvId.id,
                created: timestamp,
                language: event.language,
                url: event.doc_url,
                digest_sri: event.doc_digest_sri,
            })
                .onConflict(["gfv_id", "url"])
                .merge();

            await trx.commit();

            this.logger.info(
                `✅ TR created with DID ${event.did}, TR_ID ${trId?.id}, GFV_ID ${gfvId.id}`
            );
        } catch (err) {
            await trx.rollback();
            this.logger.error("❌ Failed to process CreateTrustRegistry", err);
        }
    }
    private async processAddGovFrameworkDocEvent(event: any) {
        const trx = await knex.transaction();

        try {
            const tr = await trx("trust_registry")
                .where({ id: event.trust_registry_id })
                .first();

            if (!tr) {
                this.logger.warn(`⚠️ TR with id ${event.trust_registry_id} not found. Skipping event.`);
                await trx.rollback();
                return;
            }
            const timestamp = formatTimestamp(event.timestamp);

            const [gfv] = await trx("governance_framework_version")
                .insert({
                    tr_id: tr.id,
                    created: timestamp,
                    version: event.version,
                    active_since: timestamp,
                })
                .onConflict(["tr_id", "version"])
                .merge()
                .returning("id");

            await trx("governance_framework_document")
                .insert({
                    gfv_id: gfv.id,
                    created: timestamp,
                    language: event.doc_language,
                    url: event.doc_url,
                    digest_sri: event.doc_digest_sri,
                })
                .onConflict(["gfv_id", "url"])
                .merge();
            await trx.commit();
            this.logger.info(
                `✅ TrustRegistry ${tr.id} upgraded to version ${event.version}, GFV_ID ${gfv.id} with new document ${event.doc_url}`
            );
        } catch (err) {
            await trx.rollback();
            this.logger.error("❌ Failed to process AddGovernanceFrameworkDocument", err);
        }


    }


    private async processIncreaseActiveGFVEvent(event: any) {
        const trx = await knex.transaction();

        try {
            const tr = await trx("trust_registry")
                .where({ id: event.trust_registry_id })
                .first();

            if (!tr) {
                this.logger.warn(`⚠️ TR with id ${event.trust_registry_id} not found. Skipping event.`);
                await trx.rollback();
                return;
            }

            if (tr.controller !== event.creator) {
                this.logger.warn(`⚠️ Creator ${event.creator} is not controller of TR ${tr.id}. Skipping event.`);
                await trx.rollback();
                return;
            }

            const nextVersion = tr.active_version + 1;

            const gfv = await trx("governance_framework_version")
                .where({ tr_id: tr.id, version: nextVersion })
                .first();

            if (!gfv) {
                this.logger.warn(`⚠️ No GovernanceFrameworkVersion for TR ${tr.id} version ${nextVersion}. Skipping event.`);
                await trx.rollback();
                return;
            }
            const gfd = await trx("governance_framework_document")
                .where({ gfv_id: gfv.id, language: tr.language })
                .first();

            if (!gfd) {
                this.logger.warn(`⚠️ No GovernanceFrameworkDocument for GFV ${gfv.id} and language ${tr.language}. Skipping event.`);
                await trx.rollback();
                return;
            }

            const timestamp = formatTimestamp(event.timestamp);

            await trx("trust_registry")
                .where({ id: tr.id })
                .update({
                    active_version: nextVersion,
                    modified: timestamp,
                });

            await trx("governance_framework_version")
                .where({ id: gfv.id })
                .update({
                    active_since: timestamp,
                });

            await trx.commit();
            this.logger.info(
                `✅ TR ${tr.id} upgraded to active_version=${nextVersion}, GFV ${gfv.id} active_since=${timestamp}`
            );
        } catch (err) {
            await trx.rollback();
            this.logger.error("❌ Failed to process IncreaseActiveGFVEvent", err);
        }
    }

    public async _start() {
        await super._start();
        this.logger.info("ProcessTREventsService started and ready.");
    }
}
