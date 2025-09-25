import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import { SERVICE, TrustRegistryMessageTypes } from "../../common";
import { formatTimestamp } from "../../common/utils/date_utils";
import knex from "../../common/utils/db_connection";

type ChangeRecord = Record<string, { old: any; new: any }>;

function computeChanges(oldData: any, newData: any): ChangeRecord {
    const changes: ChangeRecord = {};
    for (const key of Object.keys(newData)) {
        if (oldData?.[key] !== newData[key]) {
            changes[key] = { old: oldData?.[key] ?? null, new: newData[key] };
        }
    }
    return changes;
}

@Service({
    name: SERVICE.V1.TrustRegistryMessageProcessorService.key,
    version: 1,
})
export default class TrustRegistryMessageProcessorService extends BullableService {
    constructor(broker: ServiceBroker) {
        super(broker);
    }

    @Action({ name: "handleTrustRegistryMessages" })
    async handleTrustRegistryMessages(ctx: Context<{ trustRegistryList: any[] }>) {
        const { trustRegistryList } = ctx.params;

        for (const message of trustRegistryList) {
            const processedTR: any = { ...message, ...message.content };

            if (message.content?.id) {
                processedTR.trust_registry_id = message.content.id;
            }

            delete processedTR?.content;
            delete processedTR?.id;
            delete processedTR?.tx_id;
            delete processedTR?.["@type"];

            if (processedTR.type === TrustRegistryMessageTypes.Create || processedTR.type === TrustRegistryMessageTypes.CreateLegacy) {
                await this.processCreateTR(processedTR);
            }

            if (processedTR.type === TrustRegistryMessageTypes.AddGovernanceFrameworkDoc) {
                await this.processAddGovFrameworkDoc(processedTR);
            }

            if (processedTR.type === TrustRegistryMessageTypes.Update) {
                await this.processUpdateTR(processedTR);
            }

            if (processedTR.type === TrustRegistryMessageTypes.IncreaseGovernanceFrameworkVersion) {
                await this.processIncreaseActiveGFV(processedTR);
            }

            if (processedTR.type === TrustRegistryMessageTypes.Archive) {
                await this.processArchiveTR(processedTR);
            }
        }
    }

    private async recordTRHistory(
        trx: any,
        trId: number,
        eventType: string,
        height: number,
        oldData: any,
        newData: any
    ) {
        let changes: ChangeRecord | null = null;

        if (oldData) {
            const computed = computeChanges(oldData, newData);
            changes = Object.keys(computed).length > 0 ? computed : null;
        }

        await trx("trust_registry_history").insert({
            tr_id: trId,
            did: newData.did,
            controller: newData.controller,
            created: newData.created,
            modified: newData.modified,
            archived: newData.archived ?? null,
            deposit: newData.deposit,
            aka: newData.aka ?? null,
            language: newData.language,
            active_version: newData.active_version ?? null,
            event_type: eventType,
            height,
            changes: changes ? JSON.stringify(changes) : null, 
        });
    }


    private async recordGFVHistory(trx: any, gfvId: number, trId: number, eventType: string, height: number, oldData: any, newData: any) {
        let changes: ChangeRecord | null = null;
        if (oldData) {
            const computed = computeChanges(oldData, newData);
            changes = Object.keys(computed).length > 0 ? computed : null;
        }

        await trx("governance_framework_version_history").insert({
            tr_id: trId,
            created: newData.created,
            version: newData.version,
            active_since: newData.active_since,
            event_type: eventType,
            height,
            changes: changes ? JSON.stringify(changes) : null,
        });
    }

    private async recordGFDHistory(trx: any, gfdId: number, gfvId: number, trId: number, eventType: string, height: number, oldData: any, newData: any) {
        let changes: ChangeRecord | null = null;
        if (oldData) {
            const computed = computeChanges(oldData, newData);
            changes = Object.keys(computed).length > 0 ? computed : null;
        }

        await trx("governance_framework_document_history").insert({
            gfv_id: gfvId,
            tr_id: trId,
            created: newData.created,
            language: newData.language,
            url: newData.url,
            digest_sri: newData.digest_sri,
            event_type: eventType,
            height,
            changes: changes ? JSON.stringify(changes) : null,
        });
    }


    private async processArchiveTR(message: any) {
        const trx = await knex.transaction();
        try {
            const tr = await trx("trust_registry").where({ id: message.trust_registry_id }).first();
            if (!tr) {
                await trx.rollback();
                return;
            }

            const timestamp = formatTimestamp(message.timestamp);
            const newData = { ...tr, archived: message.archive ? timestamp : null, modified: timestamp };

            await trx("trust_registry").where({ id: tr.id }).update(newData);
            await this.recordTRHistory(trx, tr.id, "Archive", message.height, tr, newData);

            await trx.commit();
        } catch (err) {
            await trx.rollback();
            this.logger.error("❌ Failed to process ArchiveTrustRegistry", err);
        }
    }

    private async processUpdateTR(message: any) {
        const trx = await knex.transaction();
        try {
            const tr = await trx("trust_registry").where({ id: message.trust_registry_id }).first();
            if (!tr) {
                await trx.rollback();
                return;
            }

            const updateData: any = { ...tr };
            if (message.did) updateData.did = message.did;
            if (message.aka) updateData.aka = message.aka;
            if (message.language) updateData.language = message.language;
            if (message.deposit) updateData.deposit = message.deposit;
            if (message.height) updateData.height = message.height;
            updateData.modified = formatTimestamp(message.timestamp);

            await trx("trust_registry").where({ id: tr.id }).update(updateData);
            await this.recordTRHistory(trx, tr.id, "Update", message.height, tr, updateData);

            await trx.commit();
        } catch (err) {
            await trx.rollback();
            this.logger.error("❌ Failed to process UpdateTrustRegistry", err);
        }
    }

    private async processCreateTR(message: any) {
        const trx = await knex.transaction();
        try {
            const params = await trx("module_params").where({ module: "trustregistry" }).first();
            if (!params) {
                await trx.rollback();
                return;
            }

            const parsedParams = typeof params.params === "string" ? JSON.parse(params.params) : params.params;
            const trustDepositDenom = parsedParams?.params?.trust_registry_trust_deposit || 0;
            const trustUnitPrice = parsedParams?.params?.trust_unit_price || 1;
            const deposit = Number(trustDepositDenom) * Number(trustUnitPrice);

            const timestamp = formatTimestamp(message.timestamp);

            const [tr] = await trx("trust_registry")
                .insert({
                    did: message.did,
                    controller: message.creator,
                    created: timestamp,
                    modified: timestamp,
                    aka: message.aka,
                    language: message.language,
                    height: message.height,
                    active_version: 1,
                    deposit,
                })
                .onConflict("height")
                .merge()
                .returning("*");

            await this.recordTRHistory(trx, tr.id, "Create", message.height, null, tr);

            const [gfv] = await trx("governance_framework_version")
                .insert({
                    tr_id: tr.id,
                    created: timestamp,
                    version: 1,
                    active_since: timestamp,
                })
                .onConflict(["tr_id", "version"])
                .merge()
                .returning("*");

            await this.recordGFVHistory(trx, gfv.id, tr.id, "CreateGFV", message.height, null, gfv);

            const [gfd] = await trx("governance_framework_document")
                .insert({
                    gfv_id: gfv.id,
                    created: timestamp,
                    language: message.language,
                    url: message.doc_url,
                    digest_sri: message.doc_digest_sri,
                })
                .onConflict(["gfv_id", "url"])
                .merge()
                .returning("*");

            await this.recordGFDHistory(trx, gfd.id, gfv.id, tr.id, "CreateGFD", message.height, null, gfd);

            await trx.commit();
        } catch (err) {
            await trx.rollback();
            this.logger.error("❌ Failed to process CreateTrustRegistry", err);
        }
    }

    private async processAddGovFrameworkDoc(message: any) {
        const trx = await knex.transaction();
        try {
            const tr = await trx("trust_registry").where({ id: message.trust_registry_id }).first();
            if (!tr) {
                await trx.rollback();
                return;
            }

            const timestamp = formatTimestamp(message.timestamp);

            const [gfv] = await trx("governance_framework_version")
                .insert({
                    tr_id: tr.id,
                    created: timestamp,
                    version: message.version,
                    active_since: timestamp,
                })
                .onConflict(["tr_id", "version"])
                .merge()
                .returning("*");

            await this.recordGFVHistory(trx, gfv.id, tr.id, "AddGFV", message.height, null, gfv);

            const [gfd] = await trx("governance_framework_document")
                .insert({
                    gfv_id: gfv.id,
                    created: timestamp,
                    language: message.doc_language,
                    url: message.doc_url,
                    digest_sri: message.doc_digest_sri,
                })
                .onConflict(["gfv_id", "url"])
                .merge()
                .returning("*");

            await this.recordGFDHistory(trx, gfd.id, gfv.id, tr.id, "AddGFD", message.height, null, gfd);

            await trx.commit();
        } catch (err) {
            await trx.rollback();
            this.logger.error("❌ Failed to process AddGovernanceFrameworkDocument", err);
        }
    }

    private async processIncreaseActiveGFV(message: any) {
        const trx = await knex.transaction();
        try {
            const tr = await trx("trust_registry").where({ id: message.trust_registry_id }).first();
            if (!tr) {
                await trx.rollback();
                return;
            }

            const nextVersion = tr.active_version + 1;
            const gfv = await trx("governance_framework_version").where({ tr_id: tr.id, version: nextVersion }).first();
            if (!gfv) {
                await trx.rollback();
                return;
            }

            const timestamp = formatTimestamp(message.timestamp);

            await trx("trust_registry").where({ id: tr.id }).update({ active_version: nextVersion, modified: timestamp });
            await this.recordTRHistory(trx, tr.id, "IncreaseGFV", message.height, tr, { ...tr, active_version: nextVersion, modified: timestamp });

            await trx("governance_framework_version").where({ id: gfv.id }).update({ active_since: timestamp });
            await this.recordGFVHistory(trx, gfv.id, tr.id, "ActivateGFV", message.height, gfv, { ...gfv, active_since: timestamp });

            await trx.commit();
        } catch (err) {
            await trx.rollback();
            this.logger.error("❌ Failed to process IncreaseActiveGFV", err);
        }
    }

    public async _start() {
        await super._start();
        this.logger.info("TrustRegistryMessageProcessorService started and ready.");
    }
}
