import {
  Action,
  Service,
} from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import {
  ModulesParamsNamesTypes,
  SERVICE,
  TrustRegistryMessageTypes,
} from "../../common";
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
  async handleTrustRegistryMessages(
    ctx: Context<{ trustRegistryList: any[] }>
  ) {
    const { trustRegistryList } = ctx.params;
    this.logger.info(`🔄 Processing ${trustRegistryList?.length || 0} TrustRegistry messages`);

    for (const message of trustRegistryList) {
      try {
        this.logger.info(`📝 Processing TR message: type=${message.type}, height=${message.height}`);

        const processedTR: any = { ...message, ...message.content };

        if (message.content?.id) {
          processedTR.trust_registry_id = message.content.id;
        }

        delete processedTR?.content;
        delete processedTR?.id;
        delete processedTR?.tx_id;
        delete processedTR?.["@type"];

        if (
          processedTR.type === TrustRegistryMessageTypes.Create ||
          processedTR.type === TrustRegistryMessageTypes.CreateLegacy
        ) {
          await this.processCreateTR(processedTR);
        }

        if (
          processedTR.type === TrustRegistryMessageTypes.AddGovernanceFrameworkDoc
        ) {
          await this.processAddGovFrameworkDoc(processedTR);
        }

        if (processedTR.type === TrustRegistryMessageTypes.Update) {
          await this.processUpdateTR(processedTR);
        }

        if (
          processedTR.type ===
          TrustRegistryMessageTypes.IncreaseGovernanceFrameworkVersion
        ) {
          await this.processIncreaseActiveGFV(processedTR);
        }

        if (processedTR.type === TrustRegistryMessageTypes.Archive) {
          await this.processArchiveTR(processedTR);
        }
      } catch (err) {
        this.logger.error(`❌ Error processing TR message:`, err);
        console.error("FATAL TR ERROR:", err);
        
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
      
      if (!changes) {
        return;
      }
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

  private async recordGFVHistory(
    trx: any,
    gfvId: number,
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
      
      if (!changes) {
        return;
      }
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

  private async recordGFDHistory(
    trx: any,
    gfdId: number,
    gfvId: number,
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
      
      if (!changes) {
        return;
      }
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
      const tr = await trx("trust_registry")
        .where({ id: message.trust_registry_id })
        .first();
      if (!tr) {
        await trx.rollback();
        return;
      }

      const timestamp = formatTimestamp(message.timestamp);
      const newData = {
        ...tr,
        archived: message.archive ? timestamp : null,
        modified: timestamp,
      };

      await trx("trust_registry").where({ id: tr.id }).update(newData);
      const blockHeight = Number(message.height) || 0;
      await this.recordTRHistory(
        trx,
        tr.id,
        "Archive",
        blockHeight,
        tr,
        newData
      );

      await trx.commit();
    } catch (err) {
      await trx.rollback();
      this.logger.error("❌ Failed to process ArchiveTrustRegistry", err);
      console.error("FATAL TR ARCHIVE ERROR:", err);
      
    }
  }

  private async processUpdateTR(message: any) {
    const trx = await knex.transaction();
    try {
      const tr = await trx("trust_registry")
        .where({ id: message.trust_registry_id })
        .first();
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
      const blockHeight = Number(message.height) || 0;
      await this.recordTRHistory(
        trx,
        tr.id,
        "Update",
        blockHeight,
        tr,
        updateData
      );

      await trx.commit();
    } catch (err) {
      await trx.rollback();
      this.logger.error("❌ Failed to process UpdateTrustRegistry", err);
      console.error("FATAL TR UPDATE ERROR:", err);
      
    }
  }

  private async processCreateTR(message: any) {
    this.logger.info("🔄 Processing CreateTR message:", JSON.stringify(message));
    const trx = await knex.transaction();
    try {
      const params = await trx("module_params")
        .where({ module: ModulesParamsNamesTypes?.TR })
        .first();
      if (!params) {
        this.logger.error("❌ TR module_params not found! Cannot create TrustRegistry.");
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

      const timestamp = formatTimestamp(message.timestamp);
      const blockHeight = Number(message.height) || 0;
      
      this.logger.info(`📝 Creating TR with height: ${blockHeight}, did: ${message.did}`);

      // Check if TR already exists with same DID
      const existingTR = await trx("trust_registry").where({ did: message.did }).first();
      
      let tr;
      if (existingTR) {
        this.logger.info(`📋 TR with did ${message.did} already exists, updating...`);
        [tr] = await trx("trust_registry")
          .where({ id: existingTR.id })
          .update({
            controller: message.creator,
            modified: timestamp,
            aka: message.aka,
            language: message.language,
            height: blockHeight,
            deposit,
          })
          .returning("*");
      } else {
        this.logger.info(`🆕 Creating new TR with did ${message.did}`);
        [tr] = await trx("trust_registry")
          .insert({
            did: message.did,
            controller: message.creator,
            created: timestamp,
            modified: timestamp,
            aka: message.aka,
            language: message.language,
            height: blockHeight,
            active_version: 1,
            deposit,
          })
          .returning("*");
      }

      await this.recordTRHistory(
        trx,
        tr.id,
        "Create",
        blockHeight,
        null,
        tr
      );

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

      await this.recordGFVHistory(
        trx,
        gfv.id,
        tr.id,
        "CreateGFV",
        blockHeight,
        null,
        gfv
      );

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

      await this.recordGFDHistory(
        trx,
        gfd.id,
        gfv.id,
        tr.id,
        "CreateGFD",
        blockHeight,
        null,
        gfd
      );

      await trx.commit();
    } catch (err) {
      await trx.rollback();
      this.logger.error("❌ Failed to process CreateTrustRegistry", err);
      console.error("FATAL TR CREATE ERROR:", err);
      
    }
  }

  private async processAddGovFrameworkDoc(message: any) {
    const trx = await knex.transaction();
    try {
      const tr = await trx("trust_registry")
        .where({ id: message.trust_registry_id })
        .first();
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

      const blockHeight = Number(message.height) || 0;
      await this.recordGFVHistory(
        trx,
        gfv.id,
        tr.id,
        "AddGFV",
        blockHeight,
        null,
        gfv
      );

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

      await this.recordGFDHistory(
        trx,
        gfd.id,
        gfv.id,
        tr.id,
        "AddGFD",
        blockHeight,
        null,
        gfd
      );

      await trx.commit();
    } catch (err) {
      await trx.rollback();
      this.logger.error(
        "❌ Failed to process AddGovernanceFrameworkDocument",
        err
      );
      console.error("FATAL TR ADD GFD ERROR:", err);
      
    }
  }

  private async processIncreaseActiveGFV(message: any) {
    const trx = await knex.transaction();
    try {
      const tr = await trx("trust_registry")
        .where({ id: message.trust_registry_id })
        .first();
      if (!tr) {
        await trx.rollback();
        return;
      }

      const nextVersion = tr.active_version + 1;
      const gfv = await trx("governance_framework_version")
        .where({ tr_id: tr.id, version: nextVersion })
        .first();
      if (!gfv) {
        await trx.rollback();
        return;
      }

      const timestamp = formatTimestamp(message.timestamp);

      await trx("trust_registry")
        .where({ id: tr.id })
        .update({ active_version: nextVersion, modified: timestamp });
      const blockHeight = Number(message.height) || 0;
      await this.recordTRHistory(
        trx,
        tr.id,
        "IncreaseGFV",
        blockHeight,
        tr,
        { ...tr, active_version: nextVersion, modified: timestamp }
      );

      await trx("governance_framework_version")
        .where({ id: gfv.id })
        .update({ active_since: timestamp });
      await this.recordGFVHistory(
        trx,
        gfv.id,
        tr.id,
        "ActivateGFV",
        blockHeight,
        gfv,
        { ...gfv, active_since: timestamp }
      );

      await trx.commit();
    } catch (err) {
      await trx.rollback();
      this.logger.error("❌ Failed to process IncreaseActiveGFV", err);
      console.error("FATAL TR INCREASE GFV ERROR:", err);
      
    }
  }

  public async _start() {
    await super._start();
    this.logger.info("TrustRegistryMessageProcessorService started and ready.");
  }
}
