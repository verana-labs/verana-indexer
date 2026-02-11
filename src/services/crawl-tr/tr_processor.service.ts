import * as fs from "fs";
import * as path from "path";
import {
  Action,
  Service,
} from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import {
  ModulesParamsNamesTypes,
  SERVICE,
} from "../../common";
import { VeranaTrustRegistryMessageTypes } from "../../common/verana-message-types";
import { formatTimestamp } from "../../common/utils/date_utils";
import knex from "../../common/utils/db_connection";
import { requireController } from "../../common/utils/extract_controller";
import { MessageProcessorBase } from "../../common/utils/message_processor_base";
import { detectStartMode } from "../../common/utils/start_mode_detector";
import { calculateTrustRegistryStats } from "./tr_stats";

type ChangeRecord = Record<string, any>;

@Service({
  name: SERVICE.V1.TrustRegistryMessageProcessorService.key,
  version: 1,
})
export default class TrustRegistryMessageProcessorService extends BullableService {
  private processorBase: MessageProcessorBase;
  private _isFreshStart: boolean = false;

  constructor(broker: ServiceBroker) {
    super(broker);
    this.processorBase = new MessageProcessorBase(this);
  }

  public async _start() {
    const startMode = await detectStartMode();
    this._isFreshStart = startMode.isFreshStart;
    this.processorBase.setFreshStartMode(this._isFreshStart);
    this.logger.info(`TrustRegistry processor started | Mode: ${this._isFreshStart ? 'Fresh Start' : 'Reindexing'}`);
    await super._start();
    this.logger.info("TrustRegistryMessageProcessorService started and ready.");
  }

  @Action({ name: "handleTrustRegistryMessages" })
  async handleTrustRegistryMessages(
    ctx: Context<{ trustRegistryList: any[] }>
  ) {
    const { trustRegistryList } = ctx.params;
    this.logger.info(` Processing ${trustRegistryList?.length || 0} TrustRegistry messages`);

    if (!trustRegistryList || trustRegistryList.length === 0) {
      this.logger.warn("⚠️ No TrustRegistry messages to process");
      return;
    }

    const failThreshold = 0.1;
    const failedMessages: any[] = [];
    const totalMessages = trustRegistryList.length;

    const processMessage = async (message: any, index: number) => {
      if (!message.type) {
        this.logger.error(`TR message missing type:`, JSON.stringify(message));
        failedMessages.push({ message, error: "Missing type" });
        return;
      }

      this.logger.info(`Processing TR message ${index + 1}/${totalMessages}: type=${message.type}, height=${message.height}, tr_id=${message.content?.id}`);

      const processedTR: any = { ...message, ...message.content };

      if (message.content?.trust_registry_id !== undefined && message.content?.trust_registry_id !== null) {
        processedTR.trust_registry_id = message.content.trust_registry_id;
      } else if (message.content?.id !== undefined && message.content?.id !== null) {
        processedTR.trust_registry_id = message.content.id;
      }

      delete processedTR?.content;
      delete processedTR?.id;
      delete processedTR?.tx_id;
      delete processedTR?.["@type"];

      let processed = false;
      if (
        processedTR.type === VeranaTrustRegistryMessageTypes.CreateTrustRegistry ||
        processedTR.type === VeranaTrustRegistryMessageTypes.CreateTrustRegistryLegacy
      ) {
        await this.processCreateTR(processedTR);
        processed = true;
      }

      if (
        processedTR.type === VeranaTrustRegistryMessageTypes.AddGovernanceFrameworkDoc
      ) {
        await this.processAddGovFrameworkDoc(processedTR);
        processed = true;
      }

      if (processedTR.type === VeranaTrustRegistryMessageTypes.UpdateTrustRegistry) {
        await this.processUpdateTR(processedTR);
        processed = true;
      }

      if (
        processedTR.type ===
        VeranaTrustRegistryMessageTypes.IncreaseGovernanceFrameworkVersion
      ) {
        await this.processIncreaseActiveGFV(processedTR);
        processed = true;
      }

      if (processedTR.type === VeranaTrustRegistryMessageTypes.ArchiveTrustRegistry) {
        await this.processArchiveTR(processedTR);
        processed = true;
      }

      if (!processed) {
        this.logger.warn(`Unknown TR message type: ${processedTR.type}`);
        failedMessages.push({ message, error: `Unknown type: ${processedTR.type}` });
        throw new Error(`Unknown TR message type: ${processedTR.type}`);
      }
    };

    const sortedMessages = [...trustRegistryList].sort((a, b) => {
      const heightDiff = (a.height || 0) - (b.height || 0);
      if (heightDiff !== 0) return heightDiff;
      return (a.id || 0) - (b.id || 0);
    });

    let successCount = 0;
    for (let i = 0; i < sortedMessages.length; i++) {
      const message = sortedMessages[i];
      try {
        await processMessage(message, i);
        successCount++;
      } catch (err: any) {
        failedMessages.push({ message, error: err.message || String(err) });
      }
    }

    this.logger.info(`TrustRegistry processing complete: ${successCount} succeeded, ${failedMessages.length} failed out of ${totalMessages} total`);

    if (failedMessages.length > 0) {
      this.logger.error(`Failed to process ${failedMessages.length} TrustRegistry messages:`);
      failedMessages.forEach((failed, idx) => {
        this.logger.error(`  ${idx + 1}. Type: ${failed.message.type}, Error: ${failed.error}`);
      });

      if (failedMessages.length > totalMessages * failThreshold) {
        const failureRate = ((failedMessages.length / totalMessages) * 100).toFixed(2);
        this.logger.error(`CRITICAL: ${failureRate}% of TR messages failed (${failedMessages.length}/${totalMessages})! This indicates a serious issue.`);
        throw new Error(`Failed to process ${failedMessages.length} out of ${totalMessages} TrustRegistry messages (${failureRate}% failure rate). This exceeds the ${(failThreshold * 100).toFixed(0)}% threshold.`);
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
      const computed = this.processorBase.computeChanges(oldData, newData);
      changes = Object.keys(computed).length > 0 ? computed : null;

      if (!changes) {
        return;
      }
    } else {
      const creationChanges: ChangeRecord = {};
      for (const [key, value] of Object.entries(newData)) {
        if (value !== null && value !== undefined && key !== 'id') {
          creationChanges[key] = value;
        }
      }
      changes = Object.keys(creationChanges).length > 0 ? creationChanges : null;
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
      const computed = this.processorBase.computeChanges(oldData, newData);
      changes = Object.keys(computed).length > 0 ? computed : null;

      if (!changes) {
        return;
      }
    } else {
      const creationChanges: ChangeRecord = {};
      for (const [key, value] of Object.entries(newData)) {
        if (value !== null && value !== undefined && key !== 'id') {
          creationChanges[key] = value;
        }
      }
      changes = Object.keys(creationChanges).length > 0 ? creationChanges : null;
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
      const computed = this.processorBase.computeChanges(oldData, newData);
      changes = Object.keys(computed).length > 0 ? computed : null;

      if (!changes) {
        return;
      }
    } else {
      const creationChanges: ChangeRecord = {};
      for (const [key, value] of Object.entries(newData)) {
        if (value !== null && value !== undefined && key !== 'id') {
          creationChanges[key] = value;
        }
      }
      changes = Object.keys(creationChanges).length > 0 ? creationChanges : null;
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
        this.logger.warn(`⚠️ ArchiveTR: TR not found for id=${message.trust_registry_id}, height=${message.height}`);
        return;
      }

      const timestamp = formatTimestamp(message.timestamp);
      const newData = {
        ...tr,
        archived: message.archive ? timestamp : null,
        modified: timestamp,
      };

      await trx("trust_registry").where({ id: tr.id }).update(newData);
      const blockHeight = message.height || 0;
      await this.recordTRHistory(
        trx,
        tr.id,
        "Archive",
        blockHeight,
        tr,
        newData
      );

      await trx.commit();
      this.logger.info(`✅ Successfully archived TR: id=${tr.id}`);

      try {
        const stats = await calculateTrustRegistryStats(tr.id);
        await knex("trust_registry")
          .where("id", tr.id)
          .update({
            participants: stats.participants,
            active_schemas: stats.active_schemas,
            archived_schemas: stats.archived_schemas,
            weight: stats.weight,
            issued: stats.issued,
            verified: stats.verified,
            ecosystem_slash_events: stats.ecosystem_slash_events,
            ecosystem_slashed_amount: stats.ecosystem_slashed_amount,
            ecosystem_slashed_amount_repaid: stats.ecosystem_slashed_amount_repaid,
            network_slash_events: stats.network_slash_events,
            network_slashed_amount: stats.network_slashed_amount,
            network_slashed_amount_repaid: stats.network_slashed_amount_repaid,
          });
      } catch (statsError: any) {
        this.logger.warn(`⚠️ Failed to update statistics for TR ${tr.id}: ${statsError?.message || String(statsError)}`);
      }
    } catch (err: any) {
      await trx.rollback();
      const errorMessage = err?.message || String(err);
      this.logger.error(`❌ Failed to process ArchiveTrustRegistry for id=${message.trust_registry_id}:`, errorMessage);
      console.error("FATAL TR ARCHIVE ERROR:", err);
      throw err;
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
        this.logger.warn(`⚠️ UpdateTR: TR not found for id=${message.trust_registry_id}, height=${message.height}`);
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
      const blockHeight = message.height || 0;
      await this.recordTRHistory(
        trx,
        tr.id,
        "Update",
        blockHeight,
        tr,
        updateData
      );

      await trx.commit();
      this.logger.info(`✅ Successfully updated TR: id=${tr.id}`);

      try {
        const stats = await calculateTrustRegistryStats(tr.id);
        await knex("trust_registry")
          .where("id", tr.id)
          .update({
            participants: stats.participants,
            active_schemas: stats.active_schemas,
            archived_schemas: stats.archived_schemas,
            weight: stats.weight,
            issued: stats.issued,
            verified: stats.verified,
            ecosystem_slash_events: stats.ecosystem_slash_events,
            ecosystem_slashed_amount: stats.ecosystem_slashed_amount,
            ecosystem_slashed_amount_repaid: stats.ecosystem_slashed_amount_repaid,
            network_slash_events: stats.network_slash_events,
            network_slashed_amount: stats.network_slashed_amount,
            network_slashed_amount_repaid: stats.network_slashed_amount_repaid,
          });
      } catch (statsError: any) {
        this.logger.warn(`⚠️ Failed to update statistics for TR ${tr.id}: ${statsError?.message || String(statsError)}`);
      }
    } catch (err: any) {
      await trx.rollback();
      const errorMessage = err?.message || String(err);
      this.logger.error(`❌ Failed to process UpdateTrustRegistry for id=${message.trust_registry_id}:`, errorMessage);
      console.error("FATAL TR UPDATE ERROR:", err);
      throw err;
    }
  }

  private async processCreateTR(message: any) {
    this.logger.info(" Processing CreateTR message:", JSON.stringify(message));

    if (!message.did) {
      throw new Error("CreateTR message missing required field: did");
    }

    const trx = await knex.transaction();
    try {
      const params = await trx("module_params")
        .where({ module: ModulesParamsNamesTypes?.TR })
        .first();
      if (!params) {
        const errorMsg = "❌ TR module_params not found! Cannot create TrustRegistry.";
        this.logger.error(errorMsg);
        await trx.rollback();
        throw new Error(errorMsg);
      }

      const parsedParams =
        typeof params.params === "string"
          ? JSON.parse(params.params)
          : params.params;
      const trustDepositDenom =
        parsedParams?.params?.trust_registry_trust_deposit || 0;
      const trustUnitPrice = parsedParams?.params?.trust_unit_price || 1;
      const deposit = (trustDepositDenom || 0) * (trustUnitPrice || 1);

      const timestamp = formatTimestamp(message.timestamp);
      const blockHeight = message.height || 0;

      this.logger.info(` Creating TR with height: ${blockHeight}, did: ${message.did}`);

      const existingTR = await trx("trust_registry").where({ height: blockHeight }).first();

      let tr;
      const controller = requireController(message, `TR ${message.did}`);
      const isReindexing = !!existingTR;

      if (isReindexing) {
        this.logger.info(`TR with height ${blockHeight} already exists, updating for reindexing...`);
        [tr] = await trx("trust_registry")
          .where({ id: existingTR.id })
          .update({
            did: message.did,
            controller: controller,
            modified: timestamp,
            aka: message.aka,
            language: message.language,
            deposit,
          })
          .returning("*");
      } else {
        this.logger.info(`🆕 Creating new TR with did ${message.did} at height ${blockHeight}`);
        [tr] = await trx("trust_registry")
          .insert({
            did: message.did,
            controller: controller,
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

      let gfv = await trx("governance_framework_version")
        .where({
          tr_id: tr.id,
          version: 1,
        })
        .first();

      if (!gfv) {
        [gfv] = await trx("governance_framework_version")
          .insert({
            tr_id: tr.id,
            created: timestamp,
            version: 1,
            active_since: timestamp,
          })
          .returning("*");
      } else if (isReindexing) {
        await trx("governance_framework_version")
          .where({ id: gfv.id })
          .update({
            created: timestamp,
            active_since: timestamp,
          });
        gfv = await trx("governance_framework_version")
          .where({ id: gfv.id })
          .first();
      }

      await this.recordGFVHistory(
        trx,
        gfv.id,
        tr.id,
        "CreateGFV",
        blockHeight,
        null,
        gfv
      );

      const language = message.language;
      const digestSri = message.doc_digest_sri;

      let gfd = await trx("governance_framework_document")
        .where({
          gfv_id: gfv.id,
          language,
          digest_sri: digestSri,
        })
        .first();

      if (!gfd) {
        [gfd] = await trx("governance_framework_document")
          .insert({
            gfv_id: gfv.id,
            created: timestamp,
            language,
            url: message.doc_url,
            digest_sri: digestSri,
          })
          .returning("*");
      }

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
      this.logger.info(`✅ Successfully created/updated TR: did=${message.did}, id=${tr.id}`);

      try {
        const stats = await calculateTrustRegistryStats(tr.id);
        await knex("trust_registry")
          .where("id", tr.id)
          .update({
            participants: stats.participants,
            active_schemas: stats.active_schemas,
            archived_schemas: stats.archived_schemas,
            weight: stats.weight,
            issued: stats.issued,
            verified: stats.verified,
            ecosystem_slash_events: stats.ecosystem_slash_events,
            ecosystem_slashed_amount: stats.ecosystem_slashed_amount,
            ecosystem_slashed_amount_repaid: stats.ecosystem_slashed_amount_repaid,
            network_slash_events: stats.network_slash_events,
            network_slashed_amount: stats.network_slashed_amount,
            network_slashed_amount_repaid: stats.network_slashed_amount_repaid,
          });
      } catch (statsError: any) {
        this.logger.warn(`⚠️ Failed to update statistics for TR ${tr.id}: ${statsError?.message || String(statsError)}`);
      }
    } catch (err: any) {
      await trx.rollback();
      const errorMessage = err?.message || String(err);
      this.logger.error(`❌ Failed to process CreateTrustRegistry for did=${message.did}:`, errorMessage);
      console.error("FATAL TR CREATE ERROR:", err);
      throw err;
    }
  }

  private async processAddGovFrameworkDoc(message: any) {
    const trx = await knex.transaction();
    try {
      try {
        const logsDir = path.resolve(process.cwd(), "logs");
        if (!fs.existsSync(logsDir)) {
          fs.mkdirSync(logsDir, { recursive: true });
        }
        const logPath = path.join(logsDir, "file.log");
        fs.appendFileSync(
          logPath,
          `${new Date().toISOString()} [AddGovFrameworkDoc] Incoming message: ${JSON.stringify(message)}\n`
        );
      } catch (fileErr) {
        this.logger.warn("Failed to write debug log file:", fileErr);
      }
      const tr = await trx("trust_registry")
        .where({ id: message.trust_registry_id })
        .first();
      if (!tr) {
        await trx.rollback();
        throw new Error(
          `AddGovFrameworkDoc: TR not found for id=${message.trust_registry_id}, height=${message.height}`
        );
      }

      const timestamp = formatTimestamp(message.timestamp);
      const blockHeight = message.height || 0;

      let gfv = await trx("governance_framework_version")
        .where({
          tr_id: tr.id,
          version: message.version,
        })
        .first();

      if (!gfv) {
        const maxVersionResult = await trx("governance_framework_version")
          .where({ tr_id: tr.id })
          .max("version as max_version")
          .first();
        const maxVersion = maxVersionResult?.max_version || 0;

        if (message.version !== maxVersion + 1 || message.version <= tr.active_version) {
          await trx.rollback();
          const errMsg = `AddGovFrameworkDoc: Invalid version=${message.version} for tr_id=${tr.id}, maxVersion=${maxVersion}, active_version=${tr.active_version}`;
          this.logger.error(errMsg);
          this.logger.error("AddGovFrameworkDoc message payload:", JSON.stringify(message));
          console.error("FATAL: Invalid AddGovFrameworkDoc version. Exiting for debug.");
          throw new Error(errMsg);
        }

        [gfv] = await trx("governance_framework_version")
          .insert({
            tr_id: tr.id,
            created: timestamp,
            version: message.version,
            // active_since: null, // Omit to allow default null
          })
          .returning("*");

        await this.recordGFVHistory(
          trx,
          gfv.id,
          tr.id,
          "AddGFV",
          blockHeight,
          null,
          gfv
        );
        try {
          const logPath = path.join(process.cwd(), "logs", "file.log");
          fs.appendFileSync(
            logPath,
            `${new Date().toISOString()} [AddGovFrameworkDoc] Created GFV: ${JSON.stringify(gfv)}\n`
          );
        } catch (_) {}
      }

      const language = message.doc_language || message.language;
      const digestSri = message.doc_digest_sri || message.digest_sri;

      let gfd = await trx("governance_framework_document")
        .where({
          gfv_id: gfv.id,
          digest_sri: digestSri,
        })
        .first();
      try {
        const logPath = path.join(process.cwd(), "logs", "file.log");
        fs.appendFileSync(
          logPath,
          `${new Date().toISOString()} [AddGovFrameworkDoc] GFD lookup result: ${JSON.stringify(gfd)}\n`
        );
      } catch (_) {}

      if (gfd) {
        try {
          const logPath = path.join(process.cwd(), "logs", "file.log");
          fs.appendFileSync(
            logPath,
            `${new Date().toISOString()} [AddGovFrameworkDoc] Existing GFD found (will still INSERT new one): ${JSON.stringify(gfd)}\n`
          );
        } catch (_) {}
      }

      const oldGfd = null;
      [gfd] = await trx("governance_framework_document")
        .insert({
          gfv_id: gfv.id,
          created: timestamp,
          language,
          url: message.doc_url,
          digest_sri: digestSri,
        })
        .returning("*");
      try {
        const logPath = path.join(process.cwd(), "logs", "file.log");
        fs.appendFileSync(
          logPath,
          `${new Date().toISOString()} [AddGovFrameworkDoc] Inserted GFD: ${JSON.stringify(gfd)}\n`
        );
      } catch (_) {}

      await this.recordGFDHistory(
        trx,
        gfd.id,
        gfv.id,
        tr.id,
        oldGfd ? "UpdateGFD" : "AddGFD",
        blockHeight,
        oldGfd,
        gfd
      );

      await trx.commit();
      this.logger.info(
        `✅ AddGovFrameworkDoc OK: tr_id=${tr.id}, gfv_version=${message.version}, gfd_id=${gfd.id}`
      );
      try {
        const logPath = path.join(process.cwd(), "logs", "file.log");
        fs.appendFileSync(
          logPath,
          `${new Date().toISOString()} [AddGovFrameworkDoc] COMMIT OK: tr_id=${tr.id}, gfv_version=${message.version}, gfd_id=${gfd.id}\n`
        );
      } catch (_) {}
    } catch (err: any) {
      await trx.rollback();
      this.logger.error(
        `❌ AddGovFrameworkDoc failed for tr_id=${message.trust_registry_id}:`,
        err?.message || err
      );
      try {
        const logPath = path.join(process.cwd(), "logs", "file.log");
        fs.appendFileSync(
          logPath,
          `${new Date().toISOString()} [AddGovFrameworkDoc] ERROR: ${err?.message || String(err)} | message: ${JSON.stringify(message)}\n`
        );
      } catch (_) {}
      throw err;
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
        this.logger.warn(` IncreaseActiveGFV: TR not found for id=${message.trust_registry_id}, height=${message.height}`);
        return;
      }

      const nextVersion = tr.active_version + 1;
      const gfv = await trx("governance_framework_version")
        .where({ tr_id: tr.id, version: nextVersion })
        .first();
      if (!gfv) {
        await trx.rollback();
        this.logger.warn(` IncreaseActiveGFV: GFV version ${nextVersion} not found for tr_id=${tr.id}, height=${message.height}. Will retry.`);
        throw new Error(`GFV version ${nextVersion} not found for tr_id=${tr.id}, retry needed`);
      }

      const timestamp = formatTimestamp(message.timestamp);

      await trx("trust_registry")
        .where({ id: tr.id })
        .update({ active_version: nextVersion, modified: timestamp });
      const blockHeight = message.height || 0;
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
      this.logger.info(`✅ Successfully increased active GFV: tr_id=${tr.id}, version=${nextVersion}`);
    } catch (err: any) {
      await trx.rollback();
      const errorMessage = err?.message || String(err);
      this.logger.error(`❌ Failed to process IncreaseActiveGFV for tr_id=${message.trust_registry_id}:`, errorMessage);
      console.error("FATAL TR INCREASE GFV ERROR:", err);
      throw err;
    }
  }

}
