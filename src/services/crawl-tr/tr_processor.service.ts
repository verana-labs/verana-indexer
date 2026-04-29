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
import { calculateTrustRegistryStats, TR_STATS_FIELDS } from "./tr_stats";
import { finalizeTrustRegistryHistoryInsert } from "../../common/utils/installed_table_columns";
import { getTrustRegistry } from "../../modules/tr-height-sync/tr_height_sync_helpers";

type ChangeRecord = Record<string, any>;


function getDefaultTRStats(fallbackData?: any): any {
  return {
    participants: Number(fallbackData?.participants ?? 0) || 0,
    participants_ecosystem: Number(fallbackData?.participants_ecosystem ?? 0) || 0,
    participants_issuer_grantor: Number(fallbackData?.participants_issuer_grantor ?? 0) || 0,
    participants_issuer: Number(fallbackData?.participants_issuer ?? 0) || 0,
    participants_verifier_grantor: Number(fallbackData?.participants_verifier_grantor ?? 0) || 0,
    participants_verifier: Number(fallbackData?.participants_verifier ?? 0) || 0,
    participants_holder: Number(fallbackData?.participants_holder ?? 0) || 0,
    active_schemas: Number(fallbackData?.active_schemas ?? 0) || 0,
    archived_schemas: Number(fallbackData?.archived_schemas ?? 0) || 0,
    weight: Number(fallbackData?.weight ?? 0) || 0,
    issued: Number(fallbackData?.issued ?? 0) || 0,
    verified: Number(fallbackData?.verified ?? 0) || 0,
    ecosystem_slash_events: Number(fallbackData?.ecosystem_slash_events ?? 0) || 0,
    ecosystem_slashed_amount: Number(fallbackData?.ecosystem_slashed_amount ?? 0) || 0,
    ecosystem_slashed_amount_repaid: Number(fallbackData?.ecosystem_slashed_amount_repaid ?? 0) || 0,
    network_slash_events: Number(fallbackData?.network_slash_events ?? 0) || 0,
    network_slashed_amount: Number(fallbackData?.network_slashed_amount ?? 0) || 0,
    network_slashed_amount_repaid: Number(fallbackData?.network_slashed_amount_repaid ?? 0) || 0,
  };
}

@Service({
  name: SERVICE.V1.TrustRegistryMessageProcessorService.key,
  version: 1,
})
export default class TrustRegistryMessageProcessorService extends BullableService {
  private processorBase: MessageProcessorBase;
  private _isFreshStart: boolean = false;
  private trHistoryColumnsCache: Set<string> | null = null;

  constructor(broker: ServiceBroker) {
    super(broker);
    this.processorBase = new MessageProcessorBase(this);
  }

  private extractTrustRegistryId(raw: any, options?: { allowTopLevelId?: boolean }): number | null {
    if (!raw || typeof raw !== "object") return null;
    const allowTopLevelId = options?.allowTopLevelId === true;
    const candidates = [
      raw.trust_registry_id,
      raw.trustRegistryId,
      raw.tr_id,
      raw.trId,
      raw.trust_registry?.id,
      raw.trust_registry?.tr_id,
      raw.trust_registry?.trId,
      raw.trustRegistry?.id,
      raw.trustRegistry?.tr_id,
      raw.trustRegistry?.trId,
      ...(allowTopLevelId ? [raw.id] : []),
    ];
    for (const candidate of candidates) {
      const n = Number(candidate);
      if (Number.isInteger(n) && n > 0) return n;
    }
    return null;
  }

  private resolveTrustRegistryIdForMessage(message: any): number | null {
    const eventTrIds = Array.isArray(message?.eventTrIds) ? message.eventTrIds : [];
    for (const rawEventId of eventTrIds) {
      const eventId = Number(rawEventId);
      if (Number.isInteger(eventId) && eventId > 0) return eventId;
    }

    const contentId = this.extractTrustRegistryId(message?.content, { allowTopLevelId: true });
    if (contentId) return contentId;

    return this.extractTrustRegistryId(message);
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
      this.logger.warn(" No TrustRegistry messages to process");
      return;
    }

    const failThreshold = 0.1;
    const failedMessages: any[] = [];
    const seenTrIds: number[] = [];
    const syncedTrIds: number[] = []; 
    const seenHeightSyncKeys = new Set<string>();
    const totalMessages = trustRegistryList.length;
    const useHeightSyncTR =
      process.env.NODE_ENV !== "test" && process.env.USE_HEIGHT_SYNC_TR === "true";

    const processMessage = async (message: any, index: number) => {
      if (!message.type) {
        this.logger.error(`TR message missing type:`, JSON.stringify(message));
        failedMessages.push({ message, error: "Missing type" });
        return;
      }

      const messageTrId = this.resolveTrustRegistryIdForMessage(message);
      this.logger.info(`Processing TR message ${index + 1}/${totalMessages}: type=${message.type}, height=${message.height}, tr_id=${messageTrId ?? "n/a"}`);

      const processedTR: any = { ...message, ...message.content };
      const normalizedTrId = this.resolveTrustRegistryIdForMessage(message);
      if (normalizedTrId) {
        processedTR.trust_registry_id = normalizedTrId;
      }

      if (!useHeightSyncTR) {
        const numericId = this.resolveTrustRegistryIdForMessage(message);
        if (numericId) seenTrIds.push(numericId);
      }

      delete processedTR?.content;
      delete processedTR?.id;
      delete processedTR?.tx_id;
      delete processedTR?.["@type"];

      let processed = false;
      if (useHeightSyncTR) {
        if (normalizedTrId && Number.isFinite(Number(message.height))) {
          const dedupeKey = `${Number(message.height)}::${normalizedTrId}`;
          if (seenHeightSyncKeys.has(dedupeKey)) {
            this.logger.debug(`[TR Height-Sync] Skip duplicate message for key=${dedupeKey}`);
            processed = true;
          } else {
            seenHeightSyncKeys.add(dedupeKey);
          }
        }
        if (processed) return;
        const syncedTrId = await this.processTrustRegistryHeightSync(processedTR);
        if (syncedTrId && Number.isInteger(syncedTrId) && syncedTrId > 0) {
          syncedTrIds.push(syncedTrId);
        }
        processed = true;
      } else {
        if (
          processedTR.type === VeranaTrustRegistryMessageTypes.CreateTrustRegistry
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

  private async updateTRStatsAndSync(trId: number, messageTrId: number | string, height?: number): Promise<void> {
    try {
      const oldTr = await knex("trust_registry").where("id", trId).first();
      if (!oldTr) return;

      const stats = await calculateTrustRegistryStats(trId, height);
      const statsUpdate: any = {
        participants: Number(stats.participants ?? 0),
        participants_ecosystem: Number(stats.participants_ecosystem ?? 0),
        participants_issuer_grantor: Number(stats.participants_issuer_grantor ?? 0),
        participants_issuer: Number(stats.participants_issuer ?? 0),
        participants_verifier_grantor: Number(stats.participants_verifier_grantor ?? 0),
        participants_verifier: Number(stats.participants_verifier ?? 0),
        participants_holder: Number(stats.participants_holder ?? 0),
        active_schemas: Number(stats.active_schemas ?? 0),
        archived_schemas: Number(stats.archived_schemas ?? 0),
        weight: Number(stats.weight ?? 0),
        issued: Number(stats.issued ?? 0),
        verified: Number(stats.verified ?? 0),
        ecosystem_slash_events: Number(stats.ecosystem_slash_events ?? 0),
        ecosystem_slashed_amount: Number(stats.ecosystem_slashed_amount ?? 0),
        ecosystem_slashed_amount_repaid: Number(stats.ecosystem_slashed_amount_repaid ?? 0),
        network_slash_events: Number(stats.network_slash_events ?? 0),
        network_slashed_amount: Number(stats.network_slashed_amount ?? 0),
        network_slashed_amount_repaid: Number(stats.network_slashed_amount_repaid ?? 0),
      };

      const statsChanged = 
        Number(oldTr.participants ?? 0) !== statsUpdate.participants ||
        Number(oldTr.participants_ecosystem ?? 0) !== statsUpdate.participants_ecosystem ||
        Number(oldTr.participants_issuer_grantor ?? 0) !== statsUpdate.participants_issuer_grantor ||
        Number(oldTr.participants_issuer ?? 0) !== statsUpdate.participants_issuer ||
        Number(oldTr.participants_verifier_grantor ?? 0) !== statsUpdate.participants_verifier_grantor ||
        Number(oldTr.participants_verifier ?? 0) !== statsUpdate.participants_verifier ||
        Number(oldTr.participants_holder ?? 0) !== statsUpdate.participants_holder ||
        Number(oldTr.active_schemas ?? 0) !== statsUpdate.active_schemas ||
        Number(oldTr.archived_schemas ?? 0) !== statsUpdate.archived_schemas ||
        Number(oldTr.weight ?? 0) !== statsUpdate.weight ||
        Number(oldTr.issued ?? 0) !== statsUpdate.issued ||
        Number(oldTr.verified ?? 0) !== statsUpdate.verified ||
        Number(oldTr.ecosystem_slash_events ?? 0) !== statsUpdate.ecosystem_slash_events ||
        Number(oldTr.ecosystem_slashed_amount ?? 0) !== statsUpdate.ecosystem_slashed_amount ||
        Number(oldTr.ecosystem_slashed_amount_repaid ?? 0) !== statsUpdate.ecosystem_slashed_amount_repaid ||
        Number(oldTr.network_slash_events ?? 0) !== statsUpdate.network_slash_events ||
        Number(oldTr.network_slashed_amount ?? 0) !== statsUpdate.network_slashed_amount ||
        Number(oldTr.network_slashed_amount_repaid ?? 0) !== statsUpdate.network_slashed_amount_repaid;

      if (statsChanged) {
        this.logger.info(`Stats changed for TR ${trId}, updating main table and recording StatsUpdate history`);
      }

      await knex("trust_registry").where("id", trId).update(statsUpdate);

      if (statsChanged) {
        try {
          const updatedTr = await knex("trust_registry").where("id", trId).first();
          if (updatedTr) {
            const effectiveHeight = Number(height || updatedTr.height || oldTr.height || 0);
            await knex.transaction(async (trx) => {
              const updatedTrWithStats = { ...updatedTr, ...statsUpdate };
              await this.recordTRHistory(trx, trId, "StatsUpdate", effectiveHeight, oldTr, updatedTrWithStats);
            });
          } else {
            this.logger.warn(` Updated TR ${trId} not found after stats update`);
          }
        } catch (historyErr: any) {
          this.logger.warn(` Failed to record StatsUpdate history for TR ${trId}: ${historyErr?.message || String(historyErr)}`);
        }
      }

      if (!statsChanged) {
        this.logger.debug(` No stats changes detected for TR ${trId}, skipping history update`);
      }
    } catch (statsError: any) {
      this.logger.warn(` Failed to update statistics for TR ${trId}: ${statsError?.message || String(statsError)}`);
    }

    if (process.env.USE_HEIGHT_SYNC_TR === "true" && height) {
      try {
        const trIdNum = Number(messageTrId);
        const blockHeight = Number(height || 0);
        if (Number.isInteger(trIdNum) && trIdNum > 0 && blockHeight > 0) {
          const ledgerResponse = await getTrustRegistry(trIdNum, blockHeight);
          if (ledgerResponse?.trust_registry) {
            await this.broker.call(
              `${SERVICE.V1.TrustRegistryDatabaseService.path}.syncFromLedger`,
              {
                ledgerResponse: { trust_registry: ledgerResponse.trust_registry },
                blockHeight,
              }
            );
          } else {
            this.logger.warn(
              `[TR Ledger Sync] No ledger trust_registry found for id=${trIdNum} at height=${blockHeight}`
            );
          }
        }
      } catch (syncErr: any) {
        this.logger.warn(
          `[TR Ledger Sync] Failed to reconcile TR id=${messageTrId}: ${syncErr?.message || String(syncErr)}`
        );
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
    const hasIndexedStats =
      !!newData &&
      TR_STATS_FIELDS.some((field) => newData[field] !== undefined && newData[field] !== null);

    let stats: any;
    if (hasIndexedStats) {
      stats = getDefaultTRStats(newData);
    } else {
      try {
        stats = await calculateTrustRegistryStats(trId, height);
      } catch (err: any) {
        this.logger.warn(
          `Failed to calculate stats for TR ${trId} at height ${height}: ${err?.message || String(err)}`
        );
        stats = getDefaultTRStats(newData);
      }
    }

    const changes: ChangeRecord = {};

    if (oldData) {
      for (const [key, value] of Object.entries(newData)) {
        if (key !== 'id' && key !== 'height' && !TR_STATS_FIELDS.includes(key)) {
          const oldVal = oldData[key];
          if (oldVal !== value) {
            changes[key] = value;
          }
        }
      }

      for (const field of TR_STATS_FIELDS) {
        const oldVal = oldData[field] != null ? Number(oldData[field]) : 0;
        const newVal = stats[field] != null ? Number(stats[field]) : 0;
        if (oldVal !== newVal) {
          changes[field] = newVal;
        }
      }
    } else {
      for (const [key, value] of Object.entries(newData)) {
        if (key !== 'id' && key !== 'height' && !TR_STATS_FIELDS.includes(key) && value !== null && value !== undefined) {
          changes[key] = value;
        }
      }
      for (const field of TR_STATS_FIELDS) {
        const val = stats[field] != null ? Number(stats[field]) : 0;
        changes[field] = val;
      }
    }

    changes.height = Number(height);

    const historyPayload: any = {
      tr_id: trId,
      did: newData.did,
      corporation: newData.corporation,
      created: newData.created,
      modified: newData.modified,
      archived: newData.archived ?? null,
      aka: newData.aka ?? null,
      language: newData.language,
      active_version: newData.active_version ?? null,
      participants: Number(stats.participants ?? 0),
      participants_ecosystem: Number(stats.participants_ecosystem ?? 0),
      participants_issuer_grantor: Number(stats.participants_issuer_grantor ?? 0),
      participants_issuer: Number(stats.participants_issuer ?? 0),
      participants_verifier_grantor: Number(stats.participants_verifier_grantor ?? 0),
      participants_verifier: Number(stats.participants_verifier ?? 0),
      participants_holder: Number(stats.participants_holder ?? 0),
      active_schemas: Number(stats.active_schemas ?? 0),
      archived_schemas: Number(stats.archived_schemas ?? 0),
      weight: Number(stats.weight ?? 0),
      issued: Number(stats.issued ?? 0),
      verified: Number(stats.verified ?? 0),
      ecosystem_slash_events: Number(stats.ecosystem_slash_events ?? 0),
      ecosystem_slashed_amount: Number(stats.ecosystem_slashed_amount ?? 0),
      ecosystem_slashed_amount_repaid: Number(stats.ecosystem_slashed_amount_repaid ?? 0),
      network_slash_events: Number(stats.network_slash_events ?? 0),
      network_slashed_amount: Number(stats.network_slashed_amount ?? 0),
      network_slashed_amount_repaid: Number(stats.network_slashed_amount_repaid ?? 0),
      event_type: eventType,
      height: Number(height),
      changes: Object.keys(changes).length > 0 ? JSON.stringify(changes) : null,
      created_at: newData.modified ?? newData.created ?? new Date(),
    };

    const historyColumns = await this.getTrustRegistryHistoryColumns(trx);
    const rowForInsert = finalizeTrustRegistryHistoryInsert(
      historyColumns,
      historyPayload,
      newData
    ) as Record<string, any>;

    try {
      const existingSameEvent = await trx("trust_registry_history")
        .where({
          tr_id: trId,
          event_type: eventType,
          height: Number(height),
        })
        .orderBy("id", "desc")
        .first();
      if (existingSameEvent) {
        const existingChanges = existingSameEvent.changes ? String(existingSameEvent.changes) : null;
        const nextChanges = rowForInsert.changes ? String(rowForInsert.changes) : null;
        if (existingChanges === nextChanges) {
          this.logger.debug(`Skipping duplicate TR history for tr_id=${trId}, event_type=${eventType}, height=${height}`);
          return;
        }
      }

      await trx("trust_registry_history").insert(rowForInsert);
      this.logger.debug(` Recorded TR history for tr_id=${trId}, event_type=${eventType}, height=${height}`);
    } catch (insertErr: any) {
      this.logger.error(`❌ Failed to insert TR history for tr_id=${trId}: ${insertErr?.message || String(insertErr)}`);
      throw insertErr;
    }
  }

  private async getTrustRegistryHistoryColumns(trx: any): Promise<Set<string>> {
    if (this.trHistoryColumnsCache) {
      return this.trHistoryColumnsCache;
    }
    const info = await trx("trust_registry_history").columnInfo();
    this.trHistoryColumnsCache = new Set(Object.keys(info || {}));
    return this.trHistoryColumnsCache;
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
    const isCreation = !oldData;
    
    if (oldData) {
      const computed = this.processorBase.computeChanges(oldData, newData);
      changes = Object.keys(computed).length > 0 ? computed : null;
    } else {
      const creationChanges: ChangeRecord = {};
      for (const [key, value] of Object.entries(newData)) {
        if (value !== null && value !== undefined && key !== 'id') {
          creationChanges[key] = value;
        }
      }
      changes = Object.keys(creationChanges).length > 0 ? creationChanges : null;
    }

    if (!isCreation && !changes) {
      return;
    }

    await trx("governance_framework_version_history").insert({
      tr_id: trId,
      created: newData.created || new Date(),
      version: newData.version,
      active_since: newData.active_since || newData.created || new Date(),
      event_type: eventType,
      height,
      changes: changes ? JSON.stringify(changes) : null,
      created_at: newData.active_since || newData.created || new Date(),
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
    const isCreation = !oldData;
    
    if (oldData) {
      const computed = this.processorBase.computeChanges(oldData, newData);
      changes = Object.keys(computed).length > 0 ? computed : null;
    } else {
      const creationChanges: ChangeRecord = {};
      for (const [key, value] of Object.entries(newData)) {
        if (value !== null && value !== undefined && key !== 'id') {
          creationChanges[key] = value;
        }
      }
      changes = Object.keys(creationChanges).length > 0 ? creationChanges : null;
    }

    if (!isCreation && !changes) {
      return;
    }

    await trx("governance_framework_document_history").insert({
      gfv_id: gfvId,
      tr_id: trId,
      created: newData.created || new Date(),
      language: newData.language || "",
      url: newData.url || "",
      digest_sri: newData.digest_sri || "",
      event_type: eventType,
      height,
      changes: changes ? JSON.stringify(changes) : null,
      created_at: newData.created || new Date(),
    });
  }

  private async processTrustRegistryHeightSync(message: any): Promise<number | null> {
    const trId =
      this.resolveTrustRegistryIdForMessage(message) ??
      this.extractTrustRegistryId(message, { allowTopLevelId: true });
    const heightNum = Number(message.height || 0);

    if (!trId) {
      this.logger.warn(
        `[TR Height-Sync] Skipping message with invalid trust_registry_id=${String(
          message?.trust_registry_id ?? message?.trustRegistryId ?? message?.tr_id ?? message?.trId ?? message?.id
        )}, height=${message.height}`
      );
      return null;
    }
    if (!Number.isFinite(heightNum) || heightNum <= 0) {
      this.logger.warn(
        `[TR Height-Sync] Skipping message for tr_id=${trId} due to invalid height=${message.height}`
      );
      return null;
    }

    const blockHeight = heightNum;

    try {
      await knex("trust_registry").where({ id: trId }).first();
    } catch (err: any) {
      this.logger.warn(
        `[TR Height-Sync] Failed to load previous TR row for id=${trId}: ${
          err?.message || String(err)
        }`
      );
    }

    let actualTrId: number | null = null;
    try {
      const ledgerResponse = await getTrustRegistry(trId, blockHeight);
      if (!ledgerResponse?.trust_registry) {
        this.logger.warn(
          `[TR Height-Sync] Ledger returned no trust_registry for id=${trId} at height=${blockHeight}`
        );
        return null;
      }

      const ledgerTr = ledgerResponse.trust_registry;
      const extractedTrId = Number(ledgerTr.id ?? ledgerTr.tr_id ?? trId);
      if (Number.isInteger(extractedTrId) && extractedTrId > 0) {
        actualTrId = extractedTrId;
      } else {
        actualTrId = trId; 
      }

      const syncResult: any = await this.broker.call(
        `${SERVICE.V1.TrustRegistryDatabaseService.path}.syncFromLedger`,
        {
          ledgerResponse: { trust_registry: ledgerResponse.trust_registry },
          blockHeight,
        }
      );

      if (!syncResult || syncResult.success !== true) {
        this.logger.warn(
          `[TR Height-Sync] syncFromLedger reported failure for id=${actualTrId} at height=${blockHeight}: ${JSON.stringify(syncResult)}`
        );
        return null;
      }

      if (!actualTrId) {
        actualTrId = trId;
      }
    } catch (err: any) {
      this.logger.warn(
        `[TR Height-Sync] Failed to sync TR id=${trId} from ledger at height=${blockHeight}: ${
          err?.message || String(err)
        }`
      );
      return null;
    }

    let newTr: any | null = null;
    try {
      newTr = await knex("trust_registry").where({ id: actualTrId! }).first();
    } catch (err: any) {
      this.logger.warn(
        `[TR Height-Sync] Failed to load updated TR row for id=${actualTrId}: ${
          err?.message || String(err)
        }`
      );
    }
    if (!newTr) {
      this.logger.warn(
        `[TR Height-Sync] No persisted TR row found after sync for id=${actualTrId} at height=${blockHeight}`
      );
      return null;
    }
     return actualTrId;
  }

  private async processArchiveTR(message: any) {
    const trx = await knex.transaction();
    try {
      const tr = await trx("trust_registry")
        .where({ id: message.trust_registry_id })
        .first();
      if (!tr) {
        await trx.rollback();
        this.logger.warn(` ArchiveTR: TR not found for id=${message.trust_registry_id}, height=${message.height}`);
        return;
      }

      const timestamp = formatTimestamp(message.timestamp);
      const shouldArchive = message.archive === true || message.archive === "true";
      const newData = {
        ...tr,
        archived: shouldArchive ? timestamp : null,
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
      this.logger.info(` Successfully archived TR: id=${tr.id}`);

      await this.updateTRStatsAndSync(tr.id, message.trust_registry_id ?? tr.id, message.height);
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
        this.logger.warn(` UpdateTR: TR not found for id=${message.trust_registry_id}, height=${message.height}`);
        return;
      }

      const updateData: any = { ...tr };
      if (message.did !== undefined) updateData.did = message.did;
      if (message.aka !== undefined) updateData.aka = message.aka;
      if (message.language !== undefined) updateData.language = message.language;
      if (message.height !== undefined) updateData.height = message.height;
      updateData.modified = formatTimestamp(message.timestamp);

      await trx("trust_registry").where({ id: tr.id }).update(updateData);
      const blockHeight = message.height || 0;
      const updatedTr = await trx("trust_registry").where({ id: tr.id }).first();
      if (updatedTr) {
        await this.recordTRHistory(
          trx,
          tr.id,
          "Update",
          blockHeight,
          tr,
          updatedTr
        );
      }

      await trx.commit();
      this.logger.info(` Successfully updated TR: id=${tr.id}`);

      await this.updateTRStatsAndSync(tr.id, message.trust_registry_id ?? tr.id, message.height);
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
      const timestamp = formatTimestamp(message.timestamp);
      const blockHeight = message.height || 0;

      this.logger.info(` Creating TR with height: ${blockHeight}, did: ${message.did}`);

      const existingTR = await trx("trust_registry")
        .where({ did: message.did, height: blockHeight })
        .first();

      let tr;
      const corporation = requireController(message, `TR ${message.did}`);
      const isReindexing = !!existingTR;

      if (isReindexing) {
        this.logger.info(`TR with did ${message.did} and height ${blockHeight} already exists, updating for reindexing...`);
        [tr] = await trx("trust_registry")
          .where({ id: existingTR.id })
          .update({
            did: message.did,
            corporation,
            modified: timestamp,
            aka: message.aka,
            language: message.language,
            height: blockHeight, 
          })
          .returning("*");
      } else {
        this.logger.info(`🆕 Creating new TR with did ${message.did} at height ${blockHeight}`);
        [tr] = await trx("trust_registry")
          .insert({
            did: message.did,
            corporation,
            created: timestamp,
            modified: timestamp,
            aka: message.aka,
            language: message.language,
            height: blockHeight,
            active_version: 1,
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
      this.logger.info(` Successfully created/updated TR: did=${message.did}, id=${tr.id}`);

      await this.updateTRStatsAndSync(tr.id, tr.id, message.height);
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
      }

      const language = message.doc_language || message.language;
      const digestSri = message.doc_digest_sri || message.digest_sri;

      let gfd = await trx("governance_framework_document")
        .where({
          gfv_id: gfv.id,
          digest_sri: digestSri,
        })
        .first();

      if (gfd) {
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
        ` AddGovFrameworkDoc OK: tr_id=${tr.id}, gfv_version=${message.version}, gfd_id=${gfd.id}`
      );
    } catch (err: any) {
      await trx.rollback();
      this.logger.error(
        `❌ AddGovFrameworkDoc failed for tr_id=${message.trust_registry_id}:`,
        err?.message || err
      );
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
      this.logger.info(` Successfully increased active GFV: tr_id=${tr.id}, version=${nextVersion}`);
      
      await this.updateTRStatsAndSync(tr.id, message.trust_registry_id ?? tr.id, message.height);
    } catch (err: any) {
      await trx.rollback();
      const errorMessage = err?.message || String(err);
      this.logger.error(`❌ Failed to process IncreaseActiveGFV for tr_id=${message.trust_registry_id}:`, errorMessage);
      console.error("FATAL TR INCREASE GFV ERROR:", err);
      throw err;
    }
  }

}
