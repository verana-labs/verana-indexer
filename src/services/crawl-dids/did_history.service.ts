import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import { SERVICE } from "../../common";
import { DidHistoryRecord, DidHistoryRepository } from "../../models/did_history";
import ApiResponder from "../../common/utils/apiResponse";
import knex from "../../common/utils/db_connection";


@Service({
  name: SERVICE.V1.DidHistoryService.key,
  version: 1,
})
export default class DidHistoryService extends BullableService {
  constructor(broker: ServiceBroker) {
    super(broker);
  }

  @Action({ name: "save" })
  async save(ctx: { params: DidHistoryRecord }) {
    try {
      const record = ctx.params;
      delete record?.id
      this.logger.info("Attempting to save DID history:", record);
      await DidHistoryRepository.insertHistory(record);
      this.logger.info("✅ DID history saved successfully:", record);
      return record;
    } catch (err) {
      this.logger.error("❌ Error saving DID history:", err);
      console.error("FATAL DID HISTORY SAVE ERROR:", err);
      return {
        success: false,
        error: {
          code: 500,
          name: "DidHistorySaveError",
          message: "Failed to save DID history",
        },
      };
    }
  }
  @Action({ 
    name: "getByDid", 
    params: { 
      did: "string",
      response_max_size: { type: "number", optional: true, default: 64 },
      transaction_timestamp_older_than: { type: "string", optional: true },
    } 
  })
  async getByDid(ctx: Context<{ did: string; response_max_size?: number; transaction_timestamp_older_than?: string }>) {
    try {
      const { did, response_max_size: responseMaxSize = 64, transaction_timestamp_older_than: transactionTimestampOlderThan } = ctx.params;
      
      if (transactionTimestampOlderThan) {
        const { isValidISO8601UTC } = await import("../../common/utils/date_utils");
        if (!isValidISO8601UTC(transactionTimestampOlderThan)) {
          return ApiResponder.error(
            ctx,
            "Invalid transaction_timestamp_older_than format. Must be ISO 8601 UTC format (e.g., '2026-01-18T10:00:00Z' or '2026-01-18T10:00:00.000Z')",
            400
          );
        }
        const timestampDate = new Date(transactionTimestampOlderThan);
        if (Number.isNaN(timestampDate.getTime())) {
          return ApiResponder.error(ctx, "Invalid transaction_timestamp_older_than format", 400);
        }
      }
      const atBlockHeight = (ctx.meta as any)?.$headers?.["at-block-height"] || (ctx.meta as any)?.$headers?.["At-Block-Height"];

      const didExists = await knex("dids").where({ did }).first();
      if (!didExists) {
        return ApiResponder.error(ctx, `DID ${did} not found`, 404);
      }

      const { buildActivityTimeline } = await import("../../common/utils/activity_timeline_helper");
      const activity = await buildActivityTimeline(
        {
          entityType: "DID",
          historyTable: "did_history",
          idField: "did",
          entityId: did,
          msgTypePrefixes: ["/verana.dd.v1", "/veranablockchain.diddirectory"],
        },
        {
          responseMaxSize,
          transactionTimestampOlderThan,
          atBlockHeight,
        }
      );

      const result = {
        entity_type: "DID",
        entity_id: did,
        activity: activity || [],
      };

      return ApiResponder.success(ctx, result, 200);
    } catch (err: any) {
      this.logger.error("Error fetching DID history:", err);
      this.logger.error("Error stack:", err?.stack);
      this.logger.error("Error details:", {
        message: err?.message,
        code: err?.code,
        name: err?.name,
      });
      return ApiResponder.error(ctx, `Failed to get DID history: ${err?.message || "Unknown error"}`, 500);
    }
  }



  public async _start() {
    await super._start();
    this.logger.info("DidHistoryService started and ready.");
  }
}
