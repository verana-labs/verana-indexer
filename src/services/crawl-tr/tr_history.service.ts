/* eslint-disable @typescript-eslint/no-explicit-any */
import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BaseService from "../../base/base.service";
import { SERVICE } from "../../common";
import ApiResponder from "../../common/utils/apiResponse";
import knex from "../../common/utils/db_connection";

@Service({
  name: SERVICE.V1.TrustRegistryHistoryService.key,
  version: 1,
})
export default class TrustRegistryHistoryService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker);
  }

  @Action()
  public async getTRHistory(ctx: Context<{ tr_id: number; response_max_size?: number; transaction_timestamp_older_than?: string }>) {
    try {
      const { tr_id: trId, response_max_size: responseMaxSize = 64, transaction_timestamp_older_than: transactionTimestampOlderThan } = ctx.params;
      const atBlockHeight = (ctx.meta as any)?.$headers?.["at-block-height"] || (ctx.meta as any)?.$headers?.["At-Block-Height"];

      const tr = await knex("trust_registry").where("id", trId).first();
      if (!tr) return ApiResponder.error(ctx, "Trust Registry not found", 404);

      const { buildActivityTimeline } = await import("../../common/utils/activity_timeline_helper");
      const activity = await buildActivityTimeline(
        {
          entityType: "TrustRegistry",
          historyTable: "trust_registry_history",
          idField: "tr_id",
          entityId: trId,
          msgTypePrefixes: ["/verana.tr.v1", "/veranablockchain.trustregistry"],
          relatedEntities: [
            {
              entityType: "GovernanceFrameworkVersion",
              historyTable: "governance_framework_version_history",
              idField: "tr_id",
              entityIdField: "gfv_id",
              msgTypePrefixes: ["/verana.tr.v1", "/veranablockchain.trustregistry"],
            },
            {
              entityType: "GovernanceFrameworkDocument",
              historyTable: "governance_framework_document_history",
              idField: "tr_id",
              entityIdField: "gfd_id",
              msgTypePrefixes: ["/verana.tr.v1", "/veranablockchain.trustregistry"],
            },
          ],
        },
        {
          responseMaxSize,
          transactionTimestampOlderThan,
          atBlockHeight,
        }
      );

      const result = {
        entity_type: "TrustRegistry",
        entity_id: String(trId),
        activity: activity || [],
      };

      return ApiResponder.success(ctx, result, 200);
    } catch (err: any) {
      this.logger.error("Error fetching TR history:", err);
      this.logger.error("Error stack:", err?.stack);
      this.logger.error("Error details:", {
        message: err?.message,
        code: err?.code,
        name: err?.name,
      });
      return ApiResponder.error(ctx, `Failed to get Trust Registry history: ${err?.message || "Unknown error"}`, 500);
    }
  }
}
