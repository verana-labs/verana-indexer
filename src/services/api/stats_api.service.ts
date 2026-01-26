import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker, Errors } from "moleculer";
import BaseService from "../../base/base.service";
import { SERVICE } from "../../common";
import ApiResponder from "../../common/utils/apiResponse";
import Stats, { Granularity, EntityType } from "../../models/stats";
import knex from "../../common/utils/db_connection";

@Service({
  name: SERVICE.V1.StatsAPIService.key,
  version: 1,
})
export default class StatsAPIService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker);
  }

  @Action({
    name: "get",
    params: {
      id: { type: "number", integer: true, positive: true, optional: true, convert: true },
      granularity: { type: "enum", values: ["HOUR", "DAY", "MONTH"], optional: true },
      timestamp: { type: "string", optional: true },
      entity_type: { type: "enum", values: ["GLOBAL", "TRUST_REGISTRY", "CREDENTIAL_SCHEMA", "PERMISSION"], optional: true },
      entity_id: { type: "string", optional: true },
    },
  })
  public async get(ctx: Context<{
    id?: number;
    granularity?: Granularity;
    timestamp?: string;
    entity_type?: EntityType;
    entity_id?: string;
  }>): Promise<unknown> {
    try {
      const { id, granularity, timestamp, entity_type: entityType, entity_id: entityId } = ctx.params;

      if (id) {
        const stat = await Stats.query().findById(id);
        if (!stat) {
          return ApiResponder.error(ctx, "Stats not found", 404, "NOT_FOUND");
        }
        return ApiResponder.success(ctx, stat, 200);
      }

      if (!granularity || !timestamp || !entityType) {
        return ApiResponder.error(
          ctx,
          "Either 'id' or all of 'granularity', 'timestamp', 'entity_type' must be provided",
          400,
          "INVALID_PARAMS"
        );
      }

      if (entityType === "GLOBAL" && entityId) {
        return ApiResponder.error(ctx, "entity_id must be null for GLOBAL entity_type", 400, "INVALID_PARAMS");
      }

      if (entityType !== "GLOBAL" && !entityId) {
        return ApiResponder.error(ctx, `entity_id is required for entity_type ${entityType}`, 400, "INVALID_PARAMS");
      }

      const timestampDate = new Date(timestamp);
      if (Number.isNaN(timestampDate.getTime())) {
        return ApiResponder.error(ctx, "Invalid timestamp format", 400, "INVALID_PARAMS");
      }

      const stat = await Stats.query()
        .where("granularity", granularity)
        .where("timestamp", timestampDate)
        .where("entity_type", entityType)
        .where((builder) => {
          if (entityType === "GLOBAL") {
            builder.whereNull("entity_id");
          } else if (entityId) {
            builder.where("entity_id", entityId);
          }
        })
        .first();

      if (!stat) {
        return ApiResponder.error(ctx, "Stats not found", 404, "NOT_FOUND");
      }

      return ApiResponder.success(ctx, stat, 200);
    } catch (err: unknown) {
      this.logger.error("Error in get:", err);
      return ApiResponder.error(ctx, "Internal Server Error", 500, "INTERNAL_ERROR");
    }
  }

  @Action({
    name: "stats",
    params: {
      granularity: { type: "enum", values: ["HOUR", "DAY", "MONTH"], optional: true },
      timestamp_from: { type: "string", convert: true },
      timestamp_until: { type: "string", convert: true },
      entity_type: { type: "enum", values: ["GLOBAL", "TRUST_REGISTRY", "CREDENTIAL_SCHEMA", "PERMISSION"], convert: true },
      entity_ids: { type: "string", optional: true },
      result_type: { type: "enum", values: ["BUCKETS", "TOTAL", "BUCKETS_AND_TOTAL"], optional: true, default: "BUCKETS_AND_TOTAL" },
    },
  })
  public async stats(ctx: Context<{
    granularity?: Granularity;
    timestamp_from: string;
    timestamp_until: string;
    entity_type: EntityType;
    entity_ids?: string;
    result_type?: "BUCKETS" | "TOTAL" | "BUCKETS_AND_TOTAL";
  }>): Promise<unknown> {
    try {
      const { granularity, timestamp_from: timestampFrom, timestamp_until: timestampUntil, entity_type: entityType, entity_ids: entityIds, result_type: resultType = "BUCKETS_AND_TOTAL" } = ctx.params;

    const fromDate = new Date(timestampFrom);
    const untilDate = new Date(timestampUntil);

    if (Number.isNaN(fromDate.getTime()) || Number.isNaN(untilDate.getTime())) {
      return ApiResponder.error(ctx, "Invalid timestamp format", 400, "INVALID_PARAMS");
    }

    if (fromDate >= untilDate) {
      return ApiResponder.error(ctx, "timestamp_from must be before timestamp_until", 400, "INVALID_PARAMS");
    }

    const parsedEntityIds = (entityIds || "")
      .split(",")
      .map((id) => id.trim())
      .filter((id) => id.length > 0);

    if (entityType === "GLOBAL" && parsedEntityIds.length > 0) {
      return ApiResponder.error(ctx, "entity_ids must be empty for GLOBAL entity_type", 400, "INVALID_PARAMS");
    }

    if (entityType !== "GLOBAL" && parsedEntityIds.length === 0) {
      return ApiResponder.error(ctx, `entity_ids array is required for entity_type ${entityType}`, 400, "INVALID_PARAMS");
    }

    let effectiveGranularity = granularity;
    if (!effectiveGranularity) {
      const hoursDiff = (untilDate.getTime() - fromDate.getTime()) / (1000 * 60 * 60);
      const daysDiff = hoursDiff / 24;
      const monthsDiff = daysDiff / 30;

      if (monthsDiff >= 1) {
        effectiveGranularity = "MONTH";
      } else if (daysDiff >= 1) {
        effectiveGranularity = "DAY";
      } else {
        effectiveGranularity = "HOUR";
      }
    }

    let buckets: Stats[] = [];

    if (!granularity) {
      const hoursDiff = (untilDate.getTime() - fromDate.getTime()) / (1000 * 60 * 60);
      const daysDiff = hoursDiff / 24;
      const monthsDiff = daysDiff / 30;

      if (monthsDiff >= 1) {
        const monthBuckets = await Stats.query()
          .where("granularity", "MONTH")
          .where("timestamp", ">=", fromDate)
          .where("timestamp", "<", untilDate)
          .where("entity_type", entityType)
          .where((builder) => {
            if (entityType === "GLOBAL") {
              builder.whereNull("entity_id");
            } else {
              builder.whereIn("entity_id", parsedEntityIds);
            }
          })
          .orderBy("timestamp", "asc");

        if (monthBuckets.length > 0) {
          buckets = monthBuckets;
          effectiveGranularity = "MONTH";
          } else if (daysDiff >= 1) {
            const dayBuckets = await Stats.query()
              .where("granularity", "DAY")
              .where("timestamp", ">=", fromDate)
              .where("timestamp", "<", untilDate)
              .where("entity_type", entityType)
              .where((builder) => {
                if (entityType === "GLOBAL") {
                  builder.whereNull("entity_id");
                } else {
                  builder.whereIn("entity_id", parsedEntityIds);
                }
              })
              .orderBy("timestamp", "asc");

            if (dayBuckets.length > 0) {
              buckets = dayBuckets;
              effectiveGranularity = "DAY";
            } else {
              const hourBuckets = await Stats.query()
                .where("granularity", "HOUR")
                .where("timestamp", ">=", fromDate)
                .where("timestamp", "<", untilDate)
                .where("entity_type", entityType)
                .where((builder) => {
                  if (entityType === "GLOBAL") {
                    builder.whereNull("entity_id");
                  } else {
                    builder.whereIn("entity_id", parsedEntityIds);
                  }
                })
                .orderBy("timestamp", "asc");

              buckets = hourBuckets;
              effectiveGranularity = "HOUR";
            }
          } else {
            const hourBuckets = await Stats.query()
              .where("granularity", "HOUR")
              .where("timestamp", ">=", fromDate)
              .where("timestamp", "<", untilDate)
              .where("entity_type", entityType)
              .where((builder) => {
                if (entityType === "GLOBAL") {
                  builder.whereNull("entity_id");
                } else {
                  builder.whereIn("entity_id", parsedEntityIds);
                }
              })
              .orderBy("timestamp", "asc");

            buckets = hourBuckets;
            effectiveGranularity = "HOUR";
          }
      } else if (daysDiff >= 1) {
        const dayBuckets = await Stats.query()
          .where("granularity", "DAY")
          .where("timestamp", ">=", fromDate)
          .where("timestamp", "<", untilDate)
          .where("entity_type", entityType)
          .where((builder) => {
            if (entityType === "GLOBAL") {
              builder.whereNull("entity_id");
            } else {
              builder.whereIn("entity_id", parsedEntityIds);
            }
          })
          .orderBy("timestamp", "asc");

        if (dayBuckets.length > 0) {
          buckets = dayBuckets;
          effectiveGranularity = "DAY";
        } else {
          const hourBuckets = await Stats.query()
            .where("granularity", "HOUR")
            .where("timestamp", ">=", fromDate)
            .where("timestamp", "<", untilDate)
            .where("entity_type", entityType)
            .where((builder) => {
              if (entityType === "GLOBAL") {
                builder.whereNull("entity_id");
              } else {
                builder.whereIn("entity_id", parsedEntityIds);
              }
            })
            .orderBy("timestamp", "asc");

          buckets = hourBuckets;
          effectiveGranularity = "HOUR";
        }
      } else {
        const hourBuckets = await Stats.query()
          .where("granularity", "HOUR")
          .where("timestamp", ">=", fromDate)
          .where("timestamp", "<", untilDate)
          .where("entity_type", entityType)
          .where((builder) => {
            if (entityType === "GLOBAL") {
              builder.whereNull("entity_id");
            } else {
              builder.whereIn("entity_id", parsedEntityIds);
            }
          })
          .orderBy("timestamp", "asc");

        buckets = hourBuckets;
        effectiveGranularity = "HOUR";
      }
    } else {
      const query = Stats.query()
        .where("granularity", effectiveGranularity)
        .where("timestamp", ">=", fromDate)
        .where("timestamp", "<", untilDate)
        .where("entity_type", entityType);

      if (entityType === "GLOBAL") {
        query.whereNull("entity_id");
      } else {
        query.whereIn("entity_id", parsedEntityIds);
      }

      query.orderBy("timestamp", "asc");

      buckets = await query;
    }

    let total: Record<string, unknown> | null = null;
    if (resultType === "TOTAL" || resultType === "BUCKETS_AND_TOTAL") {
      let totalQuery = knex("stats")
        .where("timestamp", ">=", fromDate)
        .where("timestamp", "<", untilDate)
        .where("entity_type", entityType);

      if (granularity) {
        totalQuery = totalQuery.where("granularity", effectiveGranularity);
      }

      if (entityType === "GLOBAL") {
        totalQuery = totalQuery.whereNull("entity_id");
      } else {
        totalQuery = totalQuery.whereIn("entity_id", parsedEntityIds);
      }

      const totalResult = await totalQuery
        .select(
          knex.raw("COALESCE(SUM(delta_participants), 0) as delta_participants"),
          knex.raw("COALESCE(SUM(delta_active_schemas), 0) as delta_active_schemas"),
          knex.raw("COALESCE(SUM(delta_archived_schemas), 0) as delta_archived_schemas"),
          knex.raw("COALESCE(SUM(CAST(delta_weight AS NUMERIC)), 0)::text as delta_weight"),
          knex.raw("COALESCE(SUM(CAST(delta_issued AS NUMERIC)), 0)::text as delta_issued"),
          knex.raw("COALESCE(SUM(CAST(delta_verified AS NUMERIC)), 0)::text as delta_verified"),
          knex.raw("COALESCE(SUM(delta_ecosystem_slash_events), 0) as delta_ecosystem_slash_events"),
          knex.raw("COALESCE(SUM(CAST(delta_ecosystem_slashed_amount AS NUMERIC)), 0)::text as delta_ecosystem_slashed_amount"),
          knex.raw("COALESCE(SUM(CAST(delta_ecosystem_slashed_amount_repaid AS NUMERIC)), 0)::text as delta_ecosystem_slashed_amount_repaid"),
          knex.raw("COALESCE(SUM(delta_network_slash_events), 0) as delta_network_slash_events"),
          knex.raw("COALESCE(SUM(CAST(delta_network_slashed_amount AS NUMERIC)), 0)::text as delta_network_slashed_amount"),
          knex.raw("COALESCE(SUM(CAST(delta_network_slashed_amount_repaid AS NUMERIC)), 0)::text as delta_network_slashed_amount_repaid")
        )
        .first();

      total = await totalResult;
    }

    const response: Record<string, unknown> = {
      granularity: effectiveGranularity,
      timestamp_from: timestampFrom,
      timestamp_until: timestampUntil,
      entity_type: entityType,
      entity_ids: parsedEntityIds,
      result_type: resultType,
    };

    if (resultType === "BUCKETS" || resultType === "BUCKETS_AND_TOTAL") {
      response.buckets = buckets.map((bucket) => {
        const timestamp = new Date(bucket.timestamp);
        return {
          timestamp: timestamp.toISOString().replace(/\.\d{3}Z$/, "Z"),
          cumulative_participants: bucket.cumulative_participants,
          cumulative_active_schemas: bucket.cumulative_active_schemas,
          cumulative_archived_schemas: bucket.cumulative_archived_schemas,
          cumulative_weight: bucket.cumulative_weight,
          cumulative_issued: bucket.cumulative_issued,
          cumulative_verified: bucket.cumulative_verified,
          cumulative_ecosystem_slash_events: bucket.cumulative_ecosystem_slash_events,
          cumulative_ecosystem_slashed_amount: bucket.cumulative_ecosystem_slashed_amount,
          cumulative_ecosystem_slashed_amount_repaid: bucket.cumulative_ecosystem_slashed_amount_repaid,
          cumulative_network_slash_events: bucket.cumulative_network_slash_events,
          cumulative_network_slashed_amount: bucket.cumulative_network_slashed_amount,
          cumulative_network_slashed_amount_repaid: bucket.cumulative_network_slashed_amount_repaid,
          delta_participants: bucket.delta_participants,
          delta_active_schemas: bucket.delta_active_schemas,
          delta_archived_schemas: bucket.delta_archived_schemas,
          delta_weight: bucket.delta_weight,
          delta_issued: bucket.delta_issued,
          delta_verified: bucket.delta_verified,
          delta_ecosystem_slash_events: bucket.delta_ecosystem_slash_events,
          delta_ecosystem_slashed_amount: bucket.delta_ecosystem_slashed_amount,
          delta_ecosystem_slashed_amount_repaid: bucket.delta_ecosystem_slashed_amount_repaid,
          delta_network_slash_events: bucket.delta_network_slash_events,
          delta_network_slashed_amount: bucket.delta_network_slashed_amount,
          delta_network_slashed_amount_repaid: bucket.delta_network_slashed_amount_repaid,
        };
      });
    }

    if (resultType === "TOTAL" || resultType === "BUCKETS_AND_TOTAL") {
      response.total = {
        delta_participants: Number(total?.delta_participants || 0),
        delta_active_schemas: Number(total?.delta_active_schemas || 0),
        delta_archived_schemas: Number(total?.delta_archived_schemas || 0),
        delta_weight: total?.delta_weight || "0",
        delta_issued: total?.delta_issued || "0",
        delta_verified: total?.delta_verified || "0",
        delta_ecosystem_slash_events: Number(total?.delta_ecosystem_slash_events || 0),
        delta_ecosystem_slashed_amount: total?.delta_ecosystem_slashed_amount || "0",
        delta_ecosystem_slashed_amount_repaid: total?.delta_ecosystem_slashed_amount_repaid || "0",
        delta_network_slash_events: Number(total?.delta_network_slash_events || 0),
        delta_network_slashed_amount: total?.delta_network_slashed_amount || "0",
        delta_network_slashed_amount_repaid: total?.delta_network_slashed_amount_repaid || "0",
      };
    }

      return ApiResponder.success(ctx, response, 200);
    } catch (err: unknown) {
      this.logger.error("Error in stats:", err);
      return ApiResponder.error(ctx, "Internal Server Error", 500, "INTERNAL_ERROR");
    }
  }
}
