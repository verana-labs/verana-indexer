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
  }>): Promise<any> {
    const { id, granularity, timestamp, entity_type, entity_id } = ctx.params;

    if (id) {
      const stat = await Stats.query().findById(id);
      if (!stat) {
        throw new Errors.MoleculerError("Stats not found", 404, "NOT_FOUND");
      }
      return ApiResponder.success(ctx, stat, 200);
    }

    if (!granularity || !timestamp || !entity_type) {
      throw new Errors.MoleculerError(
        "Either 'id' or all of 'granularity', 'timestamp', 'entity_type' must be provided",
        400,
        "INVALID_PARAMS"
      );
    }

    if (entity_type === "GLOBAL" && entity_id) {
      throw new Errors.MoleculerError("entity_id must be null for GLOBAL entity_type", 400, "INVALID_PARAMS");
    }

    if (entity_type !== "GLOBAL" && !entity_id) {
      throw new Errors.MoleculerError(`entity_id is required for entity_type ${entity_type}`, 400, "INVALID_PARAMS");
    }

    const timestampDate = new Date(timestamp);
    if (isNaN(timestampDate.getTime())) {
      throw new Errors.MoleculerError("Invalid timestamp format", 400, "INVALID_PARAMS");
    }

    const stat = await Stats.query()
      .where("granularity", granularity)
      .where("timestamp", timestampDate)
      .where("entity_type", entity_type)
      .where((builder) => {
        if (entity_type === "GLOBAL") {
          builder.whereNull("entity_id");
        } else {
          builder.where("entity_id", entity_id);
        }
      })
      .first();

    if (!stat) {
      throw new Errors.MoleculerError("Stats not found", 404, "NOT_FOUND");
    }

    return ApiResponder.success(ctx, stat, 200);
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
  }>): Promise<any> {
    const { granularity, timestamp_from, timestamp_until, entity_type, entity_ids, result_type = "BUCKETS_AND_TOTAL" } = ctx.params;

    const fromDate = new Date(timestamp_from);
    const untilDate = new Date(timestamp_until);

    if (isNaN(fromDate.getTime()) || isNaN(untilDate.getTime())) {
      throw new Errors.MoleculerError("Invalid timestamp format", 400, "INVALID_PARAMS");
    }

    if (fromDate >= untilDate) {
      throw new Errors.MoleculerError("timestamp_from must be before timestamp_until", 400, "INVALID_PARAMS");
    }

    const entityIds = (entity_ids || "")
      .split(",")
      .map((id) => id.trim())
      .filter((id) => id.length > 0);

    if (entity_type === "GLOBAL" && entityIds.length > 0) {
      throw new Errors.MoleculerError("entity_ids must be empty for GLOBAL entity_type", 400, "INVALID_PARAMS");
    }

    if (entity_type !== "GLOBAL" && entityIds.length === 0) {
      throw new Errors.MoleculerError(`entity_ids array is required for entity_type ${entity_type}`, 400, "INVALID_PARAMS");
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
          .where("entity_type", entity_type)
          .where((builder) => {
            if (entity_type === "GLOBAL") {
              builder.whereNull("entity_id");
            } else {
              builder.whereIn("entity_id", entityIds);
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
            .where("entity_type", entity_type)
            .where((builder) => {
              if (entity_type === "GLOBAL") {
                builder.whereNull("entity_id");
              } else {
                builder.whereIn("entity_id", entityIds);
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
              .where("entity_type", entity_type)
              .where((builder) => {
                if (entity_type === "GLOBAL") {
                  builder.whereNull("entity_id");
                } else {
                  builder.whereIn("entity_id", entityIds);
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
            .where("entity_type", entity_type)
            .where((builder) => {
              if (entity_type === "GLOBAL") {
                builder.whereNull("entity_id");
              } else {
                builder.whereIn("entity_id", entityIds);
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
          .where("entity_type", entity_type)
          .where((builder) => {
            if (entity_type === "GLOBAL") {
              builder.whereNull("entity_id");
            } else {
              builder.whereIn("entity_id", entityIds);
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
            .where("entity_type", entity_type)
            .where((builder) => {
              if (entity_type === "GLOBAL") {
                builder.whereNull("entity_id");
              } else {
                builder.whereIn("entity_id", entityIds);
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
          .where("entity_type", entity_type)
          .where((builder) => {
            if (entity_type === "GLOBAL") {
              builder.whereNull("entity_id");
            } else {
              builder.whereIn("entity_id", entityIds);
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
        .where("entity_type", entity_type);

      if (entity_type === "GLOBAL") {
        query.whereNull("entity_id");
      } else {
        query.whereIn("entity_id", entityIds);
      }

      query.orderBy("timestamp", "asc");

      buckets = await query;
    }

    let total: any = null;
    if (result_type === "TOTAL" || result_type === "BUCKETS_AND_TOTAL") {
      let totalQuery = knex("stats")
        .where("timestamp", ">=", fromDate)
        .where("timestamp", "<", untilDate)
        .where("entity_type", entity_type);

      if (granularity) {
        totalQuery = totalQuery.where("granularity", effectiveGranularity);
      }

      if (entity_type === "GLOBAL") {
        totalQuery = totalQuery.whereNull("entity_id");
      } else {
        totalQuery = totalQuery.whereIn("entity_id", entityIds);
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

    const response: any = {
      granularity: effectiveGranularity,
      timestamp_from: timestamp_from,
      timestamp_until: timestamp_until,
      entity_type: entity_type,
      entity_ids: entityIds,
      result_type: result_type,
    };

    if (result_type === "BUCKETS" || result_type === "BUCKETS_AND_TOTAL") {
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

    if (result_type === "TOTAL" || result_type === "BUCKETS_AND_TOTAL") {
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
  }
}
