import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BaseService from "../../base/base.service";
import knex from "../../common/utils/db_connection";
import { BULL_JOB_NAME } from "../../common";
import { getBlockHeight, hasBlockHeight } from "../../common/utils/blockHeight";
import { computeGlobalMetrics } from "./metrics_helper";
import ApiResponder from "../../common/utils/apiResponse";

@Service({
  name: "MetricsApiService",
  version: 1,
})
export default class MetricsApiService extends BaseService {
  private historyMetricColumnsAvailabilityPromise?: Promise<{
    csHasMetricColumns: boolean;
  }>;

  public constructor(public broker: ServiceBroker) {
    super(broker);
  }

  private getHistoryMetricColumnsAvailability(): Promise<{ csHasMetricColumns: boolean }> {
    if (!this.historyMetricColumnsAvailabilityPromise) {
      this.historyMetricColumnsAvailabilityPromise = (async () => {
        const requiredCsColumns = [
          "issued",
          "verified",
          "weight",
          "ecosystem_slash_events",
          "ecosystem_slashed_amount",
          "ecosystem_slashed_amount_repaid",
          "network_slash_events",
          "network_slashed_amount",
          "network_slashed_amount_repaid",
        ];
        const checks = await Promise.all(requiredCsColumns.map((column) => knex.schema.hasColumn("credential_schema_history", column)));
        return { csHasMetricColumns: checks.every(Boolean) };
      })().catch((error) => {
        this.historyMetricColumnsAvailabilityPromise = undefined;
        throw error;
      });
    }
    return this.historyMetricColumnsAvailabilityPromise;
  }

  private getSnapshotMinIntervalMs(): number {
    const envValue = Number(process.env.METRICS_SNAPSHOT_MIN_INTERVAL_MS);
    if (Number.isFinite(envValue) && envValue >= 0) {
      return Math.floor(envValue);
    }
    return 60000;
  }

  private async insertSnapshotIfStale(metrics: any, blockHeight: number | null): Promise<void> {
    try {
      const minIntervalMs = this.getSnapshotMinIntervalMs();
      const latest = await knex("global_metrics")
        .modify((qb) => {
          if (blockHeight === null) {
            qb.whereNull("block_height");
          } else {
            qb.where("block_height", blockHeight);
          }
        })
        .orderBy("computed_at", "desc")
        .first();

      if (latest?.computed_at) {
        const lastTime = new Date(latest.computed_at).getTime();
        if (Number.isFinite(lastTime) && Date.now() - lastTime < minIntervalMs) {
          return;
        }
      }

      await knex("global_metrics").insert({
        block_height: blockHeight,
        computed_at: new Date(),
        participants: Number(metrics.participants || 0),
        active_trust_registries: Number(metrics.active_trust_registries || 0),
        archived_trust_registries: Number(metrics.archived_trust_registries || 0),
        active_schemas: Number(metrics.active_schemas || 0),
        archived_schemas: Number(metrics.archived_schemas || 0),
        weight: String(metrics.weight || "0"),
        issued: Number(metrics.issued || 0),
        verified: Number(metrics.verified || 0),
        ecosystem_slash_events: Number(metrics.ecosystem_slash_events || 0),
        ecosystem_slashed_amount: String(metrics.ecosystem_slashed_amount || "0"),
        ecosystem_slashed_amount_repaid: String(metrics.ecosystem_slashed_amount_repaid || "0"),
        network_slash_events: Number(metrics.network_slash_events || 0),
        network_slashed_amount: String(metrics.network_slashed_amount || "0"),
        network_slashed_amount_repaid: String(metrics.network_slashed_amount_repaid || "0"),
        payload: metrics,
      });
    } catch (err: any) {
      this.logger.warn(`Failed to store global metrics snapshot: ${err?.message || String(err)}`);
    }
  }

  private async getLatestBlockHeight(): Promise<number | null> {
    try {
      try {
        const cp = await knex("block_checkpoint")
          .select("height")
          .where("job_name", BULL_JOB_NAME.HANDLE_TRANSACTION)
          .first();
        const cpHeight = cp?.height;
        if (cpHeight !== undefined && cpHeight !== null && Number.isFinite(Number(cpHeight))) {
          return Number(cpHeight);
        }
      } catch (e) {
        // ignore and fallback to block table
      }

      const result = await knex("block").max("height as max").first();
      const maxValue = result && (result as { max: string | number | null }).max;
      if (maxValue === null || maxValue === undefined) {
        return null;
      }
      const parsed = parseInt(String(maxValue), 10);
      return Number.isFinite(parsed) ? parsed : null;
    } catch {
      return null;
    }
  }

  @Action({
    rest: "GET all",
    params: {},
  })
  async getAll(ctx: Context) {
    try {
      const blockHeight = getBlockHeight(ctx);
      const useHistory = hasBlockHeight(ctx) && blockHeight !== undefined;

      if (useHistory) {
        const snap = await knex("global_metrics")
          .whereNotNull("block_height")
          .andWhere("block_height", "<=", blockHeight)
          .orderBy("block_height", "desc")
          .limit(1)
          .first();
        if (snap) {
          return ApiResponder.success(ctx, {
            participants: Number(snap.participants || 0),
            active_trust_registries: Number(snap.active_trust_registries || 0),
            archived_trust_registries: Number(snap.archived_trust_registries || 0),
            active_schemas: Number(snap.active_schemas || 0),
            archived_schemas: Number(snap.archived_schemas || 0),
            weight: Number(snap.weight || "0"),
            issued: Number(snap.issued || 0),
            verified: Number(snap.verified || 0),
            ecosystem_slash_events: Number(snap.ecosystem_slash_events || 0),
            ecosystem_slashed_amount: Number(snap.ecosystem_slashed_amount || "0"),
            ecosystem_slashed_amount_repaid: Number(snap.ecosystem_slashed_amount_repaid || "0"),
            network_slash_events: Number(snap.network_slash_events || 0),
            network_slashed_amount: Number(snap.network_slashed_amount || "0"),
            network_slashed_amount_repaid: Number(snap.network_slashed_amount_repaid || "0"),
          }, 200);
        }
      } else {
        const snap = await knex("global_metrics")
          .orderBy("block_height", "desc")
          .orderBy("computed_at", "desc")
          .limit(1)
          .first();
        if (snap) {
          return ApiResponder.success(ctx, {
            participants: Number(snap.participants || 0),
            active_trust_registries: Number(snap.active_trust_registries || 0),
            archived_trust_registries: Number(snap.archived_trust_registries || 0),
            active_schemas: Number(snap.active_schemas || 0),
            archived_schemas: Number(snap.archived_schemas || 0),
            weight: Number(snap.weight || "0"),
            issued: Number(snap.issued || 0),
            verified: Number(snap.verified || 0),
            ecosystem_slash_events: Number(snap.ecosystem_slash_events || 0),
            ecosystem_slashed_amount: Number(snap.ecosystem_slashed_amount || "0"),
            ecosystem_slashed_amount_repaid: Number(snap.ecosystem_slashed_amount_repaid || "0"),
            network_slash_events: Number(snap.network_slash_events || 0),
            network_slashed_amount: Number(snap.network_slashed_amount || "0"),
            network_slashed_amount_repaid: Number(snap.network_slashed_amount_repaid || "0"),
          }, 200);
        }
      }

      if (!useHistory) {
        const result = await computeGlobalMetrics(undefined);
        const latestHeight = await this.getLatestBlockHeight();
        await this.insertSnapshotIfStale(result, latestHeight);
        return ApiResponder.success(ctx, result, 200);
      }

      const trRanked = knex("trust_registry_history as trh")
        .select(
          "trh.tr_id",
          "trh.archived",
          knex.raw("ROW_NUMBER() OVER (PARTITION BY trh.tr_id ORDER BY trh.height DESC, trh.created_at DESC, trh.id DESC) as rn")
        )
        .where("trh.height", "<=", blockHeight)
        .as("ranked_tr");
      const trLatest = knex.from(trRanked).select("tr_id", "archived").where("rn", 1).as("latest_tr");
      const trAgg = await knex.from(trLatest)
        .select(
          knex.raw("COUNT(*) FILTER (WHERE archived IS NULL) as active_trust_registries"),
          knex.raw("COUNT(*) FILTER (WHERE archived IS NOT NULL) as archived_trust_registries")
        )
        .first();

      const csRanked = knex("credential_schema_history as csh")
        .select(
          "csh.credential_schema_id",
          "csh.archived",
          "csh.weight",
          "csh.issued",
          "csh.verified",
          "csh.ecosystem_slash_events",
          "csh.ecosystem_slashed_amount",
          "csh.ecosystem_slashed_amount_repaid",
          "csh.network_slash_events",
          "csh.network_slashed_amount",
          "csh.network_slashed_amount_repaid",
          knex.raw("ROW_NUMBER() OVER (PARTITION BY csh.credential_schema_id ORDER BY csh.height DESC, csh.created_at DESC, csh.id DESC) as rn")
        )
        .where("csh.height", "<=", blockHeight)
        .as("ranked_cs");
      const csLatest = knex.from(csRanked)
        .select(
          "credential_schema_id",
          "archived",
          "weight",
          "issued",
          "verified",
          "ecosystem_slash_events",
          "ecosystem_slashed_amount",
          "ecosystem_slashed_amount_repaid",
          "network_slash_events",
          "network_slashed_amount",
          "network_slashed_amount_repaid"
        )
        .where("rn", 1)
        .as("latest_cs");

      const csStateAgg = await knex.from(csLatest)
        .select(
          knex.raw("COUNT(*) FILTER (WHERE archived IS NULL) as active_schemas"),
          knex.raw("COUNT(*) FILTER (WHERE archived IS NOT NULL) as archived_schemas")
        )
        .first();

      const { csHasMetricColumns } = await this.getHistoryMetricColumnsAvailability().catch(() => ({ csHasMetricColumns: false }));
      let metricAgg: any = null;
      if (csHasMetricColumns) {
        metricAgg = await knex.from(csLatest)
          .select(
            knex.raw("COALESCE(SUM(CAST(weight AS NUMERIC)), 0) as weight"),
            knex.raw("COALESCE(SUM(CAST(issued AS NUMERIC)), 0) as issued"),
            knex.raw("COALESCE(SUM(CAST(verified AS NUMERIC)), 0) as verified"),
            knex.raw("COALESCE(SUM(ecosystem_slash_events), 0) as ecosystem_slash_events"),
            knex.raw("COALESCE(SUM(CAST(ecosystem_slashed_amount AS NUMERIC)), 0) as ecosystem_slashed_amount"),
            knex.raw("COALESCE(SUM(CAST(ecosystem_slashed_amount_repaid AS NUMERIC)), 0) as ecosystem_slashed_amount_repaid"),
            knex.raw("COALESCE(SUM(network_slash_events), 0) as network_slash_events"),
            knex.raw("COALESCE(SUM(CAST(network_slashed_amount AS NUMERIC)), 0) as network_slashed_amount"),
            knex.raw("COALESCE(SUM(CAST(network_slashed_amount_repaid AS NUMERIC)), 0) as network_slashed_amount_repaid")
          )
          .first();
      }

      const nowIso = new Date().toISOString();
      const latestPermRanked = knex("permission_history as ph")
        .select(
          "ph.permission_id",
          "ph.grantee",
          "ph.repaid",
          "ph.slashed",
          "ph.revoked",
          "ph.effective_from",
          "ph.effective_until",
          knex.raw("ROW_NUMBER() OVER (PARTITION BY ph.permission_id ORDER BY ph.height DESC, ph.created_at DESC, ph.id DESC) as rn")
        )
        .where("ph.height", "<=", blockHeight)
        .as("ranked_perm");
      const participantsAgg = await knex
        .from(latestPermRanked)
        .where("rn", 1)
        .whereNotNull("grantee")
        .whereNull("repaid")
        .whereNull("slashed")
        .where((qb) => qb.whereNull("revoked").orWhere("revoked", ">=", nowIso))
        .whereNotNull("effective_from")
        .where("effective_from", "<=", nowIso)
        .where((qb) => qb.whereNull("effective_until").orWhere("effective_until", ">=", nowIso))
        .countDistinct("grantee as participants")
        .first();

      let weight = Number(metricAgg?.weight || 0);
      let issued = Number(metricAgg?.issued || 0);
      let verified = Number(metricAgg?.verified || 0);
      let ecosystemSlashEvents = Number(metricAgg?.ecosystem_slash_events || 0);
      let ecosystemSlashedAmount = Number(metricAgg?.ecosystem_slashed_amount || 0);
      let ecosystemSlashedAmountRepaid = Number(metricAgg?.ecosystem_slashed_amount_repaid || 0);
      let networkSlashEvents = Number(metricAgg?.network_slash_events || 0);
      let networkSlashedAmount = Number(metricAgg?.network_slashed_amount || 0);
      let networkSlashedAmountRepaid = Number(metricAgg?.network_slashed_amount_repaid || 0);

      if (!csHasMetricColumns) {
        // Backward-compatible fallback for deployments where historical metric columns do not exist.
        const fallback = await computeGlobalMetrics(blockHeight);
        weight = Number(fallback?.weight || 0);
        issued = Number(fallback?.issued || 0);
        verified = Number(fallback?.verified || 0);
        ecosystemSlashEvents = Number(fallback?.ecosystem_slash_events || 0);
        ecosystemSlashedAmount = Number(fallback?.ecosystem_slashed_amount || 0);
        ecosystemSlashedAmountRepaid = Number(fallback?.ecosystem_slashed_amount_repaid || 0);
        networkSlashEvents = Number(fallback?.network_slash_events || 0);
        networkSlashedAmount = Number(fallback?.network_slashed_amount || 0);
        networkSlashedAmountRepaid = Number(fallback?.network_slashed_amount_repaid || 0);
      }

      const result = {
        participants: Number((participantsAgg as any)?.participants || 0),
        active_trust_registries: Number((trAgg as any)?.active_trust_registries || 0),
        archived_trust_registries: Number((trAgg as any)?.archived_trust_registries || 0),
        active_schemas: Number((csStateAgg as any)?.active_schemas || 0),
        archived_schemas: Number((csStateAgg as any)?.archived_schemas || 0),
        weight,
        issued,
        verified,
        ecosystem_slash_events: ecosystemSlashEvents,
        ecosystem_slashed_amount: ecosystemSlashedAmount,
        ecosystem_slashed_amount_repaid: ecosystemSlashedAmountRepaid,
        network_slash_events: networkSlashEvents,
        network_slashed_amount: networkSlashedAmount,
        network_slashed_amount_repaid: networkSlashedAmountRepaid,
      };

      return ApiResponder.success(ctx, result, 200);
    } catch (err: any) {
      this.logger.error("Error in Metrics.getAll:", err);
      return ApiResponder.error(ctx, `Failed to get global metrics: ${err?.message || String(err)}`, 500);
    }
  }
}
