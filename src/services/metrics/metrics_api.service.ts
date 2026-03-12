import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BaseService from "../../base/base.service";
import knex from "../../common/utils/db_connection";
import { BULL_JOB_NAME } from "../../common";
import { getBlockHeight, hasBlockHeight } from "../../common/utils/blockHeight";
import { computeGlobalMetrics } from "./metrics_helper";
import ApiResponder from "../../common/utils/apiResponse";

const IS_PG_CLIENT = String((knex as any)?.client?.config?.client || "").includes("pg");

@Service({
  name: "MetricsApiService",
  version: 1,
})
export default class MetricsApiService extends BaseService {
  private static readonly SNAPSHOT_SELECT_COLUMNS = [
    "participants",
    "participants_ecosystem",
    "participants_issuer_grantor",
    "participants_issuer",
    "participants_verifier_grantor",
    "participants_verifier",
    "participants_holder",
    "active_trust_registries",
    "archived_trust_registries",
    "active_schemas",
    "archived_schemas",
    "weight",
    "issued",
    "verified",
    "ecosystem_slash_events",
    "ecosystem_slashed_amount",
    "ecosystem_slashed_amount_repaid",
    "network_slash_events",
    "network_slashed_amount",
    "network_slashed_amount_repaid",
    "computed_at",
  ] as const;

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

  private mapSnapshotToMetrics(snap: any) {
    const participantsByRole = {
      participants_ecosystem: Number(snap.participants_ecosystem || 0),
      participants_issuer_grantor: Number(snap.participants_issuer_grantor || 0),
      participants_issuer: Number(snap.participants_issuer || 0),
      participants_verifier_grantor: Number(snap.participants_verifier_grantor || 0),
      participants_verifier: Number(snap.participants_verifier || 0),
      participants_holder: Number(snap.participants_holder || 0),
    };
    const participantsTotal =
      snap.participants != null && snap.participants !== ""
        ? Number(snap.participants)
        : participantsByRole.participants_ecosystem
          + participantsByRole.participants_issuer_grantor
          + participantsByRole.participants_issuer
          + participantsByRole.participants_verifier_grantor
          + participantsByRole.participants_verifier
          + participantsByRole.participants_holder;

    return {
      participants: participantsTotal,
      ...participantsByRole,
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
    };
  }

  private setCheckpointMeta(ctx: Context, height: number | null, updatedAt?: string | Date | null): void {
    const meta = (ctx.meta || {}) as Record<string, any>;
    ctx.meta = meta;
    if (height === null || !Number.isFinite(Number(height))) return;
    meta.latestCheckpoint = {
      height: Number(height),
      updated_at: updatedAt || new Date(),
    };
  }

  private async computeAndStoreLiveMetrics(): Promise<any> {
    const result = await computeGlobalMetrics(undefined);
    const latestHeight = await this.getLatestBlockHeight();
    await this.insertSnapshotIfStale(result, latestHeight);
    return result;
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
        participants_ecosystem: Number(metrics.participants_ecosystem || 0),
        participants_issuer_grantor: Number(metrics.participants_issuer_grantor || 0),
        participants_issuer: Number(metrics.participants_issuer || 0),
        participants_verifier_grantor: Number(metrics.participants_verifier_grantor || 0),
        participants_verifier: Number(metrics.participants_verifier || 0),
        participants_holder: Number(metrics.participants_holder || 0),
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
    const startedAt = Date.now();
    let stage = "init";
    try {
      const blockHeight = getBlockHeight(ctx);
      const useHistory = hasBlockHeight(ctx) && blockHeight !== undefined;

      if (useHistory) {
        stage = "history_snapshot_query";
        const snap = await knex("global_metrics")
          .select(...MetricsApiService.SNAPSHOT_SELECT_COLUMNS, "block_height")
          .whereNotNull("block_height")
          .andWhere("block_height", "<=", blockHeight)
          .orderBy("block_height", "desc")
          .limit(1)
          .first();
        if (snap) {
          this.setCheckpointMeta(ctx, Number(snap.block_height || blockHeight), snap.computed_at);
          return ApiResponder.success(ctx, this.mapSnapshotToMetrics(snap), 200);
        }
      } else {
        // Fast path for live metrics: latest snapshot by compute time.
        // This avoids expensive order-by on block_height for very large tables.
        stage = "live_snapshot_query";
        const snap = await knex("global_metrics")
          .select(...MetricsApiService.SNAPSHOT_SELECT_COLUMNS, "block_height")
          .orderBy("computed_at", "desc")
          .limit(1)
          .first();
        if (snap) {
          this.setCheckpointMeta(ctx, snap.block_height != null ? Number(snap.block_height) : null, snap.computed_at);
          const result = this.mapSnapshotToMetrics(snap);
          return ApiResponder.success(ctx, result, 200);
        }
      }

      if (!useHistory) {
        stage = "live_compute_fallback";
        const result = await this.computeAndStoreLiveMetrics();
        const latestHeight = await this.getLatestBlockHeight();
        this.setCheckpointMeta(ctx, latestHeight, new Date().toISOString());
        return ApiResponder.success(ctx, result, 200);
      }

      stage = "history_aggregate";
      const trLatest = IS_PG_CLIENT
        ? knex("trust_registry_history as trh")
          .distinctOn("trh.tr_id")
          .select("trh.tr_id", "trh.archived")
          .where("trh.height", "<=", blockHeight)
          .orderBy("trh.tr_id", "asc")
          .orderBy("trh.height", "desc")
          .orderBy("trh.created_at", "desc")
          .orderBy("trh.id", "desc")
          .as("latest_tr")
        : knex
          .from(
            knex("trust_registry_history as trh")
              .select(
                "trh.tr_id",
                "trh.archived",
                knex.raw("ROW_NUMBER() OVER (PARTITION BY trh.tr_id ORDER BY trh.height DESC, trh.created_at DESC, trh.id DESC) as rn")
              )
              .where("trh.height", "<=", blockHeight)
              .as("ranked_tr")
          )
          .select("tr_id", "archived")
          .where("rn", 1)
          .as("latest_tr");
      const trAgg = await knex.from(trLatest)
        .select(
          knex.raw("COUNT(*) FILTER (WHERE archived IS NULL) as active_trust_registries"),
          knex.raw("COUNT(*) FILTER (WHERE archived IS NOT NULL) as archived_trust_registries")
        )
        .first();

      const { csHasMetricColumns } = await this.getHistoryMetricColumnsAvailability().catch(() => ({ csHasMetricColumns: false }));
      const csMetricColumns = csHasMetricColumns
        ? [
          "csh.weight",
          "csh.issued",
          "csh.verified",
          "csh.ecosystem_slash_events",
          "csh.ecosystem_slashed_amount",
          "csh.ecosystem_slashed_amount_repaid",
          "csh.network_slash_events",
          "csh.network_slashed_amount",
          "csh.network_slashed_amount_repaid",
        ]
        : [];
      const csLatest = IS_PG_CLIENT
        ? knex("credential_schema_history as csh")
          .distinctOn("csh.credential_schema_id")
          .select(
            "csh.credential_schema_id",
            "csh.archived",
            ...csMetricColumns
          )
          .where("csh.height", "<=", blockHeight)
          .orderBy("csh.credential_schema_id", "asc")
          .orderBy("csh.height", "desc")
          .orderBy("csh.created_at", "desc")
          .orderBy("csh.id", "desc")
          .as("latest_cs")
        : knex
          .from(
            knex("credential_schema_history as csh")
              .select(
                "csh.credential_schema_id",
                "csh.archived",
                ...csMetricColumns,
                knex.raw("ROW_NUMBER() OVER (PARTITION BY csh.credential_schema_id ORDER BY csh.height DESC, csh.created_at DESC, csh.id DESC) as rn")
              )
              .where("csh.height", "<=", blockHeight)
              .as("ranked_cs")
          )
          .select(
            "credential_schema_id",
            "archived",
            ...(csHasMetricColumns
              ? [
                "weight",
                "issued",
                "verified",
                "ecosystem_slash_events",
                "ecosystem_slashed_amount",
                "ecosystem_slashed_amount_repaid",
                "network_slash_events",
                "network_slashed_amount",
                "network_slashed_amount_repaid",
              ]
              : [])
          )
          .where("rn", 1)
          .as("latest_cs");

      const csStateAgg = await knex.from(csLatest)
        .select(
          knex.raw("COUNT(*) FILTER (WHERE archived IS NULL) as active_schemas"),
          knex.raw("COUNT(*) FILTER (WHERE archived IS NOT NULL) as archived_schemas")
        )
        .first();

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
      const latestPermRanked = IS_PG_CLIENT
        ? knex("permission_history as ph")
          .distinctOn("ph.permission_id")
          .select(
            "ph.permission_id",
            "ph.grantee",
            "ph.type",
            "ph.repaid",
            "ph.slashed",
            "ph.revoked",
            "ph.effective_from",
            "ph.effective_until"
          )
          .where("ph.height", "<=", blockHeight)
          .orderBy("ph.permission_id", "asc")
          .orderBy("ph.height", "desc")
          .orderBy("ph.created_at", "desc")
          .orderBy("ph.id", "desc")
          .as("ranked_perm")
        : knex("permission_history as ph")
          .select(
            "ph.permission_id",
            "ph.grantee",
            "ph.type",
            "ph.repaid",
            "ph.slashed",
            "ph.revoked",
            "ph.effective_from",
            "ph.effective_until",
            knex.raw("ROW_NUMBER() OVER (PARTITION BY ph.permission_id ORDER BY ph.height DESC, ph.created_at DESC, ph.id DESC) as rn")
          )
          .where("ph.height", "<=", blockHeight)
          .as("ranked_perm");

      const activePermBaseQuery = knex
        .from(latestPermRanked)
        .modify((qb) => {
          if (!IS_PG_CLIENT) qb.where("rn", 1);
        })
        .whereNotNull("grantee")
        .whereNull("repaid")
        .whereNull("slashed")
        .where((qb) => qb.whereNull("revoked").orWhere("revoked", ">=", nowIso))
        .whereNotNull("effective_from")
        .where("effective_from", "<=", nowIso)
        .where((qb) => qb.whereNull("effective_until").orWhere("effective_until", ">=", nowIso));

      let participantsAgg: any = null;
      let participantsByTypeAgg: any[] = [];
      if (IS_PG_CLIENT) {
        participantsAgg = await activePermBaseQuery
          .clone()
          .select(
            knex.raw("COUNT(DISTINCT grantee) as participants"),
            knex.raw("COUNT(DISTINCT grantee) FILTER (WHERE type = 'ECOSYSTEM') as participants_ecosystem"),
            knex.raw("COUNT(DISTINCT grantee) FILTER (WHERE type = 'ISSUER_GRANTOR') as participants_issuer_grantor"),
            knex.raw("COUNT(DISTINCT grantee) FILTER (WHERE type = 'ISSUER') as participants_issuer"),
            knex.raw("COUNT(DISTINCT grantee) FILTER (WHERE type = 'VERIFIER_GRANTOR') as participants_verifier_grantor"),
            knex.raw("COUNT(DISTINCT grantee) FILTER (WHERE type = 'VERIFIER') as participants_verifier"),
            knex.raw("COUNT(DISTINCT grantee) FILTER (WHERE type = 'HOLDER') as participants_holder")
          )
          .first();
      } else {
        participantsAgg = await activePermBaseQuery
          .clone()
          .countDistinct("grantee as participants")
          .first();
        participantsByTypeAgg = await activePermBaseQuery
          .clone()
          .groupBy("type")
          .select("type")
          .countDistinct("grantee as participants");
      }

      const participantsByType = {
        participants_ecosystem: Number((participantsAgg as any)?.participants_ecosystem || 0),
        participants_issuer_grantor: Number((participantsAgg as any)?.participants_issuer_grantor || 0),
        participants_issuer: Number((participantsAgg as any)?.participants_issuer || 0),
        participants_verifier_grantor: Number((participantsAgg as any)?.participants_verifier_grantor || 0),
        participants_verifier: Number((participantsAgg as any)?.participants_verifier || 0),
        participants_holder: Number((participantsAgg as any)?.participants_holder || 0),
      };
      if (!IS_PG_CLIENT) {
        for (const row of participantsByTypeAgg as any[]) {
          const count = Number(row?.participants || row?.count || row?.count_distinct || 0);
          if (row.type === "ECOSYSTEM") participantsByType.participants_ecosystem = count;
          if (row.type === "ISSUER_GRANTOR") participantsByType.participants_issuer_grantor = count;
          if (row.type === "ISSUER") participantsByType.participants_issuer = count;
          if (row.type === "VERIFIER_GRANTOR") participantsByType.participants_verifier_grantor = count;
          if (row.type === "VERIFIER") participantsByType.participants_verifier = count;
          if (row.type === "HOLDER") participantsByType.participants_holder = count;
        }
      }

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

      const participantsTotal =
        participantsByType.participants_ecosystem
        + participantsByType.participants_issuer_grantor
        + participantsByType.participants_issuer
        + participantsByType.participants_verifier_grantor
        + participantsByType.participants_verifier
        + participantsByType.participants_holder;

      const result = {
        participants: participantsTotal,
        participants_ecosystem: participantsByType.participants_ecosystem,
        participants_issuer_grantor: participantsByType.participants_issuer_grantor,
        participants_issuer: participantsByType.participants_issuer,
        participants_verifier_grantor: participantsByType.participants_verifier_grantor,
        participants_verifier: participantsByType.participants_verifier,
        participants_holder: participantsByType.participants_holder,
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
    } finally {
      const tookMs = Date.now() - startedAt;
      const msg = `[metrics.getAll] duration=${tookMs}ms stage=${stage}`;
      if (tookMs >= 300) this.logger.warn(msg);
      else this.logger.debug(msg);
    }
  }
}
