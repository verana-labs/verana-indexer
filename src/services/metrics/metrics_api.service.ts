import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BaseService from "../../base/base.service";
import knex from "../../common/utils/db_connection";
import { BULL_JOB_NAME } from "../../common";
import { getBlockHeight, hasBlockHeight } from "../../common/utils/blockHeight";
import { calculateCredentialSchemaStats } from "../crawl-cs/cs_stats";
import { calculateTrustRegistryStats } from "../crawl-tr/tr_stats";
import { computeGlobalMetrics } from "./metrics_helper";
import ApiResponder from "../../common/utils/apiResponse";
import { calculatePermState } from "../crawl-perm/perm_state_utils";

@Service({
  name: "MetricsApiService",
  version: 1,
})
export default class MetricsApiService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker);
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

      const trSub = knex("trust_registry_history")
        .select("tr_id")
        .select(knex.raw("ROW_NUMBER() OVER (PARTITION BY tr_id ORDER BY height DESC, created_at DESC) as rn"))
        .where("height", "<=", blockHeight)
        .as("ranked_tr");

      const trLatest = await knex.from(trSub).select("tr_id").where("rn", 1);
      const trIds = trLatest.map((r: any) => Number(r.tr_id));

      let activeTrustRegistries = 0;
      let archivedTrustRegistries = 0;
      for (const trId of trIds) {
        const trHistory = await knex("trust_registry_history")
          .where("tr_id", trId)
          .where("height", "<=", blockHeight)
          .orderBy("height", "desc")
          .orderBy("created_at", "desc")
          .first();
        if (trHistory) {
          if (trHistory.archived) archivedTrustRegistries++;
          else activeTrustRegistries++;
        }
      }

      const csSub = knex("credential_schema_history")
        .select("credential_schema_id")
        .select(knex.raw("ROW_NUMBER() OVER (PARTITION BY credential_schema_id ORDER BY height DESC, created_at DESC) as rn"))
        .where("height", "<=", blockHeight)
        .as("ranked_cs");

      const csLatest = await knex.from(csSub).select("credential_schema_id").where("rn", 1);
      const schemaIds = csLatest.map((r: any) => Number(r.credential_schema_id));

      let activeSchemas = 0;
      let archivedSchemas = 0;
      const participants = 0;
      let totalWeight = BigInt(0);
      let issued = 0;
      let verified = 0;
      let ecosystemSlashEvents = 0;
      let ecosystemSlashedAmount = BigInt(0);
      let ecosystemSlashedAmountRepaid = BigInt(0);
      let networkSlashEvents = 0;
      let networkSlashedAmount = BigInt(0);
      let networkSlashedAmountRepaid = BigInt(0);

      for (const sid of schemaIds) {
        const schHistory = await knex("credential_schema_history")
          .where("credential_schema_id", sid)
          .where("height", "<=", blockHeight)
          .orderBy("height", "desc")
          .orderBy("created_at", "desc")
          .first();
        if (!schHistory) continue;
        if (schHistory.archived) archivedSchemas++;
        else activeSchemas++;

        try {
          const stats = await calculateCredentialSchemaStats(sid, blockHeight);
              try {
            totalWeight += BigInt(stats.weight || "0");
          } catch {}
          issued += Number(stats.issued || 0);
          verified += Number(stats.verified || 0);
          ecosystemSlashEvents += Number(stats.ecosystem_slash_events || 0);
          ecosystemSlashedAmount += BigInt(stats.ecosystem_slashed_amount || "0");
          ecosystemSlashedAmountRepaid += BigInt(stats.ecosystem_slashed_amount_repaid || "0");
          networkSlashEvents += Number(stats.network_slash_events || 0);
          networkSlashedAmount += BigInt(stats.network_slashed_amount || "0");
          networkSlashedAmountRepaid += BigInt(stats.network_slashed_amount_repaid || "0");
        } catch (err: any) {
          this.logger.warn(`Failed to calculate stats for schema ${sid} at height ${blockHeight}: ${err?.message || err}`);
        }
      }
      const participantsSet = new Set<string>();
      try {
        const latestHistorySubquery = knex("permission_history")
          .select("permission_id")
          .select(
            knex.raw(
              `ROW_NUMBER() OVER (PARTITION BY permission_id ORDER BY height DESC, created_at DESC, id DESC) as rn`
            )
          )
          .where("height", "<=", blockHeight)
          .as("ranked");

        const permIdsAtHeight = await knex
          .from(latestHistorySubquery)
          .select("permission_id")
          .where("rn", 1)
          .then((rows: any[]) => rows.map((r: any) => String(r.permission_id)));

        for (const permId of permIdsAtHeight) {
          const historyRecord = await knex("permission_history")
            .where({ permission_id: String(permId) })
            .where("height", "<=", blockHeight)
            .orderBy("height", "desc")
            .orderBy("created_at", "desc")
            .first();
          if (!historyRecord) continue;
          const permState = calculatePermState(
            {
              repaid: historyRecord.repaid,
              slashed: historyRecord.slashed,
              revoked: historyRecord.revoked,
              effective_from: historyRecord.effective_from,
              effective_until: historyRecord.effective_until,
              type: historyRecord.type,
              vp_state: historyRecord.vp_state,
              vp_exp: historyRecord.vp_exp,
              validator_perm_id: historyRecord.validator_perm_id,
            },
            new Date()
          );
          if (permState === "ACTIVE" && historyRecord.grantee) {
            participantsSet.add(historyRecord.grantee);
          }
        }
      } catch (err: any) {
        this.logger.warn(`Failed to compute historical participants: ${err?.message || err}`);
      }

      const result = {
        participants: participantsSet.size || participants,
        active_trust_registries: activeTrustRegistries || 0,
        archived_trust_registries: archivedTrustRegistries || 0,
        active_schemas: activeSchemas || 0,
        archived_schemas: archivedSchemas || 0,
        weight: Number(totalWeight),
        issued,
        verified,
        ecosystem_slash_events: ecosystemSlashEvents,
        ecosystem_slashed_amount: Number(ecosystemSlashedAmount),
        ecosystem_slashed_amount_repaid: Number(ecosystemSlashedAmountRepaid),
        network_slash_events: networkSlashEvents,
        network_slashed_amount: Number(networkSlashedAmount),
        network_slashed_amount_repaid: Number(networkSlashedAmountRepaid),
      };

      return ApiResponder.success(ctx, result, 200);
    } catch (err: any) {
      this.logger.error("Error in Metrics.getAll:", err);
      return ApiResponder.error(ctx, `Failed to get global metrics: ${err?.message || String(err)}`, 500);
    }
  }
}

