import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BullableService, { QueueHandler } from "../../base/bullable.service";
import knex from "../../common/utils/db_connection";
import { computeGlobalMetrics } from "./metrics_helper";
import { getBlockHeight } from "../../common/utils/blockHeight";
import { BULL_JOB_NAME } from "../../common/constant";
import config from "../../config.json" with { type: "json" };

@Service({
  name: "MetricsSnapshotService",
  version: 1,
})
export default class MetricsSnapshotService extends BullableService {
  public constructor(public broker: ServiceBroker) {
    super(broker);
  }

  private getSnapshotIntervalMs(): number {
    const envValue = Number(process.env.METRICS_SNAPSHOT_INTERVAL_MS);
    if (Number.isFinite(envValue) && envValue >= 0) {
      return Math.floor(envValue);
    }
    return Number(config.metricsSnapshot?.millisecondCrawl || 60000);
  }

  private async insertSnapshotIfStale(metrics: any, blockHeight: number | null): Promise<boolean> {
    const minIntervalMs = Number(process.env.METRICS_SNAPSHOT_MIN_INTERVAL_MS || 0);
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
      if (Number.isFinite(lastTime) && minIntervalMs > 0 && Date.now() - lastTime < minIntervalMs) {
        return false;
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
    return true;
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

  @Action({ name: "computeAndStore" })
  public async computeAndStore(ctx: Context<{ block_height?: number }>) {
    const blockHeight = ctx.params?.block_height !== undefined ? Number(ctx.params.block_height) : getBlockHeight(ctx);
    const useHistory = typeof blockHeight === "number";
    const metrics = await computeGlobalMetrics(useHistory ? blockHeight : undefined);

    const insertRow: any = {
      block_height: useHistory ? blockHeight : null,
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
    };

    await knex("global_metrics").insert(insertRow);
    return { success: true, metrics: insertRow };
  }

  @QueueHandler({
    queueName: BULL_JOB_NAME.SNAPSHOT_GLOBAL_METRICS,
    jobName: BULL_JOB_NAME.SNAPSHOT_GLOBAL_METRICS,
  })
  public async jobSnapshotGlobalMetrics(): Promise<void> {
    try {
      const metrics = await computeGlobalMetrics(undefined);
      const latestHeight = await this.getLatestBlockHeight();
      const inserted = await this.insertSnapshotIfStale(metrics, latestHeight);
      if (!inserted) {
        this.logger.debug("Global metrics snapshot skipped (min interval not reached)");
      }
    } catch (error: any) {
      this.logger.error(`Failed to snapshot global metrics: ${error?.message || String(error)}`);
    }
  }

  async _start(): Promise<void> {
    const intervalMs = this.getSnapshotIntervalMs();
    if (intervalMs > 0 && process.env.NODE_ENV !== "test") {
      await this.createJob(
        BULL_JOB_NAME.SNAPSHOT_GLOBAL_METRICS,
        BULL_JOB_NAME.SNAPSHOT_GLOBAL_METRICS,
        {},
        {
          removeOnComplete: 1,
          removeOnFail: { count: 3 },
          repeat: { every: intervalMs },
        }
      );
    }
    return super._start();
  }
}

