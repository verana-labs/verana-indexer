import {
  Action,
  Service,
} from "@ourparentcenter/moleculer-decorators-extended";
import fs from "fs";
import { Context, ServiceBroker } from "moleculer";
import path from "path";
import BullableService from "../../base/bullable.service";
import { BULL_JOB_NAME, SERVICE } from "../../common";
import knex from "../../common/utils/db_connection";

@Service({
  name: SERVICE.V1.GenesisParamsService.key,
  version: 1,
})
export default class GenesisParamsService extends BullableService {
  private genesisPath = path.resolve("genesis.json");
  private watcher?: fs.FSWatcher;
  private intervalId?: NodeJS.Timeout;
  private syncIntervalSeconds = 30;

  public constructor(public broker: ServiceBroker) {
    super(broker);
  }

  @Action({
    name: SERVICE.V1.GenesisParamsService.key,
    description: "Manually trigger Genesis params sync",
  })
  public async syncParams(ctx: Context<unknown>) {
    return this.sync();
  }

  public async started() {
    this.logger.info("🚀 GenesisParamsService started. Running initial sync...");
    await this.sync();

    if (fs.existsSync(this.genesisPath)) {
      this.watcher = fs.watch(this.genesisPath, async (eventType) => {
        if (eventType === "change") {
          this.logger.info("🔄 Detected changes in genesis.json. Updating params...");
          await this.sync();
        }
      });
      this.logger.info("👀 Watching genesis.json for live changes...");
    } else {
      this.logger.warn("⚠️ genesis.json not found. Watching disabled.");

    }

    this.intervalId = setInterval(async () => {
      this.logger.info(`🕒 Scheduled sync check every ${this.syncIntervalSeconds}s`);
      await this.sync();
    }, this.syncIntervalSeconds * 1000);
  }

  public async stopped() {
    if (this.watcher) {
      this.watcher.close();
      this.logger.info("🛑 Stopped watching genesis.json");
    }
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.logger.info("🛑 Stopped scheduled sync interval");
    }
  }


  public async resetGenesisCheckpoints(): Promise<void> {
    try {
      const targetJobs = [
        BULL_JOB_NAME.CRAWL_GENESIS,
        BULL_JOB_NAME.CRAWL_GENESIS_ACCOUNT,
        BULL_JOB_NAME.CRAWL_GENESIS_VALIDATOR,
        BULL_JOB_NAME.CRAWL_GENESIS_PROPOSAL,
        BULL_JOB_NAME.CRAWL_GENESIS_CODE,
        BULL_JOB_NAME.CRAWL_GENESIS_CONTRACT,
        BULL_JOB_NAME.CRAWL_GENESIS_FEEGRANT,
        BULL_JOB_NAME.CRAWL_GENESIS_IBC_TAO,
      ];

      const affected = await knex("block_checkpoint")
        .whereIn("job_name", targetJobs)
        .update({ height: 0, updated_at: knex.fn.now() });

      if (affected > 0) {
        this.logger.info(
          `🔁 Reset ${affected} block_checkpoint rows (height=0) for genesis-related jobs`
        );
      } else {
        this.logger.warn("⚠️ No genesis-related block_checkpoint rows found to reset");
      }
    } catch (err) {
      this.logger.error("❌ Failed to reset genesis checkpoints", err);
    }
  }

  private computeParamsChanges(
    oldParams: any,
    newParams: any
  ): Record<string, { old: any; new: any }> | null {
    if (!oldParams) return null;
    const changes: Record<string, { old: any; new: any }> = {};
    const allKeys = new Set([
      ...Object.keys(oldParams || {}),
      ...Object.keys(newParams || {}),
    ]);
    for (const key of allKeys) {
      const oldValue = oldParams?.[key];
      const newValue = newParams?.[key];
      if (JSON.stringify(oldValue) !== JSON.stringify(newValue)) {
        changes[key] = { old: oldValue ?? null, new: newValue ?? null };
      }
    }
    return Object.keys(changes).length ? changes : null;
  }

  private async recordModuleParamsHistory(
    trx: any,
    module: string,
    params: any,
    eventType: string,
    height: number,
    previousParams?: any
  ) {
    const changes = this.computeParamsChanges(previousParams, params);
    await trx("module_params_history").insert({
      module,
      params: JSON.stringify(params),
      event_type: eventType,
      height,
      changes: changes ? JSON.stringify(changes) : null,
    });
  }

  private async sync() {
    if (!fs.existsSync(this.genesisPath)) {
      this.logger.warn("⚠️ genesis.json not found. Skipping params sync.");
      await this.resetGenesisCheckpoints();
      return { success: false, message: "genesis.json not found" };
    }

    try {
      let raw = fs.readFileSync(this.genesisPath, "utf-8").trim();

      if (!raw.startsWith("[")) {
        // eslint-disable-next-line prefer-template
        raw = "[" + raw.replace(/}\s*{/g, "},{") + "]";
      }

      let genesisList: any[] = [];
      try {
        genesisList = JSON.parse(raw);
      } catch (err) {
        this.logger.error("❌ Malformed JSON, attempting recovery parse...");
        raw = raw.replace(/}\s*{/g, "},{");
        try {
          genesisList = JSON.parse(raw);
        } catch (err2) {
          this.logger.error("❌ Still failed to parse genesis.json", err2);
          return { success: false, message: "Failed to parse genesis.json" };
        }
      }

      if (!Array.isArray(genesisList)) {
        genesisList = [genesisList];
      }

      let updatedModules = 0;
      const trx = await knex.transaction();

      try {
        for (const genesis of genesisList) {
          const appState = genesis.app_state || {};
          const genesisHeight = genesis.initial_height ? Number(genesis.initial_height) : 0;

          for (const [module, data] of Object.entries(appState)) {
            if (data && typeof data === "object" && "params" in data) {
              // Get existing params for comparison
              const existing = await trx("module_params")
                .where({ module })
                .first();
              const previousParams = existing?.params
                ? typeof existing.params === "string"
                  ? JSON.parse(existing.params)
                  : existing.params
                : null;

              const newParamsData = data as any;
              const isNewOrChanged =
                !existing ||
                JSON.stringify(previousParams) !== JSON.stringify(newParamsData);

              if (isNewOrChanged) {
                await trx("module_params")
                  .insert({
                    module,
                    params: JSON.stringify(data),
                    updated_at: trx.fn.now(),
                  })
                  .onConflict("module")
                  .merge();

                await this.recordModuleParamsHistory(
                  trx,
                  module,
                  newParamsData,
                  existing ? "UPDATE_PARAMS" : "CREATE_PARAMS",
                  genesisHeight,
                  previousParams
                );

                updatedModules++;
              }
            }
          }
        }

        await trx.commit();
        this.logger.info(`✅ Genesis params synced successfully. Modules updated: ${updatedModules}`);
        return { success: true, updatedModules };
      } catch (err) {
        await trx.rollback();
        this.logger.error("❌ DB transaction failed during sync", err);
        return { success: false, message: "DB transaction failed", error: err };
      }
    } catch (err) {
      this.logger.error("❌ Unexpected error reading genesis.json", err);
      return { success: false, message: "Unexpected error", error: err };
    }
  }
}
