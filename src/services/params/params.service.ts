import {
  Action,
  Service,
} from "@ourparentcenter/moleculer-decorators-extended";
import { fromBase64, fromUtf8 } from '@cosmjs/encoding';
import { createJsonRpcRequest } from '@cosmjs/tendermint-rpc/build/jsonrpc';
import fs from "fs";
import { Context, ServiceBroker } from "moleculer";
import path from "path";
import BullableService from "../../base/bullable.service";
import { BULL_JOB_NAME, ModulesParamsNamesTypes, SERVICE, getHttpBatchClient } from "../../common";
import knex from "../../common/utils/db_connection";
import { Network } from "../../network";
import { hasMeaningfulChanges, recordModuleParamsHistorySafe } from "../../common/utils/params_utils";
import { clearParamsCache } from "../../common/utils/params_service";
import { VeranaCredentialSchemaMessageTypes, VeranaDidMessageTypes, VeranaPermissionMessageTypes, VeranaTrustDepositMessageTypes, VeranaTrustRegistryMessageTypes } from "../../common/verana-message-types";

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

  @Action({
    name: "handleUpdateParams",
    params: {
      message: { type: "object" },
      height: { type: "number" },
      txHash: { type: "string", optional: true },
    },
  })
  public async handleUpdateParams(ctx: Context<{
    message: any;
    height: number;
    txHash?: string;
  }>) {
    const { message, height, txHash } = ctx.params;

    try {
      const result = await this.processUpdateParams(message, height, txHash);
      return result;
    } catch (err) {
      this.logger.error(`[UpdateParams] ❌ Failed to process UpdateParams message`, err);
      return { success: false, message: "Failed to process UpdateParams", error: err };
    }
  }


  public async started() {
    this.logger.info("🚀 GenesisParamsService started. Running initial sync...");
    
    if (!fs.existsSync(this.genesisPath)) {
      this.logger.info("genesis.json not found. Attempting to fetch from network...");
      await this.fetchGenesisFromNetwork();
    }
    
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
      if (!fs.existsSync(this.genesisPath)) {
        await this.fetchGenesisFromNetwork();
      }
      await this.sync();
    }, this.syncIntervalSeconds * 1000);
  }

  private async fetchGenesisFromNetwork(): Promise<void> {
    try {
      const rpc = Network?.RPC;
      if (!rpc) {
        this.logger.warn("RPC endpoint not configured. Cannot fetch genesis.json");
        return;
      }

      const httpBatchClient = getHttpBatchClient();
      
      try {
        const genesisResponse = await httpBatchClient.execute(
          createJsonRpcRequest('genesis')
        );

        fs.writeFileSync(
          this.genesisPath,
          JSON.stringify(genesisResponse.result.genesis, null, 2),
          'utf-8'
        );
        this.logger.info('Full genesis fetched successfully from network.');
      } catch (error: any) {
        let errCode = 0;
        try {
          errCode = JSON.parse(error.message).code;
        } catch {
          errCode = 0;
        }

        if (errCode !== -32603) {
          this.logger.warn(` Failed to fetch genesis (code ${errCode}): ${error.message}`);
          return;
        }

        this.logger.warn('⚙️ Falling back to chunked genesis fetch...');
        let index = 0;
        let done = false;
        const chunks: string[] = [];

        while (!done) {
          try {
            this.logger.info(`Fetching genesis_chunked: chunk ${index}`);
            const resultChunk = await httpBatchClient.execute(
              createJsonRpcRequest('genesis_chunked', { chunk: index.toString() })
            );

            const decoded = fromUtf8(fromBase64(resultChunk.result.data));
            chunks.push(decoded);
            index += 1;
          } catch (chunkError: any) {
            try {
              const parsed = JSON.parse(chunkError.message || '{}');
              if (parsed.code !== -32603) {
                this.logger.warn(`Chunk fetch failed: ${chunkError.message}`);
                return;
              }
            } catch {
              this.logger.warn(`Unknown error while parsing chunk error: ${chunkError.message}`);
            }
            done = true;
          }
        }

        if (chunks.length > 0) {
          this.logger.info(`Retrieved ${chunks.length} genesis chunks. Combining...`);
          const combinedData = chunks.join('');

          try {
            const parsedGenesis = JSON.parse(combinedData);
            fs.writeFileSync(
              this.genesisPath,
              JSON.stringify(parsedGenesis, null, 2),
              'utf-8'
            );
            this.logger.info(' Chunked genesis combined & saved successfully.');
          } catch (parseErr: any) {
            this.logger.warn(`Failed to parse combined genesis data: ${parseErr.message}`);
          }
        }
      }
    } catch (error: any) {
      this.logger.warn(`Failed to fetch genesis.json from network: ${error.message}`);
    }
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
              const isNewOrChanged = !existing || hasMeaningfulChanges(previousParams, newParamsData);

              if (isNewOrChanged) {
                await trx("module_params")
                  .insert({
                    module,
                    params: JSON.stringify(data),
                    updated_at: trx.fn.now(),
                  })
                  .onConflict("module")
                  .merge();

                await recordModuleParamsHistorySafe(
                  trx,
                  module,
                  newParamsData,
                  existing ? "UPDATE_PARAMS" : "CREATE_PARAMS",
                  genesisHeight,
                  previousParams
                );

                clearParamsCache(module);
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


  private async processUpdateParams(message: any, height: number, txHash?: string): Promise<any> {
    const { type, authority, params } = message;

    if (!type || !params) {
      return { success: false, message: "Invalid UpdateParams message structure" };
    }

    let module: string;
    if (type === VeranaCredentialSchemaMessageTypes.UpdateParams) {
      module = ModulesParamsNamesTypes.CS;
    } else if (type === VeranaDidMessageTypes.UpdateParams) {
      module = ModulesParamsNamesTypes.DD;
    } else if (type === VeranaPermissionMessageTypes.UpdateParams) {
      module = ModulesParamsNamesTypes.PERM;
    } else if (type === VeranaTrustDepositMessageTypes.UpdateParams) {
      module = ModulesParamsNamesTypes.TD;
    } else if (type === VeranaTrustRegistryMessageTypes.UpdateParams) {
      module = ModulesParamsNamesTypes.TR;
    } else {
      return { success: false, message: `Unknown UpdateParams message type: ${type}` };
    }

    this.logger.info(`[UpdateParams] Processing ${module} module params update at height ${height}`);

    try {
      const result = await this.updateModuleParams(module, params, height, authority, txHash);

      this.logger.info(`[UpdateParams] Successfully updated ${module} params at height ${height}`);
      return result;

    } catch (err) {
      this.logger.error(`[UpdateParams] ❌ Failed to update ${module} params`, err);
      return { success: false, message: `Failed to update ${module} params`, error: err };
    }
  }


  private async updateModuleParams(
    module: string,
    params: any,
    height: number,
    authority?: string,
    txHash?: string
  ): Promise<any> {
    const trx = await knex.transaction();

    try {
      const existing = await trx("module_params").where({ module }).first();
      const previousParams = existing?.params
        ? typeof existing.params === "string"
          ? JSON.parse(existing.params)
          : existing.params
        : null;

      const hasChanges = !existing || hasMeaningfulChanges(previousParams, params);

      if (!hasChanges) {
        await trx.rollback();
        this.logger.info(`[UpdateParams] No meaningful changes for ${module} params`);
        return { success: true, message: "No changes detected", module };
      }

      const paramsJson = JSON.stringify(params);
      await trx("module_params")
        .insert({
          module,
          params: paramsJson,
          updated_at: trx.fn.now(),
        })
        .onConflict("module")
        .merge();

      await recordModuleParamsHistorySafe(
        trx,
        module,
        params,
        "UPDATE_PARAMS",
        height,
        previousParams
      );

      await trx.commit();
      clearParamsCache(module);

      this.logger.info(`[UpdateParams] Updated ${module} params:`, params);

      return {
        success: true,
        message: `Successfully updated ${module} parameters`,
        module,
        params,
        height,
        authority,
        txHash,
        previousParams
      };

    } catch (err) {
      await trx.rollback();
      throw err;
    }
  }
}
