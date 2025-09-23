import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import fs from "fs";
import { Context, ServiceBroker } from "moleculer";
import path from "path";
import BullableService from "../../base/bullable.service";
import { SERVICE } from "../../common";
import knex from "../../common/utils/db_connection";


@Service({
    name: SERVICE.V1.GenesisParamsService.key,
    version: 1,
})
export default class GenesisParamsService extends BullableService {
    private genesisPath = path.resolve("genesis.json");
    private watcher?: fs.FSWatcher;

    public constructor(public broker: ServiceBroker) {
        super(broker);
    }

    @Action({
        name: SERVICE.V1.GenesisParamsService.key,
        description: "Sync genesis parameters manually",
    })
    public async syncParams(ctx: Context<unknown>) {
        return this.sync();
    }
    public async started() {
        this.logger.info("🚀 GenesisParamsService started. Initial sync in progress...");
        await this.sync();

        // Watch the genesis file for changes
        if (fs.existsSync(this.genesisPath)) {
            this.watcher = fs.watch(this.genesisPath, async (eventType) => {
                if (eventType === "change") {
                    this.logger.info("🔄 Detected changes in genesis.json. Updating params...");
                    await this.sync();
                }
            });
            this.logger.info("👀 Watching genesis.json for changes...");
        } else {
            this.logger.warn("⚠️ genesis.json not found. Watching disabled.");
        }
    }

    public async stopped() {
        if (this.watcher) {
            this.watcher.close();
            this.logger.info("🛑 Stopped watching genesis.json");
        }
    }

    // ---- Core sync logic ----
    private async sync() {
        if (!fs.existsSync(this.genesisPath)) {
            this.logger.warn("⚠️ genesis.json not found. Skipping params sync.");
            return { success: false, message: "genesis.json not found" };
        }

        try {
            const raw = fs.readFileSync(this.genesisPath, "utf-8");

            const jsonObjects = raw.split(/}\s*{/).map((chunk, index, arr) => {
                if (arr.length > 1) {
                    if (index === 0) return `${chunk}}`;
                    if (index === arr.length - 1) return `{${chunk}`;
                    return `{${chunk}}`;
                }
                return chunk;
            });

            let updatedModules = 0;

            for (const chunk of jsonObjects) {
                let genesis;
                try {
                    genesis = JSON.parse(chunk);
                } catch (err) {
                    this.logger.error("❌ Failed to parse a chunk of genesis.json", err);
                    continue;
                }

                const appState = genesis.app_state || {};

                for (const [module, data] of Object.entries(appState)) {
                    if (data && typeof data === "object" && "params" in data) {
                        await knex("module_params")
                            .insert({
                                module,
                                params: JSON.stringify((data as any)),
                                updated_at: knex.fn.now(),
                            })
                            .onConflict("module")
                            .merge();
                        updatedModules++;
                    }
                }
            }

            this.logger.info(`✅ Genesis params synced successfully. Modules updated: ${updatedModules}`);
            return { success: true, updatedModules };
        } catch (err) {
            this.logger.error("❌ Failed to read or parse genesis.json", err);
            return { success: false, message: "Error reading/parsing genesis.json", error: err };
        }
    }

}
