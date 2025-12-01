import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import { SERVICE } from "../../common";
import { DidHistoryRecord, DidHistoryRepository } from "../../models/did_history";
import ApiResponder from "../../common/utils/apiResponse";


@Service({
  name: SERVICE.V1.DidHistoryService.key,
  version: 1,
})
export default class DidHistoryService extends BullableService {
  constructor(broker: ServiceBroker) {
    super(broker);
  }

  @Action({ name: "save" })
  async save(ctx: { params: DidHistoryRecord }) {
    try {
      const record = ctx.params;
      delete record?.id
      this.logger.info("Attempting to save DID history:", record);
      await DidHistoryRepository.insertHistory(record);
      this.logger.info("✅ DID history saved successfully:", record);
      return record;
    } catch (err) {
      this.logger.error("❌ Error saving DID history:", err);
      console.error("FATAL DID HISTORY SAVE ERROR:", err);
      return {
        success: false,
        error: {
          code: 500,
          name: "DidHistorySaveError",
          message: "Failed to save DID history",
        },
      };
    }
  }
  @Action({ name: "getByDid", params: { did: "string" } })
  async getByDid(ctx: Context<{ did: string }>) {
    try {
      const history = await DidHistoryRepository.getByDid(ctx.params.did);
      if (!history || history.length === 0) {
        return {
          success: false,
          error: {
            code: 404,
            name: "DidHistoryNotFound",
            message: `No history found for DID: ${ctx.params.did}`
          }
        };
      }
      return ApiResponder.success(ctx, { did: history });
    } catch (err) {
      this.logger.error("Error fetching DID history:", err);
      return {
        success: false,
        error: {
          code: 500,
          name: "DidHistoryServiceError",
          message: "Failed to fetch DID history"
        }
      };
    }
  }



  public async _start() {
    await super._start();
    this.logger.info("DidHistoryService started and ready.");
  }
}
