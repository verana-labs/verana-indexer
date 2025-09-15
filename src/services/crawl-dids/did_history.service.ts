import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, Errors, ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import { SERVICE } from "../../common";
import { DidHistoryRecord, DidHistoryRepository } from "../../models/did_history";

const { MoleculerClientError, MoleculerServerError } = Errors;

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
    const record = ctx.params;
    await DidHistoryRepository.insertHistory(record);
    this.logger.info("DID history saved:", record);
    return record;
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

    return { success: true, data: history };
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
