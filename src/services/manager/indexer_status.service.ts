import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BaseService from "../../base/base.service";
import { SERVICE } from "../../common/constant";
import ApiResponder from "../../common/utils/apiResponse";
import { indexerStatusManager } from "./indexer_status.manager";

@Service({
  name: SERVICE.V1.IndexerStatusService.key,
  version: 1,
})
export default class IndexerStatusService extends BaseService {

  public constructor(public broker: ServiceBroker) {
    super(broker);
  }

  async started() {
    indexerStatusManager.setLogger(this.logger);
    await indexerStatusManager.resumeIndexer();
  }

  @Action()
  public async getStatus(ctx: Context): Promise<any> {
    const status = indexerStatusManager.getStatus();
    return ApiResponder.success(ctx, status, 200);
  }

  @Action()
  public async stopIndexer(ctx: Context<{ error: Error; service?: string }>): Promise<any> {
    const { error, service } = ctx.params;
    await indexerStatusManager.stopIndexer(error, service);
    return ApiResponder.success(
      ctx,
      { message: "Indexer stopped successfully", status: indexerStatusManager.getStatus() },
      200
    );
  }

  @Action()
  public async resumeIndexer(ctx: Context): Promise<any> {
    await indexerStatusManager.resumeIndexer();
    return ApiResponder.success(
      ctx,
      { message: "Indexer resumed successfully", status: indexerStatusManager.getStatus() },
      200
    );
  }

  @Action()
  public async isRunning(ctx: Context): Promise<any> {
    const isRunning = indexerStatusManager.isIndexerRunning();
    const isCrawling = indexerStatusManager.isCrawlingActive();
    return ApiResponder.success(ctx, { isRunning, isCrawling }, 200);
  }
}

