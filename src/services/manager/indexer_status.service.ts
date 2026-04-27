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
    return ApiResponder.success(ctx, this.toPublicStatus(status), 200);
  }

  @Action({ name: "getDetailedStatus" })
  public async getDetailedStatus(ctx: Context): Promise<any> {
    const status = await indexerStatusManager.getDetailedStatus();
    return ApiResponder.success(ctx, this.toPublicStatus(status), 200);
  }

  @Action()
  public async stopIndexer(ctx: Context<{ error: Error; service?: string }>): Promise<any> {
    const { error, service } = ctx.params;
    await indexerStatusManager.stopIndexer(error, service);
    return ApiResponder.success(
      ctx,
      { message: "Indexer stopped successfully", status: this.toPublicStatus(indexerStatusManager.getStatus()) },
      200
    );
  }

  @Action()
  public async resumeIndexer(ctx: Context): Promise<any> {
    await indexerStatusManager.resumeIndexer();
    return ApiResponder.success(
      ctx,
      { message: "Indexer resumed successfully", status: this.toPublicStatus(indexerStatusManager.getStatus()) },
      200
    );
  }

  @Action()
  public async isRunning(ctx: Context): Promise<any> {
    const isRunning = indexerStatusManager.isIndexerRunning();
    const isCrawling = indexerStatusManager.isCrawlingActive();
    return ApiResponder.success(ctx, { is_running: isRunning, is_crawling: isCrawling }, 200);
  }

  private toPublicStatus(status: any) {
    return {
      is_running: Boolean(status?.isRunning),
      is_crawling: Boolean(status?.isCrawling),
      stopped_at: status?.stoppedAt,
      stopped_reason: status?.stoppedReason,
      last_processed_block:
        status?.lastProcessedBlock != null ? Number(status.lastProcessedBlock) : undefined,
      blockchain_api_healthy:
        typeof status?.blockchainApiHealthy === "boolean" ? status.blockchainApiHealthy : undefined,
      blockchain_api_last_check_at: status?.blockchainApiLastCheckAt,
      last_error: status?.lastError
        ? {
            message: status.lastError.message,
            timestamp: status.lastError.timestamp,
            service: status.lastError.service,
          }
        : undefined,
    };
  }
}

