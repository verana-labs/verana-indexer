import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BaseService from "../../base/base.service";
import { SERVICE, BULL_JOB_NAME, Config } from "../../common";
import ApiResponder from "../../common/utils/apiResponse";
import QueueManager from "../../common/queue/queue-manager";
import { Queue } from "bullmq";
import Redis from "ioredis";
import { DEFAULT_PREFIX } from "../../base/bullable.service";
import { eventsBroadcaster } from "../api/events_broadcaster";

export interface IndexerStatus {
  isRunning: boolean;
  isCrawling: boolean;
  stoppedAt?: string;
  lastError?: {
    message: string;
    stack?: string;
    timestamp: string;
    service?: string;
  };
  stoppedReason?: string;
}

class IndexerStatusManager {
  private static instance: IndexerStatusManager;
  private status: IndexerStatus = {
    isRunning: true,
    isCrawling: true, 
  };
  private queueManager: QueueManager;

  private constructor() {
    this.queueManager = QueueManager.getInstance();
  }

  public static getInstance(): IndexerStatusManager {
    if (!IndexerStatusManager.instance) {
      IndexerStatusManager.instance = new IndexerStatusManager();
    }
    return IndexerStatusManager.instance;
  }

  public getStatus(): IndexerStatus {
    return { ...this.status };
  }

  public async stopIndexer(error: Error, service?: string): Promise<void> {
    if (!this.status.isRunning) {
      return; 
    }

    this.status.isRunning = false;
    this.status.isCrawling = false;
    this.status.stoppedAt = new Date().toISOString();
    this.status.lastError = {
      message: error.message || String(error),
      stack: error.stack,
      timestamp: new Date().toISOString(),
      service: service || "unknown",
    };
    this.status.stoppedReason = `Indexer stopped due to error in ${service || "unknown"}: ${error.message}`;

    await this.stopAllCrawlingJobs();

    eventsBroadcaster.broadcastIndexerStatus({
      indexerStatus: "stopped",
      crawlingStatus: "stopped",
      stoppedAt: this.status.stoppedAt,
      stoppedReason: this.status.stoppedReason,
      lastError: this.status.lastError,
    });
  }

  
  public async stopCrawlingOnly(error: Error, service?: string): Promise<void> {
    if (!this.status.isCrawling) {
      return; 
    }

    this.status.isCrawling = false;
    this.status.stoppedAt = new Date().toISOString();
    this.status.lastError = {
      message: error.message || String(error),
      stack: error.stack,
      timestamp: new Date().toISOString(),
      service: service || "unknown",
    };
    this.status.stoppedReason = `Crawling stopped due to: ${error.message}. Indexer APIs remain available.`;

    await this.stopAllCrawlingJobs();

    eventsBroadcaster.broadcastIndexerStatus({
      indexerStatus: "running",
      crawlingStatus: "stopped",
      stoppedAt: this.status.stoppedAt,
      stoppedReason: this.status.stoppedReason,
      lastError: this.status.lastError,
    });
  }

  public async resumeIndexer(): Promise<void> {
    this.status.isRunning = true;
    this.status.isCrawling = true;
    this.status.stoppedAt = undefined;
    this.status.lastError = undefined;
    this.status.stoppedReason = undefined;

    eventsBroadcaster.broadcastIndexerStatus({
      indexerStatus: "running",
      crawlingStatus: "active",
    });
  }

  public isCrawlingActive(): boolean {
    return this.status.isCrawling && this.status.isRunning;
  }

  private async stopAllCrawlingJobs(): Promise<void> {
    try {
      if (!Config.QUEUE_JOB_REDIS) {
        console.warn("QUEUE_JOB_REDIS not configured, cannot stop jobs");
        return;
      }

      const redisClient = new Redis(Config.QUEUE_JOB_REDIS);
      
      const crawlingJobs = [
        BULL_JOB_NAME.CRAWL_BLOCK,
        BULL_JOB_NAME.CRAWL_TRANSACTION,
        BULL_JOB_NAME.HANDLE_TRANSACTION,
      ];

      for (const jobName of crawlingJobs) {
        try {
          const queue = new Queue(jobName, {
            prefix: DEFAULT_PREFIX,
            connection: redisClient,
          });

          const repeatableJobs = await queue.getRepeatableJobs();
          for (const job of repeatableJobs) {
            await queue.removeRepeatableByKey(job.key);
          }

          await queue.pause();
        } catch (err) {
          console.error(`Failed to stop queue ${jobName}:`, err);
        }
      }

      await redisClient.quit();
    } catch (error) {
      console.error("Error stopping crawling jobs:", error);
    }
  }

  public isIndexerRunning(): boolean {
    return this.status.isRunning;
  }
}

@Service({
  name: SERVICE.V1.IndexerStatusService.key,
  version: 1,
})
export default class IndexerStatusService extends BaseService {
  private statusManager: IndexerStatusManager;

  public constructor(public broker: ServiceBroker) {
    super(broker);
    this.statusManager = IndexerStatusManager.getInstance();
  }

  @Action()
  public async getStatus(ctx: Context): Promise<any> {
    const status = this.statusManager.getStatus();
    return ApiResponder.success(ctx, status, 200);
  }

  @Action()
  public async stopIndexer(ctx: Context<{ error: Error; service?: string }>): Promise<any> {
    const { error, service } = ctx.params;
    await this.statusManager.stopIndexer(error, service);
    return ApiResponder.success(
      ctx,
      { message: "Indexer stopped successfully", status: this.statusManager.getStatus() },
      200
    );
  }

  @Action()
  public async resumeIndexer(ctx: Context): Promise<any> {
    await this.statusManager.resumeIndexer();
    return ApiResponder.success(
      ctx,
      { message: "Indexer resumed successfully", status: this.statusManager.getStatus() },
      200
    );
  }

  @Action()
  public async isRunning(ctx: Context): Promise<any> {
    const isRunning = this.statusManager.isIndexerRunning();
    const isCrawling = this.statusManager.isCrawlingActive();
    return ApiResponder.success(ctx, { isRunning, isCrawling }, 200);
  }
}

export const indexerStatusManager = IndexerStatusManager.getInstance();

