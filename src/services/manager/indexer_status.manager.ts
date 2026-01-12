import { Queue } from "bullmq";
import Redis from "ioredis";
import { BULL_JOB_NAME } from "../../common/constant";
import ConfigClass from "../../common/config";
import { DEFAULT_PREFIX } from "../../base/bullable.service";

const Config = new ConfigClass();

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

type StatusChangeCallback = (status: {
  indexerStatus: "running" | "stopped";
  crawlingStatus: "active" | "stopped";
  stoppedAt?: string;
  stoppedReason?: string;
  lastError?: {
    message: string;
    timestamp: string;
    service?: string;
  };
}) => void;

class IndexerStatusManager {
  private static instance: IndexerStatusManager;
  private status: IndexerStatus = {
    isRunning: true,
    isCrawling: true, 
  };
  private statusChangeCallback: StatusChangeCallback | null = null;

  private constructor() {
  }

  public static getInstance(): IndexerStatusManager {
    if (!IndexerStatusManager.instance) {
      IndexerStatusManager.instance = new IndexerStatusManager();
    }
    return IndexerStatusManager.instance;
  }

  /**
   * Register a callback to be notified when status changes
   * This breaks the dependency cycle by using a callback pattern
   */
  public setStatusChangeCallback(callback: StatusChangeCallback): void {
    this.statusChangeCallback = callback;
  }

  private notifyStatusChange(): void {
    if (this.statusChangeCallback) {
      this.statusChangeCallback({
        indexerStatus: this.status.isRunning ? "running" : "stopped",
        crawlingStatus: this.status.isCrawling ? "active" : "stopped",
        stoppedAt: this.status.stoppedAt,
        stoppedReason: this.status.stoppedReason,
        lastError: this.status.lastError,
      });
    }
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

    this.notifyStatusChange();
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

    this.notifyStatusChange();
  }

  public async resumeIndexer(): Promise<void> {
    this.status.isRunning = true;
    this.status.isCrawling = true;
    this.status.stoppedAt = undefined;
    this.status.lastError = undefined;
    this.status.stoppedReason = undefined;

    this.notifyStatusChange();
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

export const indexerStatusManager = IndexerStatusManager.getInstance();

