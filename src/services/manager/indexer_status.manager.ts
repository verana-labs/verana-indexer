import { Queue } from "bullmq";
import Redis from "ioredis";
import { LoggerInstance } from "moleculer";
import { BULL_JOB_NAME } from "../../common/constant";
import ConfigClass from "../../common/config";
import { DEFAULT_PREFIX } from "../../base/bullable.service";
import { getLcdClient } from "../../common/utils/verana_client";

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
  private logger: LoggerInstance | null = null;
  private recoveryCheckInterval: NodeJS.Timeout | null = null;
  private readonly RECOVERY_CHECK_INTERVAL = 30000;
  private readonly RECOVERY_CHECK_TIMEOUT = 10000;

  private constructor() {
    // Singleton pattern - private constructor to prevent instantiation
  }

  public static getInstance(): IndexerStatusManager {
    if (!IndexerStatusManager.instance) {
      IndexerStatusManager.instance = new IndexerStatusManager();
    }
    return IndexerStatusManager.instance;
  }

  public setStatusChangeCallback(callback: StatusChangeCallback): void {
    this.statusChangeCallback = callback;
  }

  public setLogger(logger: LoggerInstance): void {
    this.logger = logger;
  }

  private notifyStatusChange(): void {
    if (this.statusChangeCallback) {
      Promise.resolve(this.statusChangeCallback({
        indexerStatus: this.status.isRunning ? "running" : "stopped",
        crawlingStatus: this.status.isCrawling ? "active" : "stopped",
        stoppedAt: this.status.stoppedAt,
        stoppedReason: this.status.stoppedReason,
        lastError: this.status.lastError,
      })).catch((err) => {
        if (this.logger) {
          this.logger.error("Error in status change callback:", err);
        } else {
          console.error("Error in status change callback:", err);
        }
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

    const errorMessage = error.message || String(error);
    const isNetworkError = errorMessage.toLowerCase().includes('timeout') ||
                          errorMessage.toLowerCase().includes('network') ||
                          errorMessage.toLowerCase().includes('connection') ||
                          errorMessage.toLowerCase().includes('econnrefused') ||
                          errorMessage.toLowerCase().includes('etimedout');
    
    if (isNetworkError) {
      this.startRecoveryChecker();
    }

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

    const errorMessage = error.message || String(error);
    const isNetworkError = errorMessage.toLowerCase().includes('timeout') ||
                          errorMessage.toLowerCase().includes('network') ||
                          errorMessage.toLowerCase().includes('connection') ||
                          errorMessage.toLowerCase().includes('econnrefused') ||
                          errorMessage.toLowerCase().includes('etimedout');
    
    if (isNetworkError) {
      this.startRecoveryChecker();
    }

    this.notifyStatusChange();
  }

  public async resumeIndexer(): Promise<void> {
    this.status.isRunning = true;
    this.status.isCrawling = true;
    this.status.stoppedAt = undefined;
    this.status.lastError = undefined;
    this.status.stoppedReason = undefined;

    this.stopRecoveryChecker();

    await this.resumeAllCrawlingJobs();

    this.notifyStatusChange();
  }

  private startRecoveryChecker(): void {
    this.stopRecoveryChecker();

    if (this.logger) {
      this.logger.info(`Starting automatic recovery checker (checking every ${this.RECOVERY_CHECK_INTERVAL / 1000}s)...`);
    } else {
      console.log(`Starting automatic recovery checker (checking every ${this.RECOVERY_CHECK_INTERVAL / 1000}s)...`);
    }

    this.recoveryCheckInterval = setInterval(async () => {
      await this.checkAndRecover();
    }, this.RECOVERY_CHECK_INTERVAL);
  }

  private stopRecoveryChecker(): void {
    if (this.recoveryCheckInterval) {
      clearInterval(this.recoveryCheckInterval);
      this.recoveryCheckInterval = null;
      if (this.logger) {
        this.logger.info('Stopped automatic recovery checker');
      } else {
        console.log('Stopped automatic recovery checker');
      }
    }
  }

  private async checkAndRecover(): Promise<void> {
    if (!this.status.isCrawling || !this.status.isRunning) {
      try {
        if (this.logger) {
          this.logger.info('Checking if connection is restored...');
        } else {
          console.log('Checking if connection is restored...');
        }
        
        const isHealthy = await this.checkConnectionHealth();
        
        if (isHealthy) {
          if (this.logger) {
            this.logger.info('Connection restored! Automatically resuming indexer...');
          } else {
            console.log('Connection restored! Automatically resuming indexer...');
          }
          await this.resumeIndexer();
          if (this.logger) {
            this.logger.info('Indexer resumed successfully');
          } else {
            console.log('Indexer resumed successfully');
          }
          return;
        }
        if (this.logger) {
          this.logger.info('Connection not yet restored, will retry...');
        } else {
          console.log('Connection not yet restored, will retry...');
        }
      } catch (error: any) {
        if (this.logger) {
          this.logger.info(`Recovery check failed: ${error?.message || error}. Will retry...`);
        } else {
          console.log(`Recovery check failed: ${error?.message || error}. Will retry...`);
        }
      }
    } else if (this.status.isCrawling && this.status.isRunning) {
      this.stopRecoveryChecker();
    }
  }

  private async checkConnectionHealth(): Promise<boolean> {
    try {
      const lcdClient = await getLcdClient();
      if (!lcdClient?.provider) {
        return false;
      }

      const healthCheck = Promise.race([
        lcdClient.provider.cosmos.base.tendermint.v1beta1.getNodeInfo(),
        new Promise<never>((_, reject) => {
          setTimeout(() => reject(new Error('Health check timeout')), this.RECOVERY_CHECK_TIMEOUT);
        }),
      ]);

      await healthCheck;
      return true;
    } catch (error: any) {
      const errorMessage = error?.message || String(error);
      if (!errorMessage.includes('timeout') && !errorMessage.includes('Health check timeout')) {
        if (this.logger) {
          this.logger.info(`Health check error: ${errorMessage}`);
        } else {
          console.log(`Health check error: ${errorMessage}`);
        }
      }
      return false;
    }
  }

  private async resumeAllCrawlingJobs(): Promise<void> {
    try {
      if (!Config.QUEUE_JOB_REDIS) {
        if (this.logger) {
          this.logger.warn("QUEUE_JOB_REDIS not configured, cannot resume jobs");
        } else {
          console.warn("QUEUE_JOB_REDIS not configured, cannot resume jobs");
        }
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

          await queue.resume();
          if (this.logger) {
            this.logger.info(`Resumed queue: ${jobName}`);
          } else {
            console.log(`Resumed queue: ${jobName}`);
          }
        } catch (err) {
          if (this.logger) {
            this.logger.error(`Failed to resume queue ${jobName}:`, err);
          } else {
            console.error(`Failed to resume queue ${jobName}:`, err);
          }
        }
      }

      await redisClient.quit();
    } catch (error) {
      if (this.logger) {
        this.logger.error("Error resuming crawling jobs:", error);
      } else {
        console.error("Error resuming crawling jobs:", error);
      }
    }
  }

  public isCrawlingActive(): boolean {
    return this.status.isCrawling && this.status.isRunning;
  }

  private async stopAllCrawlingJobs(): Promise<void> {
    try {
      if (!Config.QUEUE_JOB_REDIS) {
        if (this.logger) {
          this.logger.warn("QUEUE_JOB_REDIS not configured, cannot stop jobs");
        } else {
          console.warn("QUEUE_JOB_REDIS not configured, cannot stop jobs");
        }
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
          if (this.logger) {
            this.logger.error(`Failed to stop queue ${jobName}:`, err);
          } else {
            console.error(`Failed to stop queue ${jobName}:`, err);
          }
        }
      }

      await redisClient.quit();
    } catch (error) {
      if (this.logger) {
        this.logger.error("Error stopping crawling jobs:", error);
      } else {
        console.error("Error stopping crawling jobs:", error);
      }
    }
  }

  public isIndexerRunning(): boolean {
    return this.status.isRunning;
  }
}

export const indexerStatusManager = IndexerStatusManager.getInstance();

