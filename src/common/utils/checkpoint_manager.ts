import { BlockCheckpoint } from '../../models';
import knex from './db_connection';
import { executeWithRetry } from './db_query_helper';

export interface CheckpointUpdateOptions {
  timeout?: number;
  retries?: number;
  logger?: any;
}

export class CheckpointManager {
  private logger?: any;
  private defaultTimeout: number;
  private defaultRetries: number;

  constructor(logger?: any, defaultTimeout: number = 10000, defaultRetries: number = 3) {
    this.logger = logger;
    this.defaultTimeout = defaultTimeout;
    this.defaultRetries = defaultRetries;
  }

  async ensureCheckpoint(
    jobName: string,
    startHeight: number = 0
  ): Promise<BlockCheckpoint> {
    return executeWithRetry(
      async () => {
        let checkpoint = await BlockCheckpoint.query(knex)
          .findOne({ job_name: jobName })
          .timeout(this.defaultTimeout);

        if (!checkpoint) {
          checkpoint = await BlockCheckpoint.query(knex)
            .insertAndFetch({
              job_name: jobName,
              height: startHeight,
            })
            .timeout(this.defaultTimeout);
          
          if (this.logger?.info) {
            this.logger.info(`Created checkpoint for ${jobName} at height ${startHeight}`);
          }
        }

        return checkpoint;
      },
      { timeout: this.defaultTimeout, retries: this.defaultRetries },
      this.logger
    );
  }

  async updateCheckpoint(
    jobName: string,
    newHeight: number,
    options: CheckpointUpdateOptions = {}
  ): Promise<void> {
    const timeout = options.timeout || this.defaultTimeout;
    const retries = options.retries || this.defaultRetries;
    const logger = options.logger || this.logger;

    await executeWithRetry(
      async () => {
        const patchObj = {
          height: newHeight,
          updated_at: new Date().toISOString()
        };

        const updated = await BlockCheckpoint.query(knex)
          .patch(patchObj)
          .where('job_name', jobName)
          .timeout(timeout);

        if (updated === 0) {
          const existing = await BlockCheckpoint.query(knex)
            .findOne({ job_name: jobName })
            .timeout(timeout);

          if (existing) {
            await BlockCheckpoint.query(knex)
              .patch(patchObj)
              .where('job_name', jobName)
              .timeout(timeout);
          } else {
            await BlockCheckpoint.query(knex)
              .insert({
                job_name: jobName,
                height: newHeight,
              })
              .timeout(timeout);
            
            if (logger?.info) {
              logger.info(`Created checkpoint for ${jobName} at height ${newHeight}`);
            }
          }
        }
        if (logger?.debug) {
          logger.debug(`Updated checkpoint for ${jobName} to height ${newHeight}`);
        }
      },
      { timeout, retries },
      logger
    );
  }

  async getCheckpoint(jobName: string): Promise<BlockCheckpoint | null> {
    return executeWithRetry(
      async () => {
        const checkpoint = await BlockCheckpoint.query(knex)
          .findOne({ job_name: jobName })
          .timeout(this.defaultTimeout);
        return checkpoint || null;
      },
      { timeout: this.defaultTimeout, retries: this.defaultRetries },
      this.logger
    );
  }
}

