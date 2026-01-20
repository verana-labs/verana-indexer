import {
  Action,
  Service,
} from '@ourparentcenter/moleculer-decorators-extended';
import { Context, ServiceBroker } from 'moleculer';
import BullableService from '../../base/bullable.service';
import { BULL_JOB_NAME } from '../../common';
import { BlockCheckpoint } from '../../models';
import config from '../../config.json' with { type: 'json' };

@Service({
  name: 'checkpoint-init',
  version: 1,
})
export default class CheckpointInitService extends BullableService {
  public constructor(public broker: ServiceBroker) {
    super(broker);
  }

  private readonly REQUIRED_CHECKPOINT_JOBS = [
    BULL_JOB_NAME.CRAWL_BLOCK,
    BULL_JOB_NAME.CRAWL_TRANSACTION,
    BULL_JOB_NAME.HANDLE_TRANSACTION,
    BULL_JOB_NAME.CRAWL_GENESIS,
    BULL_JOB_NAME.CRAWL_GENESIS_ACCOUNT,
    BULL_JOB_NAME.CRAWL_GENESIS_VALIDATOR,
    BULL_JOB_NAME.CRAWL_GENESIS_PROPOSAL,
    BULL_JOB_NAME.CRAWL_GENESIS_CODE,
    BULL_JOB_NAME.CRAWL_GENESIS_CONTRACT,
    BULL_JOB_NAME.CRAWL_GENESIS_FEEGRANT,
    BULL_JOB_NAME.CRAWL_GENESIS_IBC_TAO,
    BULL_JOB_NAME.JOB_HANDLE_ACCOUNTS,
    BULL_JOB_NAME.CRAWL_VALIDATOR,
    BULL_JOB_NAME.CRAWL_SIGNING_INFO,
    BULL_JOB_NAME.CRAWL_TALLY_PROPOSAL,
    BULL_JOB_NAME.CRAWL_PROPOSAL,
    BULL_JOB_NAME.HANDLE_STAKE_EVENT,
    BULL_JOB_NAME.CHECKPOINT_UPDATE_DELEGATOR,
    BULL_JOB_NAME.HANDLE_COIN_TRANSFER,
    BULL_JOB_NAME.HANDLE_VOTE_TX,
    BULL_JOB_NAME.HANDLE_AUTHZ_TX,
    BULL_JOB_NAME.HANDLE_FEEGRANT,
    BULL_JOB_NAME.CRAWL_IBC_TAO,
    BULL_JOB_NAME.CRAWL_IBC_APP,
    BULL_JOB_NAME.CRAWL_IBC_ICS20,
    BULL_JOB_NAME.JOB_UPDATE_SENDER_IN_TX_MESSAGES,
    BULL_JOB_NAME.JOB_UPDATE_TX_COUNT_IN_BLOCK,
    BULL_JOB_NAME.JOB_CREATE_EVENT_ATTR_PARTITION,
    BULL_JOB_NAME.CP_MIGRATE_DATA_EVENT_TABLE,
  ];

  @Action({
    name: 'initializeCheckpoints',
    params: {
      force: { type: 'boolean', optional: true, default: false },
    },
  })
  public async initializeCheckpoints(ctx: Context<{ force?: boolean }>) {
    try {
      const { force = false } = ctx.params;
      this.logger.info(' Starting checkpoint initialization...');

      const existingCheckpoints = await BlockCheckpoint.query()
        .select('job_name')
        .whereIn('job_name', this.REQUIRED_CHECKPOINT_JOBS);

      const existingJobNames = new Set(
        existingCheckpoints.map((cp) => cp.job_name)
      );

      const missingJobs = this.REQUIRED_CHECKPOINT_JOBS.filter(
        (jobName) => !existingJobNames.has(jobName)
      );

      if (missingJobs.length === 0) {
        this.logger.info('All required checkpoints already exist');
        return {
          success: true,
          message: 'All checkpoints exist',
          created: 0,
          existing: existingCheckpoints.length,
        };
      }

      this.logger.info(
        ` Found ${missingJobs.length} missing checkpoints: ${missingJobs.join(', ')}`
      );

      const startBlock = config.crawlBlock?.startBlock || 0;
      const createdCheckpoints: Array<{ job_name: string; height: number }> =
        [];

      for (const jobName of missingJobs) {
        try {
          const checkpoint = await BlockCheckpoint.query().insert({
            job_name: jobName,
            height: startBlock,
          });

          createdCheckpoints.push({
            job_name: jobName,
            height: startBlock,
          });

          this.logger.info(
            `Created checkpoint for ${jobName} at height ${startBlock}`
          );
        } catch (error: any) {
          if (error?.code === '23505' || error?.constraint === 'block_checkpoint_job_name_unique') {
            this.logger.info(
              ` Checkpoint for ${jobName} already exists (race condition handled)`
            );
          } else {
            this.logger.error(
              `❌ Failed to create checkpoint for ${jobName}: ${error?.message || error}`
            );
          }
        }
      }

      this.logger.info(
        `Checkpoint initialization complete. Created ${createdCheckpoints.length} checkpoints`
      );

      return {
        success: true,
        message: `Created ${createdCheckpoints.length} missing checkpoints`,
        created: createdCheckpoints.length,
        existing: existingCheckpoints.length,
        createdCheckpoints,
      };
    } catch (error: any) {
      this.logger.error(
        `❌ Error initializing checkpoints: ${error?.message || error}`
      );
      throw error;
    }
  }

  @Action({
    name: 'verifyCheckpoints',
  })
  public async verifyCheckpoints(ctx: Context) {
    try {
      this.logger.info(' Verifying all required checkpoints...');

      const existingCheckpoints = await BlockCheckpoint.query()
        .select('job_name', 'height')
        .whereIn('job_name', this.REQUIRED_CHECKPOINT_JOBS);

      const existingJobNames = new Set(
        existingCheckpoints.map((cp) => cp.job_name)
      );

      const missingJobs = this.REQUIRED_CHECKPOINT_JOBS.filter(
        (jobName) => !existingJobNames.has(jobName)
      );

      const result = {
        totalRequired: this.REQUIRED_CHECKPOINT_JOBS.length,
        existing: existingCheckpoints.length,
        missing: missingJobs.length,
        missingJobs,
        existingCheckpoints: existingCheckpoints.map((cp) => ({
          job_name: cp.job_name,
          height: cp.height,
        })),
      };

      if (missingJobs.length > 0) {
        this.logger.warn(
          ` Found ${missingJobs.length} missing checkpoints: ${missingJobs.join(', ')}`
        );
      } else {
        this.logger.info('All required checkpoints exist');
      }

      return result;
    } catch (error: any) {
      this.logger.error(
        `❌ Error verifying checkpoints: ${error?.message || error}`
      );
      throw error;
    }
  }

  public async _start() {
    try {
      this.logger.info(' CheckpointInitService starting...');
      
      const result = await this.initializeCheckpoints({
        params: { force: false },
      } as Context<{ force?: boolean }>);

      if (result.created > 0) {
        this.logger.info(
          `Auto-initialized ${result.created} missing checkpoints on startup`
        );
      }
    } catch (error: any) {
      this.logger.error(
        `❌ Failed to auto-initialize checkpoints on startup: ${error?.message || error}`
      );
    }

    return super._start();
  }
}
