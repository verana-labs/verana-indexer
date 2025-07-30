import Redis from 'ioredis';
import { Service } from '@ourparentcenter/moleculer-decorators-extended';
import { ServiceBroker } from 'moleculer';
import { Queue } from 'bullmq';
import BullableService, {
  QueueHandler,
  DEFAULT_PREFIX,
} from '../../base/bullable.service';
import { BULL_JOB_NAME, SERVICE, Config } from '../../common';
import knex from '../../common/utils/db_connection';
import { BlockCheckpoint, Event } from '../../models';
import config from '../../../config.json' assert { type: 'json' };

@Service({
  name: SERVICE.V1.JobService.RenameEventPartitionToEvent.key,
  version: 1,
})
export default class RenameEventWithEventPartitionJob extends BullableService {
  public constructor(public broker: ServiceBroker) {
    super(broker);
  }

  public async switchEventPartitionToEvent(): Promise<void> {
    const trx = await knex.transaction();
    try {
      // Lock both table
      await trx.raw('LOCK TABLE event, event_partition');
      // Drop FK related
      await trx.raw(`
        ALTER TABLE event_attribute DROP CONSTRAINT IF EXISTS event_attribute_partition_event_id_foreign cascade;
        ALTER TABLE smart_contract_event DROP CONSTRAINT IF EXISTS smart_contract_event_event_id_foreign cascade;
      `);
      // Switch two table
      await trx.raw('ALTER TABLE event RENAME TO event_backup;');
      await trx.raw('ALTER TABLE event_partition RENAME TO event;');

      // update id seq for event_partition
      const currentEventIdSeq = await trx.raw(
        'SELECT last_value FROM transaction_event_id_seq;'
      );
      const lastEventId = currentEventIdSeq.rows[0].last_value;
      await trx.raw(
        `SELECT setval('event_partition_id_seq', ${lastEventId}, true);`
      );
      await trx.commit();
      this.logger.info('Successfully switch table event');
    } catch (error) {
      trx.rollback();
      this.logger.error(error);
    }
  }

  private async removeRepeatJob(): Promise<void> {
    const redisClient = new Redis(Config.QUEUE_JOB_REDIS);
    const jobQueue = new Queue(BULL_JOB_NAME.JOB_RENAME_EVENT_PARTITION, {
      prefix: DEFAULT_PREFIX,
      connection: redisClient,
    });
    await jobQueue.removeRepeatable(BULL_JOB_NAME.JOB_RENAME_EVENT_PARTITION, {
      every: config.jobRenameEventTable.millisecondRepeat,
    });
    this.logger.info('Successfully stop repeatable job');
  }

  @QueueHandler({
    queueName: BULL_JOB_NAME.JOB_RENAME_EVENT_PARTITION,
    jobName: BULL_JOB_NAME.JOB_RENAME_EVENT_PARTITION,
  })
  public async migrateEventPartition(): Promise<void> {
    const eventPartitionCount = await knex.raw(`
      SELECT count(*) AS partition_count
      FROM pg_catalog.pg_inherits
      WHERE inhparent = 'event'::regclass;
    `);
    if (eventPartitionCount.rows[0].partition_count > 0) {
      await this.removeRepeatJob();
      this.logger.info('Event already partitioned');
      return;
    }

    const latestEvent = await Event.query().orderBy('id', 'DESC').limit(1);
    const migrateEventCheckPoint = await BlockCheckpoint.query()
      .where('job_name', BULL_JOB_NAME.CP_MIGRATE_DATA_EVENT_TABLE)
      .first();

    if (
      latestEvent[0]?.id.toString() !== migrateEventCheckPoint?.height.toString()
    ) {
      this.logger.info(`
        Waiting for event migration done, current event ${latestEvent[0]?.id}, current migration ${migrateEventCheckPoint?.height}
      `);
      return;
    }

    await this.switchEventPartitionToEvent();
    await this.removeRepeatJob();
  }

  public async _start(): Promise<void> {
    await this.createJob(
      BULL_JOB_NAME.JOB_RENAME_EVENT_PARTITION,
      BULL_JOB_NAME.JOB_RENAME_EVENT_PARTITION,
      {},
      {
        removeOnComplete: true,
        removeOnFail: {
          count: 3,
        },
        repeat: {
          every: config.jobRenameEventTable.millisecondRepeat,
        },
      }
    );
    return super._start();
  }
}
