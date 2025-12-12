/* eslint-disable max-classes-per-file */
import { Job, Queue, Worker, WorkerOptions } from 'bullmq';
import _ from 'underscore';
import { JobOption, QueueOptions, QueueProvider } from './queue-manager-types';
import { getRedisConnection } from './redis-connector';

class DefaultValue {
  static readonly DEFAULT_JOB_NAME = '_default_bull_job';

  static readonly DEFAULT_WORKER_OPTION: WorkerOptions = {
    concurrency: 1,
    lockDuration: 300000, 
    maxStalledCount: 1,
  };

  static readonly DEFAULT_JOB_OTION: JobOption = {
    // removeOnComplete: true,
    removeOnFail: {
      count: 4,
    },
    removeOnComplete: 3,
  };
}

export class BullQueueProvider implements QueueProvider {
  private _queues: Record<string, Queue> = {};

  private _workers: Worker[] = [];

  public submitJob(
    queueName: string,
    jobName: string,
    opts?: JobOption,
    payload?: object
  ): void {
    const q = this.getQueue(queueName);
    q.add(jobName, payload, opts);
  }

 public registerQueueHandler(
  opt: QueueOptions,
  fn: (payload: object) => Promise<void>
): void {
  // create a new worker to handle the job
  const processor = async (job: Job) => {
    try {
      await fn(job.data);
    } catch (e: any) {
      if (e?.message?.includes('Missing lock for job repeat') ||
          e?.message?.includes('not in the delayed state') ||
          e?.message?.includes('is not in the delayed state')) {
        console.warn(`[BullMQ] Repeatable job state issue for ${job.name}, this is usually safe to ignore`);
        return;
      }
      console.error(`job ${job.name} failed`);
      console.error(e);
      throw e;
    }
  };

  // 🔹 ADD THE CHECK HERE
  if (process.env.NODE_ENV === 'test') {
    return;
  }

  const wo: WorkerOptions = _.defaults(
    opt,
    DefaultValue.DEFAULT_WORKER_OPTION
  );

  console.log(`worker option: ${JSON.stringify(wo)}`);
  wo.connection = getRedisConnection();
  const worker = new Worker(opt.queueName, processor, wo);
  
  // Handle errors that occur during job processing, including lock expiration errors
  worker.on('failed', (job, err) => {
    if (err?.message?.includes('Missing lock for job repeat') ||
        err?.message?.includes('not in the delayed state') ||
        err?.message?.includes('is not in the delayed state')) {
      console.warn(`[BullMQ] Repeatable job state issue for ${job?.name || 'unknown'}, this is usually safe to ignore`);
      return;
    }
    console.error(`[BullMQ] Job ${job?.name || 'unknown'} failed:`, err);
  });

  // Handle errors that occur in the worker itself
  worker.on('error', (err) => {
    if (err?.message?.includes('Missing lock for job repeat') ||
        err?.message?.includes('not in the delayed state') ||
        err?.message?.includes('is not in the delayed state')) {
      console.warn(`[BullMQ] Repeatable job state issue in worker, this is usually safe to ignore`);
      return;
    }
    console.error(`[BullMQ] Worker error:`, err);
  });

  this._workers.push(worker);
}


  public async stopAll(): Promise<void> {
    await Promise.all(this._workers.map((w) => w.close()));
    this._workers = []; // let the rest to the GC
  }

  /**
   * Create / return a queue with name
   * @param name - Name of the queue
   * @returns
   */
  public getQueue(name: string): Queue {
    if (!this._queues[name]) {
      // queue not exist create and cache it
      this._queues[name] = new Queue(name, {
        connection: getRedisConnection(),
      });
    }

    return this._queues[name];
  }
}
// function getRedisConnection(): import('bullmq').ConnectionOptions {
//   const redisCnn = {
//       host: 'localhost',
//       port: 6379,
//       // port: 6379,
//   };
//   // const redisCnn = {
//   //   path: "127.0.0.1:6379"
//   // };
//   // return redisCnn;
//   let redis = !!path? new IORedis(path): new IORedis();
// }
