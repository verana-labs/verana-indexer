/* eslint-disable max-classes-per-file */
import { Job, Queue, Worker, WorkerOptions } from 'bullmq';
import _ from 'underscore';
import { JobOption, QueueOptions, QueueProvider } from './queue-manager-types';
import { getRedisConnection } from './redis-connector';

type LoggerLike = {
  info?: (...args: unknown[]) => void;
  warn?: (...args: unknown[]) => void;
  error?: (...args: unknown[]) => void;
  debug?: (...args: unknown[]) => void;
};

class DefaultValue {
  static readonly DEFAULT_JOB_NAME = '_default_bull_job';

  static readonly DEFAULT_WORKER_OPTION: WorkerOptions = {
    concurrency: 1,
    // Give long-running jobs enough time to finish without losing the lock
    lockDuration:  300000,
    lockRenewTime:  60000,
    stalledInterval:  30000,
    maxStalledCount:  1,
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
  public constructor(private readonly logger?: LoggerLike) {}

  private _queues: Record<string, Queue> = {};

  private _workers: Worker[] = [];

  private getLogger(): LoggerLike | undefined {
    return this.logger ?? ((global as any).logger as LoggerLike | undefined);
  }

  private log(level: keyof LoggerLike, ...args: unknown[]): void {
    const logger = this.getLogger();
    const fn = logger?.[level];
    if (typeof fn === 'function') {
      fn(...args);
    }
  }

  public submitJob(
    queueName: string,
    jobName: string,
    opts?: JobOption,
    payload?: object
  ): void {
    const q = this.getQueue(queueName);
    q.add(jobName, payload, opts).catch((err: unknown) => {
      this.log(
        'error',
        `Failed to add BullMQ job "${jobName}" to queue "${queueName}"`,
        err
      );
    });
  }

 public registerQueueHandler(
  opt: QueueOptions,
  fn: (payload: object) => Promise<void>
): void {
  // create a new worker to handle the job
  const processor = async (job: Job) => {
    try {
      await fn(job.data);
    } catch (e) {
      this.log('error', `job ${job.name} failed`);
      this.log('error', e);
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

  this.log('info', `worker option: ${JSON.stringify(wo)}`);
  wo.connection = getRedisConnection();
  const worker = new Worker(opt.queueName, processor, wo);

  worker.on('error', (err: Error) => {
    const msg = err?.message ?? String(err);
    if (typeof msg === 'string' && msg.includes('Missing key for job')) {
      this.log('warn', 'BullMQ job key already removed (repeat/delayed):', msg);
    } else {
      this.log('error', `worker ${opt.queueName} error:`, err);
    }
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
      if (process.env.NODE_ENV === 'test') {
        this._queues[name] = {
          add: async () => ({ id: 'mock-job-id' }),
          getRepeatableJobs: async () => [],
          removeRepeatableByKey: async () => true,
          isPaused: async () => false,
          resume: async () => {},
          pause: async () => {},
          close: async () => {},
        } as any;
      } else {
        // queue not exist create and cache it
        this._queues[name] = new Queue(name, {
          connection: getRedisConnection(),
          prefix: 'bull',
        });
      }
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
