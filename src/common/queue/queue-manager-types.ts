import { JobsOptions } from 'bullmq';

export interface QueueOptions {
  queueName: string;
  jobName?: string;
  jobType?: string;
  // redisUrl?: string;
  // reuseRedis?: boolean;
  prefix?: string;
  attempts?: number;
  concurrency: number;
  backoff: {
    type: string
    delay: number,
  },
}

export type JobOption = JobsOptions;

/* abstract interface to decouple the queue manager from the queue implemenation (bull, bee..) */
export interface QueueProvider {
  stopAll(): void;
  submitJob(
    queueName: string,
    jobName?: string,
    opts?: JobOption,
    payload?: object
  ): void;
  registerQueueHandler(opt: QueueOptions, fn: QueueHandlerFunc): void;
  getQueue(queueName: string): any;
}

export type QueueHandlerFunc = (payload: object) => Promise<void>;

export enum QueueProviderType {
  bullMq = 1,
  bullJs = 2,
  bee = 3,
}
