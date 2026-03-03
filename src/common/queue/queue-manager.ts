/* eslint-disable max-classes-per-file */

/* eslint-disable no-console */
/* eslint-disable func-names */
/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable @typescript-eslint/no-explicit-any */
import { JobsOptions } from 'bullmq';
import { BeeQueueProvider } from './bee-provider';
import { BullJsProvider } from './bulljs-provider';
import { BullQueueProvider } from './bullmq-provider';
import {
  QueueOptions,
  QueueProvider,
  QueueProviderType,
} from './queue-manager-types';

export default class QueueManager {
  private _queueProvider: QueueProvider;
  private _logger?: any;

  // prefix?: string = 'bull';
  private _handlerOwner?: any;

  constructor(provider: QueueProvider, logger?: any) {
    // this._name = name;
    this._queueProvider = provider;
    this._logger = logger;
  }

  /**
   * factory method to create queue manager
   * @param type - type of provider, default is bullmq
   * @returns
   */
  public static getInstance(type?: QueueProviderType, logger?: any) {
    let provider: QueueProvider;

    switch (type) {
      case QueueProviderType.bullMq:
        provider = new BullQueueProvider(logger);
        break;
      case QueueProviderType.bullJs:
        provider = new BullJsProvider();
        break;
      case QueueProviderType.bee:
        provider = new BeeQueueProvider();
        break;
      default:
        provider = new BullQueueProvider(logger);
    }

    return new QueueManager(provider, logger);
  }

  /**
   * create a bull queue and
   * register a handler for a this one
   */
  public async registerQueueHandler(
    qOpt: QueueOptions,
    fn: (payload: object) => Promise<void>
  ) {
    // bind the owner, so 'this' can be accessed in the handler function
    const f = async (payload: object) => {
      const func = this._handlerOwner ? fn.bind(this._handlerOwner) : fn;
      await func(payload);
    };
    // register the handler
    this._queueProvider.registerQueueHandler(qOpt, f);
  }

  /**
   * bind owner for the queue handler. the bound object will be refered as "this" in the queue handler
   * @param _thisObject -
   */
  public bindQueueOwner(_thisObject: any) {
    this._handlerOwner = _thisObject;
    if (!this._logger && _thisObject?.logger) {
      this._logger = _thisObject.logger;
    }
  }

  /**
   * submit a job to a queue
   * @param queueName -
   * @param jobName -
   * @param opts -
   * @param payload - data send to the queue handler
   * @returns
   */
  public async createJob(
    queueName: string,
    jobName?: string,
    opts?: JobsOptions,
    payload?: object
  ): Promise<void> {
    const sanitizedOpts = this.sanitizeJobOptions(opts, queueName, jobName);
    // prepare some input settings if not specified by user
    // jobName = jobName ?? DEFAULT_JOB_NAME;
    // const jobOptions = _.defaults(opts, DEFAULT_JOB_OTION);

    // call to the middleware to submit job
    this._queueProvider.submitJob(queueName, jobName, sanitizedOpts, payload);
  }

  private sanitizeJobOptions(
    opts?: JobsOptions,
    queueName?: string,
    jobName?: string
  ): JobsOptions | undefined {
    if (!opts) return opts;

    const normalized: JobsOptions = { ...opts };
    const repeat = (normalized as any).repeat;
    if (!repeat || typeof repeat !== 'object') {
      return normalized;
    }

    const repeatNormalized: any = { ...repeat };

    if (typeof repeatNormalized.every === 'string') {
      const trimmed = repeatNormalized.every.trim();
      if (/^\d+$/.test(trimmed)) {
        repeatNormalized.every = Number(trimmed);
      } else if (trimmed === '' || /^(undefined|null|nan)$/i.test(trimmed)) {
        delete repeatNormalized.every;
      }
    }
    if (
      repeatNormalized.every !== undefined &&
      !Number.isFinite(repeatNormalized.every)
    ) {
      delete repeatNormalized.every;
    }
    if (repeatNormalized.every !== undefined) {
      repeatNormalized.every = Math.max(1, Math.floor(Number(repeatNormalized.every)));
    }

    const sanitizeCronLikeField = (field: 'pattern' | 'cron') => {
      const value = repeatNormalized[field];
      if (typeof value !== 'string') return;
      const trimmed = value.trim();
      if (
        trimmed.length === 0 ||
        /^(undefined|null|nan)$/i.test(trimmed) ||
        /\bundefined\b/i.test(trimmed)
      ) {
        delete repeatNormalized[field];
      } else {
        repeatNormalized[field] = trimmed;
      }
    };

    sanitizeCronLikeField('pattern');
    sanitizeCronLikeField('cron');

    if (
      repeatNormalized.every === undefined &&
      repeatNormalized.pattern === undefined &&
      repeatNormalized.cron === undefined
    ) {
      delete (normalized as any).repeat;
      this._logger?.warn?.(
        `[QueueManager] Dropped invalid repeat config for queue="${queueName || ''}" job="${jobName || ''}" (no valid every/pattern/cron after sanitization).`
      );
      return normalized;
    }

    (normalized as any).repeat = repeatNormalized;
    return normalized;
  }

  public stopAll(): void {
    this._queueProvider.stopAll();
  }

  public getQueue(queueName: string): any {
    return this._queueProvider.getQueue(queueName);
  }
}
