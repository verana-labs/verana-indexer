import { ServiceBroker } from 'moleculer';
import BullableService from '../../base/bullable.service';

export interface BatchProcessorConfig {
  maxConcurrent?: number;
  batchSize?: number;
  delayBetweenBatches?: number;
  isFreshStart?: boolean;
}

export class MessageProcessorBase {
  protected logger: any;
  protected broker: ServiceBroker;
  protected _isFreshStart: boolean = false;

  constructor(service: BullableService) {
    this.logger = service.logger;
    this.broker = service.broker;
  }

  setFreshStartMode(isFreshStart: boolean) {
    this._isFreshStart = isFreshStart;
  }

  protected getBatchConfig(defaultConfig: BatchProcessorConfig): BatchProcessorConfig {
    return {
      maxConcurrent: defaultConfig.maxConcurrent || (this._isFreshStart ? 2 : 5),
      batchSize: defaultConfig.batchSize || (this._isFreshStart ? 5 : 10),
      delayBetweenBatches: defaultConfig.delayBetweenBatches || (this._isFreshStart ? 1000 : 500),
      isFreshStart: this._isFreshStart,
    };
  }

  async processInBatches<T>(
    items: T[],
    processor: (item: T) => Promise<void>,
    config: BatchProcessorConfig = {}
  ): Promise<{ success: number; failed: number }> {
    const batchConfig = this.getBatchConfig(config);
    let successCount = 0;
    let failedCount = 0;

    for (let i = 0; i < items.length; i += batchConfig.batchSize!) {
      const batch = items.slice(i, i + batchConfig.batchSize!);
      
      for (let j = 0; j < batch.length; j += batchConfig.maxConcurrent!) {
        const concurrentChunk = batch.slice(j, j + batchConfig.maxConcurrent!);
        
        const results = await Promise.allSettled(
          concurrentChunk.map(async (item: T) => {
            await processor(item);
          })
        );
        
        for (const result of results) {
          if (result.status === 'fulfilled') {
            successCount++;
          } else {
            failedCount++;
            this.logger.error(`Error processing item:`, result.reason);
          }
        }
      }
      
      if (i + batchConfig.batchSize! < items.length && batchConfig.delayBetweenBatches! > 0) {
        await new Promise<void>(resolve => {
          setTimeout(() => {
            resolve();
          }, batchConfig.delayBetweenBatches!);
        });
      }
    }

    return { success: successCount, failed: failedCount };
  }

  computeChanges(oldData: Record<string, any>, newData: Record<string, any>): Record<string, { old: any; new: any }> {
    const changes: Record<string, { old: any; new: any }> = {};
    for (const key of Object.keys(newData)) {
      if (oldData?.[key] !== newData[key]) {
        changes[key] = { old: oldData?.[key] ?? null, new: newData[key] };
      }
    }
    return changes;
  }
}

