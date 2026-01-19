import { delay } from './db_query_helper';

export interface BatchProcessorOptions {
  maxConcurrent?: number;
  batchSize?: number;
  delayBetweenBatches?: number;
  throttleDelay?: number;
  onProgress?: (processed: number, total: number) => void;
  logger?: any;
}

export class BatchProcessor {
  private logger?: any;

  constructor(logger?: any) {
    this.logger = logger;
  }

  async processInBatches<T, R = void>(
    items: T[],
    processor: (item: T, index: number) => Promise<R>,
    options: BatchProcessorOptions = {}
  ): Promise<{ success: number; failed: number; results: R[] }> {
    const {
      maxConcurrent = 5,
      batchSize = 10,
      delayBetweenBatches = 100,
      throttleDelay = 50,
      onProgress,
      logger = this.logger
    } = options;

    let successCount = 0;
    let failedCount = 0;
    const results: R[] = [];

    for (let i = 0; i < items.length; i += batchSize) {
      const batch = items.slice(i, i + batchSize);
      
      for (let j = 0; j < batch.length; j += maxConcurrent) {
        const concurrentChunk = batch.slice(j, j + maxConcurrent);
        
        const chunkResults = await Promise.allSettled(
          concurrentChunk.map(async (item, chunkIndex) => {
            const globalIndex = i + j + chunkIndex;
            return await processor(item, globalIndex);
          })
        );
        
        for (const result of chunkResults) {
          if (result.status === 'fulfilled') {
            successCount++;
            results.push(result.value);
          } else {
            failedCount++;
            if (logger?.error) {
              logger.error('Error processing item:', result.reason);
            }
          }
        }

        if (onProgress) {
          onProgress(i + j + concurrentChunk.length, items.length);
        }

        if (throttleDelay > 0 && j + maxConcurrent < batch.length) {
          await this.throttle(throttleDelay);
        }
      }
      
      if (i + batchSize < items.length && delayBetweenBatches > 0) {
        await this.throttle(delayBetweenBatches);
      }
    }

    return { success: successCount, failed: failedCount, results };
  }

  private async throttle(ms: number): Promise<void> {
    return delay(ms);
  }

  static async processWithRetry<T>(
    fn: () => Promise<T>,
    maxRetries: number = 3,
    retryDelay: number = 1000,
    shouldRetry?: (error: any) => boolean,
    logger?: any
  ): Promise<T> {
    let lastError: any;
    
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await fn();
      } catch (error: any) {
        lastError = error;
        
        if (shouldRetry && !shouldRetry(error)) {
          throw error;
        }
        
        if (attempt < maxRetries) {
          const backoffDelay = retryDelay * (2 ** (attempt - 1));
          if (logger?.warn) {
            logger.warn(`Attempt ${attempt}/${maxRetries} failed, retrying in ${backoffDelay}ms:`, error?.message || error);
          }
          await delay(backoffDelay);
        }
      }
    }
    
    throw lastError;
  }
}

