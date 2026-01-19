import { delay } from './db_query_helper';

export class PerformanceUtils {
  static async measureTime<T>(
    fn: () => Promise<T>,
    label: string,
    logger?: any
  ): Promise<T> {
    const start = Date.now();
    try {
      const result = await fn();
      const duration = Date.now() - start;
      if (logger?.debug) {
        logger.debug(`[Performance] ${label} took ${duration}ms`);
      }
      return result;
    } catch (error) {
      const duration = Date.now() - start;
      if (logger?.warn) {
        logger.warn(`[Performance] ${label} failed after ${duration}ms:`, error);
      }
      throw error;
    }
  }

  static createThrottle(ms: number): () => Promise<void> {
    let lastCall = 0;
    return async () => {
      const now = Date.now();
      const timeSinceLastCall = now - lastCall;
      if (timeSinceLastCall < ms) {
        await delay(ms - timeSinceLastCall);
      }
      lastCall = Date.now();
    };
  }

  static async withTimeout<T>(
    promise: Promise<T>,
    timeoutMs: number,
    errorMessage?: string
  ): Promise<T> {
    return Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        delay(timeoutMs).then(() => {
          reject(new Error(errorMessage || `Operation timed out after ${timeoutMs}ms`));
        });
      })
    ]);
  }
}

