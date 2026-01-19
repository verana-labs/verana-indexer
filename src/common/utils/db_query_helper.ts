export interface QueryOptions {
  timeout?: number;
  retries?: number;
  retryDelay?: number;
}

const DEFAULT_QUERY_TIMEOUT = 30000;
const DEFAULT_RETRIES = 3;
const DEFAULT_RETRY_DELAY = 1000;

export function isStatementTimeoutError(error: any): boolean {
  const errorCode = error?.code;
  const errorMessage = error?.message || String(error);
  
  return errorCode === '57014' ||
    errorMessage.includes('statement timeout') ||
    errorMessage.includes('canceling statement') ||
    errorMessage.includes('query timeout');
}

export function isPoolExhaustionError(error: any): boolean {
  const errorCode = error?.code;
  const errorMessage = error?.message || String(error);
  
  return errorCode === 'ECONNREFUSED' ||
    errorMessage.includes('timeout acquiring connection') ||
    errorMessage.includes('pool is full') ||
    errorMessage.includes('connection pool exhausted');
}

export function delay(ms: number): Promise<void> {
  return new Promise(resolve => {
    setTimeout(resolve, ms);
  });
}

export async function executeWithRetry<T>(
  queryFn: () => Promise<T>,
  options: QueryOptions = {},
  logger?: any
): Promise<T> {
  const timeout = options.timeout || DEFAULT_QUERY_TIMEOUT;
  const maxRetries = options.retries || DEFAULT_RETRIES;
  const retryDelay = options.retryDelay || DEFAULT_RETRY_DELAY;
  
  let lastError: any;
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const queryPromise = queryFn();
      const timeoutPromise = new Promise<never>((_, reject) => {
        setTimeout(() => {
          reject(new Error(`Query timeout after ${timeout}ms`));
        }, timeout);
      });
      
      return await Promise.race([queryPromise, timeoutPromise]);
    } catch (error: any) {
      lastError = error;
      
      if (isStatementTimeoutError(error)) {
        if (logger) {
          logger.warn(`Query timeout (attempt ${attempt}/${maxRetries}): ${error?.message || error}`);
        }
        
        if (attempt < maxRetries) {
          const backoffDelay = retryDelay * (2 ** (attempt - 1));
          if (logger) {
            logger.info(`Retrying query after ${backoffDelay}ms...`);
          }
          await delay(backoffDelay);
          continue;
        }
      } else if (isPoolExhaustionError(error)) {
        if (logger) {
          logger.warn(`Pool exhaustion (attempt ${attempt}/${maxRetries}): ${error?.message || error}`);
        }
        
        if (attempt < maxRetries) {
          const backoffDelay = retryDelay * (2 ** (attempt - 1));
          if (logger) {
            logger.info(`Waiting for pool availability, retrying after ${backoffDelay}ms...`);
          }
          await delay(backoffDelay);
          continue;
        }
      } else {
        throw error;
      }
    }
  }
  
  throw lastError;
}

export function withQueryTimeout<T>(
  queryBuilder: any,
  timeout: number = DEFAULT_QUERY_TIMEOUT
): any {
  return queryBuilder.timeout(timeout);
}

export async function throttleBetweenBatches(
  delayMs: number = 100,
  logger?: any
): Promise<void> {
  if (delayMs > 0) {
    if (logger?.debug) {
      logger.debug(`Throttling batch processing for ${delayMs}ms to reduce DB pool pressure`);
    }
    await delay(delayMs);
  }
}

export function createQueryWithTimeout<T>(
  queryFn: () => Promise<T>,
  timeout: number = DEFAULT_QUERY_TIMEOUT,
  logger?: any
): Promise<T> {
  return executeWithRetry(
    () => Promise.race([
      queryFn(),
      new Promise<never>((_, reject) => {
        setTimeout(() => {
          reject(new Error(`Query timeout after ${timeout}ms`));
        }, timeout);
      })
    ]),
    { timeout, retries: 1 },
    logger
  );
}

export function shouldRetryQuery(error: any): boolean {
  return isStatementTimeoutError(error) || isPoolExhaustionError(error);
}

export async function queryWithAutoRetry<T>(
  queryFn: () => Promise<T>,
  options: QueryOptions = {},
  logger?: any
): Promise<T> {
  return executeWithRetry(queryFn, options, logger);
}

