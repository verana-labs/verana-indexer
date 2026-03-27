import { indexerStatusManager } from '../../services/manager/indexer_status.manager';

export interface ErrorInfo {
  isNetworkError: boolean;
  isServerError: boolean;
  isTimeoutError: boolean;
  shouldStopIndexer: boolean;
  errorCode?: string;
  statusCode?: number;
  statusText?: string;
  errorMessage: string;
}


export function analyzeError(error: any): ErrorInfo {
  const errorCode = error?.code || '';
  const statusCode = error?.response?.status;
  const statusText = error?.response?.statusText || '';
  const errorMessage = error?.response?.data?.message ||
    error?.response?.statusText ||
    error?.message ||
    String(error);

  const networkErrorCodes = [
    'EACCES',
    'ECONNREFUSED',
    'ETIMEDOUT',
    'ENOTFOUND',
    'ECONNABORTED',
    'ECONNRESET',
    'ENETUNREACH',
    'EHOSTUNREACH',
    'EPIPE',
    'ECANCELED',
    'EBUSY',
  ];

  const httpErrorCodes = [
    'ERR_BAD_RESPONSE',
    'ERR_BAD_REQUEST',
    'ERR_NETWORK',
    'ERR_INTERNET_DISCONNECTED',
  ];

  const timeoutPatterns = [
    'timeout',
    'exceeded',
    'timed out',
    'ETIMEDOUT',
    'ECONNABORTED',
    'statement timeout',
    'query timeout',
    'canceling statement',
    'Connection terminated unexpectedly',
  ];
  
  const postgresTimeoutCodes = ['57014'];

  const serverErrorPatterns = [
    'Bad Gateway',
    'Service Unavailable',
    'Gateway Timeout',
    'Internal Server Error',
    '502',
    '503',
    '504',
    '500',
  ];

  const isNetworkError =
    networkErrorCodes.includes(errorCode) ||
    httpErrorCodes.includes(errorCode) ||
    timeoutPatterns.some(pattern =>
      errorMessage.toLowerCase().includes(pattern.toLowerCase()) ||
      errorCode.toLowerCase().includes(pattern.toLowerCase())
    );

  const isServerError =
    (statusCode && statusCode >= 500) ||
    statusCode === 502 ||
    statusCode === 503 ||
    statusCode === 504 ||
    serverErrorPatterns.some(pattern =>
      errorMessage.includes(pattern) ||
      statusText.includes(pattern)
    );

  const isTimeoutError =
    errorCode === 'ETIMEDOUT' ||
    errorCode === 'ECONNABORTED' ||
    postgresTimeoutCodes.includes(errorCode) ||
    timeoutPatterns.some(pattern =>
      errorMessage.toLowerCase().includes(pattern.toLowerCase())
    );

  const isNonCritical = errorMessage.toLowerCase().includes('non-critical') ||
    errorMessage.toLowerCase().includes('service will continue');
  const shouldStopIndexer =
    !isNonCritical && (
      isNetworkError ||
      isServerError ||
      isTimeoutError ||
      (statusCode && statusCode >= 500)
    );

  return {
    isNetworkError,
    isServerError,
    isTimeoutError,
    shouldStopIndexer,
    errorCode: errorCode || undefined,
    statusCode,
    statusText: statusText || undefined,
    errorMessage,
  };
}

export function createEnhancedError(error: any, context?: string): Error {
  const errorMessage = error?.message || error?.response?.data?.message || error?.response?.statusText || String(error);
  const errorCode = error?.code || (error?.response?.status ? `HTTP_${error.response.status}` : 'UNKNOWN_ERROR');
  const statusCode = error?.response?.status;
  const statusText = error?.response?.statusText;

  let message = errorMessage;
  if (statusCode) {
    message = `HTTP ${statusCode}: ${message}`;
  }
  if (context) {
    message = `${context} - ${message}`;
  }

  const enhancedError = new Error(message);
  (enhancedError as any).code = errorCode;
  (enhancedError as any).statusCode = statusCode;
  (enhancedError as any).statusText = statusText;
  (enhancedError as any).originalError = error;

  return enhancedError;
}


export async function handleErrorGracefully(
  error: any,
  serviceName: string,
  context?: string,
  stopCrawlingOnly: boolean = false
): Promise<boolean> {
  const errorInfo = analyzeError(error);
  const errorMessage = error?.message || error?.response?.data?.message || error?.response?.statusText || String(error);

  const isNonCritical = errorMessage.toLowerCase().includes('non-critical') ||
    errorMessage.toLowerCase().includes('service will continue');
  
  const isTimeoutError = errorInfo.isTimeoutError || 
    errorMessage.toLowerCase().includes('timeout') ||
    errorMessage.toLowerCase().includes('exceeded') ||
    errorMessage.toLowerCase().includes('timed out') ||
    error?.code === 'ETIMEDOUT' ||
    error?.code === 'ECONNABORTED';

  const logger = (global as any).logger || console;
  
  if (isNonCritical && isTimeoutError) {
    if (logger.warn) {
      logger.warn(`Non-critical timeout error in ${serviceName}: ${errorMessage}. Crawling will continue.`);
    } else {
      console.warn(`Non-critical timeout error in ${serviceName}: ${errorMessage}. Crawling will continue.`);
    }
    return false;
  }

  if (stopCrawlingOnly && !isNonCritical) {
    const enhancedError = createEnhancedError(error, context);
    if (logger.error) {
      logger.error(`Error in ${serviceName}: ${errorMessage}`);
      logger.error('Stopping crawling only - APIs remain available...');
    } else {
      console.error(`Error in ${serviceName}: ${errorMessage}`);
      console.error('Stopping crawling only - APIs remain available...');
    }
    await indexerStatusManager.stopCrawlingOnly(enhancedError, serviceName);
    if (logger.warn) {
      logger.warn(`Crawling stopped. Error details available via /verana/indexer/v1/status API`);
    } else {
      console.warn(`Crawling stopped. Error details available via /verana/indexer/v1/status API`);
    }
    return true;
  }

  if (errorInfo.shouldStopIndexer && !isNonCritical) {
    const enhancedError = createEnhancedError(error, context);
    const logMessage = errorInfo.statusCode
      ? `Error (${errorInfo.errorCode || 'NETWORK_ERROR'} HTTP ${errorInfo.statusCode}): ${errorInfo.errorMessage}`
      : `Error (${errorInfo.errorCode || 'NETWORK_ERROR'}): ${errorInfo.errorMessage}`;
    if (logger.error) {
      logger.error(logMessage);
      logger.error('Stopping indexer gracefully due to error...');
    } else {
      console.error(logMessage);
      console.error('Stopping indexer gracefully due to error...');
    }
    await indexerStatusManager.stopIndexer(enhancedError, serviceName);
    return true;
  }

  if (isNonCritical) {
    if (logger.warn) {
      logger.warn(`Non-critical error in ${serviceName}: ${errorMessage}. Crawling will continue.`);
    } else {
      console.warn(`Non-critical error in ${serviceName}: ${errorMessage}. Crawling will continue.`);
    }
    return false;
  }

  if (logger.warn) {
    logger.warn(`Non-critical error in ${serviceName}: ${errorInfo.errorMessage}`);
  } else {
    console.warn(`Non-critical error in ${serviceName}: ${errorInfo.errorMessage}`);
  }
  return false;
}

export function checkIndexerStatus(): void {
  if (!indexerStatusManager.isIndexerRunning()) {
    const status = indexerStatusManager.getStatus();
    const error = new Error(
      `Indexer is not responding. ${status.stoppedReason || 'Indexer stopped crawling.'} ${status.lastError ? `Error: ${status.lastError.message}` : ''}`
    );
    (error as any).code = 'INDEXER_STOPPED';
    (error as any).status = 503;
    throw error;
  }
}

export function checkCrawlingStatus(): void {
  if (!indexerStatusManager.isCrawlingActive()) {
    const status = indexerStatusManager.getStatus();
    const error = new Error(
      `Crawling is stopped. ${status.stoppedReason || 'Crawling stopped.'} ${status.lastError ? `Error: ${status.lastError.message}` : ''}`
    );
    (error as any).code = 'CRAWLING_STOPPED';
    (error as any).status = 503;
    throw error;
  }
}


