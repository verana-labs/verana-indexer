export function getCrawlSpeedMultiplier(): number {
  const envValue = process.env.CRAWL_SPEED_MULTIPLIER;
  if (!envValue) {
    return 1.0;
  }
  
  const multiplier = parseFloat(envValue);
  if (Number.isNaN(multiplier) || multiplier <= 0) {
    console.warn(`Invalid CRAWL_SPEED_MULTIPLIER value: ${envValue}. Using default 1.0`);
    return 1.0;
  }
  
  return Math.max(0.1, Math.min(10.0, multiplier));
}

export function applySpeedToDelay(baseDelay: number, isReindexing: boolean = false): number {
  const multiplier = getCrawlSpeedMultiplier();
  
  if (isReindexing) {
    return Math.max(50, Math.floor(baseDelay / multiplier));
  }
  return Math.max(100, Math.floor(baseDelay / multiplier));
}

export function applySpeedToBatchSize(baseBatchSize: number, isReindexing: boolean = false): number {
  const multiplier = getCrawlSpeedMultiplier();
  
  if (isReindexing) {
    return Math.max(10, Math.floor(baseBatchSize * multiplier));
  }
  return Math.max(5, Math.floor(baseBatchSize * multiplier));
}

export function getRecommendedMultiplier(isReindexing: boolean): number {
  if (isReindexing) {
    return 2.0;
  }
  return 0.5;
}
