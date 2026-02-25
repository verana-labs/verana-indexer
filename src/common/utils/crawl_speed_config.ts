function parseMultiplier(
  rawValue: string | undefined,
  fallback: number,
  maxValue: number = 100
): number {
  if (!rawValue) return fallback;
  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.min(parsed, maxValue);
}

const NORMAL_SPEED_MULTIPLIER = parseMultiplier(
  process.env.CRAWL_SPEED_MULTIPLIER,
  8
);

const REINDEX_SPEED_MULTIPLIER = parseMultiplier(
  process.env.CRAWL_SPEED_MULTIPLIER_REINDEX,
  20,
  100
);

const NORMAL_MANUAL_MULTIPLIER = 4;
const FRESH_SPEED_FACTOR = 2;

const REINDEX_MANUAL_MULTIPLIER = 20;
const REINDEX_SPEED_FACTOR = 3;

const FRESH_MULTIPLIER_CAP = 40;
const REINDEX_EFFECTIVE_MULTIPLIER_CAP = 300;

const FRESH_MAX_BATCH_SIZE = 2000;
const REINDEX_MAX_BATCH_SIZE = 5000;

const FRESH_MAX_CONCURRENCY = 120;
const REINDEX_MAX_CONCURRENCY = 300;

function clampInt(value: number, min: number, max: number): number {
  if (!Number.isFinite(value)) return min;
  return Math.min(max, Math.max(min, Math.floor(value)));
}

function normalizeBase(value: number, fallback: number): number {
  if (!Number.isFinite(value) || value <= 0) return fallback;
  return value;
}

export function getCrawlSpeedMultiplier(isReindexing: boolean = false): number {
  if (isReindexing) {
    return Math.min(
      REINDEX_SPEED_MULTIPLIER * REINDEX_MANUAL_MULTIPLIER,
      REINDEX_EFFECTIVE_MULTIPLIER_CAP
    );
  }

  return Math.min(
    NORMAL_SPEED_MULTIPLIER * NORMAL_MANUAL_MULTIPLIER,
    FRESH_MULTIPLIER_CAP
  );
}

function getEffectiveMultiplier(isReindexing: boolean): number {
  return getCrawlSpeedMultiplier(isReindexing);
}

function getEffectiveFactor(isReindexing: boolean): number {
  return isReindexing ? REINDEX_SPEED_FACTOR : FRESH_SPEED_FACTOR;
}

export function applySpeedToDelay(
  baseDelay: number,
  isReindexing: boolean = false
): number {
  const multiplier = getEffectiveMultiplier(isReindexing);
  const factor = getEffectiveFactor(isReindexing);
  const safeBaseDelay = normalizeBase(baseDelay, 200);
  const divisor = Math.max(1, multiplier * factor);
  return Math.max(0, Math.floor(safeBaseDelay / divisor));
}

export function applySpeedToBatchSize(
  baseBatchSize: number,
  isReindexing: boolean = false
): number {
  const multiplier = getEffectiveMultiplier(isReindexing);
  const factor = getEffectiveFactor(isReindexing);
  const safeBaseBatchSize = normalizeBase(baseBatchSize, 50);
  const maxBatchSize = isReindexing ? REINDEX_MAX_BATCH_SIZE : FRESH_MAX_BATCH_SIZE;

  return Math.min(
    maxBatchSize,
    Math.max(20, Math.floor(safeBaseBatchSize * multiplier * factor))
  );
}

export function getRecommendedConcurrency(
  baseConcurrency: number = 10,
  isReindexing: boolean = false
): number {
  const multiplier = getEffectiveMultiplier(isReindexing);
  const factor = getEffectiveFactor(isReindexing);
  const safeBaseConcurrency = normalizeBase(baseConcurrency, 4);
  const maxConcurrency = isReindexing ? REINDEX_MAX_CONCURRENCY : FRESH_MAX_CONCURRENCY;

  return Math.min(
    maxConcurrency,
    Math.max(8, Math.floor(safeBaseConcurrency * multiplier * factor))
  );
}

export async function runWithConcurrency<T>(
  items: T[],
  handler: (item: T) => Promise<void>,
  concurrency: number
): Promise<void> {
  const workerCount = clampInt(concurrency, 1, REINDEX_MAX_CONCURRENCY);
  let index = 0;

  const workers = Array.from({ length: workerCount }, async () => {
    while (true) {
      const current = index++;
      if (current >= items.length) break;
      await handler(items[current]);
    }
  });

  await Promise.all(workers);
}