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

function clampInt(value: number, min: number, max: number): number {
  if (!Number.isFinite(value)) return min;
  return Math.min(max, Math.max(min, Math.floor(value)));
}

function toPositiveInt(value: number, fallback: number): number {
  if (!Number.isFinite(value) || value <= 0) return fallback;
  return Math.floor(value);
}

const NORMAL_SPEED_MULTIPLIER = parseMultiplier(
  process.env.CRAWL_SPEED_MULTIPLIER,
  5,
  20
);

const REINDEX_SPEED_MULTIPLIER = parseMultiplier(
  process.env.CRAWL_SPEED_MULTIPLIER_REINDEX,
  80,
  1000
);

// Fresh mode stays conservative.
const FRESH_MANUAL_MULTIPLIER = 2;
const FRESH_EFFECTIVE_MULTIPLIER_CAP = 10;
const FRESH_MIN_DELAY_MS = 5;
const FRESH_MAX_BATCH_SIZE = 600;
const FRESH_MAX_CONCURRENCY = 40;

// Reindex mode uses direct aggressive scaling with bounded caps.
const REINDEX_MANUAL_MULTIPLIER = 2;
const REINDEX_EFFECTIVE_MULTIPLIER_CAP = 1000;
const REINDEX_MIN_DELAY_MS = 1;
const REINDEX_MAX_BATCH_SIZE = 5000;
const REINDEX_MAX_CONCURRENCY = 1000;

const FRESH_EFFECTIVE_MULTIPLIER = Math.min(
  NORMAL_SPEED_MULTIPLIER * FRESH_MANUAL_MULTIPLIER,
  FRESH_EFFECTIVE_MULTIPLIER_CAP
);

const REINDEX_EFFECTIVE_MULTIPLIER = Math.min(
  REINDEX_SPEED_MULTIPLIER * REINDEX_MANUAL_MULTIPLIER,
  REINDEX_EFFECTIVE_MULTIPLIER_CAP
);

export function getCrawlSpeedMultiplier(isReindexing: boolean = false): number {
  return isReindexing ? REINDEX_EFFECTIVE_MULTIPLIER : FRESH_EFFECTIVE_MULTIPLIER;
}

export function applySpeedToDelay(
  baseDelay: number,
  isReindexing: boolean = false
): number {
  const safeBaseDelay = toPositiveInt(baseDelay, isReindexing ? 100 : 1000);

  if (isReindexing) {
    return Math.max(
      REINDEX_MIN_DELAY_MS,
      Math.floor(safeBaseDelay / REINDEX_EFFECTIVE_MULTIPLIER)
    );
  }

  return Math.max(
    FRESH_MIN_DELAY_MS,
    Math.floor(safeBaseDelay / FRESH_EFFECTIVE_MULTIPLIER)
  );
}

export function applySpeedToBatchSize(
  baseBatchSize: number,
  isReindexing: boolean = false
): number {
  const safeBaseBatchSize = toPositiveInt(baseBatchSize, 10);

  if (isReindexing) {
    return Math.min(
      REINDEX_MAX_BATCH_SIZE,
      Math.max(5, safeBaseBatchSize * REINDEX_EFFECTIVE_MULTIPLIER)
    );
  }

  return Math.min(
    FRESH_MAX_BATCH_SIZE,
    Math.max(5, safeBaseBatchSize * FRESH_EFFECTIVE_MULTIPLIER)
  );
}

export function getRecommendedConcurrency(
  baseConcurrency: number = 5,
  isReindexing: boolean = false
): number {
  const safeBaseConcurrency = toPositiveInt(baseConcurrency, 1);

  if (isReindexing) {
    return Math.min(
      REINDEX_MAX_CONCURRENCY,
      Math.max(2, safeBaseConcurrency * REINDEX_EFFECTIVE_MULTIPLIER)
    );
  }

  return Math.min(
    FRESH_MAX_CONCURRENCY,
    Math.max(2, safeBaseConcurrency * FRESH_EFFECTIVE_MULTIPLIER)
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
