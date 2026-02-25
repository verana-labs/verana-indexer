import { delay } from './db_query_helper';

export interface DbPoolSnapshot {
  used: number;
  free: number;
  pending: number;
  max: number;
}

export class CrawlSkipError extends Error {
  constructor() {
    super('Crawl cycle skipped: DB pool saturated');
    this.name = 'CrawlSkipError';
  }
}

let crawlLockHeld = false;
const crawlLockQueue: Array<{ run: () => void }> = [];

async function acquireCrawlLock(): Promise<void> {
  if (!crawlLockHeld) {
    crawlLockHeld = true;
    return;
  }
  await new Promise<void>((resolve) => {
    crawlLockQueue.push({
      run: () => {
        crawlLockHeld = true;
        resolve();
      },
    });
  });
}

function releaseCrawlLock(): void {
  if (crawlLockQueue.length > 0) {
    const next = crawlLockQueue.shift();
    if (next) next.run();
  } else {
    crawlLockHeld = false;
  }
}

export async function runWithCrawlLock<T>(
  fn: () => Promise<T>,
  logger?: any
): Promise<T> {
  if (!(await throttleDbPoolIfNeeded(logger))) {
    throw new CrawlSkipError();
  }
  await acquireCrawlLock();
  try {
    return await fn();
  } finally {
    releaseCrawlLock();
  }
}

function getPoolSnapshot(): DbPoolSnapshot | null {
  try {
    const snapshot = (global as any).__dbPoolSnapshot;
    if (!snapshot || typeof snapshot !== 'object') {
      return null;
    }
    const used = Number(snapshot.used ?? 0);
    const free = Number(snapshot.free ?? 0);
    const pending = Number(snapshot.pending ?? 0);
    const max = Number(snapshot.max ?? 0);

    return {
      used,
      free,
      pending,
      max,
    };
  } catch {
    return null;
  }
}

const USAGE_OK_RATIO = 0.7;
const PENDING_OK_ABS = 5;
const PENDING_OK_RATIO = 0.08;
const MAX_WAIT_MS = 45000;
const STEP_MS = 500;

export async function throttleDbPoolIfNeeded(logger?: any): Promise<boolean> {
  const snapshot = getPoolSnapshot();
  if (!snapshot || !snapshot.max || snapshot.max <= 0) {
    return true;
  }

  const { used, pending, max } = snapshot;
  const usageOk = used / max < USAGE_OK_RATIO;
  const pendingOk = pending <= PENDING_OK_ABS || pending / max <= PENDING_OK_RATIO;

  if (usageOk && pendingOk) {
    return true;
  }

  let waited = 0;
  while (waited < MAX_WAIT_MS) {
    const current = getPoolSnapshot();
    if (!current || !current.max || current.max <= 0) {
      return true;
    }

    const curUsageOk = current.used / current.max < USAGE_OK_RATIO;
    const curPendingOk = current.pending <= PENDING_OK_ABS || current.pending / current.max <= PENDING_OK_RATIO;
    if (curUsageOk && curPendingOk) {
      return true;
    }

    if (logger?.warn && waited % (STEP_MS * 6) === 0 && waited > 0) {
      logger.warn(
        `[DB Pool Guard] Waiting for pool (used=${current.used}/${current.max}, pending=${current.pending})…`
      );
    }

    await delay(STEP_MS);
    waited += STEP_MS;
  }

  const final = getPoolSnapshot();
  if (logger?.warn && final) {
    logger.warn(
      `[DB Pool Guard] Pool still saturated after ${MAX_WAIT_MS}ms (used=${final.used}/${final.max}, pending=${final.pending}). Skipping this cycle.`
    );
  }
  return false;
}

export function isPoolRecovered(): boolean {
  const s = getPoolSnapshot();
  if (!s || !s.max || s.max <= 0) return true;
  const usageOk = s.used / s.max < USAGE_OK_RATIO;
  const pendingOk = s.pending <= PENDING_OK_ABS || s.pending / s.max <= PENDING_OK_RATIO;
  return usageOk && pendingOk;
}

