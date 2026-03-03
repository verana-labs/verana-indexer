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
  if (isReindexMode()) {
    await throttleDbPoolIfNeeded(logger);
    return fn();
  }

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

function isReindexMode(): boolean {
  try {
    const mode = (global as any).__indexerStartMode as { isFreshStart?: boolean } | undefined;
    return mode?.isFreshStart === false;
  } catch {
    return false;
  }
}

function getPoolGuardSettings() {
  if (isReindexMode()) {
    return {
      usageOkRatio: 10.0,
      pendingOkAbs: 100000,
      pendingOkRatio: 100.0,
      maxWaitMs: 0,
      stepMs: 25,
      warnEverySteps: 20,
      skipOnTimeout: false,
    };
  }

  return {
    usageOkRatio: 0.7,
    pendingOkAbs: 5,
    pendingOkRatio: 0.08,
    maxWaitMs: 45000,
    stepMs: 500,
    warnEverySteps: 6,
    skipOnTimeout: true,
  };
}

export async function throttleDbPoolIfNeeded(logger?: any): Promise<boolean> {
  const snapshot = getPoolSnapshot();
  if (!snapshot || !snapshot.max || snapshot.max <= 0) {
    return true;
  }

  const settings = getPoolGuardSettings();
  const { used, pending, max } = snapshot;
  const usageOk = used / max < settings.usageOkRatio;
  const pendingOk = pending <= settings.pendingOkAbs || pending / max <= settings.pendingOkRatio;

  if (usageOk && pendingOk) {
    return true;
  }

  let waited = 0;
  while (waited < settings.maxWaitMs) {
    const current = getPoolSnapshot();
    if (!current || !current.max || current.max <= 0) {
      return true;
    }

    const curUsageOk = current.used / current.max < settings.usageOkRatio;
    const curPendingOk = current.pending <= settings.pendingOkAbs || current.pending / current.max <= settings.pendingOkRatio;
    if (curUsageOk && curPendingOk) {
      return true;
    }

    if (
      logger?.warn &&
      waited > 0 &&
      waited % (settings.stepMs * settings.warnEverySteps) === 0
    ) {
      logger.warn(
        `[DB Pool Guard] Waiting for pool (used=${current.used}/${current.max}, pending=${current.pending})…`
      );
    }

    await delay(settings.stepMs);
    waited += settings.stepMs;
  }

  const final = getPoolSnapshot();
  if (logger?.warn && final) {
    logger.warn(
      `[DB Pool Guard] Pool still saturated after ${settings.maxWaitMs}ms (used=${final.used}/${final.max}, pending=${final.pending}). ${
        settings.skipOnTimeout ? 'Skipping this cycle.' : 'Continuing in reindex soft-guard mode.'
      }`
    );
  }
  return !settings.skipOnTimeout;
}

export function isPoolRecovered(): boolean {
  const s = getPoolSnapshot();
  if (!s || !s.max || s.max <= 0) return true;
  const settings = getPoolGuardSettings();
  const usageOk = s.used / s.max < settings.usageOkRatio;
  const pendingOk = s.pending <= settings.pendingOkAbs || s.pending / s.max <= settings.pendingOkRatio;
  return usageOk && pendingOk;
}
