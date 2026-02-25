type GuardLogger = {
  warn?: (...args: any[]) => void;
  error?: (...args: any[]) => void;
};

const LOOP_MEMORY_CHECK_INTERVAL_MS = 500;
const LOOP_MEMORY_ACTION_THROTTLE_MS = 10000;

function getCriticalHeapMb(): number {
  const parsed = parseInt(process.env.NODE_MEMORY_CRITICAL_MB || '2200', 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return 2200;
  return parsed;
}

function logWarn(logger: GuardLogger | undefined, message: string): void {
  if (logger?.warn) logger.warn(message);
  else console.warn(message);
}

function logError(logger: GuardLogger | undefined, message: string, err: unknown): void {
  if (logger?.error) logger.error(message, err);
  else console.error(message, err);
}

function createCrawlSkipError(message: string): Error {
  const err = new Error(message);
  err.name = 'CrawlSkipError';
  return err;
}

export async function throwIfHeapCriticalDuringCrawl(
  context: string,
  logger?: GuardLogger
): Promise<void> {
  const globalState = global as any;
  const now = Date.now();
  const lastCheckAt = globalState.__loopMemoryGuardLastCheckAt ?? 0;
  if (now - lastCheckAt < LOOP_MEMORY_CHECK_INTERVAL_MS) {
    return;
  }
  globalState.__loopMemoryGuardLastCheckAt = now;

  const criticalHeapMb = getCriticalHeapMb();
  const criticalHeapBytes = criticalHeapMb * 1024 * 1024;
  let heapUsed = process.memoryUsage().heapUsed || 0;

  if (heapUsed < criticalHeapBytes) {
    return;
  }

  if (global.gc) {
    try {
      global.gc();
      heapUsed = process.memoryUsage().heapUsed || 0;
      if (heapUsed < criticalHeapBytes) {
        return;
      }
    } catch {
      // Best-effort only
    }
  }

  const heapMb = (heapUsed / 1024 / 1024).toFixed(0);
  const msg = `[Memory] Critical heap during ${context} (${heapMb} MB >= ${criticalHeapMb} MB). Stopping crawling and exiting current cycle.`;

  const lastActionAt = globalState.__loopMemoryGuardLastActionAt ?? 0;
  if (now - lastActionAt >= LOOP_MEMORY_ACTION_THROTTLE_MS) {
    globalState.__loopMemoryGuardLastActionAt = now;
    try {
      const { indexerStatusManager } = await import('../../services/manager/indexer_status.manager');
      await indexerStatusManager.stopCrawlingOnly(new Error(msg), 'MEMORY');
    } catch (err) {
      logError(logger, '[Memory] Failed to stop crawling from loop guard:', err);
    }
  }

  logWarn(logger, msg);
  throw createCrawlSkipError(msg);
}
