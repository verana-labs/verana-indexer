import v8 from 'v8';
import path from 'path';
import fs from 'fs';

type GuardLogger = {
  warn?: (...args: any[]) => void;
  error?: (...args: any[]) => void;
  info?: (...args: any[]) => void;
};

const LOOP_MEMORY_CHECK_INTERVAL_MS = 500;
const LOOP_MEMORY_ACTION_THROTTLE_MS = 10000;
const HEAP_SNAPSHOT_MIN_INTERVAL_MS = 300000; // Max one snapshot every 5 minutes

function getCriticalHeapMb(): number {
  const parsed = parseInt(process.env.NODE_MEMORY_CRITICAL_MB || '2200', 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return 2200;
  return parsed;
}

function logWarn(logger: GuardLogger | undefined, message: string): void {
  if (logger?.warn) logger.warn(message);
  else console.warn(message);
}

function logInfo(logger: GuardLogger | undefined, message: string): void {
  if (logger?.info) logger.info(message);
  else console.log(message);
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

function logDetailedMemoryBreakdown(logger: GuardLogger | undefined): void {
  const mem = process.memoryUsage();
  const format = (bytes: number) => (bytes / 1024 / 1024).toFixed(1);
  logWarn(logger,
    `[Memory Breakdown] rss=${format(mem.rss)}MB heapTotal=${format(mem.heapTotal)}MB ` +
    `heapUsed=${format(mem.heapUsed)}MB external=${format(mem.external)}MB ` +
    `arrayBuffers=${format(mem.arrayBuffers)}MB`
  );

  try {
    const heapStats = v8.getHeapStatistics();
    logWarn(logger,
      `[Heap Stats] totalHeapSize=${format(heapStats.total_heap_size)}MB ` +
      `usedHeapSize=${format(heapStats.used_heap_size)}MB ` +
      `heapSizeLimit=${format(heapStats.heap_size_limit)}MB ` +
      `mallocedMemory=${format(heapStats.malloced_memory)}MB ` +
      `peakMallocedMemory=${format(heapStats.peak_malloced_memory)}MB ` +
      `numberOfNativeContexts=${heapStats.number_of_native_contexts} ` +
      `numberOfDetachedContexts=${heapStats.number_of_detached_contexts}`
    );
  } catch {
    // best effort
  }

  try {
    const spaces = v8.getHeapSpaceStatistics();
    const significant = spaces.filter(s => s.space_used_size > 1024 * 1024);
    for (const space of significant) {
      logWarn(logger,
        `[Heap Space] ${space.space_name}: used=${format(space.space_used_size)}MB ` +
        `size=${format(space.space_size)}MB available=${format(space.space_available_size)}MB`
      );
    }
  } catch {
    // best effort
  }
}

function tryWriteHeapSnapshot(logger: GuardLogger | undefined, context: string): void {
  if (process.env.HEAP_SNAPSHOT_ON_OOM !== '1') return;

  const globalState = global as any;
  const now = Date.now();
  const lastSnapshot = globalState.__lastHeapSnapshotAt ?? 0;
  if (now - lastSnapshot < HEAP_SNAPSHOT_MIN_INTERVAL_MS) return;
  globalState.__lastHeapSnapshotAt = now;

  try {
    const snapshotDir = process.env.HEAP_SNAPSHOT_DIR || '/tmp';
    if (!fs.existsSync(snapshotDir)) {
      fs.mkdirSync(snapshotDir, { recursive: true });
    }
    const filename = `heap-${Date.now()}-${context.replace(/[^a-zA-Z0-9_-]/g, '_')}.heapsnapshot`;
    const filepath = path.join(snapshotDir, filename);
    logInfo(logger, `[Memory] Writing heap snapshot to ${filepath}...`);
    v8.writeHeapSnapshot(filepath);
    logInfo(logger, `[Memory] Heap snapshot written: ${filepath}`);
  } catch (err) {
    logError(logger, '[Memory] Failed to write heap snapshot:', err);
  }
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

  logDetailedMemoryBreakdown(logger);
  tryWriteHeapSnapshot(logger, context);

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
