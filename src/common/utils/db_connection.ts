import Knex from "knex";
import { types as pgTypes } from "pg";
import { knexConfig } from "../../knexfile";
import { Config } from "../index";

pgTypes.setTypeParser(20, (val: string) => val === null ? null : Number(val));
pgTypes.setTypeParser(1700, (val: string) => val === null ? null : Number(val));

const environment = process.env.NODE_ENV || 'development';
const cfg = knexConfig[environment];

if (!cfg) {
  throw new Error(`Knex configuration not found for environment: ${environment}`);
}

const knex = Knex(cfg);
(global as any).__dbStorageRawQuery = (sql: string) => knex.raw(sql);

const poolMax = cfg.pool?.max || 10;
const postgresPoolMax = (Config as Record<string, unknown>).POSTGRES_POOL_MAX;
console.log(`[DB Connection] Initialized with pool size: ${poolMax} (env: ${environment}, POSTGRES_POOL_MAX: ${postgresPoolMax || 'not set'})`);

let isShuttingDown = false;

let poolStatusInterval: NodeJS.Timeout | null = null;
let memoryCheckInterval: NodeJS.Timeout | null = null;

async function requestCrawlerPause(message: string, serviceCode: string): Promise<void> {
  const hook = (global as any).__pauseCrawlingHook;
  if (typeof hook !== "function") {
    return;
  }
  await hook(new Error(message), serviceCode);
}

function shouldPauseCrawlerForMemory(): boolean {
  const mode = (global as any).__indexerStartMode as { isFreshStart?: boolean } | undefined;
  if (!mode || typeof mode.isFreshStart !== 'boolean') {
    return true; // fail-safe until start mode is known
  }
  return mode.isFreshStart;
}

function getConfiguredMemoryCriticalHeapMb(): number {
  const parsed = parseInt(process.env.NODE_MEMORY_CRITICAL_MB || '2200', 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return 2200;
  return parsed;
}

if (process.env.NODE_ENV !== 'test') {
  const warningThreshold = Math.floor(poolMax * 0.8);
  let lastWarningTime = 0;
  const WARNING_INTERVAL = 5000;

  const MEMORY_CRITICAL_HEAP_MB = getConfiguredMemoryCriticalHeapMb();
  const MEMORY_CRITICAL_HEAP_BYTES = MEMORY_CRITICAL_HEAP_MB * 1024 * 1024;
  let lastMemoryCriticalAt = 0;
  const MEMORY_CRITICAL_THROTTLE_MS = 45000;
  let lastGcTriggerAt = 0;
  const GC_TRIGGER_HEAP_BYTES = 1.8 * 1024 * 1024 * 1024; // 1.8 GB - trigger GC to try to avoid reaching critical
  const GC_TRIGGER_INTERVAL_MS = 20000;

  const runMemoryGuard = () => {
    const now = Date.now();
    const heapUsed = (process.memoryUsage().heapUsed || 0);
    if (heapUsed >= GC_TRIGGER_HEAP_BYTES && heapUsed < MEMORY_CRITICAL_HEAP_BYTES && (now - lastGcTriggerAt) >= GC_TRIGGER_INTERVAL_MS) {
      lastGcTriggerAt = now;
      if (global.gc) global.gc();
    }
    if (heapUsed >= MEMORY_CRITICAL_HEAP_BYTES && (now - lastMemoryCriticalAt) >= MEMORY_CRITICAL_THROTTLE_MS) {
      lastMemoryCriticalAt = now;
      (async () => {
        try {
          if (global.gc) global.gc();
          const msg = `Heap critical (${(heapUsed / 1024 / 1024).toFixed(0)} MB >= ${MEMORY_CRITICAL_HEAP_MB} MB). Pausing crawling to avoid OOM; will auto-resume when memory recovers.`;
          const logger = (global as any).logger;
          if (!shouldPauseCrawlerForMemory()) {
            if (logger?.warn) {
              logger.warn(`[Memory] CRITICAL heap detected during reindexing; skipping automatic crawl pause (heap ${(heapUsed / 1024 / 1024).toFixed(0)} MB).`);
            } else {
              console.warn('[Memory] CRITICAL heap detected during reindexing; skipping automatic crawl pause.');
            }
            return;
          }
          await requestCrawlerPause(msg, 'MEMORY');
          if (logger?.warn) {
            logger.warn(`[Memory] CRITICAL: pausing crawler to avoid OOM (heap ${(heapUsed / 1024 / 1024).toFixed(0)} MB). Will auto-resume when memory recovers.`);
          } else {
            console.warn(`[Memory] CRITICAL: pausing crawler to avoid OOM. Will auto-resume when memory recovers.`);
          }
        } catch (err) {
          const logger = (global as any).logger;
          if (logger?.error) logger.error('[Memory] Failed to pause crawler:', err);
          else console.error('[Memory] Failed to pause crawler:', err);
        }
      })();
    }
  };

  memoryCheckInterval = setInterval(runMemoryGuard, 2000);

  poolStatusInterval = setInterval(() => {
    runMemoryGuard();
    const now = Date.now();

    if (knex.client && (knex.client as any).pool) {
      const pool = (knex.client as any).pool;
      const usedCount = pool.used?.length || 0;
      const freeCount = pool.free?.length || 0;
      const pendingCount = pool.pendingAcquires?.length || 0;
      (global as any).__dbPoolSnapshot = { used: usedCount, free: freeCount, pending: pendingCount, max: poolMax };

      if ((usedCount >= warningThreshold || pendingCount > 10) &&
          (now - lastWarningTime > WARNING_INTERVAL)) {
        const logger = (global as any).logger;
        const message = `[DB Pool] High usage - Used: ${usedCount}/${poolMax}, Free: ${freeCount}, Pending: ${pendingCount}. Consider increasing POSTGRES_POOL_MAX (current: ${poolMax})`;
        if (logger?.warn) {
          logger.warn(message);
        } else {
          console.warn(message);
        }
        lastWarningTime = now;
      }

      if (pendingCount > 100) {
        const logger = (global as any).logger;
        const CRITICAL_ACTION_THROTTLE_MS = 90000; // only stop crawler once per 90s
        const lastCriticalAt = (global as any).__dbPoolLastCriticalActionAt ?? 0;
        if (now - lastCriticalAt >= CRITICAL_ACTION_THROTTLE_MS) {
          (global as any).__dbPoolLastCriticalActionAt = now;
          (async () => {
            try {
              const msg = `DB pool exhausted (${pendingCount} pending, ${usedCount}/${poolMax} used). Crawling paused until pool drains; will auto-resume when pool recovers.`;
              await requestCrawlerPause(msg, 'DB_POOL');
              if (logger?.warn) {
                logger.warn(`[DB Pool] CRITICAL: pausing crawler until pool drains (pending=${pendingCount}, used=${usedCount}/${poolMax}). Will auto-resume when pool recovers.`);
              } else {
                console.warn(`[DB Pool] CRITICAL: pausing crawler until pool drains. Will auto-resume when pool recovers.`);
              }
            } catch (err) {
              if (logger?.error) logger.error('[DB Pool] Failed to pause crawler:', err);
              else console.error('[DB Pool] Failed to pause crawler:', err);
            }
          })();
        }
      }

      if (pendingCount > 200) {
        const logger = (global as any).logger;
        const message = '[DB Pool] Excessive pending connections detected. Attempting cleanup...';
        if (logger?.warn) {
          logger.warn(message);
        } else {
          console.warn(message);
        }
        
        if (pool.free && pool.free.length > 0) {
          const staleConnections = pool.free.filter((conn: any) => {
            if (!conn || conn.destroyed) return true;
            const lastUsed = conn.lastUsed || 0;
            const now = Date.now();
            return (now - lastUsed) > 30000;
          });
          
          staleConnections.forEach((conn: any) => {
            try {
              if (conn && conn.destroy) {
                conn.destroy();
              }
            } catch (err) {
            }
          });
        }
        
        if (pendingCount > 300 && pool.destroyAllNow) {
          const logger = (global as any).logger;
          const message = '[DB Pool] Forcing emergency pool cleanup';
          if (logger?.error) {
            logger.error(message);
          } else {
            console.error(message);
          }
          try {
            pool.destroyAllNow();
          } catch (err) {
            const errorMsg = '[DB Pool] Error during emergency cleanup:';
            if (logger?.error) {
              logger.error(errorMsg, err);
            } else {
              console.error(errorMsg, err);
            }
          }
        }
      }
    }
  }, 5000);
}

if (process.env.NODE_ENV === 'test') {
  if (poolStatusInterval) {
    clearInterval(poolStatusInterval);
    poolStatusInterval = null;
  }
  if (memoryCheckInterval) {
    clearInterval(memoryCheckInterval);
    memoryCheckInterval = null;
  }
} 

async function gracefulShutdown() {
  if (isShuttingDown) {
    return; 
  }
  isShuttingDown = true;

  console.log('[DB] Shutting down gracefully...');
  
  if (poolStatusInterval) {
    clearInterval(poolStatusInterval);
    poolStatusInterval = null;
  }
  if (memoryCheckInterval) {
    clearInterval(memoryCheckInterval);
    memoryCheckInterval = null;
  }

  const maxWaitTime = 10000; 
  const checkInterval = 500; 
  let waited = 0;

  while (waited < maxWaitTime) {
    if (knex.client && (knex.client as any).pool) {
      const pool = (knex.client as any).pool;
      const usedCount = pool.used?.length || 0;
      const pendingCount = pool.pendingAcquires?.length || 0;

      if (usedCount === 0 && pendingCount === 0) {
        console.log(`[DB] All connections released after ${waited}ms`);
        break;
      }

      if (waited % 2000 === 0) { 
        console.log(`[DB] Waiting for connections to be released... Used: ${usedCount}, Pending: ${pendingCount}`);
      }
    }

    await new Promise<void>(resolve => {
      setTimeout(() => {
        resolve();
      }, checkInterval);
    });
    waited += checkInterval;
  }

  if (knex.client && (knex.client as any).pool) {
    const pool = (knex.client as any).pool;
    const usedCount = pool.used?.length || 0;
    const pendingCount = pool.pendingAcquires?.length || 0;
    
    if (usedCount > 0 || pendingCount > 0) {
      console.warn(`[DB] Shutting down with ${usedCount} used connections and ${pendingCount} pending acquires`);
    }
  }

  try {
    await knex.destroy();
    console.log('[DB] Connection pool destroyed successfully');
  } catch (error) {
    console.error('[DB] Error destroying connection pool:', error);
  }
}

process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);

export default knex;
