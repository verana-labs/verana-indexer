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

const poolMax = cfg.pool?.max || 10;
const postgresPoolMax = (Config as Record<string, unknown>).POSTGRES_POOL_MAX;
console.log(`[DB Connection] Initialized with pool size: ${poolMax} (env: ${environment}, POSTGRES_POOL_MAX: ${postgresPoolMax || 'not set'})`);

let isShuttingDown = false;

let poolStatusInterval: NodeJS.Timeout | null = null;

if (process.env.NODE_ENV !== 'test') {
  const warningThreshold = Math.floor(poolMax * 0.8);
  let lastWarningTime = 0;
  const WARNING_INTERVAL = 5000;

  poolStatusInterval = setInterval(() => {
    if (knex.client && (knex.client as any).pool) {
      const pool = (knex.client as any).pool;
      const usedCount = pool.used?.length || 0;
      const freeCount = pool.free?.length || 0;
      const pendingCount = pool.pendingAcquires?.length || 0;
      const now = Date.now();

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
        const message = `[DB Pool] CRITICAL: ${pendingCount} pending connections! Used: ${usedCount}/${poolMax}. Immediate action required.`;
        if (logger?.error) {
          logger.error(message);
        } else {
          console.error(message);
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

if (process.env.NODE_ENV === 'test' && poolStatusInterval) {
  clearInterval(poolStatusInterval);
  poolStatusInterval = null;
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
