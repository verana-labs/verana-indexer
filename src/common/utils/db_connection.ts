import Knex from "knex";
import { knexConfig } from "../../knexfile";
import { Config } from "../index";

const environment = process.env.NODE_ENV || 'development';
const cfg = knexConfig[environment];

if (!cfg) {
  throw new Error(`Knex configuration not found for environment: ${environment}`);
}

const knex = Knex(cfg);

const poolMax = cfg.pool?.max || 10;
const postgresPoolMax = (Config as Record<string, unknown>).POSTGRES_POOL_MAX;
console.log(`[DB Connection] Initialized with pool size: ${poolMax} (env: ${environment}, POSTGRES_POOL_MAX: ${postgresPoolMax || 'not set'})`);

if (knex.client && (knex.client as any).pool) {
  const pool = (knex.client as any).pool;
  const poolMax = cfg.pool?.max || 10;
  const warningThreshold = Math.floor(poolMax * 0.8);
  let lastWarningTime = 0;
  const WARNING_INTERVAL = 5000; 

  const originalAcquire = pool.acquire.bind(pool);
  pool.acquire = function(...args: any[]) {
    const pendingCount = pool.pendingAcquires?.length || 0;
    const usedCount = pool.used?.length || 0;
    const freeCount = pool.free?.length || 0;
    const now = Date.now();

    if ((usedCount >= warningThreshold || pendingCount > 10) && 
        (now - lastWarningTime > WARNING_INTERVAL)) {
      console.warn(
        `⚠️ [DB Pool] High usage - Used: ${usedCount}/${poolMax}, ` +
        `Free: ${freeCount}, Pending: ${pendingCount}. ` +
        `Consider increasing POSTGRES_POOL_MAX (current: ${poolMax})`
      );
      lastWarningTime = now;
    }

    return originalAcquire(...args);
  };
}

export default knex;
