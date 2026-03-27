import knex from './db_connection';
import { BlockCheckpoint } from '../../models';
import { BULL_JOB_NAME } from '../constant';
import { clearRedisCache } from './cache_cleaner';

export interface StartModeResult {
  isFreshStart: boolean;
  totalBlocks: number;
  currentBlock: number;
  cacheCleared?: boolean;
}

let reindexCacheCleared = false;
let cachedStartMode: StartModeResult | null = null;

function publishStartMode(mode: StartModeResult, jobName?: string): void {
  (global as any).__indexerStartMode = {
    ...mode,
    jobName: jobName || BULL_JOB_NAME.CRAWL_BLOCK,
    detectedAt: new Date().toISOString(),
  };
}

export async function detectStartMode(jobName?: string, logger?: any): Promise<StartModeResult> {
  // Return cached result — start mode never changes during a process's lifetime
  if (cachedStartMode) {
    publishStartMode(cachedStartMode, jobName);
    return cachedStartMode;
  }

  try {
    // We determine if it is a fresh start by checking if there are less than 100 blocks
    // in the database and if the current block is less than 1000.
    // Here we are using a bounded query instead of count(*) to avoid a full sequential scan,
    // which might be too expensive since the block table is large.
    const boundedResult = await knex.raw(
      `SELECT count(*) AS count FROM (SELECT 1 FROM block LIMIT 100) sub`
    );
    const totalBlocks = parseInt(String(boundedResult.rows?.[0]?.count ?? '0'), 10);
    
    const checkpointJobName = jobName || BULL_JOB_NAME.CRAWL_BLOCK;
    const checkpoint = await BlockCheckpoint.query().findOne({
      job_name: checkpointJobName,
    });
    const currentBlock = checkpoint ? checkpoint.height : 0;
    
    const isFreshStart = totalBlocks < 100 && currentBlock < 1000;
    
    let cacheCleared = false;
    if (isFreshStart && !reindexCacheCleared && process.env.NODE_ENV !== 'test') {
      logger?.info?.('Fresh start detected - clearing Redis cache');
      const result = await clearRedisCache(logger);
      cacheCleared = result.success;
      reindexCacheCleared = true;
    }
    
    const result = {
      isFreshStart,
      totalBlocks,
      currentBlock,
      cacheCleared,
    };
    cachedStartMode = result;
    publishStartMode(result, checkpointJobName);
    return result;
  } catch (error) {
    const result = {
      isFreshStart: false,
      totalBlocks: 0,
      currentBlock: 0,
    };
    publishStartMode(result, jobName);
    return result;
  }
}

export async function clearCacheForReindex(logger?: any): Promise<boolean> {
  if (process.env.NODE_ENV === 'test') {
    return true;
  }
  const result = await clearRedisCache(logger);
  reindexCacheCleared = result.success;
  return result.success;
}
