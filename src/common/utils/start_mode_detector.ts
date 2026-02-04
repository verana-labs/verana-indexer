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

export async function detectStartMode(jobName?: string, logger?: any): Promise<StartModeResult> {
  try {
    const blockCountResult = await knex('block').count('* as count').first();
    const totalBlocks = blockCountResult 
      ? parseInt(String((blockCountResult as { count: string | number }).count), 10) 
      : 0;
    
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
    
    return {
      isFreshStart,
      totalBlocks,
      currentBlock,
      cacheCleared,
    };
  } catch (error) {
    return {
      isFreshStart: false,
      totalBlocks: 0,
      currentBlock: 0,
    };
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

