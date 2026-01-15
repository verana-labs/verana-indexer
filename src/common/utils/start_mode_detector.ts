import knex from './db_connection';
import { BlockCheckpoint } from '../../models';
import { BULL_JOB_NAME } from '../constant';

export interface StartModeResult {
  isFreshStart: boolean;
  totalBlocks: number;
  currentBlock: number;
}

export async function detectStartMode(jobName?: string): Promise<StartModeResult> {
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
    
    return {
      isFreshStart,
      totalBlocks,
      currentBlock,
    };
  } catch (error) {
    return {
      isFreshStart: false,
      totalBlocks: 0,
      currentBlock: 0,
    };
  }
}

