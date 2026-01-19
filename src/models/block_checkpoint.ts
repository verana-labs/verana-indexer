import config from '../config.json' with { type: 'json' };
import BaseModel from './base';
import knex from '../common/utils/db_connection';

export class BlockCheckpoint extends BaseModel {
  job_name!: string;

  height!: number;

  static get tableName() {
    return 'block_checkpoint';
  }

  static get jsonSchema() {
    return {
      type: 'object',
      required: ['job_name', 'height'],
      properties: {
        job_name: { type: 'string' },
        height: { type: 'number' },
      },
    };
  }

  static async getCheckpoint(
    jobName: string,
    lastHeightJobNames: string[],
    configName?: string
  ): Promise<[number, number, BlockCheckpoint]> {
    const [jobCheckpoint, lastHeightCheckpoint, crawlBlockCheckpoint] = await Promise.all([
      BlockCheckpoint.query().select('*').where('job_name', jobName).first(),
      BlockCheckpoint.query()
        .select('*')
        .whereIn('job_name', lastHeightJobNames)
        .orderBy('height', 'ASC')
        .first(),
      BlockCheckpoint.query().select('*').where('job_name', 'crawl:block').first(),
    ]);
    
    let startHeight = 0;
    let endHeight = 0;
    let updateBlockCheckpoint: BlockCheckpoint;
    
    if (jobCheckpoint) {
      startHeight = jobCheckpoint.height;
      updateBlockCheckpoint = jobCheckpoint;
    } else {
      startHeight = config.crawlBlock.startBlock;
      updateBlockCheckpoint = BlockCheckpoint.fromJson({
        job_name: jobName,
        height: config.crawlBlock.startBlock,
      });
    }

    let lastHeight = 0;
    if (lastHeightCheckpoint) {
      lastHeight = Number(lastHeightCheckpoint.height);
    }

    const currentJobHeight = startHeight;
    const hasHighBlockCheckpoint = crawlBlockCheckpoint && crawlBlockCheckpoint.height > 1000;
    const isCurrentJobLow = currentJobHeight < 100;
    const isDependentLow = lastHeight < 100;
    
    if (hasHighBlockCheckpoint && isDependentLow && lastHeight === 0) {
      try {
        const result = await knex('block').max('height as max').first();
        if (result && (result as { max: string | number | null }).max) {
          const maxBlockHeight = parseInt(String((result as { max: string | number }).max), 10);
          if (maxBlockHeight > 0) {
            lastHeight = maxBlockHeight;
          }
        }
      } catch (error) {
      }
    }

    if (lastHeight > 0) {
      if (configName) {
        const blocksPerCall = Number((config as any)[configName]?.blocksPerCall) || 0;
        const calculatedEnd = startHeight + blocksPerCall;
        endHeight = Number.isFinite(calculatedEnd) && Number.isFinite(lastHeight)
          ? Math.min(calculatedEnd, lastHeight)
          : (Number.isFinite(lastHeight) ? lastHeight : 0);
      } else {
        endHeight = Number.isFinite(lastHeight) ? lastHeight : 0;
      }
    }

    const validStartHeight = Number.isFinite(startHeight) && !Number.isNaN(startHeight) ? Number(startHeight) : 0;
    const validEndHeight = Number.isFinite(endHeight) && !Number.isNaN(endHeight) ? Number(endHeight) : 0;

    return [validStartHeight, validEndHeight, updateBlockCheckpoint];
  }
}
