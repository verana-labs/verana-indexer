import { Context } from 'moleculer'
import { BULL_JOB_NAME } from '../constant'
import knex from './db_connection'

export function getBlockHeight(ctx: Context<any, any>): number | undefined {
  return (ctx.meta as any)?.blockHeight
}

export async function getResolvedBlockHeight(blockHeight?: number): Promise<number> {
  if (typeof blockHeight === 'number') return blockHeight
  const checkpoint = await knex('block_checkpoint').where('job_name', BULL_JOB_NAME.HANDLE_TRANSACTION).first()
  return checkpoint?.height != null ? Number(checkpoint.height) : 0
}

export function hasBlockHeight(ctx: Context<any, any>): boolean {
  const blockHeight = getBlockHeight(ctx)
  return typeof blockHeight === 'number'
}
