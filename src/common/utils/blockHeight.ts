import { Context } from 'moleculer'
import { BULL_JOB_NAME } from '../constant'
import knex from './db_connection'

export function getBlockHeight(ctx: Context<any, any>): number | undefined {
  return (ctx.meta as any)?.blockHeight
}

// The block a non-list response is evaluated at: the At-Block-Height header, else the latest indexed height.
export async function getResolvedBlockHeight(blockHeight?: number): Promise<number> {
  if (typeof blockHeight === 'number') return blockHeight
  const checkpoint = await knex('block_checkpoint').where('job_name', BULL_JOB_NAME.HANDLE_TRANSACTION).first()
  if (checkpoint?.height != null) return Number(checkpoint.height)
  const latest = await knex('block').max('height as max').first()
  const maxValue = latest != null ? (latest as { max: string | number | null }).max : null
  return maxValue != null ? Number(maxValue) : 0
}

export function hasBlockHeight(ctx: Context<any, any>): boolean {
  const blockHeight = getBlockHeight(ctx)
  return typeof blockHeight === 'number'
}
