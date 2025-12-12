import { Context } from "moleculer";
import knex from "./db_connection";
import { getBlockHeight, hasBlockHeight } from "./blockHeight";

export interface HistoricalQueryOptions {
  table: string;
  historyTable: string;
  idField: string;
  idValue: string | number;
  blockHeight?: number;
  additionalWhere?: Record<string, any>;
  selectFields?: string[];
}

export async function queryHistoricalOrCurrent<T>(
  ctx: Context<any, any>,
  options: HistoricalQueryOptions
): Promise<T | null> {
  const blockHeight = getBlockHeight(ctx);

  if (hasBlockHeight(ctx) && blockHeight !== undefined) {
    let query = knex(options.historyTable)
      .where(options.idField, options.idValue)
      .where("height", "<=", blockHeight);

    if (options.additionalWhere) {
      Object.entries(options.additionalWhere).forEach(([key, value]) => {
        query = query.where(key, value);
      });
    }

    const historyRecord = await query
      .orderBy("height", "desc")
      .orderBy("created_at", "desc")
      .first();

    return historyRecord as T | null;
  }

  let query = knex(options.table).where(options.idField, options.idValue);

  if (options.additionalWhere) {
    Object.entries(options.additionalWhere).forEach(([key, value]) => {
      query = query.where(key, value);
    });
  }

  if (options.selectFields) {
    query = query.select(options.selectFields);
  }

  const record = await query.first();
  return record as T | null;
}

