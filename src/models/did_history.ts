import knex from "../common/utils/db_connection";

export interface DidHistoryRecord {
  id?: number;
  did: string;
  event_type: string;
  height?: number;
  years?: string;
  controller?: string;
  deposit?: string;
  exp?: string;
  created?: string;
  deleted_at?: string | null;
  is_deleted?: boolean;
  changes?: Record<string, { old: any; new: any }>; 
  created_at?: Date | string;
}

export const DidHistoryRepository = {
  async insertHistory(record: DidHistoryRecord): Promise<number[]> {
    return knex("did_history").insert(record).returning("id");
  },

  async getByDid(did: string): Promise<DidHistoryRecord[]> {
    return knex("did_history").where({ did }).orderBy("created_at", "desc");
  },
};
