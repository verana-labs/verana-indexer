import BaseModel from "./base";

export class CorporationHistory extends BaseModel {
  static tableName = "corporation_history";

  id!: number;
  corporation_id!: number;
  did?: string | null;
  corporation?: string | null;
  language?: string | null;
  event_type!: string;
  height!: number;
  changes?: unknown;
  created_at!: Date;
}
