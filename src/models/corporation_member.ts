import BaseModel from "./base";

export class CorporationMember extends BaseModel {
  static tableName = "corporation_member";

  id!: number;
  corporation_id!: number;
  address!: string;
  weight!: string;
  metadata?: string | null;
  created!: Date;
}
