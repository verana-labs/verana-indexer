import BaseModel from "./base";

export class GovernanceFrameworkDocument extends BaseModel {
  static tableName = "governance_framework_document";

  id!: number;
  gfv_id!: number;
  created!: string;
  language!: string;
  url!: string;
  digest_sri!: string;
}
