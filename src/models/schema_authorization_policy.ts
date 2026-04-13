import BaseModel from "./base";

export default class SchemaAuthorizationPolicy extends BaseModel {
  static tableName = "schema_authorization_policies";

  id!: number;
  schema_id!: number;
  created!: Date;
  version!: number;
  role!: string;
  url!: string;
  digest_sri!: string;
  effective_from?: Date | null;
  effective_until?: Date | null;
  revoked!: boolean;
  height?: number | null;
  tx_hash?: string | null;
}
