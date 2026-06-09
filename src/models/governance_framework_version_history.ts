import { Model } from "objection";
import BaseModel from "./base";
import { GovernanceFrameworkVersion } from "./governance_framework_version";
import { Ecosystem } from "./ecosystem";

export class GovernanceFrameworkVersionHistory extends BaseModel {
  static tableName = "governance_framework_version_history";

  id!: number;
  gfv_id!: number;
  ecosystem_id!: number;
  created!: Date;
  version!: number;
  active_since!: Date;
  event_type!: string;
  height!: number;
  changes?: Record<string, any>;
  created_at!: Date;

  static relationMappings = () => ({
    governanceFrameworkVersion: {
      relation: Model.BelongsToOneRelation,
      modelClass: GovernanceFrameworkVersion,
      join: {
        from: "governance_framework_version_history.gfv_id",
        to: "governance_framework_version.id",
      },
    },
    ecosystem: {
      relation: Model.BelongsToOneRelation,
      modelClass: Ecosystem,
      join: {
        from: "governance_framework_version_history.ecosystem_id",
        to: "ecosystem.id",
      },
    },
  });
}
