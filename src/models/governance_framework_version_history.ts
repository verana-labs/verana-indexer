import { Model } from "objection";
import BaseModel from "./base";
import { GovernanceFrameworkVersion } from "./governance_framework_version";
import { TrustRegistry } from "./trust_registry";

export class GovernanceFrameworkVersionHistory extends BaseModel {
  static tableName = "governance_framework_version_history";

  id!: number;
  gfv_id!: number;
  tr_id!: number;
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
    trustRegistry: {
      relation: Model.BelongsToOneRelation,
      modelClass: TrustRegistry,
      join: {
        from: "governance_framework_version_history.tr_id",
        to: "trust_registry.id",
      },
    },
  });
}
