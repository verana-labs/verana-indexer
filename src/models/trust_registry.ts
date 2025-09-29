import { Model } from "objection";
import BaseModel from "./base";
import { GovernanceFrameworkVersion } from "./governance_framework_version";

export class TrustRegistry extends BaseModel {
  static tableName = "trust_registry";

  id!: number;
  did!: string;
  controller!: string;
  created!: string;
  modified!: string;
  archived?: string | null;
  deposit!: string;
  aka?: string;
  language!: string; 
  active_version?: number;

  governanceFrameworkVersions?: GovernanceFrameworkVersion[];

  static relationMappings = () => ({
    governanceFrameworkVersions: {
      relation: Model.HasManyRelation,
      modelClass: GovernanceFrameworkVersion,
      join: {
        from: "trust_registry.id",
        to: "governance_framework_version.tr_id",
      },
    },
  });
}
