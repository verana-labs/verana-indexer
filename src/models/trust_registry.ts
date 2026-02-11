import { Model } from "objection";
import BaseModel from "./base";
import { GovernanceFrameworkVersion } from "./governance_framework_version";

export class TrustRegistry extends BaseModel {
  static tableName = "trust_registry";

  id!: number;
  did!: string;
  controller!: string;
  created!: Date;
  modified!: Date;
  archived?: string | null;
  deposit!: number;
  aka?: string;
  language!: string; 
  active_version?: number;
  participants?: number;
  active_schemas?: number;
  archived_schemas?: number;
  weight?: number;
  issued?: number;
  verified?: number;
  ecosystem_slash_events?: number;
  ecosystem_slashed_amount?: number;
  ecosystem_slashed_amount_repaid?: number;
  network_slash_events?: number;
  network_slashed_amount?: number;
  network_slashed_amount_repaid?: number;

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
