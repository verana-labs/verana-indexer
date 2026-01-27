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
  participants?: number;
  active_schemas?: number;
  archived_schemas?: number;
  weight?: string;
  issued?: string;
  verified?: string;
  ecosystem_slash_events?: number;
  ecosystem_slashed_amount?: string;
  ecosystem_slashed_amount_repaid?: string;
  network_slash_events?: number;
  network_slashed_amount?: string;
  network_slashed_amount_repaid?: string;

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
