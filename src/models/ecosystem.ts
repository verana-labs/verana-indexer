import { Model } from "objection";
import BaseModel from "./base";
import { GovernanceFrameworkVersion } from "./governance_framework_version";

export class Ecosystem extends BaseModel {
  static tableName = "ecosystem";

  id!: number;
  did!: string;
  corporation_id!: number;
  created!: Date;
  modified!: Date;
  archived?: string | null;
  aka?: string;
  language!: string; 
  active_version?: number;
  participants?: number;
  participants_ecosystem?: number;
  participants_issuer_grantor?: number;
  participants_issuer?: number;
  participants_verifier_grantor?: number;
  participants_verifier?: number;
  participants_holder?: number;
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
        from: "ecosystem.id",
        to: "governance_framework_version.ecosystem_id",
      },
    },
  });
}
