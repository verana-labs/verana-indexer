import { Model } from "objection";
import BaseModel from "./base";
import { GovernanceFrameworkDocument } from "./governance_framework_document";

export class GovernanceFrameworkVersion extends BaseModel {
  static tableName = "governance_framework_version";

  id!: number;
  tr_id!: number;
  created!: Date;
  active_since!: Date;
  version!: number;

  documents?: GovernanceFrameworkDocument[];

  static relationMappings = () => ({
    documents: {
      relation: Model.HasManyRelation,
      modelClass: GovernanceFrameworkDocument,
      join: {
        from: "governance_framework_version.id",
        to: "governance_framework_document.gfv_id",
      },
    },
  });
}
