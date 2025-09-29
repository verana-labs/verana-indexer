import { Model } from "objection";
import BaseModel from "./base";
import { GovernanceFrameworkDocument } from "./governance_framework_document";
import { GovernanceFrameworkVersion } from "./governance_framework_version";
import { TrustRegistry } from "./trust_registry";

export class GovernanceFrameworkDocumentHistory extends BaseModel {
  static tableName = "governance_framework_document_history";

  id!: number;
  gfd_id!: number;
  gfv_id!: number;
  tr_id!: number;
  created!: Date;
  language!: string;
  url!: string;
  digest_sri!: string;
  event_type!: string;
  height!: number;
  changes?: Record<string, any>;
  created_at!: Date;

  static relationMappings = () => ({
    governanceFrameworkDocument: {
      relation: Model.BelongsToOneRelation,
      modelClass: GovernanceFrameworkDocument,
      join: {
        from: "governance_framework_document_history.gfd_id",
        to: "governance_framework_document.id",
      },
    },
    governanceFrameworkVersion: {
      relation: Model.BelongsToOneRelation,
      modelClass: GovernanceFrameworkVersion,
      join: {
        from: "governance_framework_document_history.gfv_id",
        to: "governance_framework_version.id",
      },
    },
    trustRegistry: {
      relation: Model.BelongsToOneRelation,
      modelClass: TrustRegistry,
      join: {
        from: "governance_framework_document_history.tr_id",
        to: "trust_registry.id",
      },
    },
  });
}
