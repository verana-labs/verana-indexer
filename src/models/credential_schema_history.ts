import BaseModel from "./base";

export default class CredentialSchemaHistory extends BaseModel {
  static tableName = "credential_schema_history";

  id!: number;
  credential_schema_id!: number;


  tr_id!: string;
  json_schema!: object;
  issuer_grantor_validation_validity_period!: number;
  verifier_grantor_validation_validity_period!: number;
  issuer_validation_validity_period!: number;
  verifier_validation_validity_period!: number;
  holder_validation_validity_period!: number;

  issuer_perm_management_mode!: string;
  verifier_perm_management_mode!: string;
  deposit!: string;

  archived!: Date | null;
  is_active!: boolean;
  created!: Date;
  modified!: Date;

  changes!: object | null;

  action!: string;
  created_at!: Date;

  static useTimestamps = false;

  static get jsonSchema() {
    return {
      type: "object",
      required: ["credential_schema_id", "action"],
      properties: {
        id: { type: "integer" },
        credential_schema_id: { type: "integer" },

        tr_id: { type: "string" },
        deposit: { type: "string" },
        json_schema: { type: "object" },

        issuer_grantor_validation_validity_period: { type: "integer" },
        verifier_grantor_validation_validity_period: { type: "integer" },
        issuer_validation_validity_period: { type: "integer" },
        verifier_validation_validity_period: { type: "integer" },
        holder_validation_validity_period: { type: "integer" },

        issuer_perm_management_mode: { type: "string" },
        verifier_perm_management_mode: { type: "string" },

        archived: { type: ["string", "null"], format: "date-time" },
        created: { type: "string", format: "date-time" },
        modified: { type: "string", format: "date-time" },

        changes: { type: ["object", "null"] },
        is_active: { type: "boolean" },
        action: { type: "string" },
        created_at: { type: "string", format: "date-time" },
      },
    };
  }
}
