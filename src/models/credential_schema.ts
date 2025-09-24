import BaseModel from "./base";

export default class CredentialSchema extends BaseModel {
  static tableName = "credential_schemas";

  id!: number;
  tr_id!: string;
  json_schema!: object;
  issuer_grantor_validation_validity_period!: number;
  verifier_grantor_validation_validity_period!: number;
  issuer_validation_validity_period!: number;
  verifier_validation_validity_period!: number;
  holder_validation_validity_period!: number;

  issuer_perm_management_mode!: number;
  deposit!: string;
  verifier_perm_management_mode!: number;

  archived!: Date | null;
  created!: Date;
  modified!: Date;
  isActive!: boolean;

  static useTimestamps = false;

  static get jsonSchema() {
    return {
      type: "object",
      required: [
        "tr_id",
        "json_schema",
        "issuer_grantor_validation_validity_period",
        "verifier_grantor_validation_validity_period",
        "issuer_validation_validity_period",
        "verifier_validation_validity_period",
        "holder_validation_validity_period",
        "issuer_perm_management_mode",
        "verifier_perm_management_mode",

      ],
      properties: {
        id: { type: "integer" },
        tr_id: { type: "string" },
        deposit: { type: "string" },
        json_schema: { type: "object" },

        issuer_grantor_validation_validity_period: { type: "integer" },
        verifier_grantor_validation_validity_period: { type: "integer" },
        issuer_validation_validity_period: { type: "integer" },
        verifier_validation_validity_period: { type: "integer" },
        holder_validation_validity_period: { type: "integer" },

        issuer_perm_management_mode: { type: "integer" },
        verifier_perm_management_mode: { type: "integer" },
        isActive: { type: "boolean" },

        archived: { type: ["string", "null"], format: "date-time" },
        created: { type: "string", format: "date-time" },
        modified: { type: "string", format: "date-time" },
      },
    };
  }
}
