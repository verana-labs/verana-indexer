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

  issuer_perm_management_mode!: string;
  deposit!: string;
  verifier_perm_management_mode!: string;

  archived!: Date | null;
  created!: Date;
  modified!: Date;
  is_active!: boolean;
  participants?: number;
  weight?: string;
  issued?: string;
  verified?: string;
  ecosystem_slash_events?: number;
  ecosystem_slashed_amount?: string;
  ecosystem_slashed_amount_repaid?: string;
  network_slash_events?: number;
  network_slashed_amount?: string;
  network_slashed_amount_repaid?: string;

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

        issuer_perm_management_mode: { type: "string" },
        verifier_perm_management_mode: { type: "string" },
        is_active: { type: "boolean" },

        archived: { type: ["string", "null"], format: "date-time" },
        created: { type: "string", format: "date-time" },
        modified: { type: "string", format: "date-time" },
      },
    };
  }
}
