import BaseModel from "./base";

export class Corporation extends BaseModel {
  static tableName = "corporations";

  id!: number;

  policy_address!: string;

  did?: string | null;

  deposit?: string | null;

  last_slashed_at?: Date | string | null;
  slashed_events?: number;
  slashed_value?: string | null;

  active_version?: number | null;

  created!: Date;
  modified!: Date;

  static get jsonSchema() {
    return {
      type: "object",
      required: ["id", "policy_address"],
      properties: {
        id: { type: "integer" },
        policy_address: { type: "string", maxLength: 255 },
        did: { type: ["string", "null"], maxLength: 255 },
        deposit: { type: ["string", "null"] },
        last_slashed_at: { type: ["string", "null"], format: "date-time" },
        slashed_events: { type: "integer" },
        slashed_value: { type: ["string", "null"] },
        active_version: { type: ["integer", "null"] },
        created: { type: "string", format: "date-time" },
        modified: { type: "string", format: "date-time" },
      },
    };
  }
}

export default Corporation;
