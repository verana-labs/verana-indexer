import BaseModel from "./base";

export type Granularity = "HOUR" | "DAY" | "MONTH";
export type EntityType = "GLOBAL" | "TRUST_REGISTRY" | "CREDENTIAL_SCHEMA" | "PERMISSION";

export default class Stats extends BaseModel {
  static tableName = "stats";

  id!: number;
  granularity!: Granularity;
  timestamp!: Date;
  entity_type!: EntityType;
  entity_id!: string | null;

  // Cumulative fields
  cumulative_participants!: number;
  cumulative_active_schemas!: number;
  cumulative_archived_schemas!: number;
  cumulative_weight!: string;
  cumulative_issued!: string;
  cumulative_verified!: string;
  cumulative_ecosystem_slash_events!: number;
  cumulative_ecosystem_slashed_amount!: string;
  cumulative_ecosystem_slashed_amount_repaid!: string;
  cumulative_network_slash_events!: number;
  cumulative_network_slashed_amount!: string;
  cumulative_network_slashed_amount_repaid!: string;

  // Delta fields
  delta_participants!: number;
  delta_active_schemas!: number;
  delta_archived_schemas!: number;
  delta_weight!: string;
  delta_issued!: string;
  delta_verified!: string;
  delta_ecosystem_slash_events!: number;
  delta_ecosystem_slashed_amount!: string;
  delta_ecosystem_slashed_amount_repaid!: string;
  delta_network_slash_events!: number;
  delta_network_slashed_amount!: string;
  delta_network_slashed_amount_repaid!: string;

  created_at!: Date;
  updated_at!: Date;

  static get jsonSchema() {
    return {
      type: "object",
      required: [
        "granularity",
        "timestamp",
        "entity_type",
        "cumulative_participants",
        "cumulative_active_schemas",
        "cumulative_archived_schemas",
        "cumulative_weight",
        "cumulative_issued",
        "cumulative_verified",
        "cumulative_ecosystem_slash_events",
        "cumulative_ecosystem_slashed_amount",
        "cumulative_ecosystem_slashed_amount_repaid",
        "cumulative_network_slash_events",
        "cumulative_network_slashed_amount",
        "cumulative_network_slashed_amount_repaid",
        "delta_participants",
        "delta_active_schemas",
        "delta_archived_schemas",
        "delta_weight",
        "delta_issued",
        "delta_verified",
        "delta_ecosystem_slash_events",
        "delta_ecosystem_slashed_amount",
        "delta_ecosystem_slashed_amount_repaid",
        "delta_network_slash_events",
        "delta_network_slashed_amount",
        "delta_network_slashed_amount_repaid",
      ],
      properties: {
        granularity: { type: "string", enum: ["HOUR", "DAY", "MONTH"] },
        timestamp: { type: "string", format: "date-time" },
        entity_type: { type: "string", enum: ["GLOBAL", "TRUST_REGISTRY", "CREDENTIAL_SCHEMA", "PERMISSION"] },
        entity_id: { type: ["string", "null"] },
      },
    };
  }
}
