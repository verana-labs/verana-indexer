import { Model } from "objection";
import BaseModel from "./base";
import { Ecosystem } from "./ecosystem";

export class EcosystemHistory extends BaseModel {
  static tableName = "ecosystem_history";

  id!: number;
  ecosystem_id!: number;
  did!: string;
  corporation_id!: number;
  created!: Date;
  modified!: Date;
  archived?: Date | null;
  aka?: string | null;
  language!: string;
  active_version?: number | null;
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
  event_type!: string;
  height!: number;
  changes?: Record<string, any>;
  created_at!: Date;

  static relationMappings = () => ({
    ecosystem: {
      relation: Model.BelongsToOneRelation,
      modelClass: Ecosystem,
      join: {
        from: "ecosystem_history.ecosystem_id",
        to: "ecosystem.id",
      },
    },
  });
}
