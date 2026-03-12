import { Model } from "objection";
import BaseModel from "./base";
import { TrustRegistry } from "./trust_registry";

export class TrustRegistryHistory extends BaseModel {
  static tableName = "trust_registry_history";

  id!: number;
  tr_id!: number;
  did!: string;
  controller!: string;
  created!: Date;
  modified!: Date;
  archived?: Date | null;
  deposit!: number;
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
    trustRegistry: {
      relation: Model.BelongsToOneRelation,
      modelClass: TrustRegistry,
      join: {
        from: "trust_registry_history.tr_id",
        to: "trust_registry.id",
      },
    },
  });
}
