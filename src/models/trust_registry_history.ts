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
  deposit!: string;
  aka?: string | null;
  language!: string;
  active_version?: number | null;
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
