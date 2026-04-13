import { Model } from "objection";
import BaseModel from "./base";
import Permission from "./permission";

export interface PermissionSessionRecord {
  created: string;
  issuer_perm_id?: number | null;
  verifier_perm_id?: number | null;
  wallet_agent_perm_id: number;
}

export default class PermissionSession extends BaseModel {
  static tableName = "permission_sessions";

  id!: string;
  corporation!: string;
  vs_operator?: string | null;
  agent_perm_id!: number;
  wallet_agent_perm_id!: number;
  session_records!: PermissionSessionRecord[];
  created!: string;
  modified!: string;

  static get jsonSchema() {
    return {
      type: "object",
      required: [
        "id",
        "corporation",
        "agent_perm_id",
        "wallet_agent_perm_id",
        "session_records",
      ],
      properties: {
        id: { type: "string" },
        corporation: { type: "string", maxLength: 255 },
        vs_operator: { type: ["string", "null"] },
        agent_perm_id: { type: "integer" },
        wallet_agent_perm_id: { type: "integer" },
        session_records: {
          type: "array",
          items: {
            type: "object",
            properties: {
              created: { type: "string" },
              issuer_perm_id: { type: ["integer", "null"] },
              verifier_perm_id: { type: ["integer", "null"] },
              wallet_agent_perm_id: { type: "integer" },
            },
            required: ["created", "wallet_agent_perm_id"],
          },
        },
        created: { type: "string" },
        modified: { type: "string" },
      },
    };
  }

  static get jsonAttributes() {
    return ["session_records"];
  }

  static get relationMappings() {
    return {
      agent_permission: {
        relation: Model.BelongsToOneRelation,
        modelClass: Permission,
        join: {
          from: "permission_sessions.agent_perm_id",
          to: "permissions.id",
        },
      },
      wallet_agent_permission: {
        relation: Model.BelongsToOneRelation,
        modelClass: Permission,
        join: {
          from: "permission_sessions.wallet_agent_perm_id",
          to: "permissions.id",
        },
      },
    };
  }

  pushSessionRecord(entry: Omit<PermissionSessionRecord, "created"> & { created?: string }): void {
    if (!this.session_records) {
      this.session_records = [];
    }
    const created = entry.created ?? new Date().toISOString();
    this.session_records.push({
      created,
      issuer_perm_id: entry.issuer_perm_id ?? null,
      verifier_perm_id: entry.verifier_perm_id ?? null,
      wallet_agent_perm_id: entry.wallet_agent_perm_id,
    });
  }
}
