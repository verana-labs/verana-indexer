import { Model } from "objection";
import BaseModel from "./base";
import Permission from "./permission";

export interface AuthzEntry {
  issuer_perm_id?: string | null;
  verifier_perm_id?: string | null;
  wallet_agent_perm_id: string;
}

export default class PermissionSession extends BaseModel {
  static tableName = "permission_sessions";

  id!: string;
  controller!: string;
  agent_perm_id!: string;
  wallet_agent_perm_id!: string;
  authz!: AuthzEntry[];
  created!: string;
  modified!: string;

  static get jsonSchema() {
    return {
      type: "object",
      required: [
        "id",
        "controller",
        "agent_perm_id",
        "wallet_agent_perm_id",
        "authz",
      ],
      properties: {
        id: { type: "string" },
        controller: { type: "string", maxLength: 255 },
        agent_perm_id: { type: "string" },
        wallet_agent_perm_id: { type: "string" },
        authz: {
          type: "array",
          items: {
            type: "object",
            properties: {
              issuer_perm_id: { type: ["string", "null"] },
              verifier_perm_id: { type: ["string", "null"] },
              wallet_agent_perm_id: { type: "string" },
            },
            required: ["wallet_agent_perm_id"],
          },
        },
        created: { type: "string" },
        modified: { type: "string" },
      },
    };
  }

  static get jsonAttributes() {
    return ["authz"];
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

  addAuthz(entry: AuthzEntry): void {
    if (!this.authz) {
      this.authz = [];
    }
    this.authz.push(entry);
  }

  hasAuthzFor(issuerPermId?: string, verifierPermId?: string): boolean {
    return this.authz.some(
      (entry) =>
        (!issuerPermId || entry.issuer_perm_id === issuerPermId) &&
        (!verifierPermId || entry.verifier_perm_id === verifierPermId)
    );
  }
}
