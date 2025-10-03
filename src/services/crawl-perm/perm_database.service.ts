import { Service, ServiceBroker } from "moleculer";
import { formatTimestamp } from "../../common/utils/date_utils";
import knex from "../../common/utils/db_connection";
import { mapPermissionType } from "../../common/utils/utils";
import {
  getPermissionTypeString,
  MsgCancelPermissionVPLastRequest,
  MsgCreateOrUpdatePermissionSession,
  MsgCreatePermission,
  MsgCreateRootPermission,
  MsgExtendPermission,
  MsgRenewPermissionVP,
  MsgRepayPermissionSlashedTrustDeposit,
  MsgRevokePermission,
  MsgSetPermissionVPToValidated,
  MsgSlashPermissionTrustDeposit,
  MsgStartPermissionVP,
} from "./perm_types";
import ModuleParams from "../../models/modules_params";

export default class PermIngestService extends Service {
  public constructor(broker: ServiceBroker) {
    super(broker);
    this.parseServiceSchema({
      name: "permIngest",
      actions: {
        handleMsgCreateRootPermission: {
          params: { data: "object" },
          handler: async (ctx) =>
            this.handleCreateRootPermission(ctx.params.data),
        },
        handleMsgCreatePermission: {
          params: { data: "object" },
          handler: async (ctx) => this.handleCreatePermission(ctx.params.data),
        },
        handleMsgExtendPermission: {
          params: { data: "object" },
          handler: async (ctx) => this.handleExtendPermission(ctx.params.data),
        },
        handleMsgRevokePermission: {
          params: { data: "object" },
          handler: async (ctx) => this.handleRevokePermission(ctx.params.data),
        },
        handleMsgStartPermissionVP: {
          params: { data: "object" },
          handler: async (ctx) => this.handleStartPermissionVP(ctx.params.data),
        },
        handleMsgSetPermissionVPToValidated: {
          params: { data: "object" },
          handler: async (ctx) =>
            this.handleSetPermissionVPToValidated(ctx.params.data),
        },
        handleMsgRenewPermissionVP: {
          params: { data: "object" },
          handler: async (ctx) => this.handleRenewPermissionVP(ctx.params.data),
        },
        handleMsgCancelPermissionVPLastRequest: {
          params: { data: "object" },
          handler: async (ctx) =>
            this.handleCancelPermissionVPLastRequest(ctx.params.data),
        },
        handleMsgCreateOrUpdatePermissionSession: {
          params: { data: "object" },
          handler: async (ctx) =>
            this.handleCreateOrUpdatePermissionSession(ctx.params.data),
        },
        handleMsgSlashPermissionTrustDeposit: {
          params: { data: "object" },
          handler: async (ctx) =>
            this.handleSlashPermissionTrustDeposit(ctx.params.data),
        },
        handleMsgRepayPermissionSlashedTrustDeposit: {
          params: { data: "object" },
          handler: async (ctx) =>
            this.handleRepayPermissionSlashedTrustDeposit(ctx.params.data),
        },
        getPermission: {
          params: { schema_id: "number", grantee: "string", type: "string" },
          handler: async (ctx) => {
            const { schema_id: schemaId, grantee, type } = ctx.params;
            return await knex("permissions")
              .where({ schema_id: schemaId, grantee, type })
              .first();
          },
        },
        listPermissions: {
          params: {
            schema_id: { type: "number", optional: true },
            grantee: { type: "string", optional: true },
            type: { type: "string", optional: true },
          },
          handler: async (ctx) => {
            let query = knex("permissions");
            if (ctx.params.schema_id)
              query = query.where("schema_id", ctx.params.schema_id);
            if (ctx.params.grantee)
              query = query.where("grantee", ctx.params.grantee);
            if (ctx.params.type) query = query.where("type", ctx.params.type);
            return await query;
          },
        },
      },
    });
  }
  public async getParams() {
    try {
      const module = await ModuleParams.query().findOne({
        module: "td",
      });

      if (!module || !module.params) {
        return false;
      }

      const parsedParams =
        typeof module.params === "string"
          ? JSON.parse(module.params)
          : module.params;
      console.log(parsedParams);
      return parsedParams.params;
    } catch (err: any) {
      this.logger.error("Error fetching trustregistry params", err);
    }
  }

  // ---------- CREATE HANDLERS ----------

  private async handleCreateRootPermission(msg: MsgCreateRootPermission) {
    const schemaId = (msg as any).schema_id ?? (msg as any).schema_id ?? null;
    if (!schemaId) {
      this.logger.warn(
        "Missing schema_id in MsgCreateRootPermission, skipping insert"
      );
      return;
    }

    await knex("permissions").insert({
      schema_id: schemaId,
      type: "ECOSYSTEM",
      vp_state: "VALIDATION_STATE_UNSPECIFIED",
      did: msg.did,
      grantee: msg.creator,
      created_by: msg.creator,
      effective_from: msg.effective_from
        ? formatTimestamp(msg.effective_from)
        : null,
      effective_until: msg.effective_until
        ? formatTimestamp(msg.effective_until)
        : null,
      country: msg.country ?? null,
      validation_fees: String(
        (msg as any).validation_fees ?? (msg as any).validation_fees ?? 0
      ),
      issuance_fees: String(
        (msg as any).issuance_fees ?? (msg as any).issuanceFees ?? 0
      ),
      verification_fees: String(
        (msg as any).verification_fees ?? (msg as any).verification_fees ?? 0
      ),
      deposit: "0",
      modified: msg?.timestamp ? formatTimestamp(msg.timestamp) : null,
      created: msg?.timestamp ? formatTimestamp(msg.timestamp) : null,
    });
  }

  private async handleCreatePermission(msg: MsgCreatePermission) {
    const schemaId = (msg as any).schema_id ?? (msg as any).schema_id ?? null;
    if (!schemaId) {
      this.logger.warn(
        "Missing schema_id in MsgCreatePermission, skipping insert"
      );
      return;
    }

    const type = mapPermissionType((msg as any).type);

    const ecosystemPerm = await knex("permissions")
      .where({ schema_id: schemaId, type: "ECOSYSTEM" })
      .first();

    if (!ecosystemPerm) {
      this.logger.warn(
        `No root ECOSYSTEM permission found for schema_id=${schemaId}, cannot create ${type}`
      );
    }

    await knex("permissions").insert({
      schema_id: schemaId,
      type,
      vp_state: "VALIDATION_STATE_UNSPECIFIED",
      did: msg.did,
      grantee: msg.creator,
      created_by: msg.creator,
      effective_from: msg.effective_from
        ? formatTimestamp(msg.effective_from)
        : null,
      effective_until: msg.effective_until
        ? formatTimestamp(msg.effective_until)
        : null,
      country: msg.country ?? null,
      verification_fees: String(
        (msg as any).verification_fees ?? (msg as any).verification_fees ?? 0
      ),
      validation_fees: "0",
      issuance_fees: "0",
      deposit: "0",
      validator_perm_id: ecosystemPerm.id,
      modified: msg?.timestamp ? formatTimestamp(msg.timestamp) : null,
      created: msg?.timestamp ? formatTimestamp(msg.timestamp) : null,
    });
    // process.exit();
  }

  // ---------- UPDATE HANDLERS WITH SAFETY ----------

  private async handleExtendPermission(msg: MsgExtendPermission) {
    try {
      const perm = await knex("permissions").where({ id: msg.id }).first();
      if (!perm) {
        this.logger.warn(`Permission ${msg.id} not found, skipping extend`);
        return { success: false, reason: "Permission not found" };
      }

      await knex("permissions")
        .where({ id: msg.id })
        .update({
          effective_until: msg.effective_until?.toISOString() || null,
          extended: new Date().toISOString(),
          extended_by: msg.creator,
          modified: new Date().toISOString(),
        });

      return { success: true };
    } catch (err) {
      this.logger.error("Error in handleExtendPermission:", err);
      return { success: false, reason: "DB error" };
    }
  }

  private async handleRevokePermission(msg: MsgRevokePermission) {
    try {
      const perm = await knex("permissions").where({ id: msg.id }).first();
      if (!perm) {
        this.logger.warn(`Permission ${msg.id} not found, skipping revoke`);
        return { success: false, reason: "Permission not found" };
      }

      await knex("permissions").where({ id: msg.id }).update({
        revoked: new Date().toISOString(),
        revoked_by: msg.creator,
        modified: new Date().toISOString(),
      });

      return { success: true };
    } catch (err) {
      this.logger.error("Error in handleRevokePermission:", err);
      return { success: false, reason: "DB error" };
    }
  }

  private async handleStartPermissionVP(msg: MsgStartPermissionVP) {
    try {
      const typeStr = getPermissionTypeString(msg);
      const now = formatTimestamp(msg.timestamp);
      const perm = await knex("permissions")
        .where({ validator_perm_id: msg.validator_perm_id, type: typeStr })
        .first();

      if (!perm) {
        this.logger.warn(
          `Permission ${msg.validator_perm_id} not found, skipping VP start`
        );
        process.exit();
        // return { success: false, reason: "Permission not found" };
      }
      console.log(msg, typeStr, "customLogs");
      await knex("permissions").insert({
        schema_id: perm?.schema_id,
        type: typeStr,
        did: msg.did,
        grantee: msg.creator,
        created_by: msg.creator,
        effective_from: msg.effective_from
          ? formatTimestamp(msg.effective_from)
          : null,
        effective_until: msg.effective_until
          ? formatTimestamp(msg.effective_until)
          : null,
        country: msg.country ?? null,
        verification_fees: String(
          (msg as any).verification_fees ?? (msg as any).verification_fees ?? 0
        ),
        validation_fees: "0",
        issuance_fees: "0",
        deposit: "0",
        validator_perm_id: msg.validator_perm_id,
        vp_state: "PENDING",
        vp_last_state_change: now,
        modified: now,
        created: now,
      });

      // return { success: true };
      process.exit();
    } catch (err) {
      this.logger.error("Error in handleStartPermissionVP:", err);
      process.exit();
      // return { success: false, reason: "DB error" };
    }
  }

  private async handleSetPermissionVPToValidated(
    msg: MsgSetPermissionVPToValidated
  ) {
    try {
      const now = new Date().toISOString();
      const perm = await knex("permissions").where({ id: msg.id }).first();

      if (!perm) {
        this.logger.warn(
          `Permission ${msg.id} not found, skipping VP validated`
        );
        return { success: false, reason: "Permission not found" };
      }

      await knex("permissions")
        .where({ id: msg.id })
        .update({
          vp_state: "VALIDATED",
          vp_last_state_change: now,
          vp_current_deposit: "0",
          vp_current_fees: "0",
          vp_summary_digest_sri: msg.vpSummaryDigestSri ?? null,
          vp_exp: msg.effective_until
            ? msg.effective_until.toISOString()
            : null,
          effective_until: msg.effective_until
            ? msg.effective_until.toISOString()
            : null,
          modified: now,
        });

      return { success: true };
    } catch (err) {
      this.logger.error("Error in handleSetPermissionVPToValidated:", err);
      return { success: false, reason: "DB error" };
    }
  }

  private async handleRenewPermissionVP(msg: MsgRenewPermissionVP) {
    try {
      const now = new Date().toISOString();
      const perm = await knex("permissions").where({ id: msg.id }).first();

      if (!perm) {
        this.logger.warn(`Permission ${msg.id} not found, skipping renew`);
        return { success: false, reason: "Permission not found" };
      }

      await knex("permissions").where({ id: msg.id }).update({
        vp_state: "PENDING",
        vp_last_state_change: now,
        modified: now,
      });

      return { success: true };
    } catch (err) {
      this.logger.error("Error in handleRenewPermissionVP:", err);
      return { success: false, reason: "DB error" };
    }
  }

  private async handleCancelPermissionVPLastRequest(
    msg: MsgCancelPermissionVPLastRequest
  ) {
    try {
      const now = new Date().toISOString();
      const perm = await knex("permissions").where({ id: msg.id }).first();

      if (!perm) {
        this.logger.warn(`Permission ${msg.id} not found, skipping VP cancel`);
        return { success: false, reason: "Permission not found" };
      }

      await knex("permissions").where({ id: msg.id }).update({
        vp_state: "VALIDATED",
        vp_last_state_change: now,
        vp_current_deposit: "0",
        vp_current_fees: "0",
        modified: now,
      });

      return { success: true };
    } catch (err) {
      this.logger.error("Error in handleCancelPermissionVPLastRequest:", err);
      return { success: false, reason: "DB error" };
    }
  }

  private async handleSlashPermissionTrustDeposit(
    msg: MsgSlashPermissionTrustDeposit
  ) {
    try {
      const now = new Date().toISOString();
      const perm = await knex("permissions").where({ id: msg.id }).first();

      if (!perm) {
        this.logger.warn(
          `Permission ${msg.id} not found, skipping slash update`
        );
        return { success: false, reason: "Permission not found" };
      }

      const prev = perm.slashed_deposit ? Number(perm.slashed_deposit) : 0;

      await knex("permissions")
        .where({ id: msg.id })
        .update({
          slashed: now,
          slashed_by: msg.creator,
          slashed_deposit: String(prev + (msg.amount || 0)),
          modified: now,
        });

      return { success: true };
    } catch (err) {
      this.logger.error("Error in handleSlashPermissionTrustDeposit:", err);
      return { success: false, reason: "DB error" };
    }
  }

  private async handleRepayPermissionSlashedTrustDeposit(
    msg: MsgRepayPermissionSlashedTrustDeposit
  ) {
    try {
      const now = new Date().toISOString();
      const perm = await knex("permissions").where({ id: msg.id }).first();

      if (!perm) {
        this.logger.warn(`Permission ${msg.id} not found, skipping repay`);
        return { success: false, reason: "Permission not found" };
      }

      await knex("permissions").where({ id: msg.id }).update({
        repaid: now,
        repaid_by: msg.creator,
        modified: now,
      });

      return { success: true };
    } catch (err) {
      this.logger.error(
        "Error in handleRepayPermissionSlashedTrustDeposit:",
        err
      );
      return { success: false, reason: "DB error" };
    }
  }

  // ---------- PERMISSION SESSION ----------

  private async handleCreateOrUpdatePermissionSession(
    msg: MsgCreateOrUpdatePermissionSession
  ) {
    try {
      const now = new Date().toISOString();
      const exists = await knex("permission_sessions")
        .where({ id: msg.id })
        .first();

      const authzEntry = {
        issuer_perm_id: msg.issuer_perm_id || null,
        verifier_perm_id: msg.validator_perm_id || null,
      } as any;

      if (!exists) {
        await knex("permission_sessions").insert({
          id: msg.id,
          controller: msg.creator,
          agent_perm_id: msg.agent_perm_id,
          wallet_agent_perm_id: msg.wallet_agent_perm_id,
          authz: JSON.stringify([authzEntry]),
          created: now,
          modified: now,
        });
      } else {
        await knex("permission_sessions")
          .where({ id: msg.id })
          .update({
            authz: JSON.stringify([...(exists.authz as any[]), authzEntry]),
            modified: now,
          });
      }

      return { success: true };
    } catch (err) {
      this.logger.error("Error in handleCreateOrUpdatePermissionSession:", err);
      return { success: false, reason: "DB error" };
    }
  }
}
