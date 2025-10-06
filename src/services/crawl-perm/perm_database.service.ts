import { Service, ServiceBroker } from "moleculer";
import { formatTimestamp } from "../../common/utils/date_utils";
import knex from "../../common/utils/db_connection";
import getGlobalVariables from "../../common/utils/global_variables";
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
        (msg as any).issuance_fees ?? (msg as any).issuance_fees ?? 0
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
  private async handleExtendPermission(msg: MsgExtendPermission) {
    try {
      if (!msg.id || !msg.effective_until) {
        this.logger.warn("Missing mandatory parameter: id or effective_until");
        return { success: false, reason: "Missing mandatory parameter" };
      }

      const applicantPerm = await knex("permissions")
        .where({ id: msg.id })
        .first();
      if (!applicantPerm) {
        this.logger.warn(`Permission ${msg.id} not found`);
        return { success: false, reason: "Permission not found" };
      }

      const newEffectiveUntil = new Date(msg.effective_until);
      const currentEffectiveUntil = applicantPerm.effective_until
        ? new Date(applicantPerm.effective_until)
        : null;

      if (currentEffectiveUntil && newEffectiveUntil <= currentEffectiveUntil) {
        this.logger.warn(
          `New effective_until ${newEffectiveUntil.toISOString()} must be greater than current ${currentEffectiveUntil.toISOString()}`
        );
        return {
          success: false,
          reason: "effective_until must be greater than current",
        };
      }

      let validatorPerm = null;

      if (!applicantPerm.validator_perm_id) {
        if (
          applicantPerm.type === "ECOSYSTEM" &&
          msg.creator !== applicantPerm.grantee
        ) {
          this.logger.warn("Only grantee can extend ECOSYSTEM permission");
          return { success: false, reason: "Unauthorized caller" };
        }
      } else {
        validatorPerm = await knex("permissions")
          .where({ id: applicantPerm.validator_perm_id })
          .first();

        if (!validatorPerm) {
          this.logger.warn(
            `Validator permission ${applicantPerm.validator_perm_id} not found`
          );
          return { success: false, reason: "Validator permission not found" };
        }

        if (applicantPerm.type === "VP_MANAGED") {
          const vpExp = applicantPerm.vp_exp
            ? new Date(applicantPerm.vp_exp)
            : null;
          if (vpExp && newEffectiveUntil > vpExp) {
            this.logger.warn(
              `effective_until ${newEffectiveUntil.toISOString()} exceeds VP expiration ${vpExp.toISOString()}`
            );
            return {
              success: false,
              reason: "effective_until exceeds VP expiration",
            };
          }

          if (msg.creator !== validatorPerm.grantee) {
            this.logger.warn(
              "Only validator grantee can extend VP-managed permission"
            );
            return { success: false, reason: "Unauthorized caller" };
          }
        } else {
          if (validatorPerm.type !== "ECOSYSTEM") {
            this.logger.warn(
              "Validator permission for self-created permission must be ECOSYSTEM"
            );
            return { success: false, reason: "Invalid validator permission" };
          }
          if (msg.creator !== applicantPerm.grantee) {
            this.logger.warn("Only grantee can extend self-created permission");
            return { success: false, reason: "Unauthorized caller" };
          }
        }
      }

      const now = formatTimestamp(msg.timestamp);
      await knex.transaction(async (trx) => {
        await trx("permissions").where({ id: msg.id }).update({
          effective_until: newEffectiveUntil.toISOString(),
          extended: now,
          extended_by: msg.creator,
          modified: now,
        });
      });

      this.logger.info(
        `Permission ${
          msg.id
        } successfully extended to ${newEffectiveUntil.toISOString()}`
      );
      return { success: true };
    } catch (err) {
      this.logger.error("Error in handleExtendPermission:", err);
      return { success: false, reason: "DB error" };
    }
  }

  private async handleRevokePermission(msg: MsgRevokePermission) {
    try {
      if (!msg.id) {
        this.logger.warn("Missing mandatory parameter: id");
        return { success: false, reason: "Missing mandatory parameter" };
      }

      const now = formatTimestamp(msg.timestamp);
      const applicantPerm = await knex("permissions")
        .where({ id: msg.id })
        .first();
      if (!applicantPerm) {
        this.logger.warn(`Permission ${msg.id} not found`);
        return { success: false, reason: "Permission not found" };
      }

      const caller = msg.creator;

      let authorized = false;
      if (applicantPerm.validator_perm_id) {
        let validatorPermId = applicantPerm.validator_perm_id;
        while (validatorPermId) {
          const validatorPerm = await knex("permissions")
            .where({ id: validatorPermId })
            .first();
          if (!validatorPerm) break;
          if (validatorPerm.grantee === caller) {
            authorized = true;
            break;
          }
          validatorPermId = validatorPerm.validator_perm_id;
        }
      }

      if (!authorized) {
        const cs = await knex("credential_schemas")
          .where({ id: applicantPerm.schema_id })
          .first();
        if (cs) {
          const tr = await knex("trust_registry")
            .where({ id: cs.tr_id })
            .first();
          if (tr?.controller === caller) {
            authorized = true;
          }
        }
      }

      if (!authorized) {
        if (applicantPerm.grantee === caller) {
          authorized = true;
        }
      }

      if (!authorized) {
        this.logger.warn("Caller is not authorized to revoke this permission");
        return { success: false, reason: "Unauthorized caller" };
      }

      await knex.transaction(async (trx) => {
        await trx("permissions").where({ id: msg.id }).update({
          revoked: now,
          revoked_by: caller,
          modified: now,
        });
      });

      this.logger.info(
        `Permission ${msg.id} successfully revoked by ${caller}`
      );
      return { success: true };
    } catch (err) {
      this.logger.error("Error in handleRevokePermission:", err);
      return { success: false, reason: "DB error" };
      // process.exit()
    }
  }

  private async handleStartPermissionVP(msg: MsgStartPermissionVP) {
    try {
      const typeStr = getPermissionTypeString(msg);
      const now = formatTimestamp(msg.timestamp);

      const perm = await knex("permissions")
        .where({ id: msg.validator_perm_id })
        .first();

      if (!perm) {
        this.logger.warn(
          `Permission ${msg.validator_perm_id} not found, skipping VP start`
        );
        return;
      }

      const globalVariables = await getGlobalVariables();
      if (!globalVariables) {
        this.logger.info(
          `Global variables: ${JSON.stringify(globalVariables)}`
        );
      }

      let validationFeesDenom = 0;
      let validationTDDenom = 0;

      if (typeStr !== "HOLDER") {
        const trustUnitPrice = Number(
          globalVariables?.tr?.trust_unit_price ?? 0
        );
        const trustDepositRate = Number(
          globalVariables?.td?.trust_deposit_rate ?? 0
        );

        validationFeesDenom =
          perm?.validation_fees && trustUnitPrice
            ? Number(perm.validation_fees) * trustUnitPrice
            : 0;

        validationTDDenom =
          validationFeesDenom && trustDepositRate
            ? validationFeesDenom * trustDepositRate
            : 0;
      }

      const Entry = {
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
        verification_fees: String((msg as any).verification_fees ?? 0),
        validation_fees: "0",
        issuance_fees: "0",
        deposit: String(validationTDDenom),
        vp_current_deposit: String(validationTDDenom),
        vp_current_fees: String(validationFeesDenom),
        validator_perm_id: msg.validator_perm_id,
        vp_state: "PENDING",
        vp_last_state_change: now,
        modified: now,
        created: now,
      };

      await knex("permissions").insert(Entry);
      this.logger.info(
        `Inserted new VP entry handleStartPermissionVP: ${JSON.stringify(
          Entry
        )}`
      );
    } catch (err) {
      this.logger.error("Error in handleStartPermissionVP:", err);
    }
  }

  public async computeVpExp(perm: any, knex: any): Promise<string | null> {
    const cs = await knex("credential_schemas")
      .where({ id: perm.schema_id })
      .first();

    if (!cs) {
      throw new Error(`CredentialSchema ${perm.schema_id} not found`);
    }

    let validityPeriodField = null;

    switch (perm.type) {
      case "ISSUER_GRANTOR":
        validityPeriodField = cs.issuer_grantor_validation_validity_period;
        break;
      case "VERIFIER_GRANTOR":
        validityPeriodField = cs.verifier_grantor_validation_validity_period;
        break;
      case "ISSUER":
        validityPeriodField = cs.issuer_validation_validity_period;
        break;
      case "VERIFIER":
        validityPeriodField = cs.verifier_validation_validity_period;
        break;
      case "HOLDER":
        validityPeriodField = cs.holder_validation_validity_period;
        break;
      default:
        validityPeriodField = null;
    }

    if (!validityPeriodField) {
      return null;
    }

    const validitySeconds = Number(validityPeriodField);
    const now = new Date();

    let vpExp: Date;

    if (!perm.vp_exp) {
      vpExp = new Date(now.getTime() + validitySeconds * 1000);
    } else {
      vpExp = new Date(
        new Date(perm.vp_exp).getTime() + validitySeconds * 1000
      );
    }

    return vpExp.toISOString();
  }

  private async handleSetPermissionVPToValidated(
    msg: MsgSetPermissionVPToValidated
  ) {
    try {
      const now = formatTimestamp(msg.timestamp);

      const perm = await knex("permissions").where({ id: msg.id }).first();
      if (!perm) {
        this.logger.warn(`Permission ${msg.id} not found`);
        return { success: false, reason: "Permission not found" };
      }

      if (perm.vp_state !== "PENDING") {
        this.logger.warn(`Permission ${msg.id} is not PENDING`);
        return { success: false, reason: "Permission not pending" };
      }

      const isFirstValidation = !perm.effective_from;

      if (msg.country && msg.country.length !== 2) {
        this.logger.warn(`Invalid country code: ${msg.country}`);
        return { success: false, reason: "Invalid country code" };
      }

      if (
        msg.validation_fees < 0 ||
        msg.issuance_fees < 0 ||
        msg.verification_fees < 0
      ) {
        this.logger.warn(`Fees must be >= 0`);
        return { success: false, reason: "Invalid fees" };
      }

      const vpExp = await this.computeVpExp(perm, knex);

      const effectiveUntil =
        msg.effective_until ?? perm.effective_until ?? vpExp ?? null;

      if (
        effectiveUntil &&
        vpExp &&
        new Date(effectiveUntil) > new Date(vpExp)
      ) {
        this.logger.warn(
          `effective_until ${effectiveUntil} exceeds vp_exp ${vpExp}`
        );
        return { success: false, reason: "effective_until exceeds vp_exp" };
      }

      const entry: any = {
        vp_state: "VALIDATED",
        vp_last_state_change: now,
        vp_current_fees: "0",
        vp_current_deposit: "0",
        vp_summary_digest_sri:
          msg.vp_summary_digest_sri ?? perm.vp_summary_digest_sri ?? null,
        vp_exp: vpExp,
        effective_until: effectiveUntil,
        modified: now,
      };

      if (isFirstValidation) {
        entry.validation_fees = msg.validation_fees ?? "0";
        entry.issuance_fees = msg.issuance_fees ?? "0";
        entry.verification_fees = msg.verification_fees ?? "0";
        entry.country = msg.country ?? null;
        entry.effective_from = now;
      } else {
        const feesChanged =
          (msg.validation_fees &&
            msg.validation_fees !== perm.validation_fees) ||
          (msg.issuance_fees && msg.issuance_fees !== perm.issuance_fees) ||
          (msg.verification_fees &&
            msg.verification_fees !== perm.verification_fees);

        const countryChanged = msg.country && msg.country !== perm.country;

        if (feesChanged || countryChanged) {
          this.logger.warn("Cannot change fees or country during renewal");
          return {
            success: false,
            reason: "Cannot change fees/country on renewal",
          };
        }
      }

      await knex("permissions").where({ id: msg.id }).update(entry);

      this.logger.info(`Permission ${msg.id} successfully validated`);
      return { success: true };
    } catch (err) {
      this.logger.error("Error in handleSetPermissionVPToValidated:", err);
      // process.exit();
      return { success: false, reason: "DB error" };
    }
  }

  private async handleRenewPermissionVP(msg: MsgRenewPermissionVP) {
    try {
      const now = formatTimestamp(msg.timestamp);
      const applicantPerm = await knex("permissions")
        .where({ id: msg.id })
        .first();
      if (!applicantPerm) {
        this.logger.warn(`Permission ${msg.id} not found`);
        return { success: false, reason: "Permission not found" };
      }

      if (
        msg.creator &&
        applicantPerm.grantee &&
        msg.creator !== applicantPerm.grantee
      ) {
        this.logger.warn(`Caller ${msg.creator} is not the permission grantee`);
        return { success: false, reason: "Caller is not grantee" };
      }

      const validatorPerm = await knex("permissions")
        .where({ id: applicantPerm.validator_perm_id })
        .first();
      if (!validatorPerm) {
        this.logger.warn(
          `Validator permission ${applicantPerm.validator_perm_id} not found`
        );
        return { success: false, reason: "Validator permission not found" };
      }

      const globalVariables = await getGlobalVariables();
      if (!globalVariables) {
        this.logger.info(
          `Global variables: ${JSON.stringify(globalVariables)}`
        );
      }

      const trustUnitPrice = globalVariables?.tr?.trust_unit_price;
      const trustDepositRate = globalVariables?.td?.trust_deposit_rate;

      if (trustUnitPrice === undefined || trustDepositRate === undefined) {
        this.logger.warn("Global variables not set for fee calculation");
        return { success: false, reason: "Invalid global variables" };
      }

      const validationFeesInDenom =
        Number(validatorPerm.validation_fees) * trustUnitPrice;
      const validationTrustDepositInDenom =
        validationFeesInDenom * trustDepositRate;
      if (
        Number.isNaN(validationFeesInDenom) ||
        Number.isNaN(validationTrustDepositInDenom)
      ) {
        this.logger.warn("Error calculating fees/deposit");
        return { success: false, reason: "Error calculating fees/deposit" };
      }
      await knex.transaction(async (trx) => {
        await trx("permissions")
          .where({ id: msg.id })
          .update({
            vp_state: "PENDING",
            vp_last_state_change: now,
            vp_current_fees: validationFeesInDenom.toString(),
            vp_current_deposit: validationTrustDepositInDenom.toString(),
            deposit: (
              Number(applicantPerm.deposit) + validationTrustDepositInDenom
            ).toString(),
            modified: now,
          });
      });

      this.logger.info(`Permission ${msg.id} successfully renewed`);
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
      const now = formatTimestamp(msg.timestamp);

      const perm = await knex("permissions").where({ id: msg.id }).first();
      if (!perm) {
        this.logger.warn(`Permission ${msg.id} not found`);
        return { success: false, reason: "Permission not found" };
      }

      if (perm.vp_state !== "PENDING") {
        this.logger.warn(`Permission ${msg.id} is not PENDING`);
        return { success: false, reason: "Permission not pending" };
      }

      if (msg.creator && perm.grantee && msg.creator !== perm.grantee) {
        this.logger.warn(`Creator ${msg.creator} is not permission grantee`);
        return { success: false, reason: "Creator is not grantee" };
      }
      const newVpState = perm.vp_exp ? "VALIDATED" : "TERMINATED";
      const refundFees = Number(perm.vp_current_fees) > 0;
      const refundDeposit = Number(perm.vp_current_deposit) > 0;

      if (refundFees) {
        this.logger.info(
          `Refunding ${perm.vp_current_fees} from escrow to ${perm.grantee}`
        );
      }

      if (refundDeposit) {
        this.logger.info(
          `Reducing trust deposit by ${perm.vp_current_deposit} for ${perm.grantee}`
        );
      }

      await knex("permissions").where({ id: msg.id }).update({
        vp_state: newVpState,
        vp_last_state_change: now,
        vp_current_fees: "0",
        vp_current_deposit: "0",
        modified: now,
      });

      this.logger.info(
        `Permission ${msg.id} validation cancelled. New state: ${newVpState}`
      );

      return { success: true };
    } catch (err) {
      this.logger.error("Error in handleCancelPermissionVPLastRequest:", err);
      return { success: false, reason: "DB error" };
      // process.exit();
    }
  }

  private async handleSlashPermissionTrustDeposit(
    msg: MsgSlashPermissionTrustDeposit
  ) {
    try {
      const now = formatTimestamp(msg.timestamp);
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
      const now = formatTimestamp(msg.timestamp);
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
      const now = formatTimestamp(msg.timestamp);
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
