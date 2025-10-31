import { Service, ServiceBroker } from "moleculer";
import { formatTimestamp } from "../../common/utils/date_utils";
import knex from "../../common/utils/db_connection";
import { SERVICE } from "../../common";
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
  }
  private async handleExtendPermission(msg: MsgExtendPermission) {
    try {
      if (!msg.id || !msg.effective_until) {
        this.logger.warn("Missing mandatory parameter: id or effective_until");
        return { success: false, reason: "Missing mandatory parameter" };
      }

      const idNum = Number(msg.id);
      if (!Number.isInteger(idNum) || idNum <= 0) {
        this.logger.warn(`Invalid id: ${msg.id}. Must be a valid uint64.`);
        return { success: false, reason: "Invalid permission ID" };
      }

      const newEffectiveUntil = new Date(msg.effective_until);
      if (Number.isNaN(newEffectiveUntil.getTime())) {
        this.logger.warn(
          `Invalid effective_until timestamp: ${msg.effective_until}`
        );
        return { success: false, reason: "Invalid effective_until timestamp" };
      }

      const applicantPerm = await knex("permissions")
        .where({ id: msg.id })
        .first();
      if (!applicantPerm) {
        this.logger.warn(`Permission ${msg.id} not found`);
        return { success: false, reason: "Permission not found" };
      }

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

      const caller = msg.creator;

      if (
        !applicantPerm.validator_perm_id &&
        applicantPerm.type === "ECOSYSTEM"
      ) {
        if (caller !== applicantPerm.grantee) {
          this.logger.warn("Only grantee can extend ECOSYSTEM permission");
          return { success: false, reason: "Unauthorized caller" };
        }
      } else if (applicantPerm.validator_perm_id) {
        const validatorPerm = await knex("permissions")
          .where({ id: applicantPerm.validator_perm_id })
          .first();

        if (!validatorPerm) {
          this.logger.warn(
            `Validator permission ${applicantPerm.validator_perm_id} not found`
          );
          return { success: false, reason: "Validator permission not found" };
        }

        if (validatorPerm.type !== "ECOSYSTEM") {
          this.logger.warn("Validator permission must be of type ECOSYSTEM");
          return {
            success: false,
            reason: "Invalid validator permission type",
          };
        }

        if (caller !== applicantPerm.grantee) {
          this.logger.warn("Only grantee can extend self-created permission");
          return { success: false, reason: "Unauthorized caller" };
        }
      } else {
        this.logger.warn("Invalid permission structure");
        return { success: false, reason: "Invalid permission structure" };
      }

      const now = formatTimestamp(msg.timestamp);
      await knex.transaction(async (trx) => {
        await trx("permissions").where({ id: msg.id }).update({
          effective_until: newEffectiveUntil.toISOString(),
          extended: now,
          modified: now,
          extended_by: caller,
        });
      });

      this.logger.info(
        `✅ Permission ${
          msg.id
        } extended until ${newEffectiveUntil.toISOString()} by ${caller}`
      );

      return { success: true };
    } catch (err) {
      this.logger.error("❌ Error in handleExtendPermission:", err);
      return { success: false, reason: "Internal server error" };
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

      const vpValidatorDeposit = isFirstValidation
        ? perm.vp_current_deposit
        : perm.vp_validator_deposit;

      const entry: any = {
        vp_state: "VALIDATED",
        vp_last_state_change: now,
        vp_current_fees: "0",
        vp_current_deposit: "0",
        vp_validator_deposit: vpValidatorDeposit,
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

        this.logger.info(
          `Setting initial vp_validator_deposit: ${vpValidatorDeposit} for permission ${msg.id}`
        );
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

        this.logger.info(
          `Preserving existing vp_validator_deposit: ${vpValidatorDeposit} for permission ${msg.id} renewal`
        );
      }

      await knex("permissions").where({ id: msg.id }).update(entry);

      this.logger.info(`Permission ${msg.id} successfully validated`);
      return { success: true };
    } catch (err) {
      this.logger.error("Error in handleSetPermissionVPToValidated:", err);
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

      const vpValidatorDeposit =
        newVpState === "TERMINATED" ? "0" : perm.vp_validator_deposit;

      await knex("permissions").where({ id: msg.id }).update({
        vp_state: newVpState,
        vp_last_state_change: now,
        vp_current_fees: "0",
        vp_current_deposit: "0",
        vp_validator_deposit: vpValidatorDeposit,
        modified: now,
      });

      this.logger.info(
        `Permission ${msg.id} validation cancelled. New state: ${newVpState}`
      );

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
      if (!msg.id || msg.amount == null) {
        this.logger.warn("Missing mandatory parameter: id or amount");
        return { success: false, reason: "Missing mandatory parameter" };
      }

      const idNum = Number(msg.id);
      if (!Number.isInteger(idNum) || idNum <= 0) {
        this.logger.warn(`Invalid id: ${msg.id}. Must be uint64`);
        return { success: false, reason: "Invalid permission ID" };
      }

      const amountNum = Number(msg.amount);
      if (Number.isNaN(amountNum) || amountNum <= 0) {
        this.logger.warn(`Invalid amount: ${msg.amount}`);
        return { success: false, reason: "Invalid amount" };
      }

      const perm = await knex("permissions").where({ id: msg.id }).first();
      if (!perm) {
        this.logger.warn(`Permission ${msg.id} not found`);
        return { success: false, reason: "Permission not found" };
      }

      const deposit = Number(perm.deposit || 0);
      if (amountNum > deposit) {
        this.logger.warn(
          `Slash amount ${amountNum} exceeds deposit ${deposit}`
        );
        return { success: false, reason: "Amount exceeds deposit" };
      }

      let isAuthorized = false;
      const caller = msg.creator;

      let validatorPerm = perm;
      while (validatorPerm && validatorPerm.validator_perm_id) {
        validatorPerm = await knex("permissions")
          .where({ id: validatorPerm.validator_perm_id })
          .first();
        if (validatorPerm && validatorPerm.grantee === caller) {
          isAuthorized = true;
          break;
        }
      }

      if (!isAuthorized && perm.schema_id) {
        const schema = await knex("credential_schemas")
          .where({ id: perm.schema_id })
          .first();
        if (schema && schema.tr_id) {
          const tr = await knex("trust_registry")
            .where({ id: schema.tr_id })
            .first();
          if (tr && tr.controller === caller) {
            isAuthorized = true;
          }
        }
      }

      if (!isAuthorized) {
        this.logger.warn("Unauthorized caller for slash operation");
        return { success: false, reason: "Unauthorized caller" };
      }

      const now = formatTimestamp(msg.timestamp);
      const prevSlashed = Number(perm.slashed_deposit || 0);

      await knex.transaction(async (trx) => {
        await trx("permissions")
          .where({ id: msg.id })
          .update({
            slashed: now,
            slashed_by: caller,
            slashed_deposit: String(prevSlashed + amountNum),
            modified: now,
          });
      });

      try {
        const schema = await knex("permissions").where({ id: msg.id }).first();
        const account = schema?.grantee;
        if (account) {
          await (this as any).broker.call(
            `${SERVICE.V1.TrustDepositDatabaseService.path}.slashPermTrustDeposit`,
            {
              account,
              amount: String(amountNum),
              ts: now,
            }
          );
        }
      } catch (err) {
        this.logger.warn("TD processor slash call failed, continuing: ", err);
      }
      this.logger.info(
        `✅ Permission ${msg.id} slashed by ${caller} amount ${amountNum}`
      );

      return { success: true };
    } catch (err) {
      this.logger.error("❌ Error in handleSlashPermissionTrustDeposit:", err);
      return { success: false, reason: "Internal server error" };
    }
  }

  private async handleRepayPermissionSlashedTrustDeposit(
    msg: MsgRepayPermissionSlashedTrustDeposit
  ) {
    try {
      if (!msg.id) {
        this.logger.warn("Missing mandatory parameter: id");
        return { success: false, reason: "Missing mandatory parameter" };
      }

      const idNum = Number(msg.id);
      if (!Number.isInteger(idNum) || idNum <= 0) {
        this.logger.warn(`Invalid id: ${msg.id}. Must be a valid uint64.`);
        return { success: false, reason: "Invalid permission ID" };
      }

      const perm = await knex("permissions").where({ id: msg.id }).first();
      if (!perm) {
        this.logger.warn(`Permission ${msg.id} not found`);
        return { success: false, reason: "Permission not found" };
      }

      const slashedDeposit = Number(perm.slashed_deposit || 0);
      if (slashedDeposit <= 0) {
        this.logger.warn(
          `Permission ${msg.id} has no slashed deposit to repay`
        );
        return { success: false, reason: "No slashed deposit to repay" };
      }

      const now = formatTimestamp(msg.timestamp);

      await knex.transaction(async (trx) => {
        await trx("permissions").where({ id: msg.id }).update({
          repaid: now,
          repaid_by: msg.creator,
          repaid_deposit: slashedDeposit,
          modified: now,
        });
      });
      this.logger.info(
        `✅ Permission ${msg.id} slashed deposit (${slashedDeposit}) repaid by ${msg.creator}`
      );

      return { success: true };
    } catch (err) {
      this.logger.error(
        "❌ Error in handleRepayPermissionSlashedTrustDeposit:",
        err
      );
      return { success: false, reason: "DB error" };
    }
  }

  private async handleCreateOrUpdatePermissionSession(
    msg: MsgCreateOrUpdatePermissionSession
  ) {
    const trx = await knex.transaction();
    try {
      const now = formatTimestamp(msg.timestamp);

      if (!msg.id || !msg.agent_perm_id || !msg.wallet_agent_perm_id) {
        throw new Error("Missing mandatory parameters");
      }
      if (!msg.issuer_perm_id && !msg.verifier_perm_id) {
        throw new Error(
          "At least one of issuer_perm_id or verifier_perm_id must be provided"
        );
      }

      const [agentPerm, walletAgentPerm, issuerPerm, verifierPerm] =
        await Promise.all([
          knex("permissions").where({ id: msg.agent_perm_id }).first(),
          knex("permissions").where({ id: msg.wallet_agent_perm_id }).first(),
          msg.issuer_perm_id
            ? knex("permissions").where({ id: msg.issuer_perm_id }).first()
            : null,
          msg.verifier_perm_id
            ? knex("permissions").where({ id: msg.verifier_perm_id }).first()
            : null,
        ]);

      if (!agentPerm || !walletAgentPerm) {
        throw new Error("Agent or Wallet Agent permission not found");
      }

      if (msg.issuer_perm_id && issuerPerm?.type !== "ISSUER") {
        throw new Error("Invalid issuer permission type");
      }
      if (msg.verifier_perm_id && verifierPerm?.type !== "VERIFIER") {
        throw new Error("Invalid verifier permission type");
      }

      const existing = await trx("permission_sessions")
        .where({ id: msg.id })
        .first();
      const authzEntry = {
        issuer_perm_id: msg.issuer_perm_id || null,
        verifier_perm_id: msg.verifier_perm_id || null,
        wallet_agent_perm_id: msg.wallet_agent_perm_id,
      };

      if (!existing) {
        await trx("permission_sessions").insert({
          id: msg.id,
          controller: msg.creator,
          agent_perm_id: msg.agent_perm_id,
          wallet_agent_perm_id: msg.wallet_agent_perm_id,
          authz: JSON.stringify([authzEntry]),
          created: now,
          modified: now,
        });
      } else {
        let existingAuthz: any[] = [];
        try {
          existingAuthz = JSON.parse(existing.authz || "[]");
        } catch {
          existingAuthz = [];
        }

        existingAuthz.push(authzEntry);

        await trx("permission_sessions")
          .where({ id: msg.id })
          .update({
            authz: JSON.stringify(existingAuthz),
            modified: now,
          });
      }

      await trx.commit();
      return { success: true };
    } catch (err) {
      await trx.rollback();
      this.logger.error("Error in handleCreateOrUpdatePermissionSession:", err);
      return { success: false, reason: err || "DB error" };
    }
  }
}
