import {
  Action,
  Service,
} from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import { SERVICE } from "../../common";
import ApiResponder from "../../common/utils/apiResponse";
import knex from "../../common/utils/db_connection";

@Service({
  name: SERVICE.V1.PermAPIService.key,
  version: 1,
})
export default class PermAPIService extends BullableService {
  constructor(broker: ServiceBroker) {
    super(broker);
  }

  /**
   * List Permissions [MOD-PERM-QRY-1]
   */
  @Action({
    rest: "GET list",
    params: {
      schema_id: { type: "number", integer: true, optional: true },
      grantee: { type: "string", optional: true },
      did: { type: "string", optional: true },
      perm_id: { type: "number", integer: true, optional: true },
      type: { type: "string", optional: true },

      only_valid: { type: "any", optional: true },
      only_slashed: { type: "any", optional: true },
      only_repaid: { type: "any", optional: true },

      modified_after: { type: "string", optional: true },
      country: { type: "string", optional: true },
      vp_state: { type: "string", optional: true },
      response_max_size: { type: "number", optional: true, default: 64 },
      when: { type: "string", optional: true },
    },
  })
  async listPermissions(ctx: Context<any>) {
    try {
      const p = ctx.params;
      const blockHeight = (ctx.meta as any)?.blockHeight;
      const now = new Date().toISOString();
      const limit = Math.min(Math.max(p.response_max_size || 64, 1), 1024);

      const onlyValid = p.only_valid === "true" || p.only_valid === true;
      const onlySlashed = p.only_slashed === "true" || p.only_slashed === true;
      const onlyRepaid = p.only_repaid === "true" || p.only_repaid === true;

      if (typeof blockHeight === "number") {
        const latestHistorySubquery = knex("permission_history")
          .select("permission_id")
          .select(
            knex.raw(
              `ROW_NUMBER() OVER (PARTITION BY permission_id ORDER BY height DESC, created_at DESC) as rn`
            )
          )
          .where("height", "<=", blockHeight)
          .as("ranked");

        const permIdsAtHeight = await knex
          .from(latestHistorySubquery)
          .select("permission_id")
          .where("rn", 1)
          .then((rows: any[]) => rows.map((r: any) => r.permission_id));

        if (permIdsAtHeight.length === 0) {
          return ApiResponder.success(ctx, { permissions: [] }, 200);
        }

        const permissions = await Promise.all(
          permIdsAtHeight.map(async (permId: string) => {
            const historyRecord = await knex("permission_history")
              .where({ permission_id: permId })
              .where("height", "<=", blockHeight)
              .orderBy("height", "desc")
              .orderBy("created_at", "desc")
              .first();

            if (!historyRecord) return null;

            return {
              id: historyRecord.permission_id,
              schema_id: historyRecord.schema_id,
              grantee: historyRecord.grantee,
              did: historyRecord.did,
              created_by: historyRecord.created_by,
              validator_perm_id: historyRecord.validator_perm_id,
              type: historyRecord.type,
              country: historyRecord.country,
              vp_state: historyRecord.vp_state,
              revoked: historyRecord.revoked,
              revoked_by: historyRecord.revoked_by,
              slashed: historyRecord.slashed,
              slashed_by: historyRecord.slashed_by,
              repaid: historyRecord.repaid,
              repaid_by: historyRecord.repaid_by,
              extended: historyRecord.extended,
              extended_by: historyRecord.extended_by,
              effective_from: historyRecord.effective_from,
              effective_until: historyRecord.effective_until,
              validation_fees: historyRecord.validation_fees,
              issuance_fees: historyRecord.issuance_fees,
              verification_fees: historyRecord.verification_fees,
              deposit: historyRecord.deposit,
              slashed_deposit: historyRecord.slashed_deposit,
              repaid_deposit: historyRecord.repaid_deposit,
              vp_last_state_change: historyRecord.vp_last_state_change,
              vp_current_fees: historyRecord.vp_current_fees,
              vp_current_deposit: historyRecord.vp_current_deposit,
              vp_summary_digest_sri: historyRecord.vp_summary_digest_sri,
              vp_exp: historyRecord.vp_exp,
              vp_validator_deposit: historyRecord.vp_validator_deposit,
              vp_term_requested: historyRecord.vp_term_requested,
              created: historyRecord.created,
              modified: historyRecord.modified,
            };
          })
        );

        let filteredPermissions = permissions.filter((perm): perm is NonNullable<typeof permissions[0]> => perm !== null);

        if (p.schema_id !== undefined) filteredPermissions = filteredPermissions.filter(perm => perm.schema_id === p.schema_id);
        if (p.grantee) filteredPermissions = filteredPermissions.filter(perm => perm.grantee === p.grantee);
        if (p.did) filteredPermissions = filteredPermissions.filter(perm => perm.did === p.did);
        if (p.perm_id !== undefined) filteredPermissions = filteredPermissions.filter(perm => perm.validator_perm_id === p.perm_id);
        if (p.type) filteredPermissions = filteredPermissions.filter(perm => perm.type === p.type);
        if (p.country) filteredPermissions = filteredPermissions.filter(perm => perm.country === p.country);
        if (p.vp_state) filteredPermissions = filteredPermissions.filter(perm => perm.vp_state === p.vp_state);

        if (p.modified_after) {
          const ts = new Date(p.modified_after);
          if (!Number.isNaN(ts.getTime())) {
            filteredPermissions = filteredPermissions.filter(perm => new Date(perm.modified) > ts);
          }
        }
        if (p.when) {
          const whenTs = new Date(p.when);
          if (!Number.isNaN(whenTs.getTime())) {
            filteredPermissions = filteredPermissions.filter(perm => new Date(perm.modified) <= whenTs);
          }
        }

        if (onlyValid) {
          filteredPermissions = filteredPermissions.filter(perm => {
            const isNotRevoked = !perm.revoked;
            const isNotSlashedOrRepaid = !perm.slashed || perm.repaid;
            const isEffective = (!perm.effective_until || new Date(perm.effective_until) > new Date(now)) &&
              (!perm.effective_from || new Date(perm.effective_from) <= new Date(now));
            return isNotRevoked && isNotSlashedOrRepaid && isEffective;
          });
        }

        if (p.only_slashed !== undefined) {
          if (onlySlashed) {
            filteredPermissions = filteredPermissions.filter(perm => perm.slashed !== null);
          } else {
            filteredPermissions = filteredPermissions.filter(perm => perm.slashed === null);
          }
        }

        if (p.only_repaid !== undefined) {
          if (onlyRepaid) {
            filteredPermissions = filteredPermissions.filter(perm => perm.repaid !== null);
          } else {
            filteredPermissions = filteredPermissions.filter(perm => perm.repaid === null);
          }
        }

        filteredPermissions.sort((a, b) => new Date(a.modified).getTime() - new Date(b.modified).getTime());
        filteredPermissions = filteredPermissions.slice(0, limit);

        return ApiResponder.success(ctx, { permissions: filteredPermissions }, 200);
      }

      const query = knex("permissions").select("*");

      if (p.schema_id !== undefined) query.where("schema_id", p.schema_id);
      if (p.grantee) query.where("grantee", p.grantee);
      if (p.did) query.where("did", p.did);
      if (p.perm_id !== undefined) query.where("validator_perm_id", p.perm_id);
      if (p.type) query.where("type", p.type);
      if (p.country) query.where("country", p.country);
      if (p.vp_state) query.where("vp_state", p.vp_state);

      if (p.modified_after) {
        const ts = new Date(p.modified_after);
        if (!Number.isNaN(ts.getTime()))
          query.where("modified", ">", ts.toISOString());
      }
      if (p.when) {
        const whenTs = new Date(p.when);
        if (!Number.isNaN(whenTs.getTime()))
          query.where("modified", "<=", whenTs.toISOString());
      }

      if (onlyValid) {
        query.where((qb) => {
          qb.whereNull("revoked")
            .andWhere((q) => q.whereNull("slashed").orWhereNotNull("repaid"))
            .andWhere((q) =>
              q
                .whereNull("effective_until")
                .orWhere("effective_until", ">", now)
            )
            .andWhere((q) =>
              q.whereNull("effective_from").orWhere("effective_from", "<=", now)
            );
        });
      }

      if (p.only_slashed !== undefined) {
        if (onlySlashed) query.whereNotNull("slashed");
        else query.whereNull("slashed");
      }

      if (p.only_repaid !== undefined) {
        if (onlyRepaid) query.whereNotNull("repaid");
        else query.whereNull("repaid");
      }

      const results = await query.orderBy("modified", "asc").limit(limit);
      return ApiResponder.success(ctx, { permissions: results }, 200);
    } catch (err: any) {
      this.logger.error("Error in listPermissions:", err);
      return ApiResponder.error(ctx, "Failed to list permissions", 500);
    }
  }

  @Action({
    rest: "GET get/:id",
    params: {
      id: { type: "string", pattern: /^[0-9]+$/ },
    },
  })
  async getPermission(ctx: Context<{ id: string }>) {
    try {
      const id = ctx.params.id;
      const blockHeight = (ctx.meta as any)?.blockHeight;

      if (typeof blockHeight === "number") {
        const historyRecord = await knex("permission_history")
          .where({ permission_id: id })
          .where("height", "<=", blockHeight)
          .orderBy("height", "desc")
          .orderBy("created_at", "desc")
          .first();

        if (!historyRecord) {
          return ApiResponder.error(ctx, "Permission not found", 404);
        }

        const historicalPermission = {
          id: historyRecord.permission_id,
          schema_id: historyRecord.schema_id,
          grantee: historyRecord.grantee,
          did: historyRecord.did,
          created_by: historyRecord.created_by,
          validator_perm_id: historyRecord.validator_perm_id,
          type: historyRecord.type,
          country: historyRecord.country,
          vp_state: historyRecord.vp_state,
          revoked: historyRecord.revoked,
          revoked_by: historyRecord.revoked_by,
          slashed: historyRecord.slashed,
          slashed_by: historyRecord.slashed_by,
          repaid: historyRecord.repaid,
          repaid_by: historyRecord.repaid_by,
          extended: historyRecord.extended,
          extended_by: historyRecord.extended_by,
          effective_from: historyRecord.effective_from,
          effective_until: historyRecord.effective_until,
          validation_fees: historyRecord.validation_fees,
          issuance_fees: historyRecord.issuance_fees,
          verification_fees: historyRecord.verification_fees,
          deposit: historyRecord.deposit,
          slashed_deposit: historyRecord.slashed_deposit,
          repaid_deposit: historyRecord.repaid_deposit,
          vp_last_state_change: historyRecord.vp_last_state_change,
          vp_current_fees: historyRecord.vp_current_fees,
          vp_current_deposit: historyRecord.vp_current_deposit,
          vp_summary_digest_sri: historyRecord.vp_summary_digest_sri,
          vp_exp: historyRecord.vp_exp,
          vp_validator_deposit: historyRecord.vp_validator_deposit,
          vp_term_requested: historyRecord.vp_term_requested,
          created: historyRecord.created,
          modified: historyRecord.modified,
        };

        return ApiResponder.success(ctx, { permission: historicalPermission }, 200);
      }

      const permission = await knex("permissions").where("id", id).first();
      if (!permission) {
        return ApiResponder.error(ctx, "Permission not found", 404);
      }
      return ApiResponder.success(ctx, { permission: permission }, 200);
    } catch (err: any) {
      this.logger.error("Error in getPermission:", err);
      return ApiResponder.error(ctx, "Failed to get permission", 500);
    }
  }

  @Action({
    rest: "GET history/:id",
    params: {
      id: { type: "string", pattern: /^[0-9]+$/ },
    },
  })
  async getPermissionHistory(ctx: Context<{ id: string }>) {
    try {
      const history = await knex("permission_history")
        .where("permission_id", ctx.params.id)
        .orderBy("height", "asc")
        .orderBy("created_at", "asc");
      if (!history.length) {
        return ApiResponder.error(ctx, "Permission history not found", 404);
      }
      return ApiResponder.success(ctx, { history }, 200);
    } catch (err: any) {
      this.logger.error("Error in getPermissionHistory:", err);
      return ApiResponder.error(ctx, "Failed to get permission history", 500);
    }
  }

  @Action({
    rest: "GET beneficiaries",
    params: {
      issuer_perm_id: { type: "number", integer: true, optional: true },
      verifier_perm_id: { type: "number", integer: true, optional: true },
    },
  })
  async findBeneficiaries(
    ctx: Context<{ issuer_perm_id?: number; verifier_perm_id?: number }>
  ) {
    const { issuer_perm_id: issuerPermId, verifier_perm_id: verifierPermId } =
      ctx.params;
    const blockHeight = (ctx.meta as any)?.blockHeight;

    if (!issuerPermId && !verifierPermId) {
      return ApiResponder.error(
        ctx,
        "issuer_perm_id or verifier_perm_id must be set",
        400
      );
    }

    const foundPermSet = new Set<any>();

    const loadPerm = async (permId: number) => {
      if (typeof blockHeight === "number") {
        const historyRecord = await knex("permission_history")
          .where({ permission_id: String(permId) })
          .where("height", "<=", blockHeight)
          .orderBy("height", "desc")
          .orderBy("created_at", "desc")
          .first();
        if (!historyRecord) throw new Error(`Permission ${permId} not found`);
        return {
          id: historyRecord.permission_id,
          schema_id: historyRecord.schema_id,
          grantee: historyRecord.grantee,
          did: historyRecord.did,
          created_by: historyRecord.created_by,
          validator_perm_id: historyRecord.validator_perm_id,
          type: historyRecord.type,
          country: historyRecord.country,
          vp_state: historyRecord.vp_state,
          revoked: historyRecord.revoked,
          revoked_by: historyRecord.revoked_by,
          slashed: historyRecord.slashed,
          slashed_by: historyRecord.slashed_by,
          repaid: historyRecord.repaid,
          repaid_by: historyRecord.repaid_by,
          extended: historyRecord.extended,
          extended_by: historyRecord.extended_by,
          effective_from: historyRecord.effective_from,
          effective_until: historyRecord.effective_until,
          validation_fees: historyRecord.validation_fees,
          issuance_fees: historyRecord.issuance_fees,
          verification_fees: historyRecord.verification_fees,
          deposit: historyRecord.deposit,
          slashed_deposit: historyRecord.slashed_deposit,
          repaid_deposit: historyRecord.repaid_deposit,
          vp_last_state_change: historyRecord.vp_last_state_change,
          vp_current_fees: historyRecord.vp_current_fees,
          vp_current_deposit: historyRecord.vp_current_deposit,
          vp_summary_digest_sri: historyRecord.vp_summary_digest_sri,
          vp_exp: historyRecord.vp_exp,
          vp_validator_deposit: historyRecord.vp_validator_deposit,
          vp_term_requested: historyRecord.vp_term_requested,
          created: historyRecord.created,
          modified: historyRecord.modified,
        };
      }
      const perm = await knex("permissions").where("id", permId).first();
      if (!perm) throw new Error(`Permission ${permId} not found`);
      return perm;
    };

    const addAncestors = async (perm: any) => {
      let currentPerm = perm;
      while (currentPerm.validator_perm_id) {
        const parent = await loadPerm(currentPerm.validator_perm_id);
        if (!parent.revoked && !parent.slashed) {
          foundPermSet.add(parent);
        }
        currentPerm = parent;
      }
    };

    try {
      if (issuerPermId) {
        const issuerPerm = await loadPerm(issuerPermId);
        if (!verifierPermId) {
          await addAncestors(issuerPerm);
        }
      }

      if (verifierPermId) {
        const verifierPerm = await loadPerm(verifierPermId);
        if (issuerPermId) {
          const issuerPerm = await loadPerm(issuerPermId);
          foundPermSet.add(issuerPerm);
        }
        await addAncestors(verifierPerm);
      }

      return ApiResponder.success(ctx, { permissions: Array.from(foundPermSet) }, 200);
    } catch (err: any) {
      this.logger.error("Error in findBeneficiaries:", err);
      return ApiResponder.error(ctx, "Failed to find beneficiaries", 500);
    }
  }

  @Action({
    rest: "GET permission-session/:id",
    params: {
      id: { type: "string", pattern: /^[0-9a-fA-F-]+$/ },
    },
  })
  async getPermissionSession(ctx: Context<{ id: string }>) {
    try {
      const { id } = ctx.params;
      const blockHeight = (ctx.meta as any)?.blockHeight;

      if (typeof blockHeight === "number") {
        const historyRecord = await knex("permission_session_history")
          .where({ session_id: id })
          .where("height", "<=", blockHeight)
          .orderBy("height", "desc")
          .orderBy("created_at", "desc")
          .first();

        if (!historyRecord) {
          return ApiResponder.error(ctx, "PermissionSession not found", 404);
        }

        const historicalSession = {
          id: historyRecord.session_id,
          controller: historyRecord.controller,
          agent_perm_id: historyRecord.agent_perm_id,
          wallet_agent_perm_id: historyRecord.wallet_agent_perm_id,
          authz: historyRecord.authz,
          created: historyRecord.created,
          modified: historyRecord.modified,
        };

        return ApiResponder.success(ctx, { session: historicalSession }, 200);
      }

      const session = await knex("permission_sessions").where("id", id).first();
      if (!session)
        return ApiResponder.error(ctx, "PermissionSession not found", 404);
      return ApiResponder.success(ctx, { session: session }, 200);
    } catch (err: any) {
      this.logger.error("Error in getPermissionSession:", err);
      return ApiResponder.error(ctx, "Failed to get PermissionSession", 500);
    }
  }

  @Action({
    rest: "GET permission-session-history/:id",
    params: {
      id: { type: "string", pattern: /^[0-9a-fA-F-]+$/ },
    },
  })
  async getPermissionSessionHistory(ctx: Context<{ id: string }>) {
    try {
      const history = await knex("permission_session_history")
        .where("session_id", ctx.params.id)
        .orderBy("height", "asc")
        .orderBy("created_at", "asc");
      if (!history.length) {
        return ApiResponder.error(
          ctx,
          "PermissionSession history not found",
          404
        );
      }
      return ApiResponder.success(ctx, { history }, 200);
    } catch (err: any) {
      this.logger.error("Error in getPermissionSessionHistory:", err);
      return ApiResponder.error(
        ctx,
        "Failed to get PermissionSession history",
        500
      );
    }
  }

  @Action({
    rest: "GET permission-sessions",
    params: {
      modified_after: { type: "string", optional: true },
      response_max_size: { type: "number", optional: true, default: 64 },
    },
  })
  async listPermissionSessions(ctx: Context<any>) {
    try {
      const {
        modified_after: modifiedAfter,
        response_max_size: responseMaxSize,
      } = ctx.params;
      const blockHeight = (ctx.meta as any)?.blockHeight;
      const limit = Math.min(Math.max(responseMaxSize || 64, 1), 1024);

      if (typeof blockHeight === "number") {
        const latestHistorySubquery = knex("permission_session_history")
          .select("session_id")
          .select(
            knex.raw(
              `ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY height DESC, created_at DESC) as rn`
            )
          )
          .where("height", "<=", blockHeight)
          .as("ranked");

        const sessionIdsAtHeight = await knex
          .from(latestHistorySubquery)
          .select("session_id")
          .where("rn", 1)
          .then((rows: any[]) => rows.map((r: any) => r.session_id));

        if (sessionIdsAtHeight.length === 0) {
          return ApiResponder.success(ctx, { sessions: [] }, 200);
        }

        const sessions = await Promise.all(
          sessionIdsAtHeight.map(async (sessionId: string) => {
            const historyRecord = await knex("permission_session_history")
              .where({ session_id: sessionId })
              .where("height", "<=", blockHeight)
              .orderBy("height", "desc")
              .orderBy("created_at", "desc")
              .first();

            if (!historyRecord) return null;

            return {
              id: historyRecord.session_id,
              controller: historyRecord.controller,
              agent_perm_id: historyRecord.agent_perm_id,
              wallet_agent_perm_id: historyRecord.wallet_agent_perm_id,
              authz: historyRecord.authz,
              created: historyRecord.created,
              modified: historyRecord.modified,
            };
          })
        );

        let filteredSessions = sessions.filter((sess): sess is NonNullable<typeof sessions[0]> => sess !== null);

        if (modifiedAfter) {
          const ts = new Date(modifiedAfter);
          if (!Number.isNaN(ts.getTime())) {
            filteredSessions = filteredSessions.filter(sess => new Date(sess.modified) > ts);
          }
        }

        filteredSessions.sort((a, b) => new Date(a.modified).getTime() - new Date(b.modified).getTime());
        filteredSessions = filteredSessions.slice(0, limit);

        return ApiResponder.success(ctx, { sessions: filteredSessions }, 200);
      }

      const query = knex("permission_sessions").select("*");
      if (modifiedAfter) {
        const ts = new Date(modifiedAfter);
        if (!Number.isNaN(ts.getTime()))
          query.where("modified", ">", ts.toISOString());
      }

      const results = await query.orderBy("modified", "asc").limit(limit);
      return ApiResponder.success(ctx, { sessions: results }, 200);
    } catch (err: any) {
      this.logger.error("Error in listPermissionSessions:", err);
      return ApiResponder.error(ctx, "Failed to list PermissionSessions", 500);
    }
  }
}
