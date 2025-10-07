import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
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

      // Convert string booleans to actual booleans
      const onlyValid = p.only_valid === "true" || p.only_valid === true;
      const onlySlashed = p.only_slashed === "true" || p.only_slashed === true;
      const onlyRepaid = p.only_repaid === "true" || p.only_repaid === true;

      const limit = Math.min(Math.max(p.response_max_size || 64, 1), 1024);
      const now = new Date().toISOString();

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
        if (!Number.isNaN(ts.getTime())) query.where("modified", ">", ts.toISOString());
      }
      if (p.when) {
        const whenTs = new Date(p.when);
        if (!Number.isNaN(whenTs.getTime())) query.where("modified", "<=", whenTs.toISOString());
      }

      // Boolean filters with logic
      if (onlyValid) {
        query.where((qb) => {
          qb.whereNull("revoked")
            .andWhere((q) => q.whereNull("slashed").orWhereNotNull("repaid"))
            .andWhere((q) => q.whereNull("effective_until").orWhere("effective_until", ">", now))
            .andWhere((q) => q.whereNull("effective_from").orWhere("effective_from", "<=", now));
        });
      }

      if (onlySlashed) query.whereNotNull("slashed");
      if (onlySlashed === false) query.whereNull("slashed");

      if (onlyRepaid) query.whereNotNull("repaid");
      if (onlyRepaid === false) query.whereNull("repaid");

      const results = await query.orderBy("modified", "asc").limit(limit);
      return ApiResponder.success(ctx, results, 200);
    } catch (err: any) {
      this.logger.error("Error in listPermissions:", err);
      return ApiResponder.error(ctx, "Failed to list permissions", 500);
    }
  }

  /**
   * Get Permission by ID [MOD-PERM-QRY-2]
   */
  @Action({
    rest: "GET get/:id",
    params: {
      id: { type: "string", pattern: /^[0-9]+$/ }, // uint64 check
    },
  })
  async getPermissionById(ctx: Context<{ id: string }>) {
    try {
      const id = ctx.params.id;
      const permission = await knex("permissions").where("id", id).first();
      if (!permission) {
        return ApiResponder.error(ctx, "Permission not found", 404);
      }
      return ApiResponder.success(ctx, permission, 200);
    } catch (err: any) {
      this.logger.error("Error in getPermissionById:", err);
      return ApiResponder.error(ctx, "Failed to get permission", 500);
    }
  }
}
