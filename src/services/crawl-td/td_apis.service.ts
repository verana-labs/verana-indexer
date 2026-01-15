import {
  Action,
  Service,
} from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import { ModulesParamsNamesTypes, MODULE_DISPLAY_NAMES, SERVICE } from "../../common";
import ApiResponder from "../../common/utils/apiResponse";
import knex from "../../common/utils/db_connection";
import TrustDeposit from "../../models/trust_deposit";

@Service({
  name: SERVICE.V1.TrustDepositApiService.key,
  version: 1,
})
export default class TrustDepositApiService extends BullableService {
  public constructor(public broker: ServiceBroker) {
    super(broker);
  }

  @Action({
    name: "getTrustDeposit",
    params: {
      account: { type: "string", min: 5 },
    },
  })
  public async getTrustDeposit(ctx: Context<{ account: string }>) {
    try {
      const { account } = ctx.params;
      const blockHeight = (ctx.meta as any)?.blockHeight;

      if (!this.isValidAccount(account)) {
        this.logger.warn(`Invalid account format: ${account}`);
        return ApiResponder.error(ctx, "Invalid account format", 400);
      }

      // If AtBlockHeight is provided, query historical state
      if (typeof blockHeight === "number") {
        const historyRecord = await knex("trust_deposit_history")
          .where({ account })
          .where("height", "<=", blockHeight)
          .orderBy("height", "desc")
          .orderBy("created_at", "desc")
          .first();

        if (!historyRecord) {
          this.logger.info(`No trust deposit found for account: ${account}`);
          return ApiResponder.error(
            ctx,
            `No trust deposit found for account: ${account}`,
            404
          );
        }

        const result = {
          trust_deposit: {
            account: historyRecord.account,
            share: historyRecord.share?.toString() || "0",
            amount: historyRecord.amount?.toString() || "0",
            claimable: historyRecord.claimable?.toString() || "0",
            slashed_deposit: historyRecord.slashed_deposit?.toString() || "0",
            repaid_deposit: historyRecord.repaid_deposit?.toString() || "0",
            last_slashed: historyRecord.last_slashed,
            last_repaid: historyRecord.last_repaid,
            slash_count: historyRecord.slash_count || 0,
            last_repaid_by: historyRecord.last_repaid_by || "",
          },
        };

        return ApiResponder.success(ctx, result, 200);
      }

      // Otherwise, return latest state
      const trustDeposit = await TrustDeposit.query().findOne({ account });

      if (!trustDeposit) {
        this.logger.info(`No trust deposit found for account: ${account}`);
        return ApiResponder.error(
          ctx,
          `No trust deposit found for account: ${account}`,
          404
        );
      }
      const result = {
        trust_deposit: {
          account: trustDeposit.account,
          share: trustDeposit.share,
          amount: trustDeposit.amount,
          claimable: trustDeposit.claimable,
          slashed_deposit: trustDeposit.slashed_deposit,
          repaid_deposit: trustDeposit.repaid_deposit,
          last_slashed: trustDeposit.last_slashed,
          last_repaid: trustDeposit.last_repaid,
          slash_count: trustDeposit.slash_count,
          last_repaid_by: trustDeposit.last_repaid_by,
        },
      }
      return ApiResponder.success(
        ctx,
        result,
        200
      );
    } catch (err: any) {
      this.logger.error("Error in getTrustDeposit:", err);
      return ApiResponder.error(ctx, "Internal Server Error", 500);
    }
  }

  @Action({
    name: "getModuleParams",
  })
  public async getModuleParams(ctx: Context) {
    const { getModuleParamsAction } = await import("../../common/utils/params_service");
    return getModuleParamsAction(ctx, ModulesParamsNamesTypes.TD, MODULE_DISPLAY_NAMES.TRUST_DEPOSIT);
  }

  @Action({
    name: "getTrustDepositHistory",
    params: {
      account: { type: "string", min: 5 },
    },
  })
  public async getTrustDepositHistory(ctx: Context<{ account: string }>) {
    try {
      const { account } = ctx.params;

      if (!this.isValidAccount(account)) {
        return ApiResponder.error(ctx, "Invalid account format", 400);
      }

      // First check if the trust deposit exists
      const trustDeposit = await TrustDeposit.query().findOne({ account });
      if (!trustDeposit) {
        return ApiResponder.error(
          ctx,
          `No trust deposit found for account: ${account}`,
          404
        );
      }

      const history = await knex("trust_deposit_history")
        .where("account", account)
        .orderBy("height", "asc")
        .orderBy("created_at", "asc");

      // If no history but trust deposit exists, return empty array (history tracking started after creation)
      const cleanHistory = history.map((record: any) => ({
        id: record.id,
        account: record.account,
        share: record.share?.toString(),
        amount: record.amount?.toString(),
        claimable: record.claimable?.toString(),
        slashed_deposit: record.slashed_deposit?.toString(),
        repaid_deposit: record.repaid_deposit?.toString(),
        last_slashed: record.last_slashed,
        last_repaid: record.last_repaid,
        slash_count: record.slash_count,
        last_repaid_by: record.last_repaid_by,
        event_type: record.event_type,
        height: record.height,
        changes: record.changes,
        created_at: record.created_at,
      }));

      return ApiResponder.success(ctx, { history: cleanHistory }, 200);
    } catch (err: any) {
      this.logger.error("Error in getTrustDepositHistory:", err);
      return ApiResponder.error(ctx, "Internal Server Error", 500);
    }
  }

  private isValidAccount(account: string): boolean {
    const accountRegex = /^verana1[0-9a-z]{10,}$/;
    return accountRegex.test(account);
  }
}
