import {
  Action,
  Service,
} from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import { ModulesParamsNamesTypes, SERVICE } from "../../common";
import ApiResponder from "../../common/utils/apiResponse";
import ModuleParams from "../../models/modules_params";
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

      if (!this.isValidAccount(account)) {
        this.logger.warn(`Invalid account format: ${account}`);
        return ApiResponder.error(ctx, "Invalid account format", 400);
      }

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
    try {
      const module = await ModuleParams.query().findOne({
        module: ModulesParamsNamesTypes.TD,
      });

      if (!module || !module.params) {
        this.logger.warn("Module parameters not found for Trust Deposit");
        return ApiResponder.error(ctx, "Module parameters not found", 404);
      }

      let parsedParams: Record<string, any>;
      try {
        parsedParams =
          typeof module.params === "string"
            ? JSON.parse(module.params)
            : module.params;
      } catch (parseErr) {
        this.logger.error("Failed to parse module.params JSON:", parseErr);
        return ApiResponder.error(ctx, "Invalid module parameters format", 500);
      }
      const params = parsedParams.params || parsedParams || {}
      return ApiResponder.success(
        ctx,
        { params },
        200
      );
    } catch (err: any) {
      this.logger.error("Error fetching module params:", err);
      return ApiResponder.error(ctx, "Internal Server Error", 500);
    }
  }

  private isValidAccount(account: string): boolean {
    const accountRegex = /^verana1[0-9a-z]{10,}$/;
    return accountRegex.test(account);
  }
}
