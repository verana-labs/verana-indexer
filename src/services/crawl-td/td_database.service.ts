import {
  Action,
  Service,
} from "@ourparentcenter/moleculer-decorators-extended";
import { ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import { SERVICE } from "../../common";
import { formatTimestamp } from "../../common/utils/date_utils";
import knex from "../../common/utils/db_connection";
import TrustDeposit from "../../models/trust_deposit";

function toBigIntSafe(value: any): bigint {
  if (value === null || value === undefined) return BigInt(0);
  const str = String(value).trim();

  if (str.includes(".")) {
    const intPart = str.split(".")[0] || "0";
    return BigInt(intPart);
  }

  if (!/^-?\d+$/.test(str)) {
    console.warn(
      `[TrustDeposit] ⚠️ Invalid BigInt value "${str}", defaulting to 0`
    );
    return BigInt(0);
  }

  return BigInt(str);
}

@Service({
  name: SERVICE.V1.TrustDepositDatabaseService.key,
  version: 1,
})
export default class TrustDepositDatabaseService extends BullableService {
  public constructor(public broker: ServiceBroker) {
    super(broker);
  }

  @Action({
    name: "adjustTrustDeposit",
    params: {
      account: { type: "string", min: 5 },
    },
  })
  public async adjustTrustDeposit(ctx: any) {
    const { account, newAmount, newShare, newClaimable } = ctx.params;
    try {
      const result = await knex.transaction(async (trx) => {
        const existing = await TrustDeposit.query(trx).findOne({ account });
        if (!existing) {
          await TrustDeposit.query(trx).insert({
            account,
            amount: (newAmount ?? newAmount ?? BigInt(0)).toString(),
            share: (newShare ?? newShare).toString(),
            claimable: (newClaimable ?? newClaimable ?? BigInt(0)).toString(),
          });
          return {
            success: true,
            message: "Inserted new trust deposit record",
          };
        }

        await TrustDeposit.query(trx).patchAndFetchById(existing.id, {
          amount: (newAmount ?? BigInt(existing.amount)).toString(),
          share: (newShare ?? BigInt(existing.share)).toString(),
          claimable: (
            newClaimable ?? BigInt(existing.claimable || "0")
          ).toString(),
        });

        return {
          success: true,
          message: "Updated existing trust deposit record",
        };
      });
      return result;
    } catch (err) {
      this.logger.error("[TrustDepositDB] ❌ adjustTrustDeposit failed:", err);
      return { success: false, message: "Database transaction failed" };
    }
  }
  @Action({
    name: "slash_trust_deposit",
    params: {
      account: { type: "string", min: 5 },
      slashed: { type: "string" },
      lastSlashed: { type: "string", optional: true },
      slashCount: { type: "number", optional: true },
    },
  })
  public async slashTrustDeposit(ctx: any) {
    const { account, slashed: amount, lastSlashed, slashCount } = ctx.params;
    const amountBig = toBigIntSafe(amount);

    if (!account) {
      this.logger.warn("[SlashTD] ❌ Missing account parameter");
      return { success: false, message: "account required" };
    }

    if (amountBig <= BigInt(0)) {
      this.logger.warn("[SlashTD] ❌ Slash amount must be > 0");
      return { success: false, message: "amount must be > 0" };
    }

    try {
      const result = await knex.transaction(async (trx) => {
        const td = await TrustDeposit.query(trx).findOne({ account });
        if (!td) {
          this.logger.warn(
            `[SlashTD] ❌ No trust deposit found for ${account}`
          );
          return {
            success: false,
            message: "TrustDeposit entry does not exist",
          };
        }

        const currentAmount = toBigIntSafe(td.amount);
        if (amountBig > currentAmount) {
          this.logger.warn(
            `[SlashTD] ❌ Slash amount ${amountBig} exceeds current deposit ${currentAmount}`
          );
          return { success: false, message: "Slash amount exceeds deposit" };
        }

        const shareValue = toBigIntSafe(
          this.trustDepositParams?.params?.trust_deposit_share_value || "1"
        );
        const now = formatTimestamp(lastSlashed);

        const newAmount = currentAmount - amountBig;
        const newShare =
          toBigIntSafe(td.share) -
          (shareValue > BigInt(0) ? amountBig / shareValue : BigInt(0));
        const newSlashed = toBigIntSafe(td.slashed_deposit) + amountBig;
        const newSlashCount = BigInt(td.slash_count || "0") + BigInt(1);

        await TrustDeposit.query(trx).patchAndFetchById(td.id, {
          amount: newAmount.toString(),
          share: newShare.toString(),
          slashed_deposit: newSlashed.toString(),
          last_slashed: now,
          slash_count: slashCount
            ? BigInt(slashCount).toString()
            : newSlashCount.toString(),
        });

        this.logger.info(
          `[SlashTD] ⚔️ Global slash: ${account} slashed ${amountBig.toString()} at ${now}`
        );

        return { success: true, message: "Slash trust deposit completed" };
      });

      return result;
    } catch (err) {
      this.logger.error("[SlashTD] ❌ Transaction failed:", err);
      return { success: false, message: "Database transaction failed" };
    }
  }

  @Action({
    name: "slash_perm_trust_deposit",
    params: {
      account: { type: "string", min: 5 },
      amount: { type: "string" },
      ts: { type: "string", optional: true },
    },
  })
  public async slashPermTrustDeposit(ctx: any) {
    const { account, amount, ts } = ctx.params;
    const amountBig = toBigIntSafe(amount);

    if (!account) {
      this.logger.warn("[SlashPermTD] ❌ Missing account parameter");
      return { success: false, message: "account required" };
    }

    if (amountBig <= BigInt(0)) {
      this.logger.warn("[SlashPermTD] ❌ amount must be > 0");
      return { success: false, message: "amount must be > 0" };
    }

    try {
      const result = await knex.transaction(async (trx) => {
        const td = await TrustDeposit.query(trx).findOne({ account });
        if (!td) {
          this.logger.warn(
            `[SlashPermTD] ❌ No trust deposit found for ${account}`
          );
          return {
            success: false,
            message: "TrustDeposit entry does not exist",
          };
        }

        const currentAmount = toBigIntSafe(td.amount);
        if (amountBig > currentAmount) {
          this.logger.warn(
            `[SlashPermTD] ❌ Slash amount ${amountBig} exceeds deposit ${currentAmount}`
          );
          return { success: false, message: "Slash amount exceeds deposit" };
        }

        const shareValue = toBigIntSafe(
          this.trustDepositParams?.params?.trust_deposit_share_value || "1"
        );

        const newAmount = currentAmount - amountBig;
        const newShare =
          toBigIntSafe(td.share) -
          (shareValue > BigInt(0) ? amountBig / shareValue : BigInt(0));
        const newSlashed = toBigIntSafe(td.slashed_deposit) + amountBig;
        const newSlashCount = BigInt(td.slash_count || "0") + BigInt(1);

        await TrustDeposit.query(trx).patchAndFetchById(td.id, {
          amount: newAmount.toString(),
          share: newShare.toString(),
          slashed_deposit: newSlashed.toString(),
          last_slashed: ts ? formatTimestamp(ts) : formatTimestamp(Date.now()),
          slash_count: newSlashCount.toString(),
        });

        this.logger.info(
          `[SlashPermTD] ⚔️ Permission slash: ${account} slashed ${amountBig.toString()}`
        );

        return {
          success: true,
          message: "Slash permission trust deposit completed",
        };
      });

      return result;
    } catch (err) {
      this.logger.error("[SlashPermTD] ❌ Transaction failed:", err);
      return { success: false, message: "Database transaction failed" };
    }
  }
}
