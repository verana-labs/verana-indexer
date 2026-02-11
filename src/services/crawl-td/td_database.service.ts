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

const TRUST_DEPOSIT_HISTORY_FIELDS = [
  "share",
  "amount",
  "claimable",
  "slashed_deposit",
  "repaid_deposit",
  "last_slashed",
  "last_repaid",
  "slash_count",
  "last_repaid_by",
];

function computeTdChanges(
  oldRecord: any,
  newRecord: any
): Record<string, any> | null {
  const changes: Record<string, any> = {};
  
  if (!oldRecord) {
    for (const field of TRUST_DEPOSIT_HISTORY_FIELDS) {
      const newValue = newRecord?.[field] ?? null;
      if (newValue !== null && newValue !== undefined) {
        changes[field] = newValue;
      }
    }
  } else {
    for (const field of TRUST_DEPOSIT_HISTORY_FIELDS) {
      const oldValue = oldRecord?.[field] ?? null;
      const newValue = newRecord?.[field] ?? null;
      if (String(oldValue) !== String(newValue)) {
        changes[field] = newValue;
      }
    }
  }
  return Object.keys(changes).length ? changes : null;
}

async function recordTrustDepositHistory(
  trxOrKnex: any,
  td: any,
  eventType: string,
  height: number,
  previousRecord?: any
) {
  if (!td) return;
  const changes = computeTdChanges(previousRecord, td);
  
  if (previousRecord && !changes) {
    return; 
  }
  
  const existingHistory = await trxOrKnex("trust_deposit_history")
    .where({ account: td.account, height })
    .first();
  
  if (existingHistory) {
    await trxOrKnex("trust_deposit_history")
      .where({ id: existingHistory.id })
      .update({
        share: td.share?.toString() ?? "0",
        amount: td.amount?.toString() ?? "0",
        claimable: td.claimable?.toString() ?? "0",
        slashed_deposit: td.slashed_deposit?.toString() ?? "0",
        repaid_deposit: td.repaid_deposit?.toString() ?? "0",
        last_slashed: td.last_slashed ?? null,
        last_repaid: td.last_repaid ?? null,
        slash_count: td.slash_count?.toString() ?? "0",
        last_repaid_by: td.last_repaid_by ?? "",
        event_type: eventType,
        changes: changes ? JSON.stringify(changes) : null,
      });
    return;
  }
  
  await trxOrKnex("trust_deposit_history").insert({
    account: td.account,
    share: td.share?.toString() ?? "0",
    amount: td.amount?.toString() ?? "0",
    claimable: td.claimable?.toString() ?? "0",
    slashed_deposit: td.slashed_deposit?.toString() ?? "0",
    repaid_deposit: td.repaid_deposit?.toString() ?? "0",
    last_slashed: td.last_slashed ?? null,
    last_repaid: td.last_repaid ?? null,
    slash_count: td.slash_count?.toString() ?? "0",
    last_repaid_by: td.last_repaid_by ?? "",
    event_type: eventType,
    height,
    changes: changes ? JSON.stringify(changes) : null,
  });
}

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
      height: { type: "number", optional: true },
    },
  })
  public async adjustTrustDeposit(ctx: any) {
    const { account, newAmount, newShare, newClaimable, height } = ctx.params;
    const blockHeight = Number(height) || 0;
    try {
      const result = await knex.transaction(async (trx) => {
        const existing = await TrustDeposit.query(trx).findOne({ account });
        if (!existing) {
          const [inserted] = await trx("trust_deposits")
            .insert({
              account,
              amount: (newAmount ?? BigInt(0)).toString(),
              share: (newShare ?? BigInt(0)).toString(),
              claimable: (newClaimable ?? BigInt(0)).toString(),
            })
            .returning("*");

          // Record history for creation
          await recordTrustDepositHistory(
            trx,
            inserted,
            "CREATE_TRUST_DEPOSIT",
            blockHeight
          );

          return {
            success: true,
            message: "Inserted new trust deposit record",
          };
        }

        const previousRecord = { ...existing };
        const updated = await TrustDeposit.query(trx).patchAndFetchById(existing.id, {
          amount: (newAmount ?? BigInt(existing.amount)).toString(),
          share: (newShare ?? BigInt(existing.share)).toString(),
          claimable: (
            newClaimable ?? BigInt(existing.claimable || "0")
          ).toString(),
        });

        // Record history for update
        await recordTrustDepositHistory(
          trx,
          updated,
          "ADJUST_TRUST_DEPOSIT",
          blockHeight,
          previousRecord
        );

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
      height: { type: "number", optional: true },
    },
  })
  public async slashTrustDeposit(ctx: any) {
    const { account, slashed: amount, lastSlashed, slashCount, height } = ctx.params;
    const amountBig = toBigIntSafe(amount);
    const blockHeight = Number(height) || 0;

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

        const previousRecord = { ...td };
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

        const updated = await TrustDeposit.query(trx).patchAndFetchById(td.id, {
          amount: Number(newAmount),
          share: Number(newShare),
          slashed_deposit: Number(newSlashed),
          last_slashed: now ? new Date(now) : null,
          slash_count: slashCount
            ? Number(slashCount)
            : Number(newSlashCount),
        });

        await recordTrustDepositHistory(
          trx,
          updated,
          "SLASH_TRUST_DEPOSIT",
          blockHeight,
          previousRecord
        );

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
      height: { type: "number", optional: true },
    },
  })
  public async slashPermTrustDeposit(ctx: any) {
    const { account, amount, ts, height } = ctx.params;
    const amountBig = toBigIntSafe(amount);
    const blockHeight = Number(height) || 0;

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

        const previousRecord = { ...td };
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

        const updated = await TrustDeposit.query(trx).patchAndFetchById(td.id, {
          amount: Number(newAmount),
          share: Number(newShare),
          slashed_deposit: Number(newSlashed),
          last_slashed: ts ? new Date(formatTimestamp(ts)) : new Date(),
          slash_count: Number(newSlashCount),
        });

        await recordTrustDepositHistory(
          trx,
          updated,
          "SLASH_PERM_TRUST_DEPOSIT",
          blockHeight,
          previousRecord
        );

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
