import {
  Action,
  Service,
} from "@ourparentcenter/moleculer-decorators-extended";
import { ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import {
  ModulesParamsNamesTypes,
  SERVICE,
  TrustDepositMessageTypes,
} from "../../common";
import { formatTimestamp } from "../../common/utils/date_utils";
import knex from "../../common/utils/db_connection";
import ModuleParams from "../../models/modules_params";
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
): Record<string, { old: any; new: any }> | null {
  if (!oldRecord) return null;
  const changes: Record<string, { old: any; new: any }> = {};
  for (const field of TRUST_DEPOSIT_HISTORY_FIELDS) {
    const oldValue = oldRecord?.[field] ?? null;
    const newValue = newRecord?.[field] ?? null;
    if (String(oldValue) !== String(newValue)) {
      changes[field] = { old: oldValue, new: newValue };
    }
  }
  return Object.keys(changes).length ? changes : null;
}

async function recordTrustDepositHistory(
  trx: any,
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
  
  const existingHistory = await trx("trust_deposit_history")
    .where({ account: td.account, height })
    .first();
  
  if (existingHistory) {
    await trx("trust_deposit_history")
      .where({ id: existingHistory.id })
      .update({
        share: td.share?.toString() ?? "0",
        amount: td.amount?.toString() ?? "0",
        claimable: td.claimable?.toString() ?? "0",
        slashed_deposit: td.slashed_deposit?.toString() ?? "0",
        repaid_deposit: td.repaid_deposit?.toString() ?? "0",
        last_slashed: td.last_slashed ?? null,
        last_repaid: td.last_repaid ?? null,
        slash_count: td.slash_count ?? 0,
        last_repaid_by: td.last_repaid_by ?? "",
        event_type: eventType,
        changes: changes ? JSON.stringify(changes) : null,
      });
    return;
  }
  
  await trx("trust_deposit_history").insert({
    account: td.account,
    share: td.share?.toString() ?? "0",
    amount: td.amount?.toString() ?? "0",
    claimable: td.claimable?.toString() ?? "0",
    slashed_deposit: td.slashed_deposit?.toString() ?? "0",
    repaid_deposit: td.repaid_deposit?.toString() ?? "0",
    last_slashed: td.last_slashed ?? null,
    last_repaid: td.last_repaid ?? null,
    slash_count: td.slash_count ?? 0,
    last_repaid_by: td.last_repaid_by ?? "",
    event_type: eventType,
    height,
    changes: changes ? JSON.stringify(changes) : null,
  });
}

function parseDecimalToIntMultiplier(value: any): bigint {
  if (value === null || value === undefined) return BigInt(0);
  const str = String(value).trim();

  if (str === "" || Number.isNaN(Number(str))) {
    console.warn(
      `[TrustDeposit] ⚠️ Invalid decimal multiplier "${str}", defaulting to 0`
    );
    return BigInt(0);
  }

  const num = Number(str);
  const scaled = Math.floor(num * 1_000_000);
  return BigInt(scaled);
}

function applyBurn(amount: bigint, burnRate: any): bigint {
  const burnInt = parseDecimalToIntMultiplier(burnRate);
  if (burnInt <= BigInt(0)) return BigInt(0);
  return (amount * burnInt) / BigInt(1_000_000);
}

@Service({
  name: SERVICE.V1.TrustDepositMessageProcessorService.key,
  version: 1,
})
export default class TrustDepositMessageProcessorService extends BullableService {
  private trustDepositParams: any = {};

  public constructor(public broker: ServiceBroker) {
    super(broker);
  }

  async started() {
    await this.loadTrustDepositParams();
  }

  private async loadTrustDepositParams() {
    try {
      const params = await ModuleParams.query()
        .where("module", ModulesParamsNamesTypes.TD)
        .first();
      this.trustDepositParams = params?.params || {};
      this.logger.info(
        "[TrustDeposit] ✅ Loaded module params",
        this.trustDepositParams
      );
    } catch (error) {
      this.logger.error("[TrustDeposit] ❌ Error loading params", error);
      this.trustDepositParams = {};
    }
  }

  @Action({ name: "handleTrustDepositMessages" })
  async handleTrustDepositMessages(ctx: any) {
    const { trustDepositList } = ctx.params;
    const params =
      this.trustDepositParams?.params || this.trustDepositParams || {};

    if (!trustDepositList?.length) {
      return { success: true };
    }

    try {
      await knex.transaction(async (trx) => {
        for (const msg of trustDepositList) {
          await this.processMessage(msg, params, trx);
        }
      });

      return { success: true };
    } catch (error) {
      this.logger.error("[TrustDeposit] ❌ Error processing messages:", error);
      return false
    }
  }

  private async processMessage(msg: any, params: any, trx: any) {
    const { type, content, timestamp, height } = msg;
    const ts = formatTimestamp(timestamp);
    const blockHeight = Number(height) || 0;
    const account = content.creator || content.account;

    this.logger.info(`[TrustDeposit] Processing ${type} for ${account}`);

    switch (type) {
      case TrustDepositMessageTypes.RECLAIM_YIELD:
        return this.reclaimYield(content, params, trx, blockHeight);

      case TrustDepositMessageTypes.RECLAIM_DEPOSIT:
        return this.reclaimDeposit(content, params, trx, blockHeight);

      case TrustDepositMessageTypes.REPAY_SLASHED:
        return this.repaySlashed(content, ts, params, trx, blockHeight);

      default:
        this.logger.warn(`[TrustDeposit] Unknown message type: ${type}`);
        return true;
    }
  }

  private async reclaimYield(content: any, params: any, trx: any, height: number) {
    try {
      const account = content.creator;
      if (!account) this.logger.warn("Account required");

      const td = await TrustDeposit.query(trx).findOne({ account });
      if (!td) {
        this.logger.warn("No trust deposit found");
        return;
      }
      const previousRecord = { ...td };
      const slashed = toBigIntSafe(td.slashed_deposit);
      const repaid = toBigIntSafe(td.repaid_deposit);
      if (slashed > repaid) this.logger.warn("Deposit slashed and not repaid");

      const shareValue = toBigIntSafe(params.trust_deposit_share_value);
      const claimableYield =
        toBigIntSafe(td.share) * shareValue - toBigIntSafe(td.amount);

      if (claimableYield <= BigInt(0)) this.logger.warn("No yield available");

      const newShare = toBigIntSafe(td.share) - claimableYield / shareValue;

      const updated = await TrustDeposit.query(trx).patchAndFetchById(td.id, {
        share: newShare.toString(),
      });

      await recordTrustDepositHistory(
        trx,
        updated,
        "RECLAIM_YIELD",
        height,
        previousRecord
      );

      this.logger.info(
        `[TrustDeposit] ✅ Yield reclaimed for ${account}: ${claimableYield}`
      );
    } catch (error) {
      this.logger.error(
        `[TrustDeposit] ❌ Yield reclaim failed for  — ${error || error}`
      );
    }
  }

  private async reclaimDeposit(content: any, params: any, trx: any, height: number) {
    try {
      const account = content.creator;
      const claimed = toBigIntSafe(content.claimed);

      if (!account) this.logger.warn("❌ Account required");
      if (claimed <= BigInt(0)) this.logger.warn("❌ Claimed must be > 0");

      const td: any = await TrustDeposit.query(trx).findOne({ account });
      if (!td) this.logger.warn(`❌ No trust deposit found for ${account}`);
      const previousRecord = { ...td };
      const slashed = toBigIntSafe(td.slashed_deposit);
      const repaid = toBigIntSafe(td.repaid_deposit);
      if (slashed > repaid)
        this.logger.warn("❌ Deposit slashed and not repaid");

      const claimable = toBigIntSafe(td.claimable);
      if (claimable < claimed) {
        this.logger.warn(
          `❌ Insufficient claimable. Requested=${claimed}, Available=${claimable}`
        );
      }

      const shareValue = toBigIntSafe(params.trust_deposit_share_value);
      const requiredMinimum = toBigIntSafe(td.share) * shareValue;
      const newDeposit = toBigIntSafe(td.amount) - claimed;

      this.logger.info(
        `[TrustDeposit] Debug: requiredMinimum=${requiredMinimum}, newDeposit=${newDeposit}, claimed=${claimed}`
      );

      if (requiredMinimum > newDeposit) {
        this.logger.warn(
          `❌ Reclaim violates minimum deposit requirement. requiredMinimum=${requiredMinimum}, newDeposit=${newDeposit}`
        );
      }

      const burnRate = params.trust_deposit_reclaim_burn_rate ?? "0";
      const burn = applyBurn(claimed, burnRate);
      const transfer = claimed - burn;
      const newClaimable = claimable - claimed;
      const newShare = toBigIntSafe(td.share) - claimed / shareValue;

      const updated = await TrustDeposit.query(trx).patchAndFetchById(td.id, {
        amount: newDeposit.toString(),
        claimable: newClaimable.toString(),
        share: newShare.toString(),
      });

      await recordTrustDepositHistory(
        trx,
        updated,
        "RECLAIM_DEPOSIT",
        height,
        previousRecord
      );

      this.logger.info(
        `[TrustDeposit] ✅ Reclaimed ${transfer} (burned ${burn}) for ${account}`
      );
    } catch (error: any) {
      this.logger.error(
        `[TrustDeposit] ❌ Reclaim failed for content=${JSON.stringify(
          content
        )} — ${error.message || error}`
      );
      console.error(error);

    }
  }

  private async repaySlashed(content: any, ts: string, params: any, trx: any, height: number) {
    try {
      const account = content.account;
      const amount = toBigIntSafe(content.amount);
      if (!account) this.logger.warn("Account required");
      if (amount <= BigInt(0)) this.logger.warn("Amount must be > 0");

      const td = await TrustDeposit.query(trx).findOne({ account });
      if (!td) {
        this.logger.warn("No trust deposit found");
        return;
      }
      const previousRecord = { ...td };
      const slashed = toBigIntSafe(td.slashed_deposit);
      const repaid = toBigIntSafe(td.repaid_deposit);
      const remaining = slashed - repaid;
      if (amount !== remaining) {
        this.logger.warn(`Repay must match remaining slashed: ${remaining}`);
      }

      const shareValue = toBigIntSafe(params.trust_deposit_share_value);
      const newDeposit = toBigIntSafe(td.amount) + amount;
      const newShare = toBigIntSafe(td.share) + amount / shareValue;
      const newRepaid = repaid + amount;

      const updated = await TrustDeposit.query(trx).patchAndFetchById(td.id, {
        amount: newDeposit.toString(),
        share: newShare.toString(),
        repaid_deposit: newRepaid.toString(),
        last_repaid: ts,
        last_repaid_by: content.creator || "unknown",
      });

      await recordTrustDepositHistory(
        trx,
        updated,
        "REPAY_SLASHED",
        height,
        previousRecord
      );

      this.logger.info(
        `[TrustDeposit] ✅ Slashed deposit repaid ${amount} for ${account}`
      );
    } catch (error) {
      this.logger.error(
        `[TrustDeposit] ❌ Slashed deposit repay failed for  — ${error || error
        }`
      );
    }
  }
}
