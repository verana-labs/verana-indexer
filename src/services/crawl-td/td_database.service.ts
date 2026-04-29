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
  "deposit",
  "claimable",
  "slashed_deposit",
  "repaid_deposit",
  "last_slashed",
  "last_repaid",
  "slash_count",
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
    .where({ corporation: td.corporation, height })
    .first();
  
  if (existingHistory) {
    await trxOrKnex("trust_deposit_history")
      .where({ id: existingHistory.id })
      .update({
        share: td.share != null ? Number(td.share) : 0,
        deposit: td.deposit != null ? Number(td.deposit) : 0,
        claimable: td.claimable != null ? Number(td.claimable) : 0,
        slashed_deposit: td.slashed_deposit != null ? Number(td.slashed_deposit) : 0,
        repaid_deposit: td.repaid_deposit != null ? Number(td.repaid_deposit) : 0,
        last_slashed: td.last_slashed ?? null,
        last_repaid: td.last_repaid ?? null,
        slash_count: td.slash_count != null ? Number(td.slash_count) : 0,
        event_type: eventType,
        changes: changes ? JSON.stringify(changes) : null,
      });
    return;
  }
  
  await trxOrKnex("trust_deposit_history").insert({
    corporation: td.corporation,
    share: td.share != null ? Number(td.share) : 0,
    deposit: td.deposit != null ? Number(td.deposit) : 0,
    claimable: td.claimable != null ? Number(td.claimable) : 0,
    slashed_deposit: td.slashed_deposit != null ? Number(td.slashed_deposit) : 0,
    repaid_deposit: td.repaid_deposit != null ? Number(td.repaid_deposit) : 0,
    last_slashed: td.last_slashed ?? null,
    last_repaid: td.last_repaid ?? null,
    slash_count: td.slash_count != null ? Number(td.slash_count) : 0,
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
    name: "syncFromLedger",
    params: {
      ledgerTrustDeposit: "object",
      blockHeight: "number",
      eventType: { type: "string", optional: true },
    },
  })
  public async syncFromLedger(ctx: any) {
    const { ledgerTrustDeposit, blockHeight, eventType } = ctx.params;
    const corporation = String(
      ledgerTrustDeposit?.corporation ?? ledgerTrustDeposit?.account ?? ""
    ).trim();
    if (!corporation) {
      return { success: false, reason: "Missing trust deposit corporation from ledger" };
    }

    try {
      await this.broker.call(`${SERVICE.V1.HANDLE_ACCOUNTS.path}.upsertAccount`, { address: corporation });
    } catch {
      //
    }

    const payload = {
      corporation,
      deposit: Number(ledgerTrustDeposit?.deposit ?? ledgerTrustDeposit?.amount ?? 0),
      share: Number(ledgerTrustDeposit?.share ?? 0),
      claimable: Number(ledgerTrustDeposit?.claimable ?? 0),
      slashed_deposit: Number(ledgerTrustDeposit?.slashed_deposit ?? ledgerTrustDeposit?.slashedDeposit ?? 0),
      repaid_deposit: Number(ledgerTrustDeposit?.repaid_deposit ?? ledgerTrustDeposit?.repaidDeposit ?? 0),
      last_slashed: ledgerTrustDeposit?.last_slashed ?? ledgerTrustDeposit?.lastSlashed ?? null,
      last_repaid: ledgerTrustDeposit?.last_repaid ?? ledgerTrustDeposit?.lastRepaid ?? null,
      slash_count: Number(ledgerTrustDeposit?.slash_count ?? ledgerTrustDeposit?.slashCount ?? 0),
    };

    const height = Number(blockHeight) || 0;

    try {
      await knex.transaction(async (trx) => {
        const existing = await TrustDeposit.query(trx).findOne({ corporation });
        const previous = existing ? { ...existing } : undefined;
        let finalRecord: any;

        if (existing) {
          finalRecord = await TrustDeposit.query(trx).patchAndFetchById(existing.id, payload);
        } else {
          [finalRecord] = await trx("trust_deposits").insert(payload).returning("*");
        }

        await recordTrustDepositHistory(
          trx,
          finalRecord,
          eventType || "SYNC_LEDGER",
          height,
          previous
        );
      });

      return { success: true, corporation };
    } catch (error: any) {
      this.logger.warn(`[TrustDepositDB] syncFromLedger failed for corporation=${corporation}: ${error?.message || error}`);
      return { success: false, reason: error?.message || String(error), corporation };
    }
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
      await this.broker.call(`${SERVICE.V1.HANDLE_ACCOUNTS.path}.upsertAccount`, { address: account });
    } catch {
      //
    }
    try {
      const result = await knex.transaction(async (trx) => {
        const existing = await TrustDeposit.query(trx).findOne({ corporation: account });
        if (!existing) {
          const [inserted] = await trx("trust_deposits")
            .insert({
              corporation: account,
              deposit: Number(newAmount ?? BigInt(0)),
              share: Number(newShare ?? BigInt(0)),
              claimable: Number(newClaimable ?? BigInt(0)),
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
          deposit: Number(newAmount ?? BigInt(existing.deposit)),
          share: Number(newShare ?? BigInt(existing.share)),
          claimable: Number(newClaimable ?? BigInt(existing.claimable || "0")),
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
      await this.broker.call(`${SERVICE.V1.HANDLE_ACCOUNTS.path}.upsertAccount`, { address: account });
    } catch {
      //
    }

    try {
      const result = await knex.transaction(async (trx) => {
        const td = await TrustDeposit.query(trx).findOne({ corporation: account });
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
        const currentAmount = toBigIntSafe(td.deposit);
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
          deposit: Number(newAmount),
          share: Number(newShare),
          slashed_deposit: Number(newSlashed),
          last_slashed: now ? new Date(now).toISOString() : null,
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
      corporation: { type: "string", min: 5, optional: true },
      account: { type: "string", min: 5, optional: true },
      amount: { type: "string" },
      ts: { type: "string", optional: true },
      height: { type: "number", optional: true },
    },
  })
  public async slashPermTrustDeposit(ctx: any) {
    const corporation = String(ctx.params.corporation ?? ctx.params.account ?? "").trim();
    const { amount, ts, height } = ctx.params;
    const amountBig = toBigIntSafe(amount);
    const blockHeight = Number(height) || 0;

    if (!corporation) {
      this.logger.warn("[SlashPermTD] ❌ Missing corporation parameter");
      return { success: false, message: "corporation required" };
    }

    if (amountBig <= BigInt(0)) {
      this.logger.warn("[SlashPermTD] ❌ amount must be > 0");
      return { success: false, message: "amount must be > 0" };
    }

    try {
      await this.broker.call(`${SERVICE.V1.HANDLE_ACCOUNTS.path}.upsertAccount`, { address: corporation });
    } catch {
      //
    }

    try {
      const result = await knex.transaction(async (trx) => {
        const td = await TrustDeposit.query(trx).findOne({ corporation });
        if (!td) {
          this.logger.warn(
            `[SlashPermTD] ❌ No trust deposit found for ${corporation}`
          );
          return {
            success: false,
            message: "TrustDeposit entry does not exist",
          };
        }

        const previousRecord = { ...td };
        const currentAmount = toBigIntSafe(td.deposit);
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
          deposit: Number(newAmount),
          share: Number(newShare),
          slashed_deposit: Number(newSlashed),
          last_slashed: ts ? new Date(formatTimestamp(ts)).toISOString() : new Date().toISOString(),
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
          `[SlashPermTD] ⚔️ Permission slash: ${corporation} slashed ${amountBig.toString()}`
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
