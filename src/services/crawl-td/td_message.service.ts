import {
  Action,
  Service,
} from "@ourparentcenter/moleculer-decorators-extended";
import { ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import {
  ModulesParamsNamesTypes,
  SERVICE,
} from "../../common";
import { VeranaTrustDepositMessageTypes } from "../../common/verana-message-types";
import { formatTimestamp } from "../../common/utils/date_utils";
import knex from "../../common/utils/db_connection";
import ModuleParams from "../../models/modules_params";
import TrustDeposit from "../../models/trust_deposit";
import { extractController, requireController } from "../../common/utils/extract_controller";
import { MessageProcessorBase } from "../../common/utils/message_processor_base";
import { detectStartMode } from "../../common/utils/start_mode_detector";

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
  newRecord: any,
  isCreation: boolean = false
): Record<string, any> | null {
  const changes: Record<string, any> = {};
  
  if (isCreation || !oldRecord) {
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
        changes[field] = { before: oldValue, after: newValue };
      }
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
    .where({ corporation: td.corporation, height })
    .first();
  
  if (existingHistory) {
    await trx("trust_deposit_history")
      .where({ id: existingHistory.id })
      .update({
        share: td.share ?? 0,
        deposit: td.deposit ?? 0,
        claimable: td.claimable ?? 0,
        slashed_deposit: td.slashed_deposit ?? 0,
        repaid_deposit: td.repaid_deposit ?? 0,
        last_slashed: td.last_slashed ?? null,
        last_repaid: td.last_repaid ?? null,
        slash_count: td.slash_count ?? 0,
        event_type: eventType,
        changes: changes ? JSON.stringify(changes) : null,
      });
    return;
  }
  
  await trx("trust_deposit_history").insert({
    corporation: td.corporation,
    share: td.share ?? 0,
    deposit: td.deposit ?? 0,
    claimable: td.claimable ?? 0,
    slashed_deposit: td.slashed_deposit ?? 0,
    repaid_deposit: td.repaid_deposit ?? 0,
    last_slashed: td.last_slashed ?? null,
    last_repaid: td.last_repaid ?? null,
    slash_count: td.slash_count ?? 0,
    event_type: eventType,
    height,
    changes: changes ? JSON.stringify(changes) : null,
  });
}

@Service({
  name: SERVICE.V1.TrustDepositMessageProcessorService.key,
  version: 1,
})
export default class TrustDepositMessageProcessorService extends BullableService {
  private trustDepositParams: any = {};
  private processorBase: MessageProcessorBase;
  private _isFreshStart: boolean = false;
  private readonly zeroShareWarnCooldownMs = 60_000;
  private readonly zeroShareWarnByAccount = new Map<string, number>();

  public constructor(public broker: ServiceBroker) {
    super(broker);
    this.processorBase = new MessageProcessorBase(this);
  }

  async started() {
    const startMode = await detectStartMode();
    this._isFreshStart = startMode.isFreshStart;
    this.processorBase.setFreshStartMode(this._isFreshStart);
    this.logger.info(`TrustDeposit message processor started | Mode: ${this._isFreshStart ? 'Fresh Start' : 'Reindexing'}`);
    await this.loadTrustDepositParams();
  }

  private warnZeroShareValueOnce(action: string, account: string) {
    const key = `${action}:${account}`;
    const now = Date.now();
    const lastWarnAt = this.zeroShareWarnByAccount.get(key) ?? 0;
    if (now - lastWarnAt < this.zeroShareWarnCooldownMs) {
      return;
    }
    this.zeroShareWarnByAccount.set(key, now);
    this.logger.warn(
      `[TrustDeposit] Division by zero prevented: trust_deposit_share_value is 0, skipping ${action} for account ${account}`
    );
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
        const processMessageInTx = async (msg: any) => {
          await this.processMessage(msg, params, trx);
        };

        await this.processorBase.processInBatches(
          trustDepositList,
          processMessageInTx,
          {
            maxConcurrent: this._isFreshStart ? 2 : 5,
            batchSize: this._isFreshStart ? 10 : 25,
            delayBetweenBatches: this._isFreshStart ? 1000 : 300,
          }
        );
      });

      return { success: true };
    } catch (error) {
      this.logger.error("[TrustDeposit] Error processing messages:", error);
      return false;
    }
  }

  private async processMessage(msg: any, params: any, trx: any) {
    const { type, content, timestamp, height } = msg;
    const ts = formatTimestamp(timestamp);
    const blockHeight = Number(height) || 0;
    const account = extractController(content);

    this.logger.info(`[TrustDeposit] Processing ${type} for ${account}`);

    switch (type) {
      case VeranaTrustDepositMessageTypes.ReclaimYield:
        return this.reclaimYield(content, params, trx, blockHeight);

      case VeranaTrustDepositMessageTypes.RepaySlashed:
        return this.repaySlashed(content, ts, params, trx, blockHeight);

      case VeranaTrustDepositMessageTypes.SlashTrustDeposit:
        return this.slashTrustDeposit(content, ts, blockHeight);

      default:
        this.logger.warn(`[TrustDeposit] Unknown message type: ${type}`);
        return true;
    }
  }

  private async reclaimYield(content: any, params: any, trx: any, height: number) {
    try {
      const account = requireController(content, "TrustDeposit RECLAIM_YIELD");

      const td = await TrustDeposit.query(trx).findOne({ corporation: account });
      if (!td) {
        this.logger.warn("No trust deposit found");
        return;
      }
      const previousRecord = { ...td };
      const slashed = toBigIntSafe(td.slashed_deposit);
      const repaid = toBigIntSafe(td.repaid_deposit);
      if (slashed > repaid) this.logger.warn("Deposit slashed and not repaid");

      const shareValue = toBigIntSafe(params.trust_deposit_share_value);
      if (shareValue === BigInt(0)) {
        this.warnZeroShareValueOnce("reclaim", account);
        return;
      }
      
      const claimableYield =
        toBigIntSafe(td.share) * shareValue - toBigIntSafe(td.deposit);

      if (claimableYield <= BigInt(0)) this.logger.warn("No yield available");

      const newShare = toBigIntSafe(td.share) - claimableYield / shareValue;

      const updated = await TrustDeposit.query(trx).patchAndFetchById(td.id, {
        share: Number(newShare),
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
    } catch (error: any) {
      const errorMessage = error?.message || String(error);
      this.logger.error(
        `[TrustDeposit] ❌ Yield reclaim failed: ${errorMessage}`
      );
      throw error;
    }
  }

  private async repaySlashed(content: any, ts: string, params: any, trx: any, height: number) {
    try {
      const account = requireController(content, "TrustDeposit REPAY_SLASHED");
      const amount = toBigIntSafe(content.deposit ?? content.amount);
      if (amount <= BigInt(0)) this.logger.warn("Amount must be > 0");

      const td = await TrustDeposit.query(trx).findOne({ corporation: account });
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
      if (shareValue === BigInt(0)) {
        this.warnZeroShareValueOnce("repay", account);
        return; 
      }
      
      const newDeposit = toBigIntSafe(td.deposit) + amount;
      const newShare = toBigIntSafe(td.share) + amount / shareValue;
      const newRepaid = repaid + amount;

      const updated = await TrustDeposit.query(trx).patchAndFetchById(td.id, {
        deposit: Number(newDeposit),
        share: Number(newShare),
        repaid_deposit: Number(newRepaid),
        last_repaid: ts ? new Date(ts).toISOString() : null,
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
    } catch (error: any) {
      const errorMessage = error?.message || String(error);
      this.logger.error(
        `[TrustDeposit] ❌ Slashed deposit repay failed: ${errorMessage}`
      );
      throw error;
    }
  }

  private async slashTrustDeposit(content: any, ts: string, height: number) {
    try {
      const account = requireController(content, "TrustDeposit SLASH");
      const amount = toBigIntSafe(content.deposit ?? content.amount);
      if (amount <= BigInt(0)) {
        this.logger.warn("[TrustDeposit] Slash deposit must be > 0");
        return;
      }

      await this.broker.call(
        `${SERVICE.V1.TrustDepositDatabaseService.path}.slash_trust_deposit`,
        {
          account,
          slashed: amount.toString(),
          lastSlashed: ts,
          height,
        }
      );
    } catch (error: any) {
      const errorMessage = error?.message || String(error);
      this.logger.error(
        `[TrustDeposit] ❌ Slash failed: ${errorMessage}`
      );
      throw error;
    }
  }
}
