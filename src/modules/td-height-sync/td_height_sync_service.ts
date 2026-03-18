import type { ServiceBroker } from "moleculer";
import { SERVICE } from "../../common";
import { extractAccountAddressesFromTdSources } from "../../common/utils/account_balance_utils";
import {
  buildDepositIdToEventTypeMap,
  extractImpactedTrustDepositIds,
  fetchTrustDeposit,
  type TdMessageLike,
} from "./td_height_sync_helpers";

const TD_CONCURRENCY = 8;

type TdHeightSyncLogger = {
  info?: (message: string) => void;
  warn?: (message: string) => void;
  error?: (message: string) => void;
  debug?: (message: string) => void;
};

function getTdHeightSyncLogger(broker: ServiceBroker): TdHeightSyncLogger {
  const brokerWithLogger = broker as ServiceBroker & {
    getLogger?: (name: string) => TdHeightSyncLogger;
    logger?: TdHeightSyncLogger;
  };
  return (
    brokerWithLogger.getLogger?.("td-height-sync") ??
    brokerWithLogger.logger ??
    console
  );
}

function formatTdHeightSyncError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export interface TdHeightSyncPayload {
  trustDepositList?: TdMessageLike[];
  tdEventsFromBlock?: TdMessageLike["txEvents"];
}

async function runWithConcurrency<T, R>(
  items: T[],
  concurrency: number,
  fn: (item: T) => Promise<R>
): Promise<R[]> {
  const results: (R | undefined)[] = new Array(items.length);
  let index = 0;
  async function worker(): Promise<void> {
    while (index < items.length) {
      const i = index++;
      const item = items[i];
      if (item === undefined) continue;
      try {
        results[i] = await fn(item);
      } catch {
        //
      }
    }
  }
  const workers = Array.from(
    { length: Math.min(concurrency, items.length) },
    () => worker()
  );
  await Promise.all(workers);
  return results as R[];
}

export async function runHeightSyncTD(
  broker: ServiceBroker,
  payload: TdHeightSyncPayload,
  blockHeight: number
): Promise<void> {
  const logger = getTdHeightSyncLogger(broker);
  const messages = payload.trustDepositList ?? [];
  const events = payload.tdEventsFromBlock ?? [];
  const hasActivity = messages.length > 0 || events.length > 0;

  if (!hasActivity || typeof blockHeight !== "number" || blockHeight <= 0) {
    return;
  }

  const ids = extractImpactedTrustDepositIds(messages, events, true);
  if (ids.length === 0) {
    logger.debug?.(
      `[TD Height Sync] TD activity at block=${blockHeight} but no trust deposit IDs extracted`
    );
    return;
  }

  const seen = new Set<string>();
  const toProcess: string[] = [];
  for (const id of ids) {
    const key = `${blockHeight}::${id}`;
    if (seen.has(key)) continue;
    seen.add(key);
    toProcess.push(id);
  }

  const allAccounts = new Set<string>(toProcess);
  for (const msg of messages) {
    extractAccountAddressesFromTdSources({
      messageContent: msg.content ?? undefined,
      events: msg.txEvents ?? events,
      decodeEventAttributes: true,
    }).forEach((a) => allAccounts.add(a));
  }
  if (events.length > 0) {
    extractAccountAddressesFromTdSources({
      events,
      decodeEventAttributes: true,
    }).forEach((a) => allAccounts.add(a));
  }

  try {
    await broker.call(`${SERVICE.V1.HANDLE_ACCOUNTS.path}.bulkEnsureAccounts`, {
      addresses: [...allAccounts],
    });
    await broker.call(`${SERVICE.V1.HANDLE_ACCOUNTS.path}.bulkRefreshAccountBalances`, {
      addresses: [...allAccounts],
    });
  } catch {
    //
  }

  logger.info?.(
    `[TD Height Sync] Starting sync at block=${blockHeight} for ${toProcess.length} deposit(s)`
  );

  const depositIdToEventType = buildDepositIdToEventTypeMap(messages, true);

  try {
    const outcomes = await runWithConcurrency(
      toProcess,
      TD_CONCURRENCY,
      async (depositId) => {
        try {
          const ledgerState = await fetchTrustDeposit(depositId, blockHeight);
          if (!ledgerState) {
            logger.warn?.(
              `[TD Height Sync] No ledger state for id=${depositId} at block=${blockHeight}`
            );
            return false;
          }
          const eventType = depositIdToEventType.get(depositId) ?? "SYNC_LEDGER";
          const result = (await broker.call(
            `${SERVICE.V1.TrustDepositDatabaseService.path}.syncFromLedger`,
            {
              ledgerTrustDeposit: ledgerState,
              blockHeight,
              eventType,
            }
          )) as { success?: boolean };
          return Boolean(result?.success);
        } catch (error: unknown) {
          logger.warn?.(
            `[TD Height Sync] Sync failed id=${depositId} at block=${blockHeight}: ${formatTdHeightSyncError(error)}`
          );
          return false;
        }
      }
    );
    const synced = outcomes.filter(Boolean).length;
    const failed = toProcess.length - synced;
    logger.info?.(
      `[TD Height Sync] Completed at block=${blockHeight}: synced=${synced}, failed=${failed}`
    );
  } catch (error: unknown) {
    logger.error?.(
      `[TD Height Sync] Failed at block=${blockHeight}: ${formatTdHeightSyncError(error)}`
    );
  }
}
