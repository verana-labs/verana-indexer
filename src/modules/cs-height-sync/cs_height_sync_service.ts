import type { ServiceBroker } from "moleculer";
import { SERVICE } from "../../common";
import {
  extractImpactedCredentialSchemaIds,
  getCredentialSchema,
} from "./cs_height_sync_helpers";

const CS_CONCURRENCY = 8;

type CsHeightSyncLogger = {
  info?: (message: string) => void;
  warn?: (message: string) => void;
  error?: (message: string) => void;
  debug?: (message: string) => void;
};

function getCsHeightSyncLogger(broker: ServiceBroker): CsHeightSyncLogger {
  const brokerWithLogger = broker as ServiceBroker & {
    getLogger?: (name: string) => CsHeightSyncLogger;
    logger?: CsHeightSyncLogger;
  };
  return brokerWithLogger.getLogger?.("cs-height-sync") ??
    brokerWithLogger.logger ??
    console;
}

function formatCsHeightSyncError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export interface CsHeightSyncPayload {
  credentialSchemaMessages?: Array<{ type: string; content?: Record<string, unknown> | null }>;
  csEventsFromBlock?: Array<{
    type?: string;
    attributes?: Array<{ key?: string; value?: string }>;
  }>;
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

export async function syncCredentialSchemas(
  broker: ServiceBroker,
  blockHeight: number,
  credentialSchemaIds: number[]
): Promise<{ synced: number; failed: number }> {
  const logger = getCsHeightSyncLogger(broker);
  const ids = [...new Set(credentialSchemaIds)].filter(
    (id): id is number => Number.isInteger(id) && id > 0
  );
  if (ids.length === 0) return { synced: 0, failed: 0 };
  const blockHeightNum = Number(blockHeight) || 0;
  const outcomes = await runWithConcurrency(ids, CS_CONCURRENCY, async (id) => {
    try {
      const ledgerResponse = await getCredentialSchema(id, blockHeightNum);
      if (!ledgerResponse?.schema) {
        logger.warn?.(
          `[CS Height Sync] Ledger returned no schema for id=${id} at block=${blockHeightNum}`
        );
        return false;
      }
      const result = (await broker.call(
        `${SERVICE.V1.CredentialSchemaDatabaseService.path}.syncFromLedger`,
        {
          ledgerResponse: { schema: ledgerResponse.schema },
          blockHeight: blockHeightNum,
        }
      )) as { success?: boolean };
      const success = Boolean(result?.success);
      if (!success) {
        logger.warn?.(
          `[CS Height Sync] syncFromLedger reported non-success for schema id=${id} at block=${blockHeightNum}`
        );
      }
      return success;
    } catch (error: unknown) {
      logger.warn?.(
        `[CS Height Sync] Failed syncing schema id=${id} at block=${blockHeightNum}: ${formatCsHeightSyncError(error)}`
      );
      return false;
    }
  });
  const synced = outcomes.filter(Boolean).length;
  const failed = ids.length - synced;
  return { synced, failed };
}

export async function runHeightSyncCS(
  broker: ServiceBroker,
  payload: CsHeightSyncPayload,
  blockHeight: number
): Promise<void> {
  const logger = getCsHeightSyncLogger(broker);
  const hasActivity =
    (payload.credentialSchemaMessages?.length ?? 0) > 0 ||
    (payload.csEventsFromBlock?.length ?? 0) > 0;

  if (!hasActivity || typeof blockHeight !== "number") {
    return;
  }

  const ids = extractImpactedCredentialSchemaIds(
    payload.credentialSchemaMessages ?? [],
    payload.csEventsFromBlock ?? [],
    true
  );

  if (ids.length === 0) {
    logger.debug?.(
      `[CS Height Sync] CS activity detected at block=${blockHeight} but no credential schema IDs were extracted`
    );
    return;
  }

  logger.info?.(
    `[CS Height Sync] Starting sync at block=${blockHeight} for ${ids.length} schema(s): ${ids.join(",")}`
  );

  try {
    const result = await syncCredentialSchemas(broker, blockHeight, ids);
    logger.info?.(
      `[CS Height Sync] Completed sync at block=${blockHeight}: synced=${result.synced}, failed=${result.failed}`
    );
  } catch (error: unknown) {
    logger.error?.(
      `[CS Height Sync] Failed sync at block=${blockHeight}: ${formatCsHeightSyncError(error)}`
    );
  }
}
