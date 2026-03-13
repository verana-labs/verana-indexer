import type { ServiceBroker } from "moleculer";
import {
  PermissionMessagePayload,
  extractImpactedPermissionIds,
  extractImpactedSessionIds,
  fetchPermLedgerJson,
} from "./perm_height_sync_helpers";

const PERM_INGEST_SERVICE = "permIngest";
const PERM_MULTI_HEIGHT_WINDOW = 3;

type PermHeightSyncLogger = {
  info?: (message: string) => void;
  warn?: (message: string) => void;
  error?: (message: string) => void;
  debug?: (message: string) => void;
};

function getPermHeightSyncLogger(broker: ServiceBroker): PermHeightSyncLogger {
  const brokerWithLogger = broker as ServiceBroker & {
    getLogger?: (name: string) => PermHeightSyncLogger;
    logger?: PermHeightSyncLogger;
  };
  return (
    brokerWithLogger.getLogger?.("perm-height-sync") ??
    brokerWithLogger.logger ??
    console
  );
}

export async function runHeightSyncPerm(
  broker: ServiceBroker,
  messages: PermissionMessagePayload[]
): Promise<void> {
  if (!Array.isArray(messages) || messages.length === 0) return;

  const logger = getPermHeightSyncLogger(broker);

  for (const msg of messages) {
    const blockHeight = Number(msg.height || 0);
    if (!Number.isInteger(blockHeight) || blockHeight <= 0) {
      continue;
    }

    const permissionIds = extractImpactedPermissionIds(msg);
    const sessionIds = extractImpactedSessionIds(msg);
    if (permissionIds.length === 0 && sessionIds.length === 0) {
      logger.warn?.(
        `[PERM Height Sync] No impacted permission/session IDs resolved from message/events; skipping txHash=${msg.txHash || "unknown"} at block=${blockHeight}`
      );
      continue;
    }

    const impactedPermissionIds = new Set<number>();

    for (const permissionId of permissionIds) {
      const ledgerPermissionResponse = await fetchPermLedgerJson(
        `/verana/perm/v1/get/${permissionId}`,
        blockHeight
      );
      const ledgerPermission = ledgerPermissionResponse?.permission;
      if (!ledgerPermission) continue;
      impactedPermissionIds.add(permissionId);

      const syncResult = (await broker.call(
        `${PERM_INGEST_SERVICE}.syncPermissionFromLedger`,
        {
          ledgerPermission,
          blockHeight,
          txHash: msg.txHash,
          msgType: msg.type,
        }
      )) as any;

      const immediateCompare = (await broker.call(
        `${PERM_INGEST_SERVICE}.comparePermissionWithLedger`,
        {
          permissionId,
          ledgerPermission,
          blockHeight,
        }
      )) as any;
      if (immediateCompare?.matches === false) {
        logger.warn?.(
          `[PERM Height Sync] Mismatch after sync for permission id=${permissionId} at block=${blockHeight}: ${JSON.stringify(
            immediateCompare?.diffs || []
          )}`
        );
      }

      const schemaId = Number(
        syncResult?.schemaId ?? ledgerPermission?.schema_id
      );
      if (!Number.isInteger(schemaId) || schemaId <= 0) {
        logger.warn?.(
          `[PERM TEMP DEBUG] synced permission missing schema reference id=${permissionId} height=${blockHeight}`
        );
      }
    }

    for (const sessionId of sessionIds) {
      const sessionResponse = await fetchPermLedgerJson(
        `/verana/perm/v1/get_session/${encodeURIComponent(sessionId)}`,
        blockHeight
      );
      const ledgerSession = sessionResponse?.session;
      if (!ledgerSession) continue;

      await broker.call(
        `${PERM_INGEST_SERVICE}.syncPermissionSessionFromLedger`,
        {
          ledgerSession,
          blockHeight,
          txHash: msg.txHash,
          msgType: msg.type,
        }
      );

      const compareSession = (await broker.call(
        `${PERM_INGEST_SERVICE}.comparePermissionSessionWithLedger`,
        {
          sessionId,
          ledgerSession,
          blockHeight,
        }
      )) as any;
      if (compareSession?.matches === false) {
        logger.warn?.(
          `[PERM Height Sync] Mismatch after sync for permission session id=${sessionId} at block=${blockHeight}: ${JSON.stringify(
            compareSession?.diffs || []
          )}`
        );
      }
    }

    for (const permissionId of impactedPermissionIds) {
      const heightsToCheck: number[] = [];
      for (let i = PERM_MULTI_HEIGHT_WINDOW - 1; i >= 0; i--) {
        const h = blockHeight - i;
        if (h > 0) heightsToCheck.push(h);
      }

      for (const h of heightsToCheck) {
        const ledgerPermissionResponse = await fetchPermLedgerJson(
          `/verana/perm/v1/get/${permissionId}`,
          h
        );
        const ledgerPermission = ledgerPermissionResponse?.permission;
        if (!ledgerPermission) continue;

        const compare = (await broker.call(
          `${PERM_INGEST_SERVICE}.comparePermissionWithLedger`,
          {
            permissionId,
            ledgerPermission,
            blockHeight: h,
          }
        )) as any;
        if (compare?.matches === false) {
          logger.warn?.(
            `[PERM Multi-Height Verify] Mismatch permission id=${permissionId} at block=${h}: ${JSON.stringify(
              compare?.diffs || []
            )}`
          );
        }
      }
    }
  }
}

