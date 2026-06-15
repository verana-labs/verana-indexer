import type { ServiceBroker } from "moleculer";
import {
  ParticipantMessagePayload,
  extractImpactedParticipantIds,
  extractImpactedSessionIds,
  fetchParticipant,
  fetchParticipantSession,
} from "./pp_height_sync_helpers";

const PARTICIPANT_INGEST_SERVICE = "participantIngest";
const PARTICIPANT_MULTI_HEIGHT_WINDOW = 3;

type ParticipantHeightSyncLogger = {
  info?: (message: string) => void;
  warn?: (message: string) => void;
  error?: (message: string) => void;
  debug?: (message: string) => void;
};

function getParticipantHeightSyncLogger(broker: ServiceBroker): ParticipantHeightSyncLogger {
  const brokerWithLogger = broker as ServiceBroker & {
    getLogger?: (name: string) => ParticipantHeightSyncLogger;
    logger?: ParticipantHeightSyncLogger;
  };
  return (
    brokerWithLogger.getLogger?.("pp-height-sync") ??
    brokerWithLogger.logger ??
    console
  );
}

export async function runHeightSyncParticipant(
  broker: ServiceBroker,
  messages: ParticipantMessagePayload[]
): Promise<void> {
  if (!Array.isArray(messages) || messages.length === 0) return;

  const logger = getParticipantHeightSyncLogger(broker);

  for (const msg of messages) {
    const blockHeight = Number(msg.height || 0);
    if (!Number.isInteger(blockHeight) || blockHeight <= 0) {
      continue;
    }

    const participantIds = extractImpactedParticipantIds(msg);
    const sessionIds = extractImpactedSessionIds(msg);
    if (participantIds.length === 0 && sessionIds.length === 0) {
      logger.warn?.(
        `[PP Height Sync] No impacted participant/session IDs resolved from message/events; skipping txHash=${msg.txHash || "unknown"} at block=${blockHeight}`
      );
      continue;
    }

    const impactedParticipantIds = new Set<number>();

    for (const participantId of participantIds) {
      const ledgerParticipantResponse = await fetchParticipant(
        participantId,
        blockHeight
      );
      const ledgerParticipant = ledgerParticipantResponse?.participant;
      if (!ledgerParticipant) continue;
      impactedParticipantIds.add(participantId);

      const syncResult = (await broker.call(
        `${PARTICIPANT_INGEST_SERVICE}.syncParticipantFromLedger`,
        {
          ledgerParticipant,
          blockHeight,
          txHash: msg.txHash,
          msgType: msg.type,
        }
      )) as any;

      const immediateCompare = (await broker.call(
        `${PARTICIPANT_INGEST_SERVICE}.compareParticipantWithLedger`,
        {
          participantId,
          ledgerParticipant,
          blockHeight,
        }
      )) as any;
      if (immediateCompare?.matches === false) {
        logger.warn?.(
          `[PP Height Sync] Mismatch after sync for participant id=${participantId} at block=${blockHeight}: ${JSON.stringify(
            immediateCompare?.diffs || []
          )}`
        );
      }

      const schemaId = Number(
        syncResult?.schemaId ?? ledgerParticipant?.schema_id
      );
      if (!Number.isInteger(schemaId) || schemaId <= 0) {
        logger.warn?.(
          `[PP TEMP DEBUG] synced participant missing schema reference id=${participantId} height=${blockHeight}`
        );
      }
    }

    for (const sessionId of sessionIds) {
      const sessionResponse = await fetchParticipantSession(
        sessionId,
        blockHeight
      );
      const ledgerSession = sessionResponse?.session;
      if (!ledgerSession) continue;

      await broker.call(
        `${PARTICIPANT_INGEST_SERVICE}.syncParticipantSessionFromLedger`,
        {
          ledgerSession,
          blockHeight,
          txHash: msg.txHash,
          msgType: msg.type,
        }
      );

      const compareSession = (await broker.call(
        `${PARTICIPANT_INGEST_SERVICE}.compareParticipantSessionWithLedger`,
        {
          sessionId,
          ledgerSession,
          blockHeight,
        }
      )) as any;
      if (compareSession?.matches === false) {
        logger.warn?.(
          `[PP Height Sync] Mismatch after sync for participant session id=${sessionId} at block=${blockHeight}: ${JSON.stringify(
            compareSession?.diffs || []
          )}`
        );
      }
    }

    for (const participantId of impactedParticipantIds) {
      const heightsToCheck: number[] = [];
      for (let i = PARTICIPANT_MULTI_HEIGHT_WINDOW - 1; i >= 0; i--) {
        const h = blockHeight - i;
        if (h > 0) heightsToCheck.push(h);
      }

      for (const h of heightsToCheck) {
        const ledgerParticipantResponse = await fetchParticipant(
          participantId,
          h
        );
        const ledgerParticipant = ledgerParticipantResponse?.participant;
        if (!ledgerParticipant) continue;

        const compare = (await broker.call(
          `${PARTICIPANT_INGEST_SERVICE}.compareParticipantWithLedger`,
          {
            participantId,
            ledgerParticipant,
            blockHeight: h,
          }
        )) as any;
        if (compare?.matches === false) {
          logger.warn?.(
            `[PP Multi-Height Verify] Mismatch participant id=${participantId} at block=${h}: ${JSON.stringify(
              compare?.diffs || []
            )}`
          );
        }
      }
    }
  }
}

