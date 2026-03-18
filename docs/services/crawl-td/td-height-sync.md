# Trust Deposit Height-Sync Refactor

## Overview

This document describes the Trust Deposit (TD) height-sync path used by the indexer to sync TD state from the ledger (LCD) by block height.

When enabled, the indexer extracts affected trust deposit identifiers from message content and transaction events, fetches authoritative state from the ledger at the processed block height (`GET /verana/td/v1/get/{id}` with header `x-cosmos-block-height`), then updates the indexer database and records history from the computed diff.

## Why This Exists

The height-sync path improves TD synchronization by:

- Using the ledger as the single source of truth at a given block height
- Reducing dependence on event payload parsing and legacy message handlers
- Aligning with the same architecture used for CS and PERM height-sync

## Enable / Disable

Use the environment variable:

- **Height-sync mode (recommended):** `USE_HEIGHT_SYNC_TD=true`
- **Legacy mode:** Omit or set to any value other than `"true"` (e.g. `false`)

When `USE_HEIGHT_SYNC_TD=true`:

- **Message path** (`crawl_tx.service`): TD messages are processed via `runHeightSyncTD` (extract IDs → fetch ledger → `TrustDepositDatabaseService.syncFromLedger`) instead of `TrustDepositMessageProcessorService.handleTrustDepositMessages`.
- **Event path** (`CrawlTrustDepositService`): Block events (e.g. `adjust_trust_deposit`, `slash_trust_deposit`) are processed via `processTrustDepositHeightSync` (extract deposit IDs from events → fetch ledger at block height → `syncFromLedger`) instead of legacy adjust/slash handlers.

When disabled, the indexer uses the legacy event and message processor paths.

## Execution Flow

1. **Message path:** `crawl_tx.service` builds `trustDepositList`; if `USE_HEIGHT_SYNC_TD=true` and `blockHeight` is set, it calls `runHeightSyncTD(broker, { trustDepositList }, blockHeight)`.
2. **Event path:** `CrawlTrustDepositService.processBlockEventsInternal`; if `USE_HEIGHT_SYNC_TD=true`, it calls `processTrustDepositHeightSync(block.height, events)` instead of legacy adjust/slash handlers.
3. **Helpers** (`td_height_sync_helpers.ts`): `extractImpactedTrustDepositIds` (from message content and events, with base64 decode); `fetchTrustDeposit(id, blockHeight)` calls LCD with `x-cosmos-block-height`.
4. **Sync:** `TrustDepositDatabaseService.syncFromLedger(ledgerTrustDeposit, blockHeight, eventType)` upserts `trust_deposits` and writes to `trust_deposit_history` only when there are changes.
5. **Deduplication:** Same (height, deposit id) is processed only once per run using key `{height}::{depositId}`.

## TD Messages Supported in Height-Sync

- `/verana.td.v1.MsgAdjustTrustDeposit`
- `/verana.td.v1.MsgReclaimTrustDepositYield`
- `/verana.td.v1.MsgReclaimTrustDeposit`
- `/verana.td.v1.MsgSlashTrustDeposit`
- `/verana.td.v1.MsgRepaySlashedTrustDeposit`
- `/verana.td.v1.MsgBurnEcosystemSlashedTrustDeposit`
- `/verana.td.v1.MsgUpdateParams` (skipped for ID extraction; params handled elsewhere)

## Blockchain Event Types (TD Module)

The chain emits these event types for the TD module (see `TrustDepositEventType` in `src/common/constant.ts`):

- `slash_trust_deposit`
- `repay_slashed_trust_deposit`
- `reclaim_trust_deposit_yield`
- `reclaim_trust_deposit`
- `adjust_trust_deposit`
- `yield_distribution`
- `yield_transfer`

Each is mapped to a history `event_type` stored in `trust_deposit_history` (e.g. `SLASH_TRUST_DEPOSIT`, `REPAY_SLASHED`, `RECLAIM_YIELD`, `RECLAIM_DEPOSIT`, `ADJUST_TRUST_DEPOSIT`, `YIELD_DISTRIBUTION`, `YIELD_TRANSFER`). When syncing from **events**, the indexer uses `buildDepositIdToEventTypeMapFromEvents` so each deposit gets the correct event type from the blockchain event that affected it.

## Event Extraction

Trust deposit identifiers are taken from transaction event attributes such as:

- `trust_deposit_id`
- `account`
- `owner`
- `deposit_account`

Attribute keys used in TD events (see `TrustDepositEventAttributeKey` in `src/common/constant.ts`) include: `account`, `amount`, `slash_count`, `repaid_by`, `timestamp`, `new_amount`, `new_share`, `new_claimable`, and others. Attributes are base64-decoded when needed; IDs are deduplicated before ledger fetch and sync.
