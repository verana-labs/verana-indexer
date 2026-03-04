# Credential Schema Height-Sync Refactor

## Overview

This document describes the Credential Schema (CS) height-sync refactor path used by the indexer to sync CS state from the ledger (LCD) by block height.

When enabled, the transaction crawler detects CS activity, extracts impacted CS IDs, fetches the latest schema state from the ledger at the target block height, and writes it through `CredentialSchemaDatabaseService.syncFromLedger`.

## Why This Exists

The height-sync path improves CS synchronization reliability by:

- Rebuilding CS state from ledger responses at a specific block height
- Reducing dependence on partial tx message payloads
- Supporting event-based fallback when message payloads do not contain enough CS data

## Enable / Disable

Use the environment variable:

- `USE_HEIGHT_SYNC_CS=true` (recommended)

This is already set to `true` in `.env.example`.

If set to `false`, the indexer uses the legacy CS message processor path (`ProcessCredentialSchemaService.handleCredentialSchemas`).

## Execution Flow

1. `crawl_tx.service.ts` collects:
   - `credentialSchemaMessages`
   - block/tx events
2. If `USE_HEIGHT_SYNC_CS=true`:
   - CS message types are checked first
   - CS event detection is used as fallback
3. `runHeightSyncCS(...)` extracts impacted CS IDs
4. `syncCredentialSchemas(...)` fetches each schema from LCD (`/verana/cs/v1/get/{id}`)
5. `CredentialSchemaDatabaseService.syncFromLedger(...)` persists normalized state to:
   - `credential_schemas`
   - `credential_schema_history`

## Module Files (Current Structure)

- `src/modules/cs-height-sync/cs_height_sync_processor.ts` - Orchestrates routing payload -> CS ID extraction -> sync execution
- `src/modules/cs-height-sync/cs_height_sync_service.ts` - Concurrent CS ledger fetch + DB sync calls
- `src/modules/cs-height-sync/cs_ledger_client.ts` - LCD client helper for `GET /verana/cs/v1/get/{id}`
- `src/modules/cs-height-sync/cs_message_wrapper.ts` - CS message/event filtering and impacted schema ID extraction
- `src/modules/cs-height-sync/cs_meta.ts` - JSON schema title/description extraction helper

