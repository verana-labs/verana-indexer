# Permission Services Documentation

This document describes the **Permission Processor Service** (`PermProcessorService`) and **Permission Ingest Service** (`PermIngestService`) used in the system. It explains how permission messages are handled, how data is saved and updated in the database, and the overall workflow.

---

## Table of Contents

1. [Overview](#overview)
2. [Services](#services)
   - [PermProcessorService](#permprocessorservice)
   - [PermIngestService](#permingestservice)
3. [Data Flow](#data-flow)
4. [Permission Sessions](#permission-sessions)
5. [Mermaid Diagrams](#mermaid-diagrams)
6. [Database Tables](#database-tables)
7. [Notes](#notes)

---

## Overview

The permission services are responsible for:

- Handling incoming **permission messages** from the blockchain.
- Creating, updating, and managing **permissions** and **permission sessions**.
- Validating, revoking, extending, or renewing permissions.
- Calculating fees and deposits based on global variables.
- Ensuring proper authorization and access control for all operations.

All database operations are performed using **Knex.js**, and transactions are used to ensure atomic updates.

---

## Permission Height-Sync Refactor

Permission processing supports two modes:

- **Height-sync mode (recommended)**: `USE_HEIGHT_SYNC_PERM=true`
- **Legacy mode**: `USE_HEIGHT_SYNC_PERM=false`

In height-sync mode, the **Permission module** uses a ledger-at-height strategy (similar in spirit to the CS height-sync refactor, but scoped only to Permission entities):

1. Decode tx message and detect Permission message type.
2. Read current processed block height from transaction context.
3. Query ledger with header `x-cosmos-block-height: <height>`.
4. Fetch authoritative state **for Permission entities only**:
   - Permission: `GET /verana/perm/v1/get/{id}`
   - PermissionSession: `GET /verana/perm/v1/get_session/{id}`
   
5. Sync DB from ledger state (permissions and permission_sessions only).
6. Compare indexer state vs ledger state at same height and log diffs.
7. Run rolling multi-height verification window (3 heights) for impacted permissions and log inconsistencies.

This mode is designed to keep Permission state aligned with blockchain state across heights and reduce drift from partial message reconstruction.

---

## Services

### PermProcessorService

Acts as a **message processor** that receives messages from external sources (like blockchain events) and delegates them to the `PermIngestService`.

**Main Responsibilities:**

- Receives an array of `permissionMessages` via `handlePermissionMessages`.
- In `USE_HEIGHT_SYNC_PERM=true`, resolves impacted entities and performs ledger-backed sync + verification.
- In `USE_HEIGHT_SYNC_PERM=false`, maps message types to legacy handlers in `PermIngestService`.
- Logs all received messages.
- Provides actions to **get** or **list permissions**.

**Message Handling Example:**

```ts

  switch (msg.type) {
        case PermissionMessageTypes.CreateRootPermission:
          await this.broker.call("permIngest.handleMsgCreateRootPermission", {
            data: payload,
          });
          break;
        case PermissionMessageTypes.CreatePermission:
          await this.broker.call("permIngest.handleMsgCreatePermission", {
            data: payload,
          });
          break;
        case PermissionMessageTypes.ExtendPermission:
          await this.broker.call("permIngest.handleMsgExtendPermission", {
            data: payload,
          });
          break;
        case PermissionMessageTypes.RevokePermission:
          await this.broker.call("permIngest.handleMsgRevokePermission", {
            data: payload,
          });
          break;
        case PermissionMessageTypes.StartPermissionVP:
          await this.broker.call("permIngest.handleMsgStartPermissionVP", {
            data: payload,
          });
          break;
        case PermissionMessageTypes.SetPermissionVPToValidated:
          await this.broker.call(
            "permIngest.handleMsgSetPermissionVPToValidated",
            { data: payload }
          );
          break;
        case PermissionMessageTypes.RenewPermissionVP:
          await this.broker.call("permIngest.handleMsgRenewPermissionVP", {
            data: payload,
          });
          break;
        case PermissionMessageTypes.CancelPermissionVPLastRequest:
          await this.broker.call(
            "permIngest.handleMsgCancelPermissionVPLastRequest",
            { data: payload }
          );
          break;
        case PermissionMessageTypes.CreateOrUpdatePermissionSession:
          await this.broker.call(
            "permIngest.handleMsgCreateOrUpdatePermissionSession",
            { data: payload }
          );
          break;
        case PermissionMessageTypes.SlashPermissionTrustDeposit:
          await this.broker.call(
            "permIngest.handleMsgSlashPermissionTrustDeposit",
            { data: payload }
          );
          break;
        case PermissionMessageTypes.RepayPermissionSlashedTrustDeposit:
          await this.broker.call(
            "permIngest.handleMsgRepayPermissionSlashedTrustDeposit",
            { data: payload }
          );
          break;
        default:
          break;
      }
````

---

### PermIngestService

Responsible for **directly interacting with the database** to manage permissions.

**Actions include:**

* `handleMsgCreateRootPermission`
* `handleMsgCreatePermission`
* `handleMsgExtendPermission`
* `handleMsgRevokePermission`
* `handleMsgStartPermissionVP`
* `handleMsgSetPermissionVPToValidated`
* `handleMsgRenewPermissionVP`
* `handleMsgCancelPermissionVPLastRequest`
* `handleMsgSlashPermissionTrustDeposit`
* `handleMsgRepayPermissionSlashedTrustDeposit`
* `handleMsgCreateOrUpdatePermissionSession`
* `syncPermissionFromLedger`
* `syncPermissionSessionFromLedger`
* `comparePermissionWithLedger`
* `comparePermissionSessionWithLedger`

**Key Points:**

* **Transactions** are used for creating/updating permission sessions and VPs to ensure atomicity.
* **Validation checks** are performed for required fields, type correctness, and authorization.
* Fee and deposit calculations rely on **global variables** (`trust_unit_price` and `trust_deposit_rate`).
* In height-sync mode, ledger state is authoritative and compare actions are used to detect and log mismatches.

---

## Data Flow

1. **Permission messages** arrive at `PermProcessorService`.
2. If `USE_HEIGHT_SYNC_PERM=true`:
   * Impacted IDs are extracted from message payload + tx events.
   * Ledger entities are queried at processed block height.
   * `PermIngestService` syncs DB from ledger responses.
   * Runtime compare checks are executed (same height + rolling multi-height window).
3. If `USE_HEIGHT_SYNC_PERM=false`:
   * Each message type is routed to legacy `handleMsg*` handlers.
4. Post-sync statistics are recalculated (permission tree participants/weight, schema/trust-registry/global metrics refresh).

---

## Permission Sessions

**Permission sessions** track authorization between agents, issuers, and verifiers.

**Example JSON Stored in DB:**

```json
{
  "id": "123e4567-e89b-12d3-a456-426614174000",
  "controller": "verana1evvrzxw9yg5staqdvumd6fupy3jhaxfflla7st",
  "agent_perm_id": "17",
  "authz": [
    {
      "executor_perm_id": "17",
      "beneficiary_perm_id": "0",
      "wallet_agent_perm_id": "17"
    }
  ],
  "created": "2025-10-07T09:58:02.067837358Z",
  "modified": "2025-10-07T09:58:02.067837358Z"
}
```
```
* **`id`**: Session ID (UUID)
* **`controller`**: The account that controls this session
* **`agent_perm_id`**: Permission ID of the agent
* **`authz`**: Array of authorization entries
* **`created` / `modified`**: Timestamps

**Handling Updates:**

* If a session exists, new `authz` entries are appended.
* Otherwise, a new session row is created.

---

## Mermaid Diagrams

### 1. Permission Message Flow

```mermaid
flowchart TD
  A[Blockchain / Client Message] --> B[PermProcessorService]
  B --> C{Message Type}
  C -->|CreateRootPermission| D[handleMsgCreateRootPermission]
  C -->|CreatePermission| E[handleMsgCreatePermission]
  C -->|CreateOrUpdatePermissionSession| F[handleMsgCreateOrUpdatePermissionSession]
  F --> G[permission_sessions Table]
  D --> H[permissions Table]
  E --> H
```

### 2. Permission Session Update Flow

```mermaid
flowchart TD
  A[New MsgCreateOrUpdatePermissionSession] --> B{Check Existing Session?}
  B -->|Yes| C[Parse Existing Authz]
  C --> D[Append New Authz Entry]
  D --> E[Update permission_sessions]
  B -->|No| F[Insert New Permission Session]
```

### 3. VP Lifecycle Flow

```mermaid
flowchart TD
  A[StartPermissionVP] --> B[Insert VP Record in permissions]
  B --> C[SetPermissionVPToValidated?]
  C -->|Yes| D[Update vp_state=VALIDATED, vp_exp]
  C -->|No| E[Pending VP]
  D --> F[RenewPermissionVP / Extend / Cancel]
```

---

## Database Tables

### permissions

* `id` (BIGINT or UUID depending on migration)
* `schema_id`
* `type` (ECOSYSTEM, ISSUER, VERIFIER, HOLDER, etc.)
* `grantee`
* `validator_perm_id`
* `vp_state` (PENDING, VALIDATED, TERMINATED)
* `vp_current_fees`, `vp_current_deposit`
* `effective_from`, `effective_until`
* `modified`, `created`

### permission_sessions

* `id` (UUID)
* `controller`
* `agent_perm_id`
* `wallet_agent_perm_id`
* `authz` (JSON array)
* `created`, `modified`

---

## Notes

* `authz` entries store the authorization relationships for a session.
* All monetary values (fees, deposits) are stored as strings to avoid floating point errors.
* Global variables (`trust_unit_price` and `trust_deposit_rate`) are essential for fee and deposit calculations.
* UUIDs are used for session IDs, while permission IDs may be numeric (`BIGINT`) or string depending on migration.
* All actions are **idempotent**: repeated messages for the same session or permission do not create duplicates.

---

## Summary

The services together provide a **robust, blockchain-integrated permission management system**.

* **PermProcessorService**: Handles incoming messages and routes them.
* **PermIngestService**: Handles DB operations for creating, updating, validating, revoking, or extending permissions.
* **Permission sessions** track authorization among agents, issuers, and verifiers.
* **VPs** (Validation Processes) have their own lifecycle with fees and deposits.
