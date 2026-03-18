# 🏦 Trust Deposit Processor

This module provides **indexing**, **processing**, and **query APIs** for Trust Deposit state on the blockchain.  
It listens to **on-chain events**, **transaction messages**, and **slash actions** — then maintains a **real-time database view** for querying trust deposit balances and stats.

---

## Height-Sync vs Legacy

- **Height-sync mode (recommended):** `USE_HEIGHT_SYNC_TD=true`  
  The indexer reconciles with the ledger at the processed block height (`GET /verana/td/v1/get/{id}` with `x-cosmos-block-height`), then updates the DB and history from the diff. Used for both the **message path** (crawl_tx → `runHeightSyncTD`) and the **event path** (CrawlTrustDepositService → `processTrustDepositHeightSync`).
- **Legacy mode:** `USE_HEIGHT_SYNC_TD` unset or not `"true"`  
  Uses the legacy event handlers (adjust/slash) and `TrustDepositMessageProcessorService.handleTrustDepositMessages` for decoded TD messages.

See **[td-height-sync.md](./td-height-sync.md)** for flow, supported messages, and module layout.

---

## 🧭 Architecture Overview

```mermaid
flowchart LR
  A[⛓ Blockchain Events] --> B[CrawlTrustDepositService]
  B -->|Adjust events| C[TrustDeposit Table]
  D[🔔 Msg Reclaim / Slash / Repay] --> E[TrustDepositMessageProcessorService]
  E -->|Updates| C
  F[📡 API Requests] --> G[TrustDepositDatabaseService]
  G -->|Read Queries| C
```

## 🧱 Database Schema

### `trust_deposits`

| Column            | Type      | Description                                           |
| ----------------- | --------- | ----------------------------------------------------- |
| `id`              | UUID / PK | Table's identity                                      |
| `account`         | string    | Account address associated with this trust deposit    |
| `share`           | string    | Total share amount currently held by this account     |
| `amount`          | string    | Total trust deposit amount                            |
| `claimable`       | string    | Amount that can currently be claimed by the account   |
| `slashed_deposit` | string    | Total amount slashed from this trust deposit          |
| `repaid_deposit`  | string    | Total amount repaid to this trust deposit after slash |
| `last_slashed`    | datetime  | Timestamp of the last slashing event                  |
| `last_repaid`     | datetime  | Timestamp of the last repayment event                 |
| `slash_count`     | integer   | Total number of slashing events                       |
| `last_repaid_by`  | string    | Account that repaid the last slashed amount           |

📝 **Notes**

- Amounts are stored as strings (to avoid precision loss on large values).
- Available balance = `amount - slashed_deposit + repaid_deposit`.

---

## 🕵️ 1. CrawlTrustDepositService

**Path:** `src/services/crawl-td/td_processor.service.ts`  
**Responsibility:**  
Continuously crawls blocks and extracts **`adjust_trust_deposit`** and **`slash_trust_deposit`** events from transaction responses. When `USE_HEIGHT_SYNC_TD=true`, uses height-sync (ledger fetch at block height + `syncFromLedger`); otherwise updates `trust_deposits` via legacy adjust/slash handlers.

### Key Features

- Tracks `BlockCheckpoint` to resume crawling from the last processed block.
- Reads transaction events and filters for `adjust_trust_deposit`.
- Updates `amount`, `share`, `claimable`, `slashed_deposit`, `repaid_deposit`, and `slash_count`.
- Handles slashing and repayment adjustments automatically.

```mermaid
sequenceDiagram
  participant Crawler as CrawlTrustDepositService
  participant Tx as Transaction DB
  participant TD as TrustDeposit Table

  Crawler->>Tx: Fetch transactions by height window
  Tx-->>Crawler: Tx list with events
  Crawler->>Crawler: Filter adjust_trust_deposit events
  Crawler->>TD: Insert or update trust deposit records
  Crawler->>BlockCheckpoint: Update height checkpoint
```

---

## 📨 2. TrustDepositMessageProcessorService

**Path:** `src/services/crawl-td/td_message.service.ts`  
**Responsibility:**  
Processes **application-level messages** related to trust deposits (reclaim yield, reclaim deposit, repay slashed, etc.). When `USE_HEIGHT_SYNC_TD=true`, the message path is handled by the height-sync flow in `crawl_tx.service` instead of this service.

### Handled Message Types

| Message Type                       | Action Description                                                    |
| ---------------------------------- | --------------------------------------------------------------------- |
| `RECLAIM_YIELD`                    | Decreases shares and releases claimable yield                         |
| `RECLAIM_DEPOSIT`                  | Burns a portion if applicable and decreases deposit amount            |
| `REPAY_SLASHED`                    | Increases deposit + share to repay previously slashed amount          |
| `SLASH_TRUST_DEPOSIT` _(internal)_ | Deducts deposit and increases slashed_deposit, increments slash_count |

### Internal Logic

- Uses `ModuleParams` for `trust_deposit_share_value` and burn rates.
- Calculates burn amount on reclaim.
- Validates slashing rules and repayment rules.
- Ensures data consistency using DB transactions.

```mermaid
flowchart TD
  A[Msg Received] --> B{Type}
  B -->|RECLAIM_YIELD| C[Update Share]
  B -->|RECLAIM_DEPOSIT| D[Burn + Reduce Claimable + Amount]
  B -->|REPAY_SLASHED| E[Add Amount + Share]
  B -->|SLASH| F[Deduct Amount + Increment Slash Count]
  C & D & E & F --> G[trust_deposits Table]
```

---

## 📡 3. TrustDepositDatabaseService

**Path:** `src/services/crawl-td/td_database.service.ts`
**Responsibility:**
Exposes API actions to **query** trust deposit data from the database.

### Actions

| Action Name         | Description                             | Params                                                   |
| ------------------- | --------------------------------------- | -------------------------------------------------------- |
| `getTrustDeposit`   | Returns a single trust deposit record   | `account` (string, required)                             |
| `listTrustDeposits` | Returns paginated deposits with filters | `min_amount`, `min_share`, `has_claimable`, `is_slashed` |

### Example Response: `getTrustDeposit`

```json
{
  "trust_deposit": {
    "account": "verana1xxx...",
    "share": "12000",
    "amount": "500000000",
    "claimable": "20000",
    "slashed_deposit": "10000",
    "repaid_deposit": "0",
    "last_slashed": "2025-10-09T12:34:56Z",
    "last_repaid": null,
    "slash_count": 1,
    "last_repaid_by": null
  }
}
```

### Example Response: `getTrustDepositStats`

```json
{
  "total_accounts": 1284,
  "total_amount": "4500000000",
  "total_share": "231000",
  "total_claimable": "122000",
  "total_slashed": "120000",
  "total_repaid": "110000",
  "total_slash_events": 88
}
```

---

## 🧭 Entity Relationship Diagram (ERD)

```mermaid
erDiagram
  TRUST_DEPOSITS {
    string id PK
    string account
    string share
    string amount
    string claimable
    string slashed_deposit
    string repaid_deposit
    datetime last_slashed
    datetime last_repaid
    int slash_count
    string last_repaid_by
  }

  BLOCK_CHECKPOINTS {
    string id PK
    string job_name
    bigint height
  }

  TRANSACTIONS {
    string id PK
    bigint height
    json data
  }

  MODULE_PARAMS {
    string id PK
    string module
    json params
  }

  BLOCK_CHECKPOINTS ||--o{ TRANSACTIONS : "tracks height"
  TRANSACTIONS ||--o{ TRUST_DEPOSITS : "adjust_trust_deposit event"
  MODULE_PARAMS ||--o{ TRUST_DEPOSITS : "param-based calculations"
```
