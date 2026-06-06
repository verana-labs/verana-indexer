# 🏛️ Ecosystem Module

This guide provides a complete overview of the **Ecosystem (EC)** module, including **data models**, **relationships**, **event processing flow**, and **audit/history tracking**.

---

## 📌 Overview

The **Ecosystem module** is responsible for managing:

1. **Ecosystems (EC)** – Core registry entities identified by DID, controlled by a creator, and containing deposits, language preferences, and versioning.
2. **Governance Framework Versions (GFV)** – Versioned governance frameworks linked to EC.
3. **Governance Framework Documents (GFD)** – Documents under each GFV, representing legal, technical, or operational frameworks.

This module ensures **auditability** by recording all EC, GFV, and GFD messages in history tables.

---

## 📊 Data Models & Relationships

```mermaid
erDiagram
    ECOSYSTEM ||--o{ GOVERNANCE_FRAMEWORK_VERSION : has
    GOVERNANCE_FRAMEWORK_VERSION ||--o{ GOVERNANCE_FRAMEWORK_DOCUMENT : contains

    ECOSYSTEM {
        int id PK
        string did
        string controller
        string aka
        string language
        timestamp created
        timestamp modified
        timestamp archived
        int active_version
        bigint height
        decimal deposit
    }

    GOVERNANCE_FRAMEWORK_VERSION {
        int id PK
        int ecosystem_id FK
        int version
        timestamp created
        timestamp active_since
    }

    GOVERNANCE_FRAMEWORK_DOCUMENT {
        int id PK
        int gfv_id FK
        timestamp created
        string language
        string url
        string digest_sri
    }
```

---

## ⚡ Event Processing Flow

```mermaid
flowchart TD
    A[Incoming EC Event] --> B{Event Type}
    B -->|Create| C[Insert EC + GFV + GFD]
    B -->|Update| D[Update EC Fields & Record Changes]
    B -->|Archive| E[Mark EC as Archived]
    B -->|Add Governance Framework Doc| F[Add GFV + GFD]
    B -->|Increase Governance Framework Version| G[Activate Next GFV]
    
    C --> H[Record EC, GFV, GFD]
    D --> H
    E --> H
    F --> H
    G --> H
```

---

## ⚙️ Event Types & Description

| Event Type                             | Description                                     |
| -------------------------------------- | ----------------------------------------------- |
| **Create / CreateLegacy**              | Create EC along with initial GFV and GFD.       |
| **Update**                             | Update EC fields (DID, AKA, language, deposit). |
| **Archive**                            | Mark EC as archived in the system.              |
| **AddGovernanceFrameworkDoc**          | Add a new GFV and associated GFD(s).            |
| **IncreaseGovernanceFrameworkVersion** | Activate the next version of GFV for the EC.    |

---

## 📜 History & Audit Reference

All changes to EC, GFV, and GFD entities are recorded in **history tables** for auditability and traceability.
You can view the **complete Ecosystem History module** here:

[📖 Ecosystem History Module – Developer Guide](./crawl-ec-history.md)

This link provides **visual ER diagrams, change tracking flows, and architecture overviews** for historical data.

---

## API: `trust_data` Enrichment

Ecosystem API methods support optional trust enrichment using query parameter `trust_data`:

- `GET /verana/ec/v1/get/{id}`
- `GET /verana/ec/v1/list`

Allowed values:

- `null` (default): no trust enrichment
- `summary`: attach trust summary payload
- `full`: attach full trust payload

Behavior:

- For each returned object that includes `did`, a sibling field `trust_data` is added at the same level.
- `trust_data` is `null` when disabled/unavailable.
