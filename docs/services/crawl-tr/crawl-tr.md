# 🏛️ Trust Registry Module

This guide provides a complete overview of the **Trust Registry (TR)** module, including **data models**, **relationships**, **event processing flow**, and **audit/history tracking**.

---

## 📌 Overview

The **Trust Registry module** is responsible for managing:

1. **Trust Registries (TR)** – Core registry entities identified by DID, controlled by a creator, and containing deposits, language preferences, and versioning.
2. **Governance Framework Versions (GFV)** – Versioned governance frameworks linked to TR.
3. **Governance Framework Documents (GFD)** – Documents under each GFV, representing legal, technical, or operational frameworks.

This module ensures **auditability** by recording all TR, GFV, and GFD messages in history tables.

---

## 📊 Data Models & Relationships

```mermaid
erDiagram
    TRUST_REGISTRY ||--o{ GOVERNANCE_FRAMEWORK_VERSION : has
    GOVERNANCE_FRAMEWORK_VERSION ||--o{ GOVERNANCE_FRAMEWORK_DOCUMENT : contains

    TRUST_REGISTRY {
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
        int tr_id FK
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
    A[Incoming TR Event] --> B{Event Type}
    B -->|Create| C[Insert TR + GFV + GFD]
    B -->|Update| D[Update TR Fields & Record Changes]
    B -->|Archive| E[Mark TR as Archived]
    B -->|Add Governance Framework Doc| F[Add GFV + GFD]
    B -->|Increase Governance Framework Version| G[Activate Next GFV]
    
    C --> H[Record TR, GFV, GFD]
    D --> H
    E --> H
    F --> H
    G --> H
```

---

## ⚙️ Event Types & Description

| Event Type                             | Description                                     |
| -------------------------------------- | ----------------------------------------------- |
| **Create / CreateLegacy**              | Create TR along with initial GFV and GFD.       |
| **Update**                             | Update TR fields (DID, AKA, language, deposit). |
| **Archive**                            | Mark TR as archived in the system.              |
| **AddGovernanceFrameworkDoc**          | Add a new GFV and associated GFD(s).            |
| **IncreaseGovernanceFrameworkVersion** | Activate the next version of GFV for the TR.    |

---

## 📜 History & Audit Reference

All changes to TR, GFV, and GFD entities are recorded in **history tables** for auditability and traceability.
You can view the **complete Trust Registry History module** here:

[📖 Trust Registry History Module – Developer Guide](./crawl-tr-history.md)

This link provides **visual ER diagrams, change tracking flows, and architecture overviews** for historical data.

---

## API: `trust_data` Enrichment

Trust Registry API methods support optional trust enrichment using query parameter `trust_data`:

- `GET /verana/tr/v1/get/{id}`
- `GET /verana/tr/v1/list`

Allowed values:

- `null` (default): no trust enrichment
- `summary`: attach trust summary payload
- `full`: attach full trust payload

Behavior:

- For each returned object that includes `did`, a sibling field `trust_data` is added at the same level.
- `trust_data` is `null` when disabled/unavailable.
