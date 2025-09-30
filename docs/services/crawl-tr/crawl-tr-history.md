
# Trust Registry History Module – Developer Guide

This document provides a complete **visual overview** of the Trust Registry History module, including all data models, relationships, and change tracking flows.

---

## 📌 Overview

The system tracks changes for auditability:

1. **TR History** – Tracks Trust Registry creation, updates, and archival.
2. **GFV History** – Tracks Governance Framework Version changes.
3. **GFD History** – Tracks Governance Framework Document additions and updates.

---

## 📊 Data Models & Relationships

```mermaid
erDiagram
    TRUST_REGISTRY ||--o{ TRUST_REGISTRY_HISTORY : logs
    GOVERNANCE_FRAMEWORK_VERSION ||--o{ GFV_HISTORY : logs
    GOVERNANCE_FRAMEWORK_DOCUMENT ||--o{ GFD_HISTORY : logs

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

    TRUST_REGISTRY_HISTORY {
        int id PK
        int tr_id FK
        string event_type
        bigint height
        json changes
        timestamp created_at
    }

    GOVERNANCE_FRAMEWORK_VERSION {
        int id PK
        int tr_id FK
        int version
        timestamp created
        timestamp active_since
    }

    GFV_HISTORY {
        int id PK
        int gfv_id FK
        int tr_id FK
        string event_type
        bigint height
        json changes
        timestamp created_at
    }

    GOVERNANCE_FRAMEWORK_DOCUMENT {
        int id PK
        int gfv_id FK
        timestamp created
        string language
        string url
        string digest_sri
    }

    GFD_HISTORY {
        int id PK
        int gfd_id FK
        int gfv_id FK
        int tr_id FK
        string event_type
        bigint height
        json changes
        timestamp created_at
    }
```

---

## 🔄 Change Tracking Flow

```mermaid
flowchart TD
    A[Trust Registry] --> B[TR History]
    C[Governance Framework Version] --> D[GFV History]
    E[Governance Framework Document] --> F[GFD History]

    B --> G[Audit Logs / Analytics]
    D --> G
    F --> G
```

---

## 🌐 Module Architecture Overview

```mermaid
graph LR
    subgraph TrustRegistry
        TR[TR Table]
        TRH[TR History Table]
    end

    subgraph GovernanceFramework
        GFV[GFV Table]
        GFVH[GFV History Table]
        GFD[GFD Table]
        GFDH[GFD History Table]
    end

    TR --> TRH
    GFV --> GFVH
    GFD --> GFDH
```
