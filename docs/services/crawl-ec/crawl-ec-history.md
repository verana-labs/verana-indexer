
# Ecosystem History Module – Developer Guide

This document provides a complete **visual overview** of the Ecosystem History module, including all data models, relationships, and change tracking flows.

---

## 📌 Overview

The system tracks changes for auditability:

1. **EC History** – Tracks Ecosystem creation, updates, and archival.
2. **GFV History** – Tracks Governance Framework Version changes.
3. **GFD History** – Tracks Governance Framework Document additions and updates.

---

## 📊 Data Models & Relationships

```mermaid
erDiagram
    ECOSYSTEM ||--o{ ECOSYSTEM_HISTORY : logs
    GOVERNANCE_FRAMEWORK_VERSION ||--o{ GFV_HISTORY : logs
    GOVERNANCE_FRAMEWORK_DOCUMENT ||--o{ GFD_HISTORY : logs

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

    ECOSYSTEM_HISTORY {
        int id PK
        int ecosystem_id FK
        string event_type
        bigint height
        json changes
        timestamp created_at
    }

    GOVERNANCE_FRAMEWORK_VERSION {
        int id PK
        int ecosystem_id FK
        int version
        timestamp created
        timestamp active_since
    }

    GFV_HISTORY {
        int id PK
        int gfv_id FK
        int ecosystem_id FK
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
        int ecosystem_id FK
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
    A[Ecosystem] --> B[EC History]
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
    subgraph Ecosystem
        EC[EC Table]
        TRH[EC History Table]
    end

    subgraph GovernanceFramework
        GFV[GFV Table]
        GFVH[GFV History Table]
        GFD[GFD Table]
        GFDH[GFD History Table]
    end

    EC --> TRH
    GFV --> GFVH
    GFD --> GFDH
```
