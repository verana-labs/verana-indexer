# Verana Indexer

The **Verana Indexer** is a specialized blockchain indexing service built on the [Horoscope V2](https://github.com/aura-nw/horoscope-v2/) framework, designed **exclusively** for the **Verana** decentralized trust ecosystem.

It not only indexes blocks, transactions, and accounts from Cosmos SDK-based blockchains, but also plays a **critical role** in the **Verifiable Trust** architecture by enabling **DID discovery**, **verifiable credential verification**, and **trust resolution** for services and agents on the Verana network.

## Purpose & Scope

While Horoscope V2 provides the base crawling and indexing capabilities, the Verana Indexer’s scope is broader:

- **Verana-Exclusive Integration** – Adapted to Verana’s governance, trust registries, and DID directory.
- **Real-Time DID Crawling & Updating** – Listens for DID-related blockchain events to keep an up-to-date registry of verifiable services (VS) and verifiable user agents (VUA).
- **Trust Resolution Support** – Integrates with the Trust Resolver to validate credentials and return concise Proof-of-Trust results.
- **Service Discovery** – Feeds the DID Directory for indexing verifiable services, enabling fast search for wallets, applications, and other services.
- **Off-chain Enriched Index** – Bridges minimal on-chain records with rich off-chain metadata for high-performance queries.

## Overview Architecture

Indexer consists of multiple services.
All services are small Node applications written in Typescript, built with [Moleculerjs](https://moleculer.services/) framework using [Moleculer TS base](https://github.com/aura-nw/moleculer-ts-base).
The crawler servires utilize [Bull](https://github.com/OptimalBits/bull) for efficient queue management of crawling jobs.

An overview of the architecture is shown below:

```mermaid
flowchart LR
    subgraph EXT["External"]
        CLIENTS["REST Clients"]
        BLOCKCHAIN["Verana Blockchain"]
    end

    subgraph INDEXER["Verana Indexer"]
        API["API Gateway"]
        CRAWLERS["Crawler Services<br/>(block, tx, account, etc.)"]
        PROCESSORS["Verana Processors<br/>(DID, TR, CS, Perm, TD, AR)"]
        DB_SERVICES["Database Services<br/>(Query APIs)"]
    end

    subgraph INFRA["Infrastructure"]
        REDIS["Redis<br/>Transporter | Cache | Queues"]
        POSTGRES["PostgreSQL"]
    end

    CLIENTS -->|HTTP| API
    BLOCKCHAIN -->|RPC/LCD| CRAWLERS
    BLOCKCHAIN -->|RPC/LCD| PROCESSORS
    
    API -.->|Moleculer Broker| DB_SERVICES
    DB_SERVICES -->|Query| POSTGRES
    
    CRAWLERS -->|Create Jobs| REDIS
    PROCESSORS -->|Create Jobs| REDIS
    REDIS -->|Process Jobs| CRAWLERS
    REDIS -->|Process Jobs| PROCESSORS
    
    CRAWLERS -->|Write| POSTGRES
    PROCESSORS -->|Write| POSTGRES

    style API fill:#2196F3,stroke:#1976D2,stroke-width:3px,color:#fff
    style POSTGRES fill:#4CAF50,stroke:#388E3C,stroke-width:3px,color:#fff
    style REDIS fill:#F44336,stroke:#D32F2F,stroke-width:3px,color:#fff
    style BLOCKCHAIN fill:#FF9800,stroke:#F57C00,stroke-width:3px,color:#fff
    style CRAWLERS fill:#9C27B0,stroke:#7B1FA2,stroke-width:2px,color:#fff
    style PROCESSORS fill:#00BCD4,stroke:#0097A7,stroke-width:2px,color:#fff
    style DB_SERVICES fill:#607D8B,stroke:#455A64,stroke-width:2px,color:#fff
```

**Architecture Overview:**

All services are **Moleculer microservices** that communicate via the **Moleculer broker** (using Redis as transporter). The system operates in two main flows:

1. **Crawling Flow**: Crawler services fetch data from the blockchain, create jobs in Redis queues, and workers process these jobs to write indexed data to PostgreSQL.

2. **API Flow**: REST clients call the API Gateway, which routes requests to database services via the Moleculer broker. Database services query PostgreSQL and return results.

**Redis serves three purposes:**
- **Transporter**: Enables Moleculer service-to-service communication
- **Cache**: Provides caching layer for frequently accessed data
- **Job Queues**: Stores and manages crawling jobs using Bull/BullMQ

All services are stateless and can be scaled horizontally.

## Services

A list of services is shown below:

- [**crawl-account**](./docs/services/crawl-account/crawl-account.md): get account auth and its balances
- [**crawl-block**](./docs/services/crawl-block/crawl-block.md): get block from network and insert to DB
- [**crawl-transaction**](./docs/services/crawl-transaction/crawl-tx.md): get transaction in a block and decode to readable
- [**crawl-proposal**](./docs/services/crawl-proposal/crawl-proposal.md): get proposal and its status
- [**crawl-validator**](./docs/services/crawl-validator/crawl-validator.md): get validator and their power event, signing info
- [**crawl-genesis**](./docs/services/crawl-genesis/crawl-genesis.md): get state from genesis chunk
- [**crawl-dids**](./docs/services/crawl-did/crawl-did.md): Crawl and updates DIDs in real time by listening to blockchain events.
- [**crawl-tr**](./docs/services/crawl-tr/crawl-tr.md): Crawl Trust Registry, governance frameworks, and track version changes.
- [**crawl-cs**](./docs/services/crawl-cs//crawl-cs.md): Crawl all credential schema–related transactions and update their state in the database.
- [**crawl-perm**](./docs/services/crawl-perm/crawl-perm.md): Crawl all permissions related to Trust Registry and Credential Schema transactions, and synchronize their current state in the database.
- [**crawl-td**](./docs/services/crawl-td/crawl-td.md):This service is responsible for crawling and indexing all Trust Deposit states in the database to keep the data up to date.
- [**crawl-ar**](./docs/services/crawl-ar/crawl-ar.md): Crawl all blockchain accounts, get their Account Reputation, and save it to the DB.
- [**handle-vote**](./docs/services/handle-vote/handle-vote.md): parse vote message

## Database schema

You can view detail database schema [here](./docs/database_schema.md)

## Setup

This setup guide is for **local development** and connecting to the **Verana testnet**. For production deployments or other networks, adjust the configuration accordingly.

### 1) Install dependencies

```bash
pnpm i
```

### 2) Create your environment file

```bash
cp .env.example .env
# then open .env and fill in the required values
```

### 3) Start infrastructure (PostgreSQL, Redis)

```bash
pnpm run docker:dev
```

This will start the PostgreSQL and Redis containers in the background.

> **Note:** Make sure the required ports aren't already in use on your machine.

## Configuration

### Environment Variables

The application is configured through environment variables. See `.env.example` for a complete list.

#### Mandatory Environment Variables

These must be configured for the indexer to function:

**Network Configuration:**
- `CHAIN_ID` - The Verana chain ID (e.g., `vna-testnet-1`)
- `RPC_ENDPOINT` - Blockchain RPC endpoint URL (e.g., `https://rpc.testnet.verana.network/`)
- `LCD_ENDPOINT` - Blockchain LCD/REST API endpoint URL (e.g., `https://api.testnet.verana.network`)

**Database Configuration:**
- `POSTGRES_HOST` - PostgreSQL host address
- `POSTGRES_PORT` - PostgreSQL port (default: `5432`)
- `POSTGRES_USER` - PostgreSQL username
- `POSTGRES_PASSWORD` - PostgreSQL password
- `POSTGRES_DB` - PostgreSQL database name

**Redis Configuration:**
- `TRANSPORTER` - Redis connection string for Moleculer transporter (e.g., `redis://127.0.0.1:6379`)
- `CACHER` - Redis connection string for caching (e.g., `redis://127.0.0.1:6379`)
- `QUEUE_JOB_REDIS` - Redis connection string for job queues (e.g., `redis://127.0.0.1:6379`)

#### Important Optional Variables

These have sensible defaults but may need adjustment for specific deployments:

- `MOLECULER_NAMESPACE` - Namespace for service isolation (default: `verana-indexer`)
- `REDIS_DB_NUMBER` - Redis database number (default: `20`)
- `PORT` - API Gateway port (default: `3001`)
- `LOGLEVEL` - Logging level (default: `info`)
- `NODE_ENV` - Environment mode: `development` or `production`

#### Advanced Configuration

For advanced usage, you can configure:
- Moleculer framework settings (transporter, cacher, serializer, etc.)
- Metrics and tracing settings
- Circuit breaker and bulkhead settings
- Request timeout and retry policies

Refer to the [Moleculer documentation](https://moleculer.services/docs/0.14/configuration.html) for detailed configuration options.

### Chain Configuration (`config.json`)

The `src/config.json` file contains chain-specific and job-specific configuration:

**Chain Information:**
- `chainName` - Chain identifier
- `networkPrefixAddress`, `consensusPrefixAddress`, `validatorPrefixAddress` - Address prefixes
- `networkDenom` - Native denomination (e.g., `uvna`)

**Crawling Job Configuration:**
Each service has its own configuration section (e.g., `crawlBlock`, `crawlTransaction`, `crawlDids`, etc.) that controls:
- Crawling intervals and timing
- Batch sizes and chunk sizes
- Retry policies
- Start blocks for initial sync

**What to modify for different deployments:**
- Chain-specific settings (prefixes, denomination) if deploying to a different Verana network variant
- Crawling job intervals and batch sizes based on network performance and requirements
- Start blocks if you need to reindex from a specific block height

For most Verana deployments, the default configuration should work without modification.

## Deployment Configuration

The Verana Indexer is designed specifically for the Verana blockchain. For different Verana network deployments (e.g., testnet vs mainnet), you typically only need to adjust:

1. **Environment Variables** - Update network endpoints (`RPC_ENDPOINT`, `LCD_ENDPOINT`, `CHAIN_ID`) and database credentials
2. **Chain Settings in `config.json`** - If the network uses different address prefixes or denomination, update:
   - `networkPrefixAddress`
   - `consensusPrefixAddress`
   - `validatorPrefixAddress`
   - `networkDenom`

The crawling job configurations in `config.json` (intervals, batch sizes, etc.) are usually fine with defaults, but can be tuned based on network performance requirements.
