# Reindexing Module Tables and Transactions

This guide explains how to rebuild transaction and module data from scratch while keeping your block data intact.

## When You Need This

Sometimes your transaction data or module data (Credential Schemas, Trust Registry, Permissions, Trust Deposits, etc.) gets corrupted or needs to be rebuilt, but your block data is still good. Instead of wiping everything and starting over, you can keep the blocks and rebuild everything else.

## The Basic Idea

Keep the block table, drop everything else, then rebuild from scratch. Blocks are the source of truth, so if they're good, you can derive everything else from them.

## What Gets Kept

Only the `block` table stays untouched. This is your source of truth from the blockchain, and rebuilding it would take forever. If your blocks are correct, everything else can be rebuilt.

## What Gets Dropped

### Transaction Tables
- `transaction`
- `transaction_message`

### Module Tables
- **Credential Schema**: `credential_schemas` and `credential_schema_history`
- **Trust Registry**: `trust_registry`, `governance_framework_version`, `governance_framework_document`, plus all their history tables
- **Permissions**: `permissions`, `permission_sessions`, and their history tables
- **Trust Deposits**: `trust_deposits` and `trust_deposit_history`
- **DID**: `dids` and `did_history`
- **Module Params**: `module_params` and `module_params_history`

One thing to note about trust deposits: even though they're read directly from block events, we still drop them. They can be fully rebuilt from blocks, and keeping them might cause inconsistencies with other data that's being rebuilt.

### Checkpoint Tables
We clear the data from these (don't drop the tables):
- `block_checkpoint` - delete all rows
- `checkpoint` - delete all rows

## How to Run It

### Quick Way

**Just reindex (you'll start services manually after):**
```bash
# For local development
pnpm run reindex:dev

# For production
pnpm run build
pnpm run reindex
```

**Reindex and start the indexer automatically:**
```bash
# For local development
pnpm run reindex:dev:start

# For production
pnpm run build
pnpm run reindex:start
```

### What the Script Does

1. Connects to your database
2. Verifies the blocks table exists (safety check)
3. Drops all transaction and module tables
4. Clears all checkpoint data
5. Runs migrations to recreate all the tables
6. Resets all ID sequences to start from 1

After that, you start your services and they'll rebuild everything from block 0.

## The Process Flow

Here's what happens when you reindex:

```
Blocks (already exist)
   ↓
Extract transactions from blocks
   ↓
Extract module messages from transactions
   ↓
Create module records
   ↓
Trust Deposits also read block events directly
```

Trust deposits are a bit special - they read directly from block events stored in the blocks table, not from transactions. But we still drop and rebuild them to keep everything consistent.

## Why We Do It This Way

**Why keep blocks?** They're the original source of truth. Rebuilding them is slow and expensive. If your blocks are correct, you can rebuild everything else.

**Why drop transactions?** Transactions are extracted from blocks. If your blocks are good, you can rebuild transactions. This also ensures your transaction data matches your current processing logic.

**Why drop module tables?** Module data comes from transactions (or block events). If you're rebuilding transactions, you need to rebuild modules too. Plus, starting with fresh IDs makes everything cleaner.

**Why drop trust deposits even though they read from blocks?** They can be fully rebuilt from block events. Keeping them might cause inconsistencies with other rebuilt data. The service will rebuild them correctly.

**Why reset checkpoints?** Checkpoints track where each service stopped. Resetting makes all services start from block 0. If something breaks, services can resume from checkpoints.

## What Happens During Reindexing

Once you start the services:

1. The Block Service processes blocks (most already exist, it just catches up)
2. The Transaction Service extracts transactions from each block
3. The Trust Deposit Service reads block events and creates trust deposits
4. Other module services read transactions and create:
   - Credential Schemas
   - Trust Registry entries
   - Permissions
   - DID records
   - Module parameters

Everything processes from block 0 to the latest block.

## How to Tell It's Working

You'll know things are working when:
- The transaction table starts filling up
- Trust deposits table starts getting rows (from block events)
- Module tables start getting rows
- Checkpoints show progress (height keeps increasing)
- All IDs start from 1
- No errors in your service logs

## Things That Could Go Wrong

**Data loss** - Always backup before starting. Seriously, do this.

**Services get out of sync** - Keep an eye on checkpoint heights. If one service falls way behind, something might be wrong.

**Something breaks mid-process** - Services can resume from checkpoints, so you don't have to start completely over.

## If Something Goes Wrong

1. Stop all services immediately
2. Restore from your backup
3. Figure out what went wrong
4. Fix it before trying again

That's it. The script handles all the heavy lifting, and your services will rebuild everything from your existing blocks.
