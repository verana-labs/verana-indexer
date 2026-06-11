import { Knex } from "knex";

// Corporation model normalization on the indexer (single migration for this PR).
//
// VPR v4 models Ecosystem/Participant ownership exclusively by the uint64 `corporation_id`
// (FK to Corporation.id), provided directly by the chain. The embedded account address was
// removed on-chain (Participant `reserved 5`, Ecosystem never had it); the account now lives
// only in Corporation.policy_address and is resolved on demand (see corporation_resolve).
//
// This migration, in order:
//   1. Adds the canonical `corporation_id` (+ index) to the Ecosystem/Participant family of
//      tables that don't already have it.
//   2. Drops the legacy `corporation` (address) column from all of them. Postgres drops any
//      index depending on the dropped column automatically (incl. the composite
//      idx_ec_corporation_archived_modified_id and the participant corporation indexes).
//
// Out of scope (kept): corporation/corporation_history (the address is the source of truth)
// and trust_deposits/trust_deposit_history (the TD proto is still address-based).
//
// Runs after the dev.13 rename (20260604000003), so it targets the current table names.
// Idempotent. No backfill — corporation_id is populated by the ingest on (re)indexing.

const TABLES = [
  "ecosystem",
  "ecosystem_history",
  "ecosystem_snapshot",
  "participants",
  "participant_history",
  "participant_sessions",
  "participant_session_history",
] as const;

export async function up(knex: Knex): Promise<void> {
  for (const table of TABLES) {
    if (!(await knex.schema.hasTable(table))) continue;
    if (!(await knex.schema.hasColumn(table, "corporation_id"))) {
      await knex.raw(
        `ALTER TABLE "${table}" ADD COLUMN "corporation_id" bigint NOT NULL DEFAULT 0`
      );
      await knex.raw(
        `CREATE INDEX IF NOT EXISTS "idx_${table}_corporation_id" ON "${table}" ("corporation_id")`
      );
    }
    if (await knex.schema.hasColumn(table, "corporation")) {
      await knex.raw(`ALTER TABLE "${table}" DROP COLUMN IF EXISTS "corporation"`);
    }
  }
}

export async function down(knex: Knex): Promise<void> {
  for (const table of TABLES) {
    if (!(await knex.schema.hasTable(table))) continue;
    // Re-create the legacy column as nullable (the original was NOT NULL, but the addresses
    // cannot be reconstructed here; ingest/backfill would repopulate it).
    if (!(await knex.schema.hasColumn(table, "corporation"))) {
      await knex.raw(`ALTER TABLE "${table}" ADD COLUMN "corporation" varchar(255)`);
    }
    if (await knex.schema.hasColumn(table, "corporation_id")) {
      await knex.raw(`DROP INDEX IF EXISTS "idx_${table}_corporation_id"`);
      await knex.raw(`ALTER TABLE "${table}" DROP COLUMN IF EXISTS "corporation_id"`);
    }
  }
}
