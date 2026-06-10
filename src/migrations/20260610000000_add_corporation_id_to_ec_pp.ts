import { Knex } from "knex";

// VPR v4 models Ecosystem/Participant ownership by the uint64 `corporation_id`
// (FK to Corporation.id), provided directly by the chain. The legacy `corporation`
// (string address) column stays as-is — it serves the account/`participant` filter.
//
// Runs after the dev.13 rename (20260604000003), so it targets the current table
// names (`ecosystem`, `participants`). Idempotent: a no-op where the column already
// exists. No backfill — values are populated by the ingest on (re)indexing.

const TABLES = ["ecosystem", "participants"] as const;

export async function up(knex: Knex): Promise<void> {
  for (const table of TABLES) {
    if (!(await knex.schema.hasTable(table))) continue;
    if (await knex.schema.hasColumn(table, "corporation_id")) continue;
    await knex.schema.alterTable(table, (t) => {
      t.bigInteger("corporation_id").notNullable().defaultTo(0);
      t.index(["corporation_id"], `idx_${table}_corporation_id`);
    });
  }
}

export async function down(knex: Knex): Promise<void> {
  for (const table of TABLES) {
    if (!(await knex.schema.hasTable(table))) continue;
    if (!(await knex.schema.hasColumn(table, "corporation_id"))) continue;
    await knex.schema.alterTable(table, (t) => {
      t.dropIndex(["corporation_id"], `idx_${table}_corporation_id`);
      t.dropColumn("corporation_id");
    });
  }
}
