import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  const exists = await knex.schema.hasTable("indexer_events");
  if (!exists) return;

  await knex.raw(`ALTER TABLE indexer_events DROP CONSTRAINT IF EXISTS idx_events_did_tx_msg_type_unique`);
  await knex.raw(`ALTER TABLE indexer_events DROP CONSTRAINT IF EXISTS indexer_events_did_tx_hash_message_index_event_type_unique`);
  await knex.raw(`DROP INDEX IF EXISTS idx_events_did_tx_msg_type_unique`);
  await knex.raw(`DROP INDEX IF EXISTS indexer_events_did_tx_hash_message_index_event_type_unique`);
}

export async function down(knex: Knex): Promise<void> {
  const exists = await knex.schema.hasTable("indexer_events");
  if (!exists) return;
}
