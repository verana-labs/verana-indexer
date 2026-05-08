import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  const exists = await knex.schema.hasTable("indexer_events");
  if (!exists) return;

  await knex.raw(`ALTER TABLE indexer_events DROP CONSTRAINT IF EXISTS idx_events_did_tx_msg_type_unique`);
  await knex.raw(`ALTER TABLE indexer_events DROP CONSTRAINT IF EXISTS indexer_events_did_tx_hash_message_index_event_type_unique`);
  await knex.raw(`DROP INDEX IF EXISTS idx_events_did_tx_msg_type_unique`);
  await knex.raw(`DROP INDEX IF EXISTS indexer_events_did_tx_hash_message_index_event_type_unique`);

  await knex.raw(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_events_tx_msg_entity_unique
    ON indexer_events (
      tx_hash,
      tx_index,
      message_index,
      event_type,
      entity_type,
      COALESCE(entity_id, '')
    )
  `);

  await knex.raw(`CREATE INDEX IF NOT EXISTS idx_events_block_height ON indexer_events (block_height)`);
  await knex.raw(`CREATE INDEX IF NOT EXISTS idx_events_did ON indexer_events (did)`);
  await knex.raw(`CREATE INDEX IF NOT EXISTS idx_events_tx_hash ON indexer_events (tx_hash)`);
  await knex.raw(`CREATE INDEX IF NOT EXISTS idx_events_event_type ON indexer_events (event_type)`);
  await knex.raw(`
    CREATE INDEX IF NOT EXISTS idx_events_replay_order
    ON indexer_events (block_height, tx_index, message_index, id)
  `);
  await knex.raw(`
    CREATE INDEX IF NOT EXISTS idx_events_did_replay_order
    ON indexer_events (did, block_height, tx_index, message_index, id)
  `);
  await knex.raw(`
    CREATE INDEX IF NOT EXISTS idx_events_payload_related_dids_gin
    ON indexer_events USING GIN ((payload -> 'related_dids'))
  `);
  await knex.raw(`
    CREATE INDEX IF NOT EXISTS idx_events_payload_related_dids_camel_gin
    ON indexer_events USING GIN ((payload -> 'relatedDids'))
  `);
}

export async function down(knex: Knex): Promise<void> {
  const exists = await knex.schema.hasTable("indexer_events");
  if (!exists) return;

  await knex.raw(`DROP INDEX IF EXISTS idx_events_tx_msg_entity_unique`);
  await knex.raw(`DROP INDEX IF EXISTS idx_events_block_height`);
  await knex.raw(`DROP INDEX IF EXISTS idx_events_did`);
  await knex.raw(`DROP INDEX IF EXISTS idx_events_tx_hash`);
}
