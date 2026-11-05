import { Knex } from 'knex'

export async function up(knex: Knex): Promise<void> {
  const exists = await knex.schema.hasTable('indexer_events')
  if (!exists) return

  await knex.raw(`DROP INDEX IF EXISTS idx_events_tx_msg_entity_unique`)
  await knex.raw(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_events_tx_msg_entity_proposal_unique
    ON indexer_events (
      tx_hash,
      tx_index,
      message_index,
      event_type,
      entity_type,
      COALESCE(entity_id, ''),
      COALESCE(payload ->> 'proposal_id', ''),
      COALESCE(payload ->> 'proposal_message_index', '')
    )
  `)
}

export async function down(knex: Knex): Promise<void> {
  const exists = await knex.schema.hasTable('indexer_events')
  if (!exists) return

  await knex.raw(`DROP INDEX IF EXISTS idx_events_tx_msg_entity_proposal_unique`)
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
  `)
}
