import { Knex } from 'knex'

export async function up(knex: Knex): Promise<void> {
  if (!(await knex.schema.hasTable('indexer_events'))) return

  await knex.raw(`
    CREATE INDEX IF NOT EXISTS idx_events_payload_related_corp_ids_gin
    ON indexer_events USING GIN ((payload -> 'related_corporation_ids'))
  `)

  await knex.raw(`
    CREATE INDEX IF NOT EXISTS idx_events_payload_related_corp_ids_camel_gin
    ON indexer_events USING GIN ((payload -> 'relatedCorporationIds'))
  `)
}

export async function down(knex: Knex): Promise<void> {
  await knex.raw(`DROP INDEX IF EXISTS idx_events_payload_related_corp_ids_gin`)
  await knex.raw(`DROP INDEX IF EXISTS idx_events_payload_related_corp_ids_camel_gin`)
}
