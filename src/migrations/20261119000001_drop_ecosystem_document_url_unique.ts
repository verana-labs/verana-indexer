import type { Knex } from 'knex'

// Two languages of one version can share a url on chain, so (version_id, url) is not a key.
export async function up(knex: Knex): Promise<void> {
  await knex.raw('ALTER TABLE ecosystem_document DROP CONSTRAINT IF EXISTS tr_document_version_url_unique')
}

export async function down(knex: Knex): Promise<void> {
  await knex.raw(
    `DELETE FROM ecosystem_document newer USING ecosystem_document older
      WHERE newer.version_id = older.version_id AND newer.url = older.url AND newer.id > older.id`
  )
  await knex.schema.alterTable('ecosystem_document', (table) => {
    table.unique(['version_id', 'url'], 'tr_document_version_url_unique')
  })
}
