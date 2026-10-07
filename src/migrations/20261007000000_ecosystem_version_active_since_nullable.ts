import type { Knex } from 'knex'

// Draft EGF versions synced from the ledger have no active_since.
export async function up(knex: Knex): Promise<void> {
  await knex.raw('ALTER TABLE ecosystem_version ALTER COLUMN active_since DROP NOT NULL')
}

export async function down(knex: Knex): Promise<void> {
  await knex.raw('UPDATE ecosystem_version SET active_since = created WHERE active_since IS NULL')
  await knex.raw('ALTER TABLE ecosystem_version ALTER COLUMN active_since SET NOT NULL')
}
