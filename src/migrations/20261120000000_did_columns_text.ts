import type { Knex } from 'knex'

const TABLES = [
  'ecosystem',
  'ecosystem_history',
  'ecosystem_snapshot',
  'corporation',
  'corporation_history',
  'participants',
  'participant_history',
  'trust_results',
]

// The chain checks DID syntax only and puts no length limit on it.
export async function up(knex: Knex): Promise<void> {
  for (const table of TABLES) {
    if (await knex.schema.hasColumn(table, 'did')) {
      await knex.raw('ALTER TABLE ?? ALTER COLUMN did TYPE text', [table])
    }
  }
}

// Fails, and changes nothing, if a stored DID is longer than 255 characters.
export async function down(knex: Knex): Promise<void> {
  for (const table of TABLES) {
    if (await knex.schema.hasColumn(table, 'did')) {
      await knex.raw('ALTER TABLE ?? ALTER COLUMN did TYPE varchar(255)', [table])
    }
  }
}
