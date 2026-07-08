import type { Knex } from 'knex'

export async function up(knex: Knex): Promise<void> {
  await knex.schema.alterTable('corporation_history', (table) => {
    table.text('account').nullable()
  })
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.alterTable('corporation_history', (table) => {
    table.dropColumn('account')
  })
}
