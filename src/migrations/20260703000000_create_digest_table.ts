import { Knex } from 'knex'

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable('digests', (table) => {
    table.string('digest', 512).primary()
    table.timestamp('created').notNullable()
    table.integer('height').notNullable()
    table.timestamp('created_at').defaultTo(knex.fn.now())

    table.index(['height'])
  })
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists('digests')
}
