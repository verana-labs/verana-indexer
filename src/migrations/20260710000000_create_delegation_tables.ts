import { Knex } from 'knex'

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable('operator_authorizations', (table) => {
    table.bigInteger('id').primary()
    table.bigInteger('corporation_id').notNullable()
    table.string('operator', 255).notNullable()
    table.jsonb('msg_types').notNullable()
    table.jsonb('spend_limit').nullable()
    table.jsonb('remaining_spend').nullable()
    table.jsonb('fee_spend_limit').nullable()
    table.jsonb('remaining_fee_spend').nullable()
    table.timestamp('expiration').nullable()
    table.text('period').nullable()
    table.integer('height').notNullable()
    table.timestamp('created_at').defaultTo(knex.fn.now())

    table.unique(['corporation_id', 'operator'])
    table.index(['corporation_id'])
    table.index(['operator'])
    table.index(['height'])
  })

  await knex.schema.createTable('operator_authorization_history', (table) => {
    table.increments('id').primary()
    table.bigInteger('operator_authorization_id').notNullable()
    table.bigInteger('corporation_id').notNullable()
    table.string('operator', 255).notNullable()
    table.jsonb('msg_types').nullable()
    table.jsonb('spend_limit').nullable()
    table.jsonb('remaining_spend').nullable()
    table.jsonb('fee_spend_limit').nullable()
    table.jsonb('remaining_fee_spend').nullable()
    table.timestamp('expiration').nullable()
    table.text('period').nullable()
    table.boolean('revoked').notNullable().defaultTo(false)
    table.integer('height').notNullable()
    table.timestamp('created_at').defaultTo(knex.fn.now())

    table.index(['operator_authorization_id', 'height'])
    table.index(['height'])
  })

  await knex.schema.createTable('vs_operator_authorizations', (table) => {
    table.bigInteger('id').primary()
    table.bigInteger('corporation_id').notNullable()
    table.string('vs_operator', 255).notNullable()
    table.jsonb('records').notNullable()
    table.integer('height').notNullable()
    table.timestamp('created_at').defaultTo(knex.fn.now())

    table.unique(['corporation_id', 'vs_operator'])
    table.index(['corporation_id'])
    table.index(['vs_operator'])
    table.index(['height'])
  })
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists('vs_operator_authorizations')
  await knex.schema.dropTableIfExists('operator_authorization_history')
  await knex.schema.dropTableIfExists('operator_authorizations')
}
