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
    table.timestamp('modified').nullable()
    table.integer('height').notNullable()
    table.timestamp('created_at').defaultTo(knex.fn.now())

    table.unique(['corporation_id', 'operator'])
    table.index(['corporation_id'])
    table.index(['operator'])
    table.index(['modified'])
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
    table.timestamp('modified').nullable()
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
    table.timestamp('modified').nullable()
    table.integer('height').notNullable()
    table.timestamp('created_at').defaultTo(knex.fn.now())

    table.unique(['corporation_id', 'vs_operator'])
    table.index(['corporation_id'])
    table.index(['vs_operator'])
    table.index(['modified'])
    table.index(['height'])
  })

  await knex.schema.createTable('vs_operator_authorization_history', (table) => {
    table.increments('id').primary()
    table.bigInteger('vs_operator_authorization_id').notNullable()
    table.bigInteger('corporation_id').notNullable()
    table.string('vs_operator', 255).notNullable()
    table.jsonb('records').nullable()
    table.timestamp('modified').nullable()
    table.boolean('revoked').notNullable().defaultTo(false)
    table.integer('height').notNullable()
    table.timestamp('created_at').defaultTo(knex.fn.now())

    table.index(['vs_operator_authorization_id', 'height'])
    table.index(['height'])
  })

  await knex.schema.createTable('fee_grants', (table) => {
    // FeeGrant has no on-chain id; this surrogate id is the spec's pagination cursor.
    table.bigIncrements('id').primary()
    table.bigInteger('grantor_corporation_id').notNullable()
    table.string('grantee', 255).notNullable()
    table.jsonb('msg_types').notNullable()
    table.jsonb('spend_limit').nullable()
    table.jsonb('remaining_spend').nullable()
    table.timestamp('expiration').nullable()
    table.text('period').nullable()
    table.timestamp('modified').nullable()
    table.integer('height').notNullable()
    table.timestamp('created_at').defaultTo(knex.fn.now())

    table.unique(['grantor_corporation_id', 'grantee'])
    table.index(['grantor_corporation_id'])
    table.index(['grantee'])
    table.index(['modified'])
    table.index(['height'])
  })

  await knex.schema.createTable('fee_grant_history', (table) => {
    table.increments('id').primary()
    table.bigInteger('fee_grant_id').notNullable()
    table.bigInteger('grantor_corporation_id').notNullable()
    table.string('grantee', 255).notNullable()
    table.jsonb('msg_types').nullable()
    table.jsonb('spend_limit').nullable()
    table.jsonb('remaining_spend').nullable()
    table.timestamp('expiration').nullable()
    table.text('period').nullable()
    table.timestamp('modified').nullable()
    table.boolean('revoked').notNullable().defaultTo(false)
    table.integer('height').notNullable()
    table.timestamp('created_at').defaultTo(knex.fn.now())

    table.index(['fee_grant_id', 'height'])
    table.index(['height'])
  })
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists('fee_grant_history')
  await knex.schema.dropTableIfExists('fee_grants')
  await knex.schema.dropTableIfExists('vs_operator_authorization_history')
  await knex.schema.dropTableIfExists('vs_operator_authorizations')
  await knex.schema.dropTableIfExists('operator_authorization_history')
  await knex.schema.dropTableIfExists('operator_authorizations')
}
