import { Knex } from 'knex'

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable('corporation_group', (table) => {
    table.bigInteger('corporation_id').primary()
    table.bigInteger('group_id').notNullable().unique()
    table.text('policy_address').notNullable().unique()
    table.bigInteger('group_version').notNullable().defaultTo(1)
    table.bigInteger('policy_version').notNullable().defaultTo(1)
    table.text('total_weight').notNullable().defaultTo('0')
    table.jsonb('decision_policy').nullable()
    table.timestamp('created').notNullable()
    table.timestamp('modified').notNullable()
    table.bigInteger('height').notNullable()

    table.foreign('corporation_id').references('id').inTable('corporation').onDelete('CASCADE')
  })

  await knex.schema.createTable('corporation_group_history', (table) => {
    table.bigIncrements('id').primary()
    table.bigInteger('corporation_id').notNullable()
    table.bigInteger('group_id').notNullable()
    table.bigInteger('group_version').notNullable()
    table.bigInteger('policy_version').notNullable()
    table.text('total_weight').notNullable()
    table.jsonb('decision_policy').nullable()
    table.jsonb('members').notNullable()
    table.bigInteger('height').notNullable()
    table.timestamp('created_at').notNullable().defaultTo(knex.fn.now())

    table.unique(['corporation_id', 'height'])
    table.index(['height'])
  })

  await knex.schema.createTable('group_proposal', (table) => {
    table.bigInteger('id').primary()
    table.bigInteger('corporation_id').notNullable()
    table.text('group_policy_address').notNullable()
    table.text('metadata').nullable()
    table.jsonb('proposers').notNullable().defaultTo('[]')
    table.timestamp('submit_time').notNullable()
    table.bigInteger('group_version').nullable()
    table.bigInteger('group_policy_version').nullable()
    table.string('status', 16).notNullable().defaultTo('SUBMITTED')
    table.string('executor_result', 16).notNullable().defaultTo('NOT_RUN')
    table.timestamp('voting_period_end').nullable()
    table.jsonb('messages').notNullable().defaultTo('[]')
    table.jsonb('final_tally_result').nullable()
    table.timestamp('modified').notNullable()
    table.bigInteger('height').notNullable()

    table.foreign('corporation_id').references('corporation_id').inTable('corporation_group').onDelete('CASCADE')

    table.index(['corporation_id'])
    table.index(['status'])
    table.index(['modified'])
  })

  await knex.schema.createTable('group_proposal_history', (table) => {
    table.bigIncrements('id').primary()
    table.bigInteger('proposal_id').notNullable()
    table.jsonb('snapshot').notNullable()
    table.bigInteger('height').notNullable()
    table.timestamp('created_at').notNullable().defaultTo(knex.fn.now())

    table.unique(['proposal_id', 'height'])
    table.index(['height'])
  })

  await knex.schema.createTable('group_vote', (table) => {
    table.bigIncrements('id').primary()
    table.bigInteger('proposal_id').notNullable()
    table.text('voter').notNullable()
    table.string('option', 16).notNullable()
    table.text('metadata').nullable()
    table.timestamp('submit_time').notNullable()
    table.bigInteger('height').notNullable()

    table.unique(['proposal_id', 'voter'])
    table.index(['proposal_id'])
    table.index(['voter'])
    table.index(['height'])
  })
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists('group_vote')
  await knex.schema.dropTableIfExists('group_proposal_history')
  await knex.schema.dropTableIfExists('group_proposal')
  await knex.schema.dropTableIfExists('corporation_group_history')
  await knex.schema.dropTableIfExists('corporation_group')
}
