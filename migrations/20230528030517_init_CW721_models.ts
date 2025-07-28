import { Knex } from 'knex';

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable('cw721_contract', (table) => {
    table.increments('id').primary();
    table.integer('contract_id').unique().index().notNullable();
    table.string('symbol');
    table.string('minter');
    table.foreign('contract_id').references('smart_contract.id');
    table.timestamp('created_at').notNullable().defaultTo(knex.raw('now()'));
    table.timestamp('updated_at').notNullable().defaultTo(knex.raw('now()'));
  });

  await knex.schema.createTable('cw721_token', (table) => {
    table.increments('id').primary();
    table.string('token_id').index().notNullable();
    table.string('token_uri');
    table.jsonb('extension');
    table.string('owner');
    table.integer('cw721_contract_id').index().notNullable();
    table.integer('last_updated_height').index();
    table.unique(['token_id', 'cw721_contract_id']);
    table.foreign('cw721_contract_id').references('cw721_contract.id');
    table.timestamp('created_at').notNullable().defaultTo(knex.raw('now()'));
    table.boolean('burned').defaultTo(false);
  });

  await knex.schema.createTable('cw721_activity', (table) => {
    table.increments('id').primary();
    table.string('tx_hash').index().notNullable();
    table.string('sender').index();
    table.string('action');
    table.integer('cw721_contract_id').index().notNullable();
    table.integer('cw721_token_id').index();
    table.string('from');
    table.string('to');
    table.integer('height').index();
    table.foreign('cw721_contract_id').references('cw721_contract.id');
    table.timestamp('created_at').notNullable().defaultTo(knex.raw('now()'));
    table.timestamp('updated_at').notNullable().defaultTo(knex.raw('now()'));
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTable('cw721_activity');
  await knex.schema.dropTable('cw721_token');
  await knex.schema.dropTable('cw721_contract');
}
