import { Knex } from 'knex';

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable('block', (table: any) => {
    table.integer('height').primary();
    table.string('hash').unique().notNullable();
    table.timestamp('time').notNullable();
    table.string('proposer_address').index().notNullable();
    table.jsonb('data').notNullable();
  });

  await knex.schema.createTable('block_signature', (table: any) => {
    table.increments('id').primary();
    table.integer('height').index().notNullable();
    table.integer('block_id_flag').notNullable();
    table.string('validator_address').index().notNullable();
    table.timestamp('timestamp').notNullable();
    table.text('signature').notNullable();
    table.foreign('height').references('block.height');
  });

  await knex.schema.createTable('transaction', (table: any) => {
    table.increments('id').primary();
    table.integer('height').index().notNullable();
    table.string('hash').unique().notNullable();
    table.string('codespace').notNullable();
    table.integer('code').notNullable();
    table.bigint('gas_used').notNullable();
    table.bigint('gas_wanted').notNullable();
    table.bigint('gas_limit').notNullable();
    table.jsonb('fee').notNullable();
    table.timestamp('timestamp').notNullable();
    table.jsonb('data').notNullable();
    table.foreign('height').references('block.height');
  });

  await knex.schema.createTable('transaction_message', (table: any) => {
    table.increments('id').primary();
    table.integer('tx_id').index().notNullable();
    table.integer('index').notNullable();
    table.string('type').index().notNullable();
    table.string('sender').index().notNullable();
    table.jsonb('content').notNullable();
    table.foreign('tx_id').references('transaction.id');
  });

  await knex.schema.createTable(
    'transaction_message_receiver',
    (table: any) => {
      table.increments('id').primary();
      table.integer('tx_msg_id').index().notNullable();
      table.string('address').index().notNullable();
      table.string('reason');
      table.foreign('tx_msg_id').references('transaction_message.id');
    }
  );

  await knex.schema.createTable('transaction_event', (table: any) => {
    table.increments('id').primary();
    table.integer('tx_id').index().notNullable();
    table.integer('tx_msg_index');
    table.string('type').index().notNullable();
    table.foreign('tx_id').references('transaction.id');
  });

  await knex.schema.createTable('transaction_event_attribute', (table: any) => {
    table.increments('id').primary();
    table.integer('event_id').index().notNullable();
    table.string('key').index().notNullable();
    table.text('value').notNullable();
    table.foreign('event_id').references('transaction_event.id');
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTable('transaction_event_attribute');
  await knex.schema.dropTable('transaction_message_receiver');
  await knex.schema.dropTable('transaction_event');
  await knex.schema.dropTable('transaction_message');
  await knex.schema.dropTable('transaction');
  await knex.schema.dropTable('block_signature');
  await knex.schema.dropTable('block');
}
