import { Knex } from 'knex';

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable('erc721_contract', (table) => {
    table.increments();
    table.integer('evm_smart_contract_id').notNullable().unique();
    table.string('address').unique().notNullable();
    table.string('symbol').index();
    table.string('name');
    table.boolean('track').defaultTo(false).index();
    table.integer('last_updated_height').index();
    table.foreign('evm_smart_contract_id').references('evm_smart_contract.id');
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists('erc721_contract');
}
