import { Knex } from 'knex';

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable('dids', (table) => {
    table.increments('id').primary();
    table.integer('height').notNullable();
    table.string('did', 255).notNullable().unique(); 
    table.string('controller', 255).notNullable();
    table.timestamp('created').defaultTo(knex.fn.now());
    table.timestamp('modified').defaultTo(knex.fn.now());
    table.timestamp('exp');
    table.integer('deposit');
    table.string('event_type', 255);
    table.integer('years');
    table.boolean('is_deleted').defaultTo(false);
    table.timestamp('deleted_at');
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists('dids');
}
