import { Knex } from 'knex';

export async function up(knex: Knex): Promise<void> {
    await knex.schema.createTable('dids', (table) => {
        table.increments('id').primary();
        table.integer('height').notNullable();
        table.string('did').unique().notNullable();
        table.string('controller').notNullable();
        table.timestamp('created').notNullable();
        table.timestamp('modified').notNullable();
        table.timestamp('exp').notNullable();
        table.string('deposit').notNullable();

    });
}

export async function down(knex: Knex): Promise<void> {
    await knex.schema.dropTable('dids');
}
