import { Knex } from 'knex';

export async function up(knex: Knex): Promise<void> {
  const hasColumn = await knex.schema.hasColumn('transaction_message', 'parent_id');
  if (!hasColumn) {
    await knex.schema.alterTable('transaction_message', (table) => {
      table.integer('parent_id').index();
    });
  }
}

export async function down(knex: Knex): Promise<void> {
  const hasColumn = await knex.schema.hasColumn('transaction_message', 'parent_id');
  if (hasColumn) {
    await knex.schema.alterTable('transaction_message', (table) => {
      table.dropColumn('parent_id');
    });
  }
}
