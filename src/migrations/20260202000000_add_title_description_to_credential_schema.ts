import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.schema.alterTable("credential_schemas", (table) => {
    table.string("title").nullable();
    table.text("description").nullable();
  });

  await knex.schema.alterTable("credential_schema_history", (table) => {
    table.string("title").nullable();
    table.text("description").nullable();
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.alterTable("credential_schemas", (table) => {
    table.dropColumn("title");
    table.dropColumn("description");
  });

  await knex.schema.alterTable("credential_schema_history", (table) => {
    table.dropColumn("title");
    table.dropColumn("description");
  });
}

