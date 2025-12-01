import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable("module_params_history", (table) => {
    table.increments("id").primary();
    table.string("module", 255).notNullable();
    table.jsonb("params").notNullable();
    table.string("event_type").notNullable();
    table.integer("height").notNullable();
    table.jsonb("changes").nullable();
    table.timestamp("created_at").defaultTo(knex.fn.now());

    table.index(["module"]);
    table.index(["height"]);
    table.index(["event_type"]);
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists("module_params_history");
}

