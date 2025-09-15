import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable("did_history", (table) => {
    table.increments("id").primary();
    table.string("did").notNullable();
    table.string("event_type").notNullable();
    table.integer("height").nullable();
    table.string("years").nullable();
    table.string("controller").nullable();
    table.string("deposit").nullable();
    table.string("exp").nullable();
    table.timestamp("created").nullable();
    table.timestamp("deleted_at").nullable();
    table.boolean("is_deleted").defaultTo(false);
    table.jsonb("changes").nullable(); 
    table.timestamp("created_at").defaultTo(knex.fn.now());
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists("did_history");
}
