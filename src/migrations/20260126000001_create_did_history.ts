import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  const exists = await knex.schema.hasTable("did_history");
  if (!exists) {
    await knex.schema.createTable("did_history", (table) => {
      table.increments("id").primary();
      table.string("did").notNullable();
      table.string("event_type").notNullable();
      table.integer("height").nullable();
      table.integer("years").nullable();
      table.string("controller").nullable();
      table.specificType("deposit", "NUMERIC(38,0)").nullable();
      table.string("exp").nullable();
      table.timestamp("created").nullable();
      table.timestamp("deleted_at").nullable();
      table.boolean("is_deleted").defaultTo(false);
      table.jsonb("changes").nullable();
      table.timestamp("created_at").defaultTo(knex.fn.now());
    });
  }
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists("did_history");
}
