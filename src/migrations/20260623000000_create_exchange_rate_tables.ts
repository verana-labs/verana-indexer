import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable("exchange_rates", (table) => {
    table.integer("id").primary();
    table.string("base_asset_type").notNullable();
    table.string("base_asset", 255).notNullable();
    table.string("quote_asset_type").notNullable();
    table.string("quote_asset", 255).notNullable();
    table.specificType("rate", "NUMERIC(78,0)").notNullable().defaultTo(0);
    table.integer("rate_scale").notNullable().defaultTo(0);
    table.integer("validity_duration").notNullable().defaultTo(0);
    table.timestamp("updated").nullable();
    table.timestamp("expires").nullable();
    table.boolean("state").notNullable().defaultTo(true);

    table.index(["base_asset_type", "base_asset", "quote_asset_type", "quote_asset"]);
    table.index(["state"]);
    table.index(["expires"]);
  });

  await knex.schema.createTable("exchange_rate_history", (table) => {
    table.increments("id").primary();
    table.integer("exchange_rate_id").notNullable();
    table.string("base_asset_type").notNullable();
    table.string("base_asset", 255).notNullable();
    table.string("quote_asset_type").notNullable();
    table.string("quote_asset", 255).notNullable();
    table.specificType("rate", "NUMERIC(78,0)").notNullable().defaultTo(0);
    table.integer("rate_scale").notNullable().defaultTo(0);
    table.integer("validity_duration").notNullable().defaultTo(0);
    table.timestamp("updated").nullable();
    table.timestamp("expires").nullable();
    table.boolean("state").notNullable().defaultTo(true);
    table.string("event_type").notNullable();
    table.integer("height").notNullable();
    table.jsonb("changes").nullable();
    table.timestamp("created_at").defaultTo(knex.fn.now());

    table.unique(["exchange_rate_id", "height"]);
    table.index(["exchange_rate_id"]);
    table.index(["height"]);
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists("exchange_rate_history");
  await knex.schema.dropTableIfExists("exchange_rates");
}
