import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable("trust_deposit_history", (table) => {
    table.increments("id").primary();
    table.string("account", 255).notNullable();
    table.specificType("share", "NUMERIC(38,0)").defaultTo(0);
    table.specificType("amount", "NUMERIC(38,0)").defaultTo(0);
    table.specificType("claimable", "NUMERIC(38,0)").defaultTo(0);
    table.specificType("slashed_deposit", "NUMERIC(38,0)").defaultTo(0);
    table.specificType("repaid_deposit", "NUMERIC(38,0)").defaultTo(0);
    table.timestamp("last_slashed").nullable();
    table.timestamp("last_repaid").nullable();
    table.integer("slash_count").defaultTo(0);
    table.string("last_repaid_by", 255).defaultTo("");
    table.string("event_type").notNullable();
    table.integer("height").notNullable();
    table.jsonb("changes").nullable();
    table.timestamp("created_at").defaultTo(knex.fn.now());

    table.index(["account"]);
    table.index(["height"]);
    table.index(["event_type"]);
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists("trust_deposit_history");
}

