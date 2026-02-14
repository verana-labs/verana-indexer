import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable("trust_deposits", (table) => {
    table.bigIncrements("id").primary();
    table.string("account", 255).notNullable().unique();

    table.specificType("share", "NUMERIC(38,0)").defaultTo(0);
    table.specificType("amount", "NUMERIC(38,0)").defaultTo(0);
    table.specificType("claimable", "NUMERIC(38,0)").defaultTo(0);
    table.specificType("slashed_deposit", "NUMERIC(38,0)").defaultTo(0);
    table.specificType("repaid_deposit", "NUMERIC(38,0)").defaultTo(0);
    table.timestamp("last_slashed").nullable();
    table.timestamp("last_repaid").nullable();
    table.integer("slash_count").defaultTo(0);
    table.string("last_repaid_by", 255).defaultTo("");

    table.index(["account"]);
    table.index(["share"]);
    table.index(["amount"]);
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists("trust_deposits");
}
