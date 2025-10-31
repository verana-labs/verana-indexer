import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable("trust_deposits", (table) => {
    table.bigIncrements("id").primary();
    table.string("account", 255).notNullable().unique();

    table.bigInteger("share").defaultTo(0);
    table.bigInteger("amount").defaultTo(0);
    table.bigInteger("claimable").defaultTo(0);
    table.bigInteger("slashed_deposit").defaultTo(0);
    table.bigInteger("repaid_deposit").defaultTo(0);
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
