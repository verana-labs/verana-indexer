import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.schema.alterTable("credential_schemas", (table) => {
    table.bigInteger("participants").defaultTo(0).notNullable();
    table.string("weight", 50).defaultTo("0").notNullable();
    table.string("issued", 50).defaultTo("0").notNullable();
    table.string("verified", 50).defaultTo("0").notNullable();
    table.bigInteger("ecosystem_slash_events").defaultTo(0).notNullable();
    table.string("ecosystem_slashed_amount", 50).defaultTo("0").notNullable();
    table.string("ecosystem_slashed_amount_repaid", 50).defaultTo("0").notNullable();
    table.bigInteger("network_slash_events").defaultTo(0).notNullable();
    table.string("network_slashed_amount", 50).defaultTo("0").notNullable();
    table.string("network_slashed_amount_repaid", 50).defaultTo("0").notNullable();
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.alterTable("credential_schemas", (table) => {
    table.dropColumn("participants");
    table.dropColumn("weight");
    table.dropColumn("issued");
    table.dropColumn("verified");
    table.dropColumn("ecosystem_slash_events");
    table.dropColumn("ecosystem_slashed_amount");
    table.dropColumn("ecosystem_slashed_amount_repaid");
    table.dropColumn("network_slash_events");
    table.dropColumn("network_slashed_amount");
    table.dropColumn("network_slashed_amount_repaid");
  });
}
