import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.schema.alterTable("credential_schemas", (table) => {
    table.bigInteger("participants").defaultTo(0).notNullable();
    table.specificType("weight", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("issued", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("verified", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.bigInteger("ecosystem_slash_events").defaultTo(0).notNullable();
    table.specificType("ecosystem_slashed_amount", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("ecosystem_slashed_amount_repaid", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.bigInteger("network_slash_events").defaultTo(0).notNullable();
    table.specificType("network_slashed_amount", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("network_slashed_amount_repaid", "NUMERIC(38,0)").notNullable().defaultTo(0);
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
