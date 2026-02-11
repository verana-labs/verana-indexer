import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.schema.alterTable("trust_registry", (table) => {
    table.bigInteger("participants").defaultTo(0).notNullable();
    table.bigInteger("active_schemas").defaultTo(0).notNullable();
    table.bigInteger("archived_schemas").defaultTo(0).notNullable();
    table.specificType("weight", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.bigInteger("issued").defaultTo(0).notNullable();
    table.bigInteger("verified").defaultTo(0).notNullable();
    table.bigInteger("ecosystem_slash_events").defaultTo(0).notNullable();
    table.specificType("ecosystem_slashed_amount", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("ecosystem_slashed_amount_repaid", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.bigInteger("network_slash_events").defaultTo(0).notNullable();
    table.specificType("network_slashed_amount", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("network_slashed_amount_repaid", "NUMERIC(38,0)").notNullable().defaultTo(0);
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.alterTable("trust_registry", (table) => {
    table.dropColumn("participants");
    table.dropColumn("active_schemas");
    table.dropColumn("archived_schemas");
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
