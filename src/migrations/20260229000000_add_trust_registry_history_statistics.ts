import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.schema.alterTable("trust_registry_history", (table) => {
    table.bigInteger("participants").notNullable().defaultTo(0);
    table.bigInteger("active_schemas").notNullable().defaultTo(0);
    table.bigInteger("archived_schemas").notNullable().defaultTo(0);
    table.specificType("weight", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("issued", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("verified", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.bigInteger("ecosystem_slash_events").notNullable().defaultTo(0);
    table
      .specificType("ecosystem_slashed_amount", "NUMERIC(38,0)")
      .notNullable()
      .defaultTo(0);
    table
      .specificType("ecosystem_slashed_amount_repaid", "NUMERIC(38,0)")
      .notNullable()
      .defaultTo(0);
    table.bigInteger("network_slash_events").notNullable().defaultTo(0);
    table
      .specificType("network_slashed_amount", "NUMERIC(38,0)")
      .notNullable()
      .defaultTo(0);
    table
      .specificType("network_slashed_amount_repaid", "NUMERIC(38,0)")
      .notNullable()
      .defaultTo(0);
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.alterTable("trust_registry_history", (table) => {
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

