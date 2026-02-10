import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable("global_metrics", (table) => {
    table.increments("id").primary();
    table.bigInteger("block_height").nullable();
    table.timestamp("computed_at").notNullable().defaultTo(knex.fn.now());

    table.bigInteger("participants").notNullable().defaultTo(0);
    table.integer("active_trust_registries").notNullable().defaultTo(0);
    table.integer("archived_trust_registries").notNullable().defaultTo(0);
    table.integer("active_schemas").notNullable().defaultTo(0);
    table.integer("archived_schemas").notNullable().defaultTo(0);

    table.text("weight").notNullable().defaultTo("0");
    table.bigInteger("issued").notNullable().defaultTo(0);
    table.bigInteger("verified").notNullable().defaultTo(0);

    table.integer("ecosystem_slash_events").notNullable().defaultTo(0);
    table.text("ecosystem_slashed_amount").notNullable().defaultTo("0");
    table.text("ecosystem_slashed_amount_repaid").notNullable().defaultTo("0");

    table.integer("network_slash_events").notNullable().defaultTo(0);
    table.text("network_slashed_amount").notNullable().defaultTo("0");
    table.text("network_slashed_amount_repaid").notNullable().defaultTo("0");

    table.jsonb("payload").nullable();
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists("global_metrics");
}

