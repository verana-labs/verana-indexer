import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable("stats", (table) => {
    table.bigIncrements("id").primary();
    table.enum("granularity", ["HOUR", "DAY", "MONTH"]).notNullable();
    table.timestamp("timestamp", { useTz: true }).notNullable();
    table.enum("entity_type", ["GLOBAL", "TRUST_REGISTRY", "CREDENTIAL_SCHEMA", "PERMISSION"]).notNullable();
    table.bigInteger("entity_id").nullable();

    table.bigInteger("cumulative_participants").defaultTo(0).notNullable();
    table.bigInteger("cumulative_active_schemas").defaultTo(0).notNullable();
    table.bigInteger("cumulative_archived_schemas").defaultTo(0).notNullable();
    table.specificType("cumulative_weight", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("cumulative_issued", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("cumulative_verified", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.bigInteger("cumulative_ecosystem_slash_events").defaultTo(0).notNullable();
    table.specificType("cumulative_ecosystem_slashed_amount", "NUMERIC(38,0)").defaultTo(0).notNullable();
    table.specificType("cumulative_ecosystem_slashed_amount_repaid", "NUMERIC(38,0)").defaultTo(0).notNullable();
    table.bigInteger("cumulative_network_slash_events").defaultTo(0).notNullable();
    table.specificType("cumulative_network_slashed_amount", "NUMERIC(38,0)").defaultTo(0).notNullable();
    table.specificType("cumulative_network_slashed_amount_repaid", "NUMERIC(38,0)").defaultTo(0).notNullable();

    table.bigInteger("delta_participants").defaultTo(0).notNullable();
    table.bigInteger("delta_active_schemas").defaultTo(0).notNullable();
    table.bigInteger("delta_archived_schemas").defaultTo(0).notNullable();
    table.specificType("delta_weight", "NUMERIC(38,0)").defaultTo(0).notNullable();
    table.specificType("delta_issued", "NUMERIC(38,0)").defaultTo(0).notNullable();
    table.specificType("delta_verified", "NUMERIC(38,0)").defaultTo(0).notNullable();
    table.bigInteger("delta_ecosystem_slash_events").defaultTo(0).notNullable();
    table.specificType("delta_ecosystem_slashed_amount", "NUMERIC(38,0)").defaultTo(0).notNullable();
    table.specificType("delta_ecosystem_slashed_amount_repaid", "NUMERIC(38,0)").defaultTo(0).notNullable();
    table.bigInteger("delta_network_slash_events").defaultTo(0).notNullable();
    table.specificType("delta_network_slashed_amount", "NUMERIC(38,0)").defaultTo(0).notNullable();
    table.specificType("delta_network_slashed_amount_repaid", "NUMERIC(38,0)").defaultTo(0).notNullable();

    table.timestamps(true, true);

    table.unique(["granularity", "timestamp", "entity_type", "entity_id"], "stats_unique_key");

    table.index(["granularity", "timestamp", "entity_type", "entity_id"], "stats_lookup_idx");
    table.index(["entity_type", "entity_id", "timestamp"], "stats_entity_time_idx");
    table.index(["timestamp"], "stats_timestamp_idx");
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists("stats");
}
