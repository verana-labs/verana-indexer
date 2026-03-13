import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {

  const hasParticipantsColumn = await knex.schema.hasColumn(
    "trust_registry_history",
    "participants"
  );

  if (hasParticipantsColumn) {
    // Columns already exist - nothing to do.
    return;
  }

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
  const columns = [
    "participants",
    "active_schemas",
    "archived_schemas",
    "weight",
    "issued",
    "verified",
    "ecosystem_slash_events",
    "ecosystem_slashed_amount",
    "ecosystem_slashed_amount_repaid",
    "network_slash_events",
    "network_slashed_amount",
    "network_slashed_amount_repaid",
  ] as const;

  for (const col of columns) {
    // eslint-disable-next-line no-await-in-loop
    const exists = await knex.schema.hasColumn("trust_registry_history", col);
    if (exists) {
      // eslint-disable-next-line no-await-in-loop
      await knex.schema.alterTable("trust_registry_history", (table) => {
        table.dropColumn(col);
      });
    }
  }
}

