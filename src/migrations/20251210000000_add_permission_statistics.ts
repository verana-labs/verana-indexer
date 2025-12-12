import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  const hasIssuedColumn = await knex.schema.hasColumn("permissions", "issued");
  if (!hasIssuedColumn) {
    await knex.schema.alterTable("permissions", (table) => {
      table.bigInteger("issued").defaultTo(0).notNullable();
      table.bigInteger("verified").defaultTo(0).notNullable();
    });
  }

  const hasHistoryIssuedColumn = await knex.schema.hasColumn("permission_history", "issued");
  if (!hasHistoryIssuedColumn) {
    await knex.schema.alterTable("permission_history", (table) => {
      table.bigInteger("issued").defaultTo(0).notNullable();
      table.bigInteger("verified").defaultTo(0).notNullable();
    });
  }
}

export async function down(knex: Knex): Promise<void> {
  const hasIssuedColumn = await knex.schema.hasColumn("permissions", "issued");
  if (hasIssuedColumn) {
    await knex.schema.alterTable("permissions", (table) => {
      table.dropColumn("issued");
      table.dropColumn("verified");
    });
  }

  const hasHistoryIssuedColumn = await knex.schema.hasColumn("permission_history", "issued");
  if (hasHistoryIssuedColumn) {
    await knex.schema.alterTable("permission_history", (table) => {
      table.dropColumn("issued");
      table.dropColumn("verified");
    });
  }
}

