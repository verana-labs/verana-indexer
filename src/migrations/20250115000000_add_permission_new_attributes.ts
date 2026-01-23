import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  const hasParticipantsColumn = await knex.schema.hasColumn("permissions", "participants");
  if (!hasParticipantsColumn) {
    await knex.schema.alterTable("permissions", (table) => {
      table.bigInteger("participants").defaultTo(0).notNullable();
      table.string("weight", 50).defaultTo("0").notNullable();
      table.bigInteger("ecosystem_slash_events").defaultTo(0).notNullable();
      table.string("ecosystem_slashed_amount", 50).defaultTo("0").notNullable();
      table.string("ecosystem_slashed_amount_repaid", 50).defaultTo("0").notNullable();
      table.bigInteger("network_slash_events").defaultTo(0).notNullable();
      table.string("network_slashed_amount", 50).defaultTo("0").notNullable();
      table.string("network_slashed_amount_repaid", 50).defaultTo("0").notNullable();
    });
  }

  const hasPermissionHistoryTable = await knex.schema.hasTable("permission_history");
  if (hasPermissionHistoryTable) {
    const hasHistoryParticipantsColumn = await knex.schema.hasColumn("permission_history", "participants");
    if (!hasHistoryParticipantsColumn) {
      await knex.schema.alterTable("permission_history", (table) => {
        table.bigInteger("participants").defaultTo(0).notNullable();
        table.string("weight", 50).defaultTo("0").notNullable();
        table.bigInteger("ecosystem_slash_events").defaultTo(0).notNullable();
        table.string("ecosystem_slashed_amount", 50).defaultTo("0").notNullable();
        table.string("ecosystem_slashed_amount_repaid", 50).defaultTo("0").notNullable();
        table.bigInteger("network_slash_events").defaultTo(0).notNullable();
        table.string("network_slashed_amount", 50).defaultTo("0").notNullable();
        table.string("network_slashed_amount_repaid", 50).defaultTo("0").notNullable();
      });
    }
  }
}

export async function down(knex: Knex): Promise<void> {
  const hasParticipantsColumn = await knex.schema.hasColumn("permissions", "participants");
  if (hasParticipantsColumn) {
    await knex.schema.alterTable("permissions", (table) => {
      table.dropColumn("participants");
      table.dropColumn("weight");
      table.dropColumn("ecosystem_slash_events");
      table.dropColumn("ecosystem_slashed_amount");
      table.dropColumn("ecosystem_slashed_amount_repaid");
      table.dropColumn("network_slash_events");
      table.dropColumn("network_slashed_amount");
      table.dropColumn("network_slashed_amount_repaid");
    });
  }

  const hasPermissionHistoryTable = await knex.schema.hasTable("permission_history");
  if (hasPermissionHistoryTable) {
    const hasHistoryParticipantsColumn = await knex.schema.hasColumn("permission_history", "participants");
    if (hasHistoryParticipantsColumn) {
      await knex.schema.alterTable("permission_history", (table) => {
        table.dropColumn("participants");
        table.dropColumn("weight");
        table.dropColumn("ecosystem_slash_events");
        table.dropColumn("ecosystem_slashed_amount");
        table.dropColumn("ecosystem_slashed_amount_repaid");
        table.dropColumn("network_slash_events");
        table.dropColumn("network_slashed_amount");
        table.dropColumn("network_slashed_amount_repaid");
      });
    }
  }
}
