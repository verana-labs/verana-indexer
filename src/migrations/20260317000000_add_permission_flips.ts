import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  if (!(await knex.schema.hasColumn("permissions", "last_valid_flip_version"))) {
    await knex.schema.alterTable("permissions", (table) => {
      table.specificType("last_valid_flip_version", "INTEGER").notNullable().defaultTo(0);
    });
  }

  if (!(await knex.schema.hasColumn("permissions", "is_active_now"))) {
    await knex.schema.alterTable("permissions", (table) => {
      table.boolean("is_active_now").notNullable().defaultTo(false);
    });
  }

  if (!(await knex.schema.hasTable("permission_scheduled_flips"))) {
    await knex.schema.createTable("permission_scheduled_flips", (table) => {
      table.bigInteger("perm_id").notNullable()
        .references("id").inTable("permissions")
        .onDelete("CASCADE");

      table.timestamp("flip_at_time", { useTz: true }).notNullable();

      table.specificType("flip_kind", "SMALLINT").notNullable();

      table.specificType("status", "SMALLINT").notNullable().defaultTo(0);

      table.specificType("version", "INTEGER").notNullable();

      table.bigInteger("applied_height").nullable();
      table.timestamp("applied_time", { useTz: true }).nullable();

      table.timestamp("created_at", { useTz: true }).notNullable().defaultTo(knex.fn.now());

      table.primary(["perm_id", "version", "flip_at_time", "flip_kind"]);
    });

    await knex.schema.alterTable("permission_scheduled_flips", (table) => {
      table.index(["flip_at_time", "perm_id"], "psf_pending_idx");
      table.index(["status", "flip_at_time", "perm_id"], "psf_pending_status_time_idx");
    });
  }

  if (!(await knex.schema.hasTable("entity_participant_changes"))) {
    await knex.schema.createTable("entity_participant_changes", (table) => {
      table.bigInteger("height").notNullable();
      table.timestamp("block_time", { useTz: true }).notNullable();

      table.specificType("entity_kind", "SMALLINT").notNullable();
      table.bigInteger("entity_id").nullable();

      table.specificType("type", "SMALLINT").notNullable();

      table.bigInteger("value").notNullable();

      table.primary(["entity_kind", "entity_id", "type", "height"]);
    });

    await knex.schema.alterTable("entity_participant_changes", (table) => {
      table.index(["entity_kind", "entity_id", "type", "height"], "epc_lookup_idx");
    });
  }
}

export async function down(knex: Knex): Promise<void> {
  if (await knex.schema.hasTable("entity_participant_changes")) {
    await knex.schema.dropTable("entity_participant_changes");
  }

  if (await knex.schema.hasTable("permission_scheduled_flips")) {
    await knex.schema.dropTable("permission_scheduled_flips");
  }

  if (await knex.schema.hasColumn("permissions", "is_active_now")) {
    await knex.schema.alterTable("permissions", (table) => {
      table.dropColumn("is_active_now");
    });
  }

  if (await knex.schema.hasColumn("permissions", "last_valid_flip_version")) {
    await knex.schema.alterTable("permissions", (table) => {
      table.dropColumn("last_valid_flip_version");
    });
  }
}

