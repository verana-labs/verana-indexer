import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.schema.alterTable("trust_registry_history", (table) => {
    table.index(["tr_id", "height", "created_at", "id"], "idx_trust_registry_history_tr_height_created_id");
  });

  await knex.schema.alterTable("credential_schema_history", (table) => {
    table.index(
      ["credential_schema_id", "height", "created_at", "id"],
      "idx_credential_schema_history_schema_height_created_id"
    );
  });

  await knex.schema.alterTable("permission_history", (table) => {
    table.index(
      ["permission_id", "height", "created_at", "id"],
      "idx_permission_history_perm_height_created_id"
    );
    table.index(
      ["schema_id", "height", "created_at", "id"],
      "idx_permission_history_schema_height_created_id"
    );
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.alterTable("permission_history", (table) => {
    table.dropIndex(["schema_id", "height", "created_at", "id"], "idx_permission_history_schema_height_created_id");
    table.dropIndex(["permission_id", "height", "created_at", "id"], "idx_permission_history_perm_height_created_id");
  });

  await knex.schema.alterTable("credential_schema_history", (table) => {
    table.dropIndex(
      ["credential_schema_id", "height", "created_at", "id"],
      "idx_credential_schema_history_schema_height_created_id"
    );
  });

  await knex.schema.alterTable("trust_registry_history", (table) => {
    table.dropIndex(["tr_id", "height", "created_at", "id"], "idx_trust_registry_history_tr_height_created_id");
  });
}

