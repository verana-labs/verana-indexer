import { Knex } from "knex";
export const config = { transaction: false };

export async function up(knex: Knex): Promise<void> {
  const client = (knex.client.config && (knex.client.config.client || "")).toString();

  if (client.includes("pg")) {
    if (await knex.schema.hasColumn("permissions", "grantee")) {
      await knex.raw(`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_permissions_grantee ON permissions (grantee)`);
    }
    if (await knex.schema.hasColumn("permissions", "validator_perm_id")) {
      await knex.raw(`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_permissions_validator_perm_id ON permissions (validator_perm_id)`);
    }
    if (await knex.schema.hasColumn("permission_history", "permission_id") && await knex.schema.hasColumn("permission_history", "height")) {
      await knex.raw(`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_permission_history_permission_height_desc ON permission_history (permission_id, height DESC)`);
    }
    if (await knex.schema.hasColumn("permission_history", "schema_id") && await knex.schema.hasColumn("permission_history", "height")) {
      await knex.raw(`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_permission_history_schema_height_desc ON permission_history (schema_id, height DESC)`);
    }
    if (await knex.schema.hasColumn("permission_history", "grantee") && await knex.schema.hasColumn("permission_history", "height")) {
      await knex.raw(`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_permission_history_grantee_height_desc ON permission_history (grantee, height DESC)`);
    }
    if (await knex.schema.hasColumn("permission_sessions", "session_id")) {
      await knex.raw(`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_permission_sessions_session_id ON permission_sessions (session_id)`);
    }
  } else {
    await knex.schema.table("permissions", (table) => {
      table.index(["grantee"], "idx_permissions_grantee");
      table.index(["validator_perm_id"], "idx_permissions_validator_perm_id");
    });
    await knex.schema.table("permission_history", (table) => {
      table.index(["permission_id", "height"], "idx_permission_history_permission_height");
      table.index(["schema_id", "height"], "idx_permission_history_schema_height");
      table.index(["grantee", "height"], "idx_permission_history_grantee_height");
    });
    await knex.schema.table("permission_sessions", (table) => {
      table.index(["session_id"], "idx_permission_sessions_session_id");
    });
  }
}

export async function down(knex: Knex): Promise<void> {
  const client = (knex.client.config && (knex.client.config.client || "")).toString();
  if (client.includes("pg")) {
    await knex.raw(`DROP INDEX IF EXISTS idx_permissions_grantee`);
    await knex.raw(`DROP INDEX IF EXISTS idx_permissions_validator_perm_id`);
    await knex.raw(`DROP INDEX IF EXISTS idx_permission_history_permission_height_desc`);
    await knex.raw(`DROP INDEX IF EXISTS idx_permission_history_schema_height_desc`);
    await knex.raw(`DROP INDEX IF EXISTS idx_permission_history_grantee_height_desc`);
    await knex.raw(`DROP INDEX IF EXISTS idx_permission_sessions_session_id`);
  } else {
    await knex.schema.table("permissions", (table) => {
      table.dropIndex(["grantee"], "idx_permissions_grantee");
      table.dropIndex(["validator_perm_id"], "idx_permissions_validator_perm_id");
    });
    await knex.schema.table("permission_history", (table) => {
      table.dropIndex(["permission_id", "height"], "idx_permission_history_permission_height");
      table.dropIndex(["schema_id", "height"], "idx_permission_history_schema_height");
      table.dropIndex(["grantee", "height"], "idx_permission_history_grantee_height");
    });
    await knex.schema.table("permission_sessions", (table) => {
      table.dropIndex(["session_id"], "idx_permission_sessions_session_id");
    });
  }
}

