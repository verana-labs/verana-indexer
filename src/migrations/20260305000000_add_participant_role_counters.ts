import { Knex } from "knex";

const PARTICIPANT_ROLE_COLUMNS = [
  "participants_ecosystem",
  "participants_issuer_grantor",
  "participants_issuer",
  "participants_verifier_grantor",
  "participants_verifier",
  "participants_holder",
] as const;

const CS_HISTORY_STATS_BIGINT = [
  "participants",
  "participants_ecosystem",
  "participants_issuer_grantor",
  "participants_issuer",
  "participants_verifier_grantor",
  "participants_verifier",
  "participants_holder",
  "ecosystem_slash_events",
  "network_slash_events",
] as const;
const CS_HISTORY_STATS_NUMERIC = [
  "weight",
  "issued",
  "verified",
  "ecosystem_slashed_amount",
  "ecosystem_slashed_amount_repaid",
  "network_slashed_amount",
  "network_slashed_amount_repaid",
] as const;

async function addCredentialSchemaHistoryStatsColumnsIfMissing(knex: Knex): Promise<void> {
  if (!(await knex.schema.hasTable("credential_schema_history"))) return;
  for (const col of CS_HISTORY_STATS_BIGINT) {
    if (await knex.schema.hasColumn("credential_schema_history", col)) continue;
    await knex.schema.alterTable("credential_schema_history", (table) => {
      table.bigInteger(col).notNullable().defaultTo(0);
    });
  }
  for (const col of CS_HISTORY_STATS_NUMERIC) {
    if (await knex.schema.hasColumn("credential_schema_history", col)) continue;
    await knex.schema.alterTable("credential_schema_history", (table) => {
      table.specificType(col, "NUMERIC(38,0)").notNullable().defaultTo(0);
    });
  }
}

async function dropCredentialSchemaHistoryStatsColumnsIfExists(knex: Knex): Promise<void> {
  if (!(await knex.schema.hasTable("credential_schema_history"))) return;
  const cols = [...CS_HISTORY_STATS_BIGINT, ...CS_HISTORY_STATS_NUMERIC];
  for (const col of cols) {
    if (!(await knex.schema.hasColumn("credential_schema_history", col))) continue;
    await knex.schema.alterTable("credential_schema_history", (table) => {
      table.dropColumn(col);
    });
  }
}

async function addColumnsIfMissing(knex: Knex, tableName: string, columns: readonly string[]) {
  for (const col of columns) {
    const exists = await knex.schema.hasColumn(tableName, col);
    if (!exists) {
      await knex.schema.alterTable(tableName, (table) => {
        table.bigInteger(col).notNullable().defaultTo(0);
      });
    }
  }
}

async function dropColumnsIfExists(knex: Knex, tableName: string, columns: readonly string[]) {
  for (const col of columns) {
    const exists = await knex.schema.hasColumn(tableName, col);
    if (exists) {
      await knex.schema.alterTable(tableName, (table) => {
        table.dropColumn(col);
      });
    }
  }
}

async function createIndexIfMissing(knex: Knex, tableName: string, indexName: string, columns: string[]) {
  try {
    await knex.schema.alterTable(tableName, (table) => {
      table.index(columns, indexName);
    });
  } catch {
    // index already exists
  }
}

async function dropIndexIfExists(knex: Knex, tableName: string, indexName: string, columns: string[]) {
  try {
    await knex.schema.alterTable(tableName, (table) => {
      table.dropIndex(columns, indexName);
    });
  } catch {
    // index does not exist
  }
}

async function createRawIndexIfMissing(knex: Knex, sql: string) {
  try {
    await knex.raw(sql);
  } catch {
    // index already exists or not supported by driver
  }
}

async function dropRawIndexIfExists(knex: Knex, indexName: string) {
  try {
    await knex.raw(`DROP INDEX IF EXISTS ${indexName}`);
  } catch {
    // index does not exist
  }
}

export async function up(knex: Knex): Promise<void> {
  await addColumnsIfMissing(knex, "permissions", PARTICIPANT_ROLE_COLUMNS);
  await addColumnsIfMissing(knex, "credential_schemas", PARTICIPANT_ROLE_COLUMNS);
  await addColumnsIfMissing(knex, "trust_registry", PARTICIPANT_ROLE_COLUMNS);
  await addColumnsIfMissing(knex, "global_metrics", PARTICIPANT_ROLE_COLUMNS);

  await addColumnsIfMissing(knex, "permission_history", PARTICIPANT_ROLE_COLUMNS);
  await addColumnsIfMissing(knex, "credential_schema_history", PARTICIPANT_ROLE_COLUMNS);
  await addCredentialSchemaHistoryStatsColumnsIfMissing(knex);
  if (await knex.schema.hasTable("trust_registry_history")) {
    await addColumnsIfMissing(knex, "trust_registry_history", PARTICIPANT_ROLE_COLUMNS);
  }

  const statsColumns: string[] = [];
  for (const col of PARTICIPANT_ROLE_COLUMNS) {
    statsColumns.push(`cumulative_${col}`);
    statsColumns.push(`delta_${col}`);
  }
  await addColumnsIfMissing(knex, "stats", statsColumns);

  for (const col of PARTICIPANT_ROLE_COLUMNS) {
    await createIndexIfMissing(knex, "permissions", `idx_permissions_${col}`, [col, "id"]);
    await createIndexIfMissing(knex, "credential_schemas", `idx_cs_${col}`, [col, "id"]);
    await createIndexIfMissing(knex, "trust_registry", `idx_tr_${col}`, [col, "id"]);
  }

  await createRawIndexIfMissing(
    knex,
    "CREATE INDEX IF NOT EXISTS idx_global_metrics_block_height_computed_at ON global_metrics (block_height DESC, computed_at DESC)"
  );
  await createRawIndexIfMissing(
    knex,
    "CREATE INDEX IF NOT EXISTS idx_global_metrics_computed_at ON global_metrics (computed_at DESC)"
  );
  await createRawIndexIfMissing(
    knex,
    "CREATE INDEX IF NOT EXISTS idx_global_metrics_null_block_height_computed_at ON global_metrics (computed_at DESC) WHERE block_height IS NULL"
  );

  await createRawIndexIfMissing(
    knex,
    "CREATE INDEX IF NOT EXISTS idx_global_metrics_computed_at_desc ON global_metrics (computed_at DESC)"
  );
  await createRawIndexIfMissing(
    knex,
    "CREATE INDEX IF NOT EXISTS idx_global_metrics_block_height_computed_at_desc_v2 ON global_metrics (block_height DESC, computed_at DESC)"
  );
  await createRawIndexIfMissing(
    knex,
    "CREATE INDEX IF NOT EXISTS idx_permission_history_metrics_latest_desc ON permission_history (permission_id, height DESC, created_at DESC, id DESC)"
  );
  await createRawIndexIfMissing(
    knex,
    "CREATE INDEX IF NOT EXISTS idx_credential_schema_history_metrics_latest_desc ON credential_schema_history (credential_schema_id, height DESC, created_at DESC, id DESC)"
  );
  if (await knex.schema.hasTable("trust_registry_history")) {
    await createRawIndexIfMissing(
      knex,
      "CREATE INDEX IF NOT EXISTS idx_trust_registry_history_metrics_latest_desc ON trust_registry_history (tr_id, height DESC, created_at DESC, id DESC)"
    );
  }
}

export async function down(knex: Knex): Promise<void> {
  await dropRawIndexIfExists(knex, "idx_trust_registry_history_metrics_latest_desc");
  await dropRawIndexIfExists(knex, "idx_credential_schema_history_metrics_latest_desc");
  await dropRawIndexIfExists(knex, "idx_permission_history_metrics_latest_desc");
  await dropRawIndexIfExists(knex, "idx_global_metrics_block_height_computed_at_desc_v2");
  await dropRawIndexIfExists(knex, "idx_global_metrics_computed_at_desc");

  await dropRawIndexIfExists(knex, "idx_global_metrics_null_block_height_computed_at");
  await dropRawIndexIfExists(knex, "idx_global_metrics_computed_at");
  await dropRawIndexIfExists(knex, "idx_global_metrics_block_height_computed_at");

  for (const col of PARTICIPANT_ROLE_COLUMNS) {
    await dropIndexIfExists(knex, "trust_registry", `idx_tr_${col}`, [col, "id"]);
    await dropIndexIfExists(knex, "credential_schemas", `idx_cs_${col}`, [col, "id"]);
    await dropIndexIfExists(knex, "permissions", `idx_permissions_${col}`, [col, "id"]);
  }

  const statsColumns: string[] = [];
  for (const col of PARTICIPANT_ROLE_COLUMNS) {
    statsColumns.push(`cumulative_${col}`);
    statsColumns.push(`delta_${col}`);
  }
  await dropColumnsIfExists(knex, "stats", statsColumns);

  if (await knex.schema.hasTable("trust_registry_history")) {
    await dropColumnsIfExists(knex, "trust_registry_history", PARTICIPANT_ROLE_COLUMNS);
  }
  await dropCredentialSchemaHistoryStatsColumnsIfExists(knex);
  await dropColumnsIfExists(knex, "credential_schema_history", PARTICIPANT_ROLE_COLUMNS);
  await dropColumnsIfExists(knex, "permission_history", PARTICIPANT_ROLE_COLUMNS);

  await dropColumnsIfExists(knex, "global_metrics", PARTICIPANT_ROLE_COLUMNS);
  await dropColumnsIfExists(knex, "trust_registry", PARTICIPANT_ROLE_COLUMNS);
  await dropColumnsIfExists(knex, "credential_schemas", PARTICIPANT_ROLE_COLUMNS);
  await dropColumnsIfExists(knex, "permissions", PARTICIPANT_ROLE_COLUMNS);
}
