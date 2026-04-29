import { Knex } from "knex";

const TR_HISTORY_STATS_COLUMNS = [
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

async function ensureTrustRegistryHistoryTables(knex: Knex): Promise<void> {
  if (!(await knex.schema.hasTable("trust_registry_history"))) {
    await knex.schema.createTable("trust_registry_history", (table) => {
      table.bigIncrements("id").primary();
      table.bigInteger("tr_id").notNullable();
      table.string("did").notNullable();
      table.string("corporation").notNullable();
      table.timestamp("created").notNullable();
      table.timestamp("modified").notNullable();
      table.timestamp("archived").nullable();
      table.string("aka").nullable();
      table.string("language", 2).notNullable();
      table.integer("active_version").nullable();
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
      table.text("event_type").notNullable();
      table.bigInteger("height").notNullable();
      table.jsonb("changes").nullable();
      table.timestamp("created_at").defaultTo(knex.fn.now());
    });
  }

  if (!(await knex.schema.hasTable("governance_framework_version_history"))) {
    await knex.schema.createTable("governance_framework_version_history", (table) => {
      table.bigIncrements("id").primary();
      table.bigInteger("tr_id").notNullable();
      table.timestamp("created").notNullable();
      table.integer("version").notNullable();
      table.timestamp("active_since").nullable();
      table.text("event_type").notNullable();
      table.bigInteger("height").notNullable();
      table.jsonb("changes").nullable();
      table.timestamp("created_at").defaultTo(knex.fn.now());
    });
  }

  if (!(await knex.schema.hasTable("governance_framework_document_history"))) {
    await knex.schema.createTable("governance_framework_document_history", (table) => {
      table.bigIncrements("id").primary();
      table.bigInteger("gfv_id").notNullable();
      table.bigInteger("tr_id").notNullable();
      table.timestamp("created").notNullable();
      table.string("language", 2).notNullable();
      table.text("url").notNullable();
      table.text("digest_sri").notNullable();
      table.text("event_type").notNullable();
      table.bigInteger("height").notNullable();
      table.jsonb("changes").nullable();
      table.timestamp("created_at").defaultTo(knex.fn.now());
    });
  }
}

async function addTrustRegistryHistoryStatsColumns(knex: Knex): Promise<void> {
  const hasParticipants = await knex.schema.hasColumn("trust_registry_history", "participants");
  if (hasParticipants) return;

  await knex.schema.alterTable("trust_registry_history", (table) => {
    table.bigInteger("participants").notNullable().defaultTo(0);
    table.bigInteger("active_schemas").notNullable().defaultTo(0);
    table.bigInteger("archived_schemas").notNullable().defaultTo(0);
    table.specificType("weight", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("issued", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("verified", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.bigInteger("ecosystem_slash_events").notNullable().defaultTo(0);
    table.specificType("ecosystem_slashed_amount", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("ecosystem_slashed_amount_repaid", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.bigInteger("network_slash_events").notNullable().defaultTo(0);
    table.specificType("network_slashed_amount", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("network_slashed_amount_repaid", "NUMERIC(38,0)").notNullable().defaultTo(0);
  });
}

async function dropTrustRegistryHistoryStatsColumns(knex: Knex): Promise<void> {
  for (const col of TR_HISTORY_STATS_COLUMNS) {
    // eslint-disable-next-line no-await-in-loop
    const exists = await knex.schema.hasColumn("trust_registry_history", col);
    if (!exists) continue;

    // eslint-disable-next-line no-await-in-loop
    await knex.schema.alterTable("trust_registry_history", (table) => {
      table.dropColumn(col);
    });
  }
}

export async function up(knex: Knex): Promise<void> {
  await ensureTrustRegistryHistoryTables(knex);
  await addTrustRegistryHistoryStatsColumns(knex);

  await knex.raw(
    "CREATE INDEX IF NOT EXISTS idx_trust_registry_history_tr_height_created_id ON trust_registry_history (tr_id, height, created_at, id)"
  );
  await knex.raw(
    "CREATE INDEX IF NOT EXISTS idx_credential_schema_history_schema_height_created_id ON credential_schema_history (credential_schema_id, height, created_at, id)"
  );
  await knex.raw(
    "CREATE INDEX IF NOT EXISTS idx_permission_history_perm_height_created_id ON permission_history (permission_id, height, created_at, id)"
  );
  await knex.raw(
    "CREATE INDEX IF NOT EXISTS idx_permission_history_schema_height_created_id ON permission_history (schema_id, height, created_at, id)"
  );

  if (!(await knex.schema.hasTable("trust_registry_version"))) {
    await knex.schema.createTable("trust_registry_version", (table) => {
      table.bigInteger("id").primary();
      table.bigInteger("tr_id").notNullable();
      table.timestamp("created").notNullable();
      table.integer("version").notNullable();
      table.timestamp("active_since").notNullable();
      table.foreign("tr_id").references("id").inTable("trust_registry").onDelete("CASCADE");
      table.unique(["tr_id", "version"], "tr_version_trid_version_unique");
    });
  }
  await knex.raw("CREATE INDEX IF NOT EXISTS idx_tr_version_tr_id ON trust_registry_version (tr_id)");

  if (!(await knex.schema.hasTable("trust_registry_document"))) {
    await knex.schema.createTable("trust_registry_document", (table) => {
      table.bigInteger("id").primary();
      table.bigInteger("version_id").notNullable();
      table.timestamp("created").notNullable();
      table.string("language", 2).notNullable();
      table.text("url").notNullable();
      table.text("digest_sri").notNullable();
      table.foreign("version_id").references("id").inTable("trust_registry_version").onDelete("CASCADE");
      table.unique(["version_id", "url"], "tr_document_version_url_unique");
    });
  }
  await knex.raw("CREATE INDEX IF NOT EXISTS idx_tr_doc_version_id ON trust_registry_document (version_id)");

  if (!(await knex.schema.hasTable("trust_registry_snapshot"))) {
    await knex.schema.createTable("trust_registry_snapshot", (table) => {
      table.bigIncrements("id").primary();
      table.bigInteger("tr_id").notNullable();
      table.bigInteger("height").notNullable();
      table.text("event_type").notNullable();
      table.string("did").notNullable();
      table.string("corporation").notNullable();
      table.timestamp("created").notNullable();
      table.timestamp("modified").notNullable();
      table.timestamp("archived").nullable();
      table.string("aka").nullable();
      table.string("language", 2).notNullable();
      table.integer("active_version").nullable();
      table.bigInteger("participants").notNullable().defaultTo(0);
      table.bigInteger("participants_ecosystem").notNullable().defaultTo(0);
      table.bigInteger("participants_issuer_grantor").notNullable().defaultTo(0);
      table.bigInteger("participants_issuer").notNullable().defaultTo(0);
      table.bigInteger("participants_verifier_grantor").notNullable().defaultTo(0);
      table.bigInteger("participants_verifier").notNullable().defaultTo(0);
      table.bigInteger("participants_holder").notNullable().defaultTo(0);
      table.bigInteger("active_schemas").notNullable().defaultTo(0);
      table.bigInteger("archived_schemas").notNullable().defaultTo(0);
      table.specificType("weight", "NUMERIC(38,0)").notNullable().defaultTo(0);
      table.specificType("issued", "NUMERIC(38,0)").notNullable().defaultTo(0);
      table.specificType("verified", "NUMERIC(38,0)").notNullable().defaultTo(0);
      table.bigInteger("ecosystem_slash_events").notNullable().defaultTo(0);
      table.specificType("ecosystem_slashed_amount", "NUMERIC(38,0)").notNullable().defaultTo(0);
      table.specificType("ecosystem_slashed_amount_repaid", "NUMERIC(38,0)").notNullable().defaultTo(0);
      table.bigInteger("network_slash_events").notNullable().defaultTo(0);
      table.specificType("network_slashed_amount", "NUMERIC(38,0)").notNullable().defaultTo(0);
      table.specificType("network_slashed_amount_repaid", "NUMERIC(38,0)").notNullable().defaultTo(0);
      table.jsonb("versions_snapshot").nullable();
      table.timestamp("created_at").defaultTo(knex.fn.now());
    });
  }
  await knex.raw(
    "CREATE INDEX IF NOT EXISTS idx_tr_snapshot_tr_height ON trust_registry_snapshot (tr_id, height DESC)"
  );
  await knex.raw(
    "CREATE INDEX IF NOT EXISTS idx_tr_snapshot_tr_height_created_id ON trust_registry_snapshot (tr_id, height DESC, created_at DESC, id DESC)"
  );

  if (!(await knex.schema.hasTable("trust_registry_snapshot_diff"))) {
    await knex.schema.createTable("trust_registry_snapshot_diff", (table) => {
      table.bigIncrements("id").primary();
      table.bigInteger("tr_id").notNullable();
      table.bigInteger("height").notNullable();
      table.text("event_type").notNullable();
      table.bigInteger("prev_snapshot_id").nullable();
      table.bigInteger("next_snapshot_id").notNullable();
      table.jsonb("diff").notNullable();
      table.timestamp("created_at").defaultTo(knex.fn.now());
      table.foreign("prev_snapshot_id").references("id").inTable("trust_registry_snapshot");
      table.foreign("next_snapshot_id").references("id").inTable("trust_registry_snapshot");
    });
  }
  await knex.raw(
    "CREATE INDEX IF NOT EXISTS idx_tr_snapshot_diff_tr_height ON trust_registry_snapshot_diff (tr_id, height)"
  );
  await knex.raw(
    "CREATE INDEX IF NOT EXISTS idx_tr_snapshot_diff_next ON trust_registry_snapshot_diff (next_snapshot_id)"
  );
  await knex.raw(
    "CREATE INDEX IF NOT EXISTS idx_tr_snapshot_diff_prev ON trust_registry_snapshot_diff (prev_snapshot_id)"
  );

  const STATS_BIGINT = [
    "participants", "participants_ecosystem", "participants_issuer_grantor", "participants_issuer",
    "participants_verifier_grantor", "participants_verifier", "participants_holder",
    "active_schemas", "archived_schemas", "ecosystem_slash_events", "network_slash_events",
  ];
  const STATS_NUMERIC = [
    "weight", "issued", "verified",
    "ecosystem_slashed_amount", "ecosystem_slashed_amount_repaid",
    "network_slashed_amount", "network_slashed_amount_repaid",
  ];
  const addColumnIfMissing = async (
    tableName: string,
    columnName: string,
    bigint: boolean
  ): Promise<void> => {
    const exists = await knex.schema.hasColumn(tableName, columnName);
    if (!exists) {
      await knex.schema.alterTable(tableName, (table) => {
        if (bigint) {
          table.bigInteger(columnName).notNullable().defaultTo(0);
        } else {
          table.specificType(columnName, "NUMERIC(38,0)").notNullable().defaultTo(0);
        }
      });
    }
  };
  for (const col of STATS_BIGINT) {
    await addColumnIfMissing("trust_registry", col, true);
    await addColumnIfMissing("trust_registry_history", col, true);
  }
  for (const col of STATS_NUMERIC) {
    await addColumnIfMissing("trust_registry", col, false);
    await addColumnIfMissing("trust_registry_history", col, false);
  }
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists("trust_registry_snapshot_diff");
  await knex.schema.dropTableIfExists("trust_registry_snapshot");
  await knex.schema.dropTableIfExists("trust_registry_document");
  await knex.schema.dropTableIfExists("trust_registry_version");

  await knex.raw("DROP INDEX IF EXISTS idx_permission_history_schema_height_created_id");
  await knex.raw("DROP INDEX IF EXISTS idx_permission_history_perm_height_created_id");
  await knex.raw("DROP INDEX IF EXISTS idx_credential_schema_history_schema_height_created_id");
  await knex.raw("DROP INDEX IF EXISTS idx_trust_registry_history_tr_height_created_id");

  await dropTrustRegistryHistoryStatsColumns(knex);

  await knex.schema.dropTableIfExists("governance_framework_document_history");
  await knex.schema.dropTableIfExists("governance_framework_version_history");
  await knex.schema.dropTableIfExists("trust_registry_history");
}
