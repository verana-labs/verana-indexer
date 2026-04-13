import { Knex } from "knex";

async function renameColumnIfExists(
  knex: Knex,
  table: string,
  from: string,
  to: string
): Promise<void> {
  if (!(await knex.schema.hasTable(table))) return;
  const hasFrom = await knex.schema.hasColumn(table, from);
  const hasTo = await knex.schema.hasColumn(table, to);
  if (hasFrom && !hasTo) {
    await knex.schema.alterTable(table, (t) => {
      t.renameColumn(from, to);
    });
  }
}

async function dropColumnIfExists(knex: Knex, table: string, col: string): Promise<void> {
  if (!(await knex.schema.hasTable(table))) return;
  if (await knex.schema.hasColumn(table, col)) {
    await knex.schema.alterTable(table, (t) => t.dropColumn(col));
  }
}


export async function up(knex: Knex): Promise<void> {
   for (const table of ["permissions", "permission_history"] as const) {
    const hasOld = await knex.schema.hasColumn(table, "vp_summary_digest_sri");
    const hasNew = await knex.schema.hasColumn(table, "vp_summary_digest");
    if (hasOld && !hasNew) {
      await knex.schema.alterTable(table, (t) => {
        t.renameColumn("vp_summary_digest_sri", "vp_summary_digest");
      });
    }
  }
  await renameColumnIfExists(knex, "trust_registry", "controller", "corporation");
  await renameColumnIfExists(knex, "trust_registry_history", "controller", "corporation");
  await renameColumnIfExists(knex, "trust_registry_snapshot", "controller", "corporation");
  await dropColumnIfExists(knex, "trust_registry", "deposit");
  await dropColumnIfExists(knex, "trust_registry_history", "deposit");
  await dropColumnIfExists(knex, "trust_registry_snapshot", "deposit");

  await renameColumnIfExists(knex, "credential_schemas", "issuer_perm_management_mode", "issuer_onboarding_mode");
  await renameColumnIfExists(knex, "credential_schemas", "verifier_perm_management_mode", "verifier_onboarding_mode");
  await renameColumnIfExists(knex, "credential_schema_history", "issuer_perm_management_mode", "issuer_onboarding_mode");
  await renameColumnIfExists(knex, "credential_schema_history", "verifier_perm_management_mode", "verifier_onboarding_mode");
  await dropColumnIfExists(knex, "credential_schemas", "deposit");
  await dropColumnIfExists(knex, "credential_schema_history", "deposit");

  if (await knex.schema.hasTable("permissions")) {
    await knex.raw(`
      UPDATE permissions
      SET adjusted = COALESCE(adjusted, extended),
          adjusted_by = COALESCE(adjusted_by, extended_by)
      WHERE extended IS NOT NULL OR extended_by IS NOT NULL
    `);
  }
  if (await knex.schema.hasTable("permission_history")) {
    await knex.raw(`
      UPDATE permission_history
      SET adjusted = COALESCE(adjusted, extended),
          adjusted_by = COALESCE(adjusted_by, extended_by)
      WHERE extended IS NOT NULL OR extended_by IS NOT NULL
    `);
  }

  await renameColumnIfExists(knex, "permissions", "grantee", "corporation");
  await renameColumnIfExists(knex, "permission_history", "grantee", "corporation");

  for (const col of [
    "created_by",
    "extended",
    "extended_by",
    "slashed_by",
    "repaid_by",
    "revoked_by",
    "country",
    "vp_term_requested",
  ]) {
    await dropColumnIfExists(knex, "permissions", col);
    await dropColumnIfExists(knex, "permission_history", col);
  }

  if (await knex.schema.hasTable("permission_sessions")) {
    if (!(await knex.schema.hasColumn("permission_sessions", "session_records"))) {
      await knex.schema.alterTable("permission_sessions", (t) => {
        t.jsonb("session_records").nullable();
      });
    }
    if (!(await knex.schema.hasColumn("permission_sessions", "vs_operator"))) {
      await knex.schema.alterTable("permission_sessions", (t) => {
        t.text("vs_operator").nullable();
      });
    }
    await knex.raw(`
      UPDATE permission_sessions
      SET session_records = COALESCE(
        (
          SELECT jsonb_agg(
            jsonb_build_object(
              'created', to_jsonb(permission_sessions.modified),
              'issuer_perm_id', elem->'issuer_perm_id',
              'verifier_perm_id', elem->'verifier_perm_id',
              'wallet_agent_perm_id', elem->'wallet_agent_perm_id'
            )
          )
          FROM jsonb_array_elements(
            CASE
              WHEN permission_sessions.authz IS NULL THEN '[]'::jsonb
              WHEN jsonb_typeof(permission_sessions.authz::jsonb) = 'array' THEN permission_sessions.authz::jsonb
              ELSE '[]'::jsonb
            END
          ) AS elem
        ),
        '[]'::jsonb
      )
      WHERE session_records IS NULL
    `);
    await knex.raw(`UPDATE permission_sessions SET session_records = '[]'::jsonb WHERE session_records IS NULL`);
    await renameColumnIfExists(knex, "permission_sessions", "controller", "corporation");
    await dropColumnIfExists(knex, "permission_sessions", "authz");
    await knex.raw(`
      ALTER TABLE permission_sessions
      ALTER COLUMN session_records SET DEFAULT '[]'::jsonb,
      ALTER COLUMN session_records SET NOT NULL
    `);
  }

  if (await knex.schema.hasTable("permission_session_history")) {
    if (!(await knex.schema.hasColumn("permission_session_history", "session_records"))) {
      await knex.schema.alterTable("permission_session_history", (t) => {
        t.jsonb("session_records").nullable();
      });
    }
    if (!(await knex.schema.hasColumn("permission_session_history", "vs_operator"))) {
      await knex.schema.alterTable("permission_session_history", (t) => {
        t.text("vs_operator").nullable();
      });
    }
    await knex.raw(`
      UPDATE permission_session_history
      SET session_records = COALESCE(
        (
          SELECT jsonb_agg(
            jsonb_build_object(
              'created', to_jsonb(permission_session_history.modified),
              'issuer_perm_id', elem->'issuer_perm_id',
              'verifier_perm_id', elem->'verifier_perm_id',
              'wallet_agent_perm_id', elem->'wallet_agent_perm_id'
            )
          )
          FROM jsonb_array_elements(
            CASE
              WHEN permission_session_history.authz IS NULL THEN '[]'::jsonb
              WHEN jsonb_typeof(permission_session_history.authz::jsonb) = 'array' THEN permission_session_history.authz::jsonb
              ELSE '[]'::jsonb
            END
          ) AS elem
        ),
        '[]'::jsonb
      )
      WHERE session_records IS NULL
    `);
    await knex.raw(
      `UPDATE permission_session_history SET session_records = '[]'::jsonb WHERE session_records IS NULL`
    );
    await renameColumnIfExists(knex, "permission_session_history", "controller", "corporation");
    await dropColumnIfExists(knex, "permission_session_history", "authz");
    await knex.raw(`
      ALTER TABLE permission_session_history
      ALTER COLUMN session_records SET DEFAULT '[]'::jsonb,
      ALTER COLUMN session_records SET NOT NULL
    `);
  }

  await renameColumnIfExists(knex, "trust_deposits", "account", "corporation");
  await renameColumnIfExists(knex, "trust_deposits", "amount", "deposit");
  await dropColumnIfExists(knex, "trust_deposits", "last_repaid_by");

  await renameColumnIfExists(knex, "trust_deposit_history", "account", "corporation");
  await renameColumnIfExists(knex, "trust_deposit_history", "amount", "deposit");
  await dropColumnIfExists(knex, "trust_deposit_history", "last_repaid_by");

  if (!(await knex.schema.hasTable("schema_authorization_policies"))) {
    await knex.schema.createTable("schema_authorization_policies", (table) => {
      table.bigIncrements("id").primary();
      table.integer("schema_id").notNullable().index();
      table.timestamp("created", { useTz: true }).notNullable();
      table.integer("version").notNullable();
      table.text("role").notNullable();
      table.text("url").notNullable();
      table.text("digest_sri").notNullable();
      table.timestamp("effective_from", { useTz: true }).nullable();
      table.timestamp("effective_until", { useTz: true }).nullable();
      table.boolean("revoked").notNullable().defaultTo(false);
      table.integer("height").nullable();
      table.text("tx_hash").nullable();
      table.unique(["schema_id", "version"], "sap_schema_version_unique");
      table.index(["schema_id", "role"]);
    });
  }
}

export async function down(knex: Knex): Promise<void> {
   for (const table of ["permissions", "permission_history"] as const) {
    const hasNew = await knex.schema.hasColumn(table, "vp_summary_digest");
    const hasOld = await knex.schema.hasColumn(table, "vp_summary_digest_sri");
    if (hasNew && !hasOld) {
      await knex.schema.alterTable(table, (t) => {
        t.renameColumn("vp_summary_digest", "vp_summary_digest_sri");
      });
    }
  }
  await knex.schema.dropTableIfExists("schema_authorization_policies");

  await renameColumnIfExists(knex, "trust_deposit_history", "corporation", "account");
  await renameColumnIfExists(knex, "trust_deposit_history", "deposit", "amount");
  if (await knex.schema.hasTable("trust_deposit_history")) {
    if (!(await knex.schema.hasColumn("trust_deposit_history", "last_repaid_by"))) {
      await knex.schema.alterTable("trust_deposit_history", (t) => {
        t.string("last_repaid_by", 255).defaultTo("");
      });
    }
  }

  await renameColumnIfExists(knex, "trust_deposits", "corporation", "account");
  await renameColumnIfExists(knex, "trust_deposits", "deposit", "amount");
  if (await knex.schema.hasTable("trust_deposits")) {
    if (!(await knex.schema.hasColumn("trust_deposits", "last_repaid_by"))) {
      await knex.schema.alterTable("trust_deposits", (t) => {
        t.string("last_repaid_by", 255).defaultTo("");
      });
    }
  }

  if (await knex.schema.hasTable("permission_session_history")) {
    await knex.schema.alterTable("permission_session_history", (t) => {
      t.jsonb("authz").notNullable().defaultTo("[]");
    });
    await renameColumnIfExists(knex, "permission_session_history", "corporation", "controller");
    await dropColumnIfExists(knex, "permission_session_history", "session_records");
    await dropColumnIfExists(knex, "permission_session_history", "vs_operator");
  }

  if (await knex.schema.hasTable("permission_sessions")) {
    await knex.schema.alterTable("permission_sessions", (t) => {
      t.jsonb("authz").notNullable().defaultTo("[]");
    });
    await renameColumnIfExists(knex, "permission_sessions", "corporation", "controller");
    await dropColumnIfExists(knex, "permission_sessions", "session_records");
    await dropColumnIfExists(knex, "permission_sessions", "vs_operator");
  }

  await renameColumnIfExists(knex, "permission_history", "corporation", "grantee");
  await renameColumnIfExists(knex, "permissions", "corporation", "grantee");

  for (const table of ["permissions", "permission_history"]) {
    if (await knex.schema.hasTable(table)) {
      await knex.schema.alterTable(table, (t) => {
        t.text("created_by").nullable();
        t.timestamp("extended", { useTz: true }).nullable();
        t.text("extended_by").nullable();
        t.text("slashed_by").nullable();
        t.text("repaid_by").nullable();
        t.text("revoked_by").nullable();
        t.string("country", 2).nullable();
        t.text("vp_term_requested").nullable();
      });
    }
  }

  await knex.schema.alterTable("credential_schemas", (t) => {
    t.bigInteger("deposit").nullable();
  });
  await knex.schema.alterTable("credential_schema_history", (t) => {
    t.bigInteger("deposit").nullable();
  });
  await renameColumnIfExists(knex, "credential_schemas", "issuer_onboarding_mode", "issuer_perm_management_mode");
  await renameColumnIfExists(knex, "credential_schemas", "verifier_onboarding_mode", "verifier_perm_management_mode");
  await renameColumnIfExists(knex, "credential_schema_history", "issuer_onboarding_mode", "issuer_perm_management_mode");
  await renameColumnIfExists(knex, "credential_schema_history", "verifier_onboarding_mode", "verifier_perm_management_mode");

  await knex.schema.alterTable("trust_registry", (t) => {
    t.specificType("deposit", "NUMERIC(38,0)").notNullable().defaultTo(0);
  });
  await knex.schema.alterTable("trust_registry_history", (t) => {
    t.specificType("deposit", "NUMERIC(38,0)").nullable();
  });
  await renameColumnIfExists(knex, "trust_registry", "corporation", "controller");
  await renameColumnIfExists(knex, "trust_registry_history", "corporation", "controller");
  await renameColumnIfExists(knex, "trust_registry_snapshot", "corporation", "controller");
  await knex.schema.alterTable("trust_registry_snapshot", (t) => {
    t.specificType("deposit", "NUMERIC(38,0)").notNullable().defaultTo(0);
  });
}
