import { Knex } from "knex";

const CS_V4 = [
  "holder_onboarding_mode",
  "pricing_asset_type",
  "pricing_asset",
  "digest_algorithm",
] as const;

const PERM_V4 = [
  "vs_operator",
  "adjusted",
  "adjusted_by",
  "vs_operator_authz_enabled",
  "vs_operator_authz_spend_limit",
  "vs_operator_authz_with_feegrant",
  "vs_operator_authz_fee_spend_limit",
  "vs_operator_authz_spend_period",
] as const;

async function hasTable(knex: Knex, table: string): Promise<boolean> {
  return knex.schema.hasTable(table);
}

async function hasColumn(knex: Knex, table: string, col: string): Promise<boolean> {
  return knex.schema.hasColumn(table, col);
}

async function renameColumnIfExists(
  knex: Knex,
  table: string,
  from: string,
  to: string
): Promise<void> {
  if (!(await hasTable(knex, table))) return;
  const hasFrom = await hasColumn(knex, table, from);
  const hasTo = await hasColumn(knex, table, to);
  if (hasFrom && !hasTo) {
    await knex.schema.alterTable(table, (t) => t.renameColumn(from, to));
  }
}

async function dropColumnIfExists(knex: Knex, table: string, col: string): Promise<void> {
  if (!(await hasTable(knex, table))) return;
  if (await hasColumn(knex, table, col)) {
    await knex.schema.alterTable(table, (t) => t.dropColumn(col));
  }
}

async function addCsV4ColumnsIfMissing(knex: Knex, table: string): Promise<void> {
  if (!(await hasTable(knex, table))) return;
  for (const col of CS_V4) {
    if (await hasColumn(knex, table, col)) continue;
    await knex.schema.alterTable(table, (t) => {
      t.text(col).nullable();
    });
  }
}

async function addPermV4ColumnsIfMissing(knex: Knex, table: string): Promise<void> {
  if (!(await hasTable(knex, table))) return;
  for (const col of PERM_V4) {
    if (await hasColumn(knex, table, col)) continue;
    await knex.schema.alterTable(table, (t) => {
      if (col === "vs_operator") {
        t.text("vs_operator").nullable();
      } else if (col === "adjusted") {
        t.timestamp("adjusted", { useTz: true }).nullable();
      } else if (col === "adjusted_by") {
        t.text("adjusted_by").nullable();
      } else if (col === "vs_operator_authz_enabled") {
        t.boolean("vs_operator_authz_enabled").notNullable().defaultTo(false);
      } else if (col === "vs_operator_authz_spend_limit") {
        t.jsonb("vs_operator_authz_spend_limit").nullable();
      } else if (col === "vs_operator_authz_with_feegrant") {
        t.boolean("vs_operator_authz_with_feegrant").notNullable().defaultTo(false);
      } else if (col === "vs_operator_authz_fee_spend_limit") {
        t.jsonb("vs_operator_authz_fee_spend_limit").nullable();
      } else {
        t.text("vs_operator_authz_spend_period").nullable();
      }
    });
  }
}

export async function up(knex: Knex): Promise<void> {
  // Add new V4 columns first (so later data moves can rely on them).
  await addCsV4ColumnsIfMissing(knex, "credential_schemas");
  await addCsV4ColumnsIfMissing(knex, "credential_schema_history");
  await addPermV4ColumnsIfMissing(knex, "permissions");
  await addPermV4ColumnsIfMissing(knex, "permission_history");

  // Terminology / schema adjustments.
  for (const table of ["permissions", "permission_history"] as const) {
    if (!(await hasTable(knex, table))) continue;
    const hasOld = await hasColumn(knex, table, "vp_summary_digest_sri");
    const hasNew = await hasColumn(knex, table, "vp_summary_digest");
    if (hasOld && !hasNew) {
      await knex.schema.alterTable(table, (t) => t.renameColumn("vp_summary_digest_sri", "vp_summary_digest"));
    }
  }

  await renameColumnIfExists(knex, "trust_registry", "controller", "corporation");
  await renameColumnIfExists(knex, "trust_registry_history", "controller", "corporation");
  await renameColumnIfExists(knex, "trust_registry_snapshot", "controller", "corporation");
  await dropColumnIfExists(knex, "trust_registry", "deposit");
  await dropColumnIfExists(knex, "trust_registry_history", "deposit");
  await dropColumnIfExists(knex, "trust_registry_snapshot", "deposit");

  await renameColumnIfExists(
    knex,
    "credential_schemas",
    "issuer_perm_management_mode",
    "issuer_onboarding_mode"
  );
  await renameColumnIfExists(
    knex,
    "credential_schemas",
    "verifier_perm_management_mode",
    "verifier_onboarding_mode"
  );
  await renameColumnIfExists(
    knex,
    "credential_schema_history",
    "issuer_perm_management_mode",
    "issuer_onboarding_mode"
  );
  await renameColumnIfExists(
    knex,
    "credential_schema_history",
    "verifier_perm_management_mode",
    "verifier_onboarding_mode"
  );
  await dropColumnIfExists(knex, "credential_schemas", "deposit");
  await dropColumnIfExists(knex, "credential_schema_history", "deposit");

  // Move "extended" -> "adjusted" when applicable.
  if (await hasTable(knex, "permissions")) {
    const hasExtended = await hasColumn(knex, "permissions", "extended");
    const hasExtendedBy = await hasColumn(knex, "permissions", "extended_by");
    const hasAdjusted = await hasColumn(knex, "permissions", "adjusted");
    const hasAdjustedBy = await hasColumn(knex, "permissions", "adjusted_by");
    if (hasExtended && hasExtendedBy && hasAdjusted && hasAdjustedBy) {
      await knex.raw(`
        UPDATE permissions
        SET adjusted = COALESCE(adjusted, extended),
            adjusted_by = COALESCE(adjusted_by, extended_by)
        WHERE extended IS NOT NULL OR extended_by IS NOT NULL
      `);
    }
  }
  if (await hasTable(knex, "permission_history")) {
    const hasExtended = await hasColumn(knex, "permission_history", "extended");
    const hasExtendedBy = await hasColumn(knex, "permission_history", "extended_by");
    const hasAdjusted = await hasColumn(knex, "permission_history", "adjusted");
    const hasAdjustedBy = await hasColumn(knex, "permission_history", "adjusted_by");
    if (hasExtended && hasExtendedBy && hasAdjusted && hasAdjustedBy) {
      await knex.raw(`
        UPDATE permission_history
        SET adjusted = COALESCE(adjusted, extended),
            adjusted_by = COALESCE(adjusted_by, extended_by)
        WHERE extended IS NOT NULL OR extended_by IS NOT NULL
      `);
    }
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

  // Permission sessions: introduce session_records, drop authz, rename controller -> corporation.
  if (await hasTable(knex, "permission_sessions")) {
    if (!(await hasColumn(knex, "permission_sessions", "session_records"))) {
      await knex.schema.alterTable("permission_sessions", (t) => t.jsonb("session_records").nullable());
    }
    if (!(await hasColumn(knex, "permission_sessions", "vs_operator"))) {
      await knex.schema.alterTable("permission_sessions", (t) => t.text("vs_operator").nullable());
    }
    if (await hasColumn(knex, "permission_sessions", "authz")) {
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
    }
    await knex.raw(`UPDATE permission_sessions SET session_records = '[]'::jsonb WHERE session_records IS NULL`);
    await renameColumnIfExists(knex, "permission_sessions", "controller", "corporation");
    await dropColumnIfExists(knex, "permission_sessions", "authz");
    await knex.raw(`
      ALTER TABLE permission_sessions
      ALTER COLUMN session_records SET DEFAULT '[]'::jsonb,
      ALTER COLUMN session_records SET NOT NULL
    `);
  }

  if (await hasTable(knex, "permission_session_history")) {
    if (!(await hasColumn(knex, "permission_session_history", "session_records"))) {
      await knex.schema.alterTable("permission_session_history", (t) => t.jsonb("session_records").nullable());
    }
    if (!(await hasColumn(knex, "permission_session_history", "vs_operator"))) {
      await knex.schema.alterTable("permission_session_history", (t) => t.text("vs_operator").nullable());
    }
    if (await hasColumn(knex, "permission_session_history", "authz")) {
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
    }
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

  // Trust deposits terminology.
  await renameColumnIfExists(knex, "trust_deposits", "account", "corporation");
  await renameColumnIfExists(knex, "trust_deposits", "amount", "deposit");
  await dropColumnIfExists(knex, "trust_deposits", "last_repaid_by");

  await renameColumnIfExists(knex, "trust_deposit_history", "account", "corporation");
  await renameColumnIfExists(knex, "trust_deposit_history", "amount", "deposit");
  await dropColumnIfExists(knex, "trust_deposit_history", "last_repaid_by");
}

export async function down(knex: Knex): Promise<void> {
  // Revert terminology / schema adjustments first.
  for (const table of ["permissions", "permission_history"] as const) {
    if (!(await hasTable(knex, table))) continue;
    const hasNew = await hasColumn(knex, table, "vp_summary_digest");
    const hasOld = await hasColumn(knex, table, "vp_summary_digest_sri");
    if (hasNew && !hasOld) {
      await knex.schema.alterTable(table, (t) => t.renameColumn("vp_summary_digest", "vp_summary_digest_sri"));
    }
  }

  await renameColumnIfExists(knex, "trust_deposit_history", "corporation", "account");
  await renameColumnIfExists(knex, "trust_deposit_history", "deposit", "amount");
  if (await hasTable(knex, "trust_deposit_history")) {
    if (!(await hasColumn(knex, "trust_deposit_history", "last_repaid_by"))) {
      await knex.schema.alterTable("trust_deposit_history", (t) => t.string("last_repaid_by", 255).defaultTo(""));
    }
  }

  await renameColumnIfExists(knex, "trust_deposits", "corporation", "account");
  await renameColumnIfExists(knex, "trust_deposits", "deposit", "amount");
  if (await hasTable(knex, "trust_deposits")) {
    if (!(await hasColumn(knex, "trust_deposits", "last_repaid_by"))) {
      await knex.schema.alterTable("trust_deposits", (t) => t.string("last_repaid_by", 255).defaultTo(""));
    }
  }

  if (await hasTable(knex, "permission_session_history")) {
    if (!(await hasColumn(knex, "permission_session_history", "authz"))) {
      await knex.schema.alterTable("permission_session_history", (t) => t.jsonb("authz").notNullable().defaultTo("[]"));
    }
    await renameColumnIfExists(knex, "permission_session_history", "corporation", "controller");
    await dropColumnIfExists(knex, "permission_session_history", "session_records");
    await dropColumnIfExists(knex, "permission_session_history", "vs_operator");
  }

  if (await hasTable(knex, "permission_sessions")) {
    if (!(await hasColumn(knex, "permission_sessions", "authz"))) {
      await knex.schema.alterTable("permission_sessions", (t) => t.jsonb("authz").notNullable().defaultTo("[]"));
    }
    await renameColumnIfExists(knex, "permission_sessions", "corporation", "controller");
    await dropColumnIfExists(knex, "permission_sessions", "session_records");
    await dropColumnIfExists(knex, "permission_sessions", "vs_operator");
  }

  await renameColumnIfExists(knex, "permission_history", "corporation", "grantee");
  await renameColumnIfExists(knex, "permissions", "corporation", "grantee");

  for (const table of ["permissions", "permission_history"]) {
    if (!(await hasTable(knex, table))) continue;
    const columnsToReAdd: Array<{
      name: string;
      add: (t: Knex.AlterTableBuilder) => void;
    }> = [
      { name: "created_by", add: (t) => t.text("created_by").nullable() },
      { name: "extended", add: (t) => t.timestamp("extended", { useTz: true }).nullable() },
      { name: "extended_by", add: (t) => t.text("extended_by").nullable() },
      { name: "slashed_by", add: (t) => t.text("slashed_by").nullable() },
      { name: "repaid_by", add: (t) => t.text("repaid_by").nullable() },
      { name: "revoked_by", add: (t) => t.text("revoked_by").nullable() },
      { name: "country", add: (t) => t.string("country", 2).nullable() },
      { name: "vp_term_requested", add: (t) => t.text("vp_term_requested").nullable() },
    ];

    for (const c of columnsToReAdd) {
      if (await hasColumn(knex, table, c.name)) continue;
      await knex.schema.alterTable(table, (t) => c.add(t));
    }
  }

  if (await hasTable(knex, "credential_schemas")) {
    if (!(await hasColumn(knex, "credential_schemas", "deposit"))) {
      await knex.schema.alterTable("credential_schemas", (t) => t.bigInteger("deposit").nullable());
    }
  }
  if (await hasTable(knex, "credential_schema_history")) {
    if (!(await hasColumn(knex, "credential_schema_history", "deposit"))) {
      await knex.schema.alterTable("credential_schema_history", (t) => t.bigInteger("deposit").nullable());
    }
  }
  await renameColumnIfExists(knex, "credential_schemas", "issuer_onboarding_mode", "issuer_perm_management_mode");
  await renameColumnIfExists(knex, "credential_schemas", "verifier_onboarding_mode", "verifier_perm_management_mode");
  await renameColumnIfExists(
    knex,
    "credential_schema_history",
    "issuer_onboarding_mode",
    "issuer_perm_management_mode"
  );
  await renameColumnIfExists(
    knex,
    "credential_schema_history",
    "verifier_onboarding_mode",
    "verifier_perm_management_mode"
  );

  if (await hasTable(knex, "trust_registry")) {
    if (!(await hasColumn(knex, "trust_registry", "deposit"))) {
      await knex.schema.alterTable("trust_registry", (t) =>
        t.specificType("deposit", "NUMERIC(38,0)").notNullable().defaultTo(0)
      );
    }
  }
  if (await hasTable(knex, "trust_registry_history")) {
    if (!(await hasColumn(knex, "trust_registry_history", "deposit"))) {
      await knex.schema.alterTable("trust_registry_history", (t) => t.specificType("deposit", "NUMERIC(38,0)").nullable());
    }
  }
  if (await hasTable(knex, "trust_registry_snapshot")) {
    if (!(await hasColumn(knex, "trust_registry_snapshot", "deposit"))) {
      await knex.schema.alterTable("trust_registry_snapshot", (t) =>
        t.specificType("deposit", "NUMERIC(38,0)").notNullable().defaultTo(0)
      );
    }
  }
  await renameColumnIfExists(knex, "trust_registry", "corporation", "controller");
  await renameColumnIfExists(knex, "trust_registry_history", "corporation", "controller");
  await renameColumnIfExists(knex, "trust_registry_snapshot", "corporation", "controller");

  // Finally drop the V4-added columns.
  const dropCs = async (table: string) => {
    if (!(await hasTable(knex, table))) return;
    for (const col of CS_V4) {
      if (await hasColumn(knex, table, col)) {
        await knex.schema.alterTable(table, (t) => t.dropColumn(col));
      }
    }
  };
  await dropCs("credential_schemas");
  await dropCs("credential_schema_history");

  const dropPerm = async (table: string) => {
    if (!(await hasTable(knex, table))) return;
    for (const col of PERM_V4) {
      if (await hasColumn(knex, table, col)) {
        await knex.schema.alterTable(table, (t) => t.dropColumn(col));
      }
    }
  };
  await dropPerm("permissions");
  await dropPerm("permission_history");
}

