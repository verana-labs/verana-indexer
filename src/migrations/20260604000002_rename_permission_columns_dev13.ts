import { Knex } from "knex";

// dev.13 renamed the Participant (former Permission) fields:
//   - PG enum types: permission_type -> participant_role, validation_state -> onboarding_state
//   - columns: type -> role, validator_perm_id -> validator_participant_id, vp_* -> op_*
// Additive, idempotent ALTER ... RENAME (metadata-only; indexes auto-follow). Guarded so
// it is a no-op on fresh installs (create-table already produces the new names) and renames
// on databases still on the dev.12 names.

const ENUM_TYPE_RENAMES: Array<{ from: string; to: string }> = [
  { from: "permission_type", to: "participant_role" },
  { from: "validation_state", to: "onboarding_state" },
];

const COLUMN_RENAMES: Array<{ from: string; to: string }> = [
  { from: "type", to: "role" },
  { from: "validator_perm_id", to: "validator_participant_id" },
  { from: "vp_state", to: "op_state" },
  { from: "vp_last_state_change", to: "op_last_state_change" },
  { from: "vp_current_fees", to: "op_current_fees" },
  { from: "vp_current_deposit", to: "op_current_deposit" },
  { from: "vp_summary_digest", to: "op_summary_digest" },
  { from: "vp_exp", to: "op_exp" },
  { from: "vp_validator_deposit", to: "op_validator_deposit" },
];

// Tables that carry the renamed Participant columns (sessions don't).
const TABLES = ["permissions", "permission_history"];

async function renameEnumTypes(
  knex: Knex,
  renames: Array<{ from: string; to: string }>
): Promise<void> {
  for (const { from, to } of renames) {
    await knex.raw(`DO $$ BEGIN
      IF EXISTS (SELECT 1 FROM pg_type WHERE typname = '${from}')
         AND NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = '${to}') THEN
        ALTER TYPE ${from} RENAME TO ${to};
      END IF;
    END $$;`);
  }
}

async function renameColumns(
  knex: Knex,
  tables: string[],
  renames: Array<{ from: string; to: string }>
): Promise<void> {
  for (const table of tables) {
    if (!(await knex.schema.hasTable(table))) continue;
    for (const { from, to } of renames) {
      const hasFrom = await knex.schema.hasColumn(table, from);
      const hasTo = await knex.schema.hasColumn(table, to);
      if (hasFrom && !hasTo) {
        await knex.schema.alterTable(table, (t) => t.renameColumn(from, to));
      }
    }
  }
}

export async function up(knex: Knex): Promise<void> {
  await knex.raw("SET lock_timeout = '5s'");
  await renameEnumTypes(knex, ENUM_TYPE_RENAMES);
  await renameColumns(knex, TABLES, COLUMN_RENAMES);
}

export async function down(knex: Knex): Promise<void> {
  await knex.raw("SET lock_timeout = '5s'");
  await renameColumns(
    knex,
    TABLES,
    COLUMN_RENAMES.map(({ from, to }) => ({ from: to, to: from }))
  );
  await renameEnumTypes(
    knex,
    ENUM_TYPE_RENAMES.map(({ from, to }) => ({ from: to, to: from }))
  );
}
