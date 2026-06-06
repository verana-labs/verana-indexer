import { Knex } from "knex";

// dev.13 final rename: Permission -> Participant and TrustRegistry -> Ecosystem.
// Renames the remaining database objects (tables, leftover *_perm_id / tr_id columns,
// owned sequences, indexes) and migrates the internal module_params.module labels
// (perm -> pp, tr -> ec) so the indexer code (which now references the new names)
// stays in lock-step with the schema.
//
// Everything is additive, idempotent and guarded: a no-op on fresh installs where the
// create-table migrations already produced the new names, and a rename on databases
// still carrying the dev.12 names. Metadata-only (no table rewrites).

const TABLE_RENAMES: Array<[string, string]> = [
  ["permissions", "participants"],
  ["permission_history", "participant_history"],
  ["permission_sessions", "participant_sessions"],
  ["permission_session_history", "participant_session_history"],
  ["permission_scheduled_flips", "participant_scheduled_flips"],
  ["trust_registry", "ecosystem"],
  ["trust_registry_history", "ecosystem_history"],
  ["trust_registry_version", "ecosystem_version"],
  ["trust_registry_document", "ecosystem_document"],
  ["trust_registry_snapshot", "ecosystem_snapshot"],
  ["trust_registry_snapshot_diff", "ecosystem_snapshot_diff"],
];

// [table (new name), from, to] — applied after the table renames above.
const COLUMN_RENAMES: Array<[string, string, string]> = [
  ["participant_history", "permission_id", "participant_id"],
  ["participant_sessions", "agent_perm_id", "agent_participant_id"],
  ["participant_sessions", "wallet_agent_perm_id", "wallet_agent_participant_id"],
  ["participant_session_history", "agent_perm_id", "agent_participant_id"],
  ["participant_session_history", "wallet_agent_perm_id", "wallet_agent_participant_id"],
  ["participant_scheduled_flips", "perm_id", "participant_id"],
  ["ecosystem_history", "tr_id", "ecosystem_id"],
  ["ecosystem_version", "tr_id", "ecosystem_id"],
  ["ecosystem_snapshot", "tr_id", "ecosystem_id"],
  ["ecosystem_snapshot_diff", "tr_id", "ecosystem_id"],
  ["governance_framework_version", "tr_id", "ecosystem_id"],
  ["governance_framework_version_history", "tr_id", "ecosystem_id"],
  ["governance_framework_document_history", "tr_id", "ecosystem_id"],
];

const SEQUENCE_RENAMES: Array<[string, string]> = [
  ["permissions_id_seq", "participants_id_seq"],
  ["permission_history_id_seq", "participant_history_id_seq"],
  ["permission_sessions_id_seq", "participant_sessions_id_seq"],
  ["permission_session_history_id_seq", "participant_session_history_id_seq"],
  ["permission_scheduled_flips_id_seq", "participant_scheduled_flips_id_seq"],
  ["trust_registry_id_seq", "ecosystem_id_seq"],
  ["trust_registry_history_id_seq", "ecosystem_history_id_seq"],
];

// module_params.module is an internal label written and read by the indexer.
const MODULE_VALUE_RENAMES: Array<[string, string]> = [
  ["perm", "pp"],
  ["tr", "ec"],
];

async function renameTables(knex: Knex, pairs: Array<[string, string]>): Promise<void> {
  for (const [from, to] of pairs) {
    const hasFrom = await knex.schema.hasTable(from);
    const hasTo = await knex.schema.hasTable(to);
    if (hasFrom && !hasTo) {
      await knex.schema.renameTable(from, to);
    }
  }
}

async function renameColumns(
  knex: Knex,
  triples: Array<[string, string, string]>
): Promise<void> {
  for (const [table, from, to] of triples) {
    if (!(await knex.schema.hasTable(table))) continue;
    const hasFrom = await knex.schema.hasColumn(table, from);
    const hasTo = await knex.schema.hasColumn(table, to);
    if (hasFrom && !hasTo) {
      await knex.schema.alterTable(table, (t) => t.renameColumn(from, to));
    }
  }
}

async function renameSequences(knex: Knex, pairs: Array<[string, string]>): Promise<void> {
  // `from`/`to` are hardcoded constants below, so inlining them as literals (DO blocks
  // cannot take bind parameters) is safe.
  for (const [from, to] of pairs) {
    await knex.raw(
      `DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM pg_class WHERE relkind='S' AND relname=${quote(from)})
           AND NOT EXISTS (SELECT 1 FROM pg_class WHERE relkind='S' AND relname=${quote(to)}) THEN
          EXECUTE format('ALTER SEQUENCE %I RENAME TO %I', ${quote(from)}, ${quote(to)});
        END IF;
      END $$;`
    );
  }
}

// Rename every leftover index whose name still carries a Permission / TrustRegistry token.
// Handles explicit, auto-generated and unique-constraint-backed index names uniformly.
const INDEX_RENAME_SQL = (forward: boolean) => {
  const subs = forward
    ? [
        ["permission", "participant"],
        ["perm", "participant"],
        ["trust_registry", "ecosystem"],
        ["idx_tr_", "idx_ec_"],
        ["_tr_", "_ec_"],
        ["_vp_", "_op_"],
      ]
    : [
        ["participant", "permission"],
        ["ecosystem", "trust_registry"],
        ["idx_ec_", "idx_tr_"],
        ["_ec_", "_tr_"],
        ["_op_", "_vp_"],
      ];
  const replaces = subs
    .map(([a, b]) => `newname := replace(newname, ${quote(a)}, ${quote(b)});`)
    .join("\n      ");
  const likeFilter = forward
    ? `c.relname LIKE '%permission%' OR c.relname LIKE '%perm\\_%' OR c.relname LIKE '%\\_perm%'
        OR c.relname LIKE '%trust\\_registry%' OR c.relname LIKE 'idx\\_tr\\_%'
        OR c.relname LIKE '%\\_tr\\_%' OR c.relname LIKE '%\\_vp\\_%'`
    : `c.relname LIKE '%participant%' OR c.relname LIKE '%ecosystem%'
        OR c.relname LIKE 'idx\\_ec\\_%' OR c.relname LIKE '%\\_ec\\_%' OR c.relname LIKE '%\\_op\\_%'`;
  return `DO $$
    DECLARE r record; newname text;
    BEGIN
      FOR r IN
        SELECT c.relname AS idxname
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'i' AND n.nspname = 'public'
          AND (${likeFilter})
      LOOP
        newname := r.idxname;
        ${replaces}
        IF newname <> r.idxname
           AND NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = newname) THEN
          BEGIN
            EXECUTE format('ALTER INDEX IF EXISTS public.%I RENAME TO %I', r.idxname, newname);
          EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'skip index rename % -> %: %', r.idxname, newname, SQLERRM;
          END;
        END IF;
      END LOOP;
    END $$;`;
};

function quote(s: string): string {
  return `'${s.replace(/'/g, "''")}'`;
}

async function migrateModuleValues(
  knex: Knex,
  pairs: Array<[string, string]>
): Promise<void> {
  for (const table of ["module_params", "module_params_history"]) {
    if (!(await knex.schema.hasTable(table))) continue;
    for (const [from, to] of pairs) {
      await knex(table).where("module", from).update({ module: to });
    }
  }
}

export async function up(knex: Knex): Promise<void> {
  await knex.raw("SET lock_timeout = '5s'");
  await renameTables(knex, TABLE_RENAMES);
  await renameColumns(knex, COLUMN_RENAMES);
  await renameSequences(knex, SEQUENCE_RENAMES);
  await knex.raw(INDEX_RENAME_SQL(true));
  await migrateModuleValues(knex, MODULE_VALUE_RENAMES);
}

export async function down(knex: Knex): Promise<void> {
  await knex.raw("SET lock_timeout = '5s'");
  await migrateModuleValues(
    knex,
    MODULE_VALUE_RENAMES.map(([from, to]) => [to, from] as [string, string])
  );
  await knex.raw(INDEX_RENAME_SQL(false));
  await renameSequences(
    knex,
    SEQUENCE_RENAMES.map(([from, to]) => [to, from] as [string, string])
  );
  await renameColumns(
    knex,
    COLUMN_RENAMES.map(
      ([table, from, to]) =>
        [
          TABLE_RENAMES.find(([o]) => o === table)?.[1] ?? table,
          to,
          from,
        ] as [string, string, string]
    )
  );
  await renameTables(
    knex,
    TABLE_RENAMES.map(([from, to]) => [to, from] as [string, string])
  );
}
