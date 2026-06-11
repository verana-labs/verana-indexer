import { Knex } from "knex";

// dev.13 renamed the modules tr->ec (TRUST_REGISTRY->ECOSYSTEM) and
// perm->pp (PERMISSION->PARTICIPANT). The stats code and model were updated
// but the `stats_entity_type_check` constraint created in
// 20260122000000_create_stats_table.ts still allowed only the old names,
// so every ECOSYSTEM/PARTICIPANT insert violated the check. Realign the
// constraint with the current EntityType set and migrate any legacy rows.
const NEW_VALUES = ["GLOBAL", "ECOSYSTEM", "CREDENTIAL_SCHEMA", "PARTICIPANT"];
const OLD_VALUES = ["GLOBAL", "TRUST_REGISTRY", "CREDENTIAL_SCHEMA", "PERMISSION"];

export async function up(knex: Knex): Promise<void> {
  await knex.raw("ALTER TABLE stats DROP CONSTRAINT IF EXISTS stats_entity_type_check");
  await knex("stats").where("entity_type", "TRUST_REGISTRY").update({ entity_type: "ECOSYSTEM" });
  await knex("stats").where("entity_type", "PERMISSION").update({ entity_type: "PARTICIPANT" });
  await knex.raw(
    `ALTER TABLE stats ADD CONSTRAINT stats_entity_type_check CHECK (entity_type IN (${NEW_VALUES.map((v) => `'${v}'`).join(", ")}))`
  );
}

export async function down(knex: Knex): Promise<void> {
  await knex.raw("ALTER TABLE stats DROP CONSTRAINT IF EXISTS stats_entity_type_check");
  await knex("stats").where("entity_type", "ECOSYSTEM").update({ entity_type: "TRUST_REGISTRY" });
  await knex("stats").where("entity_type", "PARTICIPANT").update({ entity_type: "PERMISSION" });
  await knex.raw(
    `ALTER TABLE stats ADD CONSTRAINT stats_entity_type_check CHECK (entity_type IN (${OLD_VALUES.map((v) => `'${v}'`).join(", ")}))`
  );
}
