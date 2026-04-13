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

export async function up(knex: Knex): Promise<void> {
  if (await knex.schema.hasTable("credential_schemas")) {
    for (const col of CS_V4) {
      if (!(await knex.schema.hasColumn("credential_schemas", col))) {
        await knex.schema.alterTable("credential_schemas", (table) => {
          if (col === "holder_onboarding_mode") {
            table.text("holder_onboarding_mode").nullable();
          } else if (col === "pricing_asset_type") {
            table.text("pricing_asset_type").nullable();
          } else if (col === "pricing_asset") {
            table.text("pricing_asset").nullable();
          } else {
            table.text("digest_algorithm").nullable();
          }
        });
      }
    }
  }

  if (await knex.schema.hasTable("credential_schema_history")) {
    for (const col of CS_V4) {
      if (!(await knex.schema.hasColumn("credential_schema_history", col))) {
        await knex.schema.alterTable("credential_schema_history", (table) => {
          if (col === "holder_onboarding_mode") {
            table.text("holder_onboarding_mode").nullable();
          } else if (col === "pricing_asset_type") {
            table.text("pricing_asset_type").nullable();
          } else if (col === "pricing_asset") {
            table.text("pricing_asset").nullable();
          } else {
            table.text("digest_algorithm").nullable();
          }
        });
      }
    }
  }

  if (await knex.schema.hasTable("permissions")) {
    for (const col of PERM_V4) {
      if (!(await knex.schema.hasColumn("permissions", col))) {
        await knex.schema.alterTable("permissions", (table) => {
          if (col === "vs_operator") {
            table.text("vs_operator").nullable();
          } else if (col === "adjusted") {
            table.timestamp("adjusted", { useTz: true }).nullable();
          } else if (col === "adjusted_by") {
            table.text("adjusted_by").nullable();
          } else if (col === "vs_operator_authz_enabled") {
            table.boolean("vs_operator_authz_enabled").notNullable().defaultTo(false);
          } else if (col === "vs_operator_authz_spend_limit") {
            table.jsonb("vs_operator_authz_spend_limit").nullable();
          } else if (col === "vs_operator_authz_with_feegrant") {
            table.boolean("vs_operator_authz_with_feegrant").notNullable().defaultTo(false);
          } else if (col === "vs_operator_authz_fee_spend_limit") {
            table.jsonb("vs_operator_authz_fee_spend_limit").nullable();
          } else {
            table.text("vs_operator_authz_spend_period").nullable();
          }
        });
      }
    }
  }

  if (await knex.schema.hasTable("permission_history")) {
    for (const col of PERM_V4) {
      if (!(await knex.schema.hasColumn("permission_history", col))) {
        await knex.schema.alterTable("permission_history", (table) => {
          if (col === "vs_operator") {
            table.text("vs_operator").nullable();
          } else if (col === "adjusted") {
            table.timestamp("adjusted", { useTz: true }).nullable();
          } else if (col === "adjusted_by") {
            table.text("adjusted_by").nullable();
          } else if (col === "vs_operator_authz_enabled") {
            table.boolean("vs_operator_authz_enabled").notNullable().defaultTo(false);
          } else if (col === "vs_operator_authz_spend_limit") {
            table.jsonb("vs_operator_authz_spend_limit").nullable();
          } else if (col === "vs_operator_authz_with_feegrant") {
            table.boolean("vs_operator_authz_with_feegrant").notNullable().defaultTo(false);
          } else if (col === "vs_operator_authz_fee_spend_limit") {
            table.jsonb("vs_operator_authz_fee_spend_limit").nullable();
          } else {
            table.text("vs_operator_authz_spend_period").nullable();
          }
        });
      }
    }
  }
}

export async function down(knex: Knex): Promise<void> {
  const dropCs = async (table: string) => {
    if (!(await knex.schema.hasTable(table))) return;
    for (const col of CS_V4) {
      if (await knex.schema.hasColumn(table, col)) {
        await knex.schema.alterTable(table, (t) => t.dropColumn(col));
      }
    }
  };
  await dropCs("credential_schemas");
  await dropCs("credential_schema_history");

  const dropPerm = async (table: string) => {
    if (!(await knex.schema.hasTable(table))) return;
    for (const col of PERM_V4) {
      if (await knex.schema.hasColumn(table, col)) {
        await knex.schema.alterTable(table, (t) => t.dropColumn(col));
      }
    }
  };
  await dropPerm("permissions");
  await dropPerm("permission_history");
}
