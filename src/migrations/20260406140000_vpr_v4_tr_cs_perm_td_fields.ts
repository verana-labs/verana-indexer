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
            table.string("holder_onboarding_mode", 64).nullable();
          } else if (col === "pricing_asset_type") {
            table.string("pricing_asset_type", 32).nullable();
          } else if (col === "pricing_asset") {
            table.string("pricing_asset", 128).nullable();
          } else {
            table.string("digest_algorithm", 64).nullable();
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
            table.string("holder_onboarding_mode", 64).nullable();
          } else if (col === "pricing_asset_type") {
            table.string("pricing_asset_type", 32).nullable();
          } else if (col === "pricing_asset") {
            table.string("pricing_asset", 128).nullable();
          } else {
            table.string("digest_algorithm", 64).nullable();
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
            table.string("vs_operator", 255).nullable();
          } else if (col === "adjusted") {
            table.timestamp("adjusted").nullable();
          } else if (col === "vs_operator_authz_enabled") {
            table.boolean("vs_operator_authz_enabled").notNullable().defaultTo(false);
          } else if (col === "vs_operator_authz_spend_limit") {
            table.jsonb("vs_operator_authz_spend_limit").nullable();
          } else if (col === "vs_operator_authz_with_feegrant") {
            table.boolean("vs_operator_authz_with_feegrant").notNullable().defaultTo(false);
          } else if (col === "vs_operator_authz_fee_spend_limit") {
            table.jsonb("vs_operator_authz_fee_spend_limit").nullable();
          } else {
            table.string("vs_operator_authz_spend_period", 64).nullable();
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
            table.string("vs_operator", 255).nullable();
          } else if (col === "adjusted") {
            table.timestamp("adjusted").nullable();
          } else if (col === "vs_operator_authz_enabled") {
            table.boolean("vs_operator_authz_enabled").notNullable().defaultTo(false);
          } else if (col === "vs_operator_authz_spend_limit") {
            table.jsonb("vs_operator_authz_spend_limit").nullable();
          } else if (col === "vs_operator_authz_with_feegrant") {
            table.boolean("vs_operator_authz_with_feegrant").notNullable().defaultTo(false);
          } else if (col === "vs_operator_authz_fee_spend_limit") {
            table.jsonb("vs_operator_authz_fee_spend_limit").nullable();
          } else {
            table.string("vs_operator_authz_spend_period", 64).nullable();
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
