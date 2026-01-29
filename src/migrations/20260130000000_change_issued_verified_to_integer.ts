import { Knex } from "knex";
export async function up(knex: Knex): Promise<void> {
  const csHasIssued = await knex.schema.hasColumn("credential_schemas", "issued");
  if (csHasIssued) {
    const csIssuedInfo = await knex.raw(`
      SELECT data_type 
      FROM information_schema.columns 
      WHERE table_name = 'credential_schemas' 
      AND column_name = 'issued'
    `);
    
    if (csIssuedInfo.rows[0]?.data_type === 'character varying' || csIssuedInfo.rows[0]?.data_type === 'text') {
      await knex.schema.alterTable("credential_schemas", (table) => {
        table.bigInteger("issued").defaultTo(0).notNullable().alter();
        table.bigInteger("verified").defaultTo(0).notNullable().alter();
      });
    }
  }

  const trHasIssued = await knex.schema.hasColumn("trust_registry", "issued");
  if (trHasIssued) {
    const trIssuedInfo = await knex.raw(`
      SELECT data_type 
      FROM information_schema.columns 
      WHERE table_name = 'trust_registry' 
      AND column_name = 'issued'
    `);
    
    if (trIssuedInfo.rows[0]?.data_type === 'character varying' || trIssuedInfo.rows[0]?.data_type === 'text') {
      await knex.schema.alterTable("trust_registry", (table) => {
        table.bigInteger("issued").defaultTo(0).notNullable().alter();
        table.bigInteger("verified").defaultTo(0).notNullable().alter();
      });
    }
  }
}

export async function down(knex: Knex): Promise<void> {
  const csHasIssued = await knex.schema.hasColumn("credential_schemas", "issued");
  if (csHasIssued) {
    const csIssuedInfo = await knex.raw(`
      SELECT data_type 
      FROM information_schema.columns 
      WHERE table_name = 'credential_schemas' 
      AND column_name = 'issued'
    `);
    
    if (csIssuedInfo.rows[0]?.data_type === 'bigint') {
      await knex.schema.alterTable("credential_schemas", (table) => {
        table.string("issued", 50).defaultTo("0").notNullable().alter();
        table.string("verified", 50).defaultTo("0").notNullable().alter();
      });
    }
  }

  const trHasIssued = await knex.schema.hasColumn("trust_registry", "issued");
  if (trHasIssued) {
    const trIssuedInfo = await knex.raw(`
      SELECT data_type 
      FROM information_schema.columns 
      WHERE table_name = 'trust_registry' 
      AND column_name = 'issued'
    `);
    
    if (trIssuedInfo.rows[0]?.data_type === 'bigint') {
      await knex.schema.alterTable("trust_registry", (table) => {
        table.string("issued", 50).defaultTo("0").notNullable().alter();
        table.string("verified", 50).defaultTo("0").notNullable().alter();
      });
    }
  }
}
