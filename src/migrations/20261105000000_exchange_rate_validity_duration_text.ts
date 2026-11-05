import type { Knex } from 'knex'

const TABLES = ['exchange_rates', 'exchange_rate_history']

// validity_duration holds the protobuf JSON Duration ("3600s"); null replaces the old zero default.
export async function up(knex: Knex): Promise<void> {
  for (const table of TABLES) {
    await knex.raw(
      `ALTER TABLE ?? ALTER COLUMN validity_duration DROP DEFAULT,
        ALTER COLUMN validity_duration DROP NOT NULL,
        ALTER COLUMN validity_duration TYPE text
          USING CASE WHEN validity_duration > 0 THEN validity_duration || 's' END`,
      [table]
    )
  }
}

export async function down(knex: Knex): Promise<void> {
  for (const table of TABLES) {
    await knex.raw(
      `ALTER TABLE ?? ALTER COLUMN validity_duration TYPE integer
          USING COALESCE(floor(rtrim(validity_duration, 's')::numeric)::integer, 0),
        ALTER COLUMN validity_duration SET DEFAULT 0,
        ALTER COLUMN validity_duration SET NOT NULL`,
      [table]
    )
  }
}
